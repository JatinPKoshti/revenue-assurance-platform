"""Operational health monitoring for the Revenue Assurance platform.

This version avoids a stale CloudWatch age datapoint causing a false
backlog alert after the SQS queue has been drained. It still raises a
health alert if visible messages exist and the newest usable age metric
shows an old message, or if visible messages exist but the age metric is
missing/stale.

Before enabling this DAG:
1. Set QUEUE_URL to the exact SQS Queue URL in ap-south-1.
2. Ensure Airflow connections revenue_aws and revenue_postgres work.
3. Grant the revenue_aws identity cloudwatch:GetMetricStatistics.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pendulum
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
AWS_CONNECTION_ID = "revenue_aws"
POSTGRES_CONNECTION_ID = "revenue_postgres"
AWS_REGION = "ap-south-1"

# IMPORTANT: preserve/copy the exact Queue URL from your current local file.
QUEUE_URL = "QUEUE_URL"

CONTROL_TABLE = "s3_event_trigger_control"
HEALTH_TABLE = "ops_health_checks"

MAX_VISIBLE_MESSAGES = 100
MAX_MESSAGE_AGE_SECONDS = 900  # 15 minutes
MAX_PENDING_AGE_MINUTES = 30
CLOUDWATCH_LOOKBACK_MINUTES = 60
CLOUDWATCH_PERIOD_SECONDS = 60
METRIC_FRESHNESS_LIMIT_SECONDS = 15 * 60


@dag(
    dag_id="revenue_assurance_ops_monitor",
    description=(
        "Monitor SQS backlog, CloudWatch message age, and pending event records."
    ),
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["revenue-assurance", "monitoring", "operations"],
)
def revenue_assurance_ops_monitor():

    @task(retries=1, retry_delay=pendulum.duration(minutes=2))
    def check_platform_health() -> dict:
        if (
            not QUEUE_URL.startswith("https://sqs.")
            or "REPLACE_WITH" in QUEUE_URL
        ):
            raise ValueError(
                "Set QUEUE_URL in revenue_assurance_ops_monitor.py "
                "to the actual SQS Queue URL."
            )

        sqs = AwsBaseHook(
            aws_conn_id=AWS_CONNECTION_ID,
            client_type="sqs",
            region_name=AWS_REGION,
        ).get_conn()

        queue_response = sqs.get_queue_attributes(
            QueueUrl=QUEUE_URL,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )
        attrs = queue_response.get("Attributes", {})
        visible_messages = int(attrs.get("ApproximateNumberOfMessages", "0"))
        in_flight_messages = int(
            attrs.get("ApproximateNumberOfMessagesNotVisible", "0")
        )
        delayed_messages = int(
            attrs.get("ApproximateNumberOfMessagesDelayed", "0")
        )

        queue_name = QUEUE_URL.rstrip("/").split("/")[-1]
        metric_end = datetime.now(timezone.utc)
        metric_start = metric_end - timedelta(minutes=CLOUDWATCH_LOOKBACK_MINUTES)
        oldest_message_age: int | None = None
        metric_timestamp: datetime | None = None
        metric_is_fresh = False
        problems: list[str] = []

        cloudwatch = AwsBaseHook(
            aws_conn_id=AWS_CONNECTION_ID,
            client_type="cloudwatch",
            region_name=AWS_REGION,
        ).get_conn()

        try:
            metric_response = cloudwatch.get_metric_statistics(
                Namespace="AWS/SQS",
                MetricName="ApproximateAgeOfOldestMessage",
                Dimensions=[{"Name": "QueueName", "Value": queue_name}],
                StartTime=metric_start,
                EndTime=metric_end,
                Period=CLOUDWATCH_PERIOD_SECONDS,
                Statistics=["Maximum"],
            )
            datapoints = metric_response.get("Datapoints", [])

            # Use only the newest datapoint, not max() across the whole window.
            # Older high values otherwise survive in the rolling query window.
            if datapoints:
                latest = max(datapoints, key=lambda p: p["Timestamp"])
                metric_timestamp = latest["Timestamp"]
                if metric_timestamp.tzinfo is None:
                    metric_timestamp = metric_timestamp.replace(tzinfo=timezone.utc)
                metric_age_seconds = max(
                    0.0,
                    (metric_end - metric_timestamp.astimezone(timezone.utc)).total_seconds(),
                )
                metric_is_fresh = metric_age_seconds <= METRIC_FRESHNESS_LIMIT_SECONDS
                if metric_is_fresh:
                    oldest_message_age = int(float(latest["Maximum"]))

        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "UnknownCloudWatchError")
            problems.append(
                "Could not read the SQS age metric from CloudWatch "
                f"({error_code}). Check cloudwatch:GetMetricStatistics."
            )
            print(f"CloudWatch metric error: {error_code}: {exc}")

        # Key fix: only judge message age when SQS currently reports visible
        # messages. Do not let an older CloudWatch datapoint fail an empty queue.
        if visible_messages > 0:
            if oldest_message_age is not None and metric_is_fresh:
                if oldest_message_age > MAX_MESSAGE_AGE_SECONDS:
                    problems.append(
                        "The newest CloudWatch datapoint reports the oldest "
                        f"visible SQS message is older than {MAX_MESSAGE_AGE_SECONDS} seconds."
                    )
            else:
                problems.append(
                    "SQS has visible messages, but no fresh oldest-message-age "
                    "metric is available. Check CloudWatch metric publication/permissions."
                )
        else:
            print(
                "SQS has no visible messages; ignoring historical CloudWatch age "
                "datapoints for the backlog-age threshold."
            )

        if visible_messages >= MAX_VISIBLE_MESSAGES:
            problems.append(
                f"SQS visible-message count reached or exceeded {MAX_VISIBLE_MESSAGES}."
            )

        postgres = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        connection = postgres.get_conn()
        stale_pending_count = 0
        health_status = "HEALTHY"

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS public.{CONTROL_TABLE} (
                        business_date DATE PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        trigger_run_id TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS public.{HEALTH_TABLE} (
                        id BIGSERIAL PRIMARY KEY,
                        check_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        visible_messages INTEGER NOT NULL,
                        in_flight_messages INTEGER NOT NULL,
                        oldest_message_age_seconds INTEGER,
                        stale_pending_count INTEGER NOT NULL,
                        health_status TEXT NOT NULL,
                        details JSONB NOT NULL
                    )
                    """
                )
                # Backward compatible if an earlier table version used NOT NULL.
                cursor.execute(
                    f"ALTER TABLE public.{HEALTH_TABLE} "
                    "ALTER COLUMN oldest_message_age_seconds DROP NOT NULL"
                )
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM public.{CONTROL_TABLE}
                    WHERE status = 'pending'
                      AND updated_at < NOW() - (%s * INTERVAL '1 minute')
                    """,
                    (MAX_PENDING_AGE_MINUTES,),
                )
                stale_pending_count = int(cursor.fetchone()[0])

                if stale_pending_count > 0:
                    problems.append(
                        f"{stale_pending_count} event-trigger record(s) have "
                        f"remained pending for more than {MAX_PENDING_AGE_MINUTES} minutes."
                    )

                health_status = "DEGRADED" if problems else "HEALTHY"
                details = {
                    "problems": problems,
                    "metric_timestamp_utc": (
                        metric_timestamp.astimezone(timezone.utc).isoformat()
                        if metric_timestamp else None
                    ),
                    "metric_is_fresh": metric_is_fresh,
                    "max_visible_messages_threshold": MAX_VISIBLE_MESSAGES,
                    "max_message_age_seconds_threshold": MAX_MESSAGE_AGE_SECONDS,
                    "max_pending_age_minutes_threshold": MAX_PENDING_AGE_MINUTES,
                    "cloudwatch_lookback_minutes": CLOUDWATCH_LOOKBACK_MINUTES,
                    "metric_freshness_limit_seconds": METRIC_FRESHNESS_LIMIT_SECONDS,
                }
                cursor.execute(
                    f"""
                    INSERT INTO public.{HEALTH_TABLE} (
                        visible_messages,
                        in_flight_messages,
                        oldest_message_age_seconds,
                        stale_pending_count,
                        health_status,
                        details
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        visible_messages,
                        in_flight_messages,
                        oldest_message_age,
                        stale_pending_count,
                        health_status,
                        json.dumps(details),
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        report = {
            "health_status": health_status,
            "visible_messages": visible_messages,
            "in_flight_messages": in_flight_messages,
            "delayed_messages": delayed_messages,
            "oldest_message_age_seconds": oldest_message_age,
            "metric_is_fresh": metric_is_fresh,
            "metric_timestamp_utc": (
                metric_timestamp.astimezone(timezone.utc).isoformat()
                if metric_timestamp else None
            ),
            "stale_pending_count": stale_pending_count,
            "problems": problems,
        }
        print("REVENUE ASSURANCE HEALTH REPORT")
        print(json.dumps(report, indent=2))

        if problems:
            raise AirflowException(
                "Platform health check is DEGRADED: " + " | ".join(problems)
            )
        return report

    check_platform_health()


revenue_assurance_ops_monitor()

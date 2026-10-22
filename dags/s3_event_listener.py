
"""
Revenue Assurance Platform
Event-driven S3 -> SQS -> Airflow master pipeline.

Processing flow:
1. Receive S3 notifications from SQS.
2. Identify supported business dates from object keys.
3. Check whether all six required CSV files exist in S3.
4. Select one eligible business date per listener run.
5. Trigger the master DAG with a deterministic logical date.
6. Record the trigger and acknowledge eligible SQS messages.

Keep this DAG paused until configuration and import checks pass.
"""

from __future__ import annotations

import json
import re
from datetime import date
from urllib.parse import unquote_plus

import pendulum

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)


# ============================================================
# CONFIGURATION — UPDATE THESE VALUES
# ============================================================

AWS_CONNECTION_ID = "revenue_aws"
POSTGRES_CONNECTION_ID = "revenue_postgres"
AWS_REGION = "ap-south-1"

# Replace with the exact Queue URL from the SQS console.
QUEUE_URL = "QUEUE_URL"

# Replace if the actual bucket name is different.
BUCKET_NAME = "BUCKET_NAME"

# Must exactly match your master DAG's dag_id.
MASTER_DAG_ID = "revenue_assurance_master_pipeline"

CONTROL_TABLE = "s3_event_trigger_control"

SOURCE_NAMES = (
    "orders",
    "payments",
    "settlements",
    "shipments",
    "returns",
    "refunds",
)

SOURCE_KEY_PATTERN = re.compile(
    r"^raw/(?P<source>"
    r"orders|payments|settlements|shipments|returns|refunds"
    r")/(?P=source)_(?P<business_date>\d{4}-\d{2}-\d{2})\.csv$"
)

# SQS visibility timeout should exceed normal listener-task time.
SQS_VISIBILITY_TIMEOUT_SECONDS = 180
SQS_MAX_MESSAGES = 10


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="revenue_assurance_s3_event_listener",
    description=(
        "Check S3 upload events and trigger the revenue "
        "assurance master pipeline when all six files exist."
    ),
    schedule="* * * * *",
    start_date=pendulum.datetime(
        2026, 1, 1, tz="Asia/Kolkata"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["revenue-assurance", "s3", "sqs", "event-driven"],
)
def revenue_assurance_s3_event_listener():

    # --------------------------------------------------------
    # TASK 1: Receive events and find a ready business date
    # --------------------------------------------------------

    @task(
        retries=2,
        retry_delay=pendulum.duration(minutes=1),
    )
    def collect_ready_business_date() -> dict:

        if (
            not QUEUE_URL.startswith("https://sqs.")
            or "REPLACE_WITH" in QUEUE_URL
        ):
            raise ValueError(
                "Replace QUEUE_URL with the actual SQS Queue URL."
            )

        if not BUCKET_NAME or BUCKET_NAME == "YOUR_BUCKET_NAME":
            raise ValueError(
                "Set BUCKET_NAME to the actual S3 bucket."
            )

        if not MASTER_DAG_ID:
            raise ValueError("Set MASTER_DAG_ID.")

        # Create AWS clients using the existing Airflow connection.
        sqs = AwsBaseHook(
            aws_conn_id=AWS_CONNECTION_ID,
            client_type="sqs",
            region_name=AWS_REGION,
        ).get_conn()

        s3_hook = S3Hook(
            aws_conn_id=AWS_CONNECTION_ID
        )

        postgres = PostgresHook(
            postgres_conn_id=POSTGRES_CONNECTION_ID
        )

        # Create the trigger-control table if needed.
        connection = postgres.get_conn()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS
                    public.s3_event_trigger_control (
                        business_date DATE PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        trigger_run_id TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        # Receive notifications without deleting them immediately.
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=SQS_MAX_MESSAGES,
            WaitTimeSeconds=1,
            VisibilityTimeout=SQS_VISIBILITY_TIMEOUT_SECONDS,
            AttributeNames=["ApproximateReceiveCount"],
        )

        messages = response.get("Messages", [])

        if not messages:
            print("No SQS messages found.")
            return {
                "business_date": None,
                "logical_date": None,
                "trigger_run_id": None,
                "ready_dates": [],
                "already_triggered_dates": [],
                "held_messages": [],
            }

        # Each message can contain one or more S3 event records.
        # Store dates at message level so a message containing
        # events for multiple dates is not acknowledged too early.
        event_messages = []
        candidate_dates = set()

        for message in messages:
            receipt = message["ReceiptHandle"]
            message_dates = set()

            try:
                body = json.loads(message["Body"])
            except (json.JSONDecodeError, TypeError):
                print("Deleting malformed SQS event message.")
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=receipt,
                )
                continue

            # S3 can send a test event without a Records array.
            if body.get("Event") == "s3:TestEvent":
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=receipt,
                )
                print("Acknowledged S3 test event.")
                continue

            for record in body.get("Records", []):
                if record.get("eventSource") != "aws:s3":
                    continue

                s3_record = record.get("s3", {})
                object_key = s3_record.get(
                    "object", {}
                ).get("key")

                if not object_key:
                    continue

                # Decode URL-encoded keys; '+' may mean a space.
                object_key = unquote_plus(object_key)

                match = SOURCE_KEY_PATTERN.fullmatch(object_key)

                if not match:
                    print(
                        "Ignoring unsupported object key: "
                        f"{object_key}"
                    )
                    continue

                business_date = match.group("business_date")

                try:
                    parsed_date = date.fromisoformat(business_date)
                except ValueError:
                    print(
                        "Ignoring invalid business date: "
                        f"{business_date}"
                    )
                    continue

                if parsed_date.isoformat() != business_date:
                    continue

                message_dates.add(business_date)
                candidate_dates.add(business_date)

            if message_dates:
                event_messages.append(
                    {
                        "receipt_handle": receipt,
                        "dates": sorted(message_dates),
                    }
                )
            else:
                # This event does not refer to any of the six
                # supported dated source-file patterns.
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=receipt,
                )

        if not candidate_dates:
            print("No supported source-file events.")
            return {
                "business_date": None,
                "logical_date": None,
                "trigger_run_id": None,
                "ready_dates": [],
                "already_triggered_dates": [],
                "held_messages": [],
            }

        # Check whether all six required objects exist.
        ready_dates = set()
        incomplete_dates = set()

        for business_date in sorted(candidate_dates):
            required_keys = [
                (
                    f"raw/{source}/"
                    f"{source}_{business_date}.csv"
                )
                for source in SOURCE_NAMES
            ]

            all_files_exist = all(
                s3_hook.check_for_key(
                    key=key,
                    bucket_name=BUCKET_NAME,
                )
                for key in required_keys
            )

            if all_files_exist:
                ready_dates.add(business_date)
                print(
                    f"{business_date}: all six source files exist."
                )
            else:
                incomplete_dates.add(business_date)
                print(
                    f"{business_date}: waiting for the remaining "
                    "source files."
                )

        # Look up dates that have already been triggered.
        connection = postgres.get_conn()
        status_by_date = {}

        try:
            with connection.cursor() as cursor:
                for business_date in sorted(ready_dates):
                    cursor.execute(
                        """
                        SELECT status
                        FROM public.s3_event_trigger_control
                        WHERE business_date = %s
                        """,
                        (date.fromisoformat(business_date),),
                    )

                    row = cursor.fetchone()
                    status_by_date[business_date] = (
                        row[0] if row else "pending"
                    )

        finally:
            connection.close()

        already_triggered_dates = {
            business_date
            for business_date, status in status_by_date.items()
            if status in {"triggered", "completed"}
        }

        # Select the first ready date not already triggered.
        eligible_dates = sorted(
            ready_dates - already_triggered_dates
        )

        selected_date = (
            eligible_dates[0]
            if eligible_dates
            else None
        )

        # Delete receipts that only refer to incomplete dates or
        # dates already handled. A future file upload will create
        # another event for an incomplete date.
        #
        # Retain messages containing a date that is ready and
        # not yet triggered. This includes other dates that can
        # be processed by subsequent listener runs.
        held_messages = []

        for event_message in event_messages:
            dates_in_message = set(event_message["dates"])

            must_keep = any(
                business_date in ready_dates
                and business_date not in already_triggered_dates
                for business_date in dates_in_message
            )

            if must_keep:
                held_messages.append(event_message)
            else:
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=event_message["receipt_handle"],
                )

        if not selected_date:
            print("No new ready business date to trigger.")
            return {
                "business_date": None,
                "logical_date": None,
                "trigger_run_id": None,
                "ready_dates": sorted(ready_dates),
                "already_triggered_dates": sorted(
                    already_triggered_dates
                ),
                "held_messages": held_messages,
            }

        # Use a stable run ID AND logical date per business date.
        # TriggerDagRunOperator's skip_when_already_exists checks
        # an existing run for the specified logical date.
        trigger_run_id = (
            "s3_event__" + selected_date.replace("-", "")
        )

        logical_date = (
            selected_date + "T00:00:00+00:00"
        )

        # Record a pending trigger. If the trigger task fails,
        # the SQS message remains available for retry.
        connection = postgres.get_conn()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO public.s3_event_trigger_control (
                        business_date,
                        status,
                        trigger_run_id,
                        updated_at
                    )
                    VALUES (%s, 'pending', %s, NOW())
                    ON CONFLICT (business_date) DO NOTHING
                    """,
                    (
                        date.fromisoformat(selected_date),
                        trigger_run_id,
                    ),
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        print(f"Selected business date: {selected_date}")
        print(f"Logical date: {logical_date}")
        print(f"Trigger run ID: {trigger_run_id}")

        return {
            "business_date": selected_date,
            "logical_date": logical_date,
            "trigger_run_id": trigger_run_id,
            "ready_dates": sorted(ready_dates),
            "already_triggered_dates": sorted(
                already_triggered_dates
            ),
            "held_messages": held_messages,
        }

    # --------------------------------------------------------
    # TASK 2: Branch depending on readiness
    # --------------------------------------------------------

    @task.branch
    def choose_next_step(payload: dict) -> str:
        if payload and payload.get("business_date"):
            return "trigger_master"
        return "no_ready_files"

    payload = collect_ready_business_date()
    route = choose_next_step(payload)

    # --------------------------------------------------------
    # TASK 3: Trigger the master DAG
    # --------------------------------------------------------

    trigger_master = TriggerDagRunOperator(
        task_id="trigger_master",
        trigger_dag_id=MASTER_DAG_ID,
        trigger_run_id=(
            "{{ ti.xcom_pull("
            "task_ids='collect_ready_business_date'"
            ")['trigger_run_id'] }}"
        ),
        logical_date=(
            "{{ ti.xcom_pull("
            "task_ids='collect_ready_business_date'"
            ")['logical_date'] }}"
        ),
        conf={
            "business_date": (
                "{{ ti.xcom_pull("
                "task_ids='collect_ready_business_date'"
                ")['business_date'] }}"
            )
        },
        wait_for_completion=False,
        skip_when_already_exists=True,
    )

    no_ready_files = EmptyOperator(
        task_id="no_ready_files"
    )

    # --------------------------------------------------------
    # TASK 4: Mark trigger and acknowledge relevant SQS events
    # --------------------------------------------------------

    @task(trigger_rule="none_failed")
    def mark_triggered_and_ack() -> None:
        from airflow.sdk import get_current_context

        context = get_current_context()
        ti = context["ti"]

        payload = ti.xcom_pull(
            task_ids="collect_ready_business_date"
        )

        # This task may run after the branch chose no_ready_files,
        # or after the operator skipped an already-existing run.
        if not payload or not payload.get("business_date"):
            print("No business date was selected.")
            return

        business_date = payload["business_date"]
        trigger_run_id = payload["trigger_run_id"]

        ready_dates = set(payload.get("ready_dates", []))
        already_triggered_dates = set(
            payload.get("already_triggered_dates", [])
        )

        held_messages = payload.get("held_messages", [])

        postgres = PostgresHook(
            postgres_conn_id=POSTGRES_CONNECTION_ID
        )

        connection = postgres.get_conn()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE public.s3_event_trigger_control
                    SET
                        status = 'triggered',
                        trigger_run_id = %s,
                        updated_at = NOW()
                    WHERE business_date = %s
                    """,
                    (
                        trigger_run_id,
                        date.fromisoformat(business_date),
                    ),
                )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

        # Acknowledge only held messages that do not also contain
        # another ready, untriggered business date.
        #
        # If a single SQS message contains events for two ready
        # dates, keep it until each date has been handled.
        sqs = AwsBaseHook(
            aws_conn_id=AWS_CONNECTION_ID,
            client_type="sqs",
            region_name=AWS_REGION,
        ).get_conn()

        acknowledged_count = 0

        for event_message in held_messages:
            dates_in_message = set(event_message["dates"])

            # This message belongs to another date that has not
            # been selected yet. Leave it for a later poll.
            if business_date not in dates_in_message:
                continue

            other_ready_dates = {
                d
                for d in dates_in_message
                if d != business_date
                and d in ready_dates
                and d not in already_triggered_dates
            }

            if other_ready_dates:
                print(
                    "Keeping message for other ready dates: "
                    f"{sorted(other_ready_dates)}"
                )
                continue

            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=event_message["receipt_handle"],
            )

            acknowledged_count += 1

        print(
            f"Marked {business_date} as triggered. "
            f"Acknowledged {acknowledged_count} SQS message(s)."
        )

    ack_events = mark_triggered_and_ack()

    # --------------------------------------------------------
    # DEPENDENCIES
    # --------------------------------------------------------

    route >> trigger_master >> ack_events
    route >> no_ready_files


revenue_assurance_s3_event_listener()

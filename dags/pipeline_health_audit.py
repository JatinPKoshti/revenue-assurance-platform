
import json
import pendulum

from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "revenue_assurance_pipeline_health_audit"
CONNECTION_ID = "revenue_postgres"


@dag(
    dag_id=DAG_ID,
    description="Audit ingestion, reconciliation and data quality",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["revenue-assurance", "monitoring", "data-quality"],
)
def pipeline_health_audit():

    @task
    def run_health_audit():
        context = get_current_context()
        dag_run = context["dag_run"]

        hook = PostgresHook(
            postgres_conn_id=CONNECTION_ID
        )
        connection = hook.get_conn()

        try:
            with connection:
                with connection.cursor() as cursor:

                    # Save a history of health-audit reports.
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS pipeline_health_audits (
                            audit_id BIGINT GENERATED ALWAYS AS IDENTITY
                                PRIMARY KEY,
                            dag_id VARCHAR(250) NOT NULL,
                            dag_run_id VARCHAR(250) NOT NULL,
                            overall_status VARCHAR(30) NOT NULL
                                CHECK (
                                    overall_status IN (
                                        'healthy',
                                        'attention_required'
                                    )
                                ),
                            checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            report JSONB NOT NULL DEFAULT '{}'::jsonb,
                            UNIQUE (dag_id, dag_run_id)
                        );
                    """)

                    # Latest ingestion batch per source.
                    cursor.execute("""
                        SELECT DISTINCT ON (source_system)
                            source_system,
                            file_name,
                            status,
                            row_count,
                            loaded_count,
                            created_at,
                            completed_at,
                            error_message
                        FROM ingestion_batches
                        ORDER BY source_system, batch_id DESC;
                    """)

                    batch_rows = cursor.fetchall()
                    batch_columns = [
                        item[0] for item in cursor.description
                    ]

                    batches = [
                        dict(zip(batch_columns, row))
                        for row in batch_rows
                    ]

                    # Latest reconciliation run per DAG.
                    cursor.execute("""
                        SELECT DISTINCT ON (dag_id)
                            dag_id,
                            dag_run_id,
                            business_date,
                            run_status,
                            started_at,
                            completed_at,
                            result_summary
                        FROM reconciliation_runs
                        ORDER BY dag_id, run_id DESC;
                    """)

                    run_rows = cursor.fetchall()
                    run_columns = [
                        item[0] for item in cursor.description
                    ]

                    runs = [
                        dict(zip(run_columns, row))
                        for row in run_rows
                    ]

                    # Basic data-quality counts.
                    cursor.execute("""
                        SELECT
                            (SELECT COUNT(*)
                             FROM orders) AS order_count,

                            (SELECT COUNT(*)
                             FROM payments) AS payment_count,

                            (SELECT COUNT(*)
                             FROM settlements) AS settlement_count,

                            (SELECT COUNT(*)
                             FROM shipments) AS shipment_count,

                            (SELECT COUNT(*)
                             FROM returns) AS return_count,

                            (SELECT COUNT(*)
                             FROM refunds) AS refund_count,

                            (SELECT COUNT(*)
                             FROM payments p
                             LEFT JOIN orders o
                               ON o.order_id = p.order_id
                             WHERE o.order_id IS NULL)
                                AS payments_without_order,

                            (SELECT COUNT(*)
                             FROM settlements
                             WHERE gross_amount <>
                                   fee_amount + net_amount)
                                AS invalid_settlement_math,

                            (SELECT COUNT(*)
                             FROM shipments s
                             LEFT JOIN orders o
                               ON o.order_id = s.order_id
                             WHERE o.order_id IS NULL)
                                AS shipments_without_order,

                            (SELECT COUNT(*)
                             FROM refunds f
                             LEFT JOIN orders o
                               ON o.order_id = f.order_id
                             WHERE o.order_id IS NULL)
                                AS refunds_without_order,

                            (SELECT COUNT(*)
                             FROM reconciliation_exceptions
                             WHERE exception_status IN (
                                 'open', 'in_progress'
                             ))
                                AS open_exception_count;
                    """)

                    quality_columns = [
                        item[0] for item in cursor.description
                    ]
                    quality_values = cursor.fetchone()

                    quality = dict(
                        zip(quality_columns, quality_values)
                    )

                    failed_batches = [
                        {
                            "source_system": batch["source_system"],
                            "status": batch["status"],
                            "file_name": batch["file_name"],
                            "error_message": batch["error_message"],
                        }
                        for batch in batches
                        if batch["status"] in ("failed", "quarantined")
                    ]

                    unsuccessful_runs = [
                        {
                            "dag_id": run["dag_id"],
                            "run_status": run["run_status"],
                        }
                        for run in runs
                        if run["run_status"] != "succeeded"
                    ]

                    # These findings indicate attention is needed,
                    # not necessarily that a financial loss occurred.
                    quality_problems = {
                        key: int(value)
                        for key, value in quality.items()
                        if key in (
                            "payments_without_order",
                            "invalid_settlement_math",
                            "shipments_without_order",
                            "refunds_without_order",
                        )
                        and int(value) > 0
                    }

                    issues_exist = bool(
                        failed_batches
                        or unsuccessful_runs
                        or quality_problems
                        or int(quality["open_exception_count"]) > 0
                    )

                    overall_status = (
                        "attention_required"
                        if issues_exist
                        else "healthy"
                    )

                    def serialize(value):
                        if hasattr(value, "isoformat"):
                            return value.isoformat()
                        return value

                    report = {
                        "overall_status": overall_status,
                        "latest_ingestion_batches": batches,
                        "latest_reconciliation_runs": runs,
                        "data_quality_counts": {
                            key: int(value)
                            for key, value in quality.items()
                        },
                        "failed_latest_batches": failed_batches,
                        "unsuccessful_latest_runs": unsuccessful_runs,
                        "quality_problems": quality_problems,
                        "checked_at": pendulum.now(
                            "Asia/Kolkata"
                        ).to_iso8601_string(),
                    }

                    # Convert database dates and timestamps to strings
                    # before serializing the JSON report.
                    report_json = json.dumps(
                        report,
                        default=serialize,
                    )

                    cursor.execute(
                        """
                        INSERT INTO pipeline_health_audits (
                            dag_id,
                            dag_run_id,
                            overall_status,
                            report
                        )
                        VALUES (%s, %s, %s, %s::jsonb)
                        ON CONFLICT (dag_id, dag_run_id)
                        DO UPDATE SET
                            overall_status = EXCLUDED.overall_status,
                            checked_at = NOW(),
                            report = EXCLUDED.report;
                        """,
                        (
                            DAG_ID,
                            dag_run.run_id,
                            overall_status,
                            report_json,
                        ),
                    )

            print(f"Health status: {overall_status}")
            print(f"Data quality counts: {quality}")
            print(f"Failed latest batches: {failed_batches}")
            print(f"Unsuccessful latest runs: {unsuccessful_runs}")
            print(f"Quality problems: {quality_problems}")
            print("Health audit report saved to PostgreSQL.")

        finally:
            connection.close()

    run_health_audit()


pipeline_health_audit()

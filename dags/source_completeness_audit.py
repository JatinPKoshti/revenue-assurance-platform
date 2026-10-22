
import json
from datetime import date

import pendulum

from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "revenue_assurance_source_completeness_audit"
CONNECTION_ID = "revenue_postgres"

BUSINESS_DATE = date(2026, 10, 15)

EXPECTED_SOURCES = [
    {
        "name": "Orders",
        "source_system": "S3-DemoOrders",
        "file_name": "raw/orders/orders_2026-10-15.csv",
        "expected_rows": 5,
    },
    {
        "name": "Payments",
        "source_system": "S3-DemoPayments",
        "file_name": "raw/payments/payments_2026-10-15.csv",
        "expected_rows": 5,
    },
    {
        "name": "Settlements",
        "source_system": "S3-DemoPay",
        "file_name": "raw/settlements/settlements_2026-10-15.csv",
        "expected_rows": 5,
    },
    {
        "name": "Shipments",
        "source_system": "S3-DemoCarrier",
        "file_name": "raw/shipments/shipments_2026-10-15.csv",
        "expected_rows": 5,
    },
    {
        "name": "Returns",
        "source_system": "S3-DemoReturns",
        "file_name": "raw/returns/returns_2026-10-15.csv",
        "expected_rows": 3,
    },
    {
        "name": "Refunds",
        "source_system": "S3-DemoRefunds",
        "file_name": "raw/refunds/refunds_2026-10-15.csv",
        "expected_rows": 3,
    },
]


@dag(
    dag_id=DAG_ID,
    description="Audit source ingestion completeness for a business date",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["revenue-assurance", "monitoring", "data-quality"],
)
def source_completeness_audit():

    @task
    def audit_source_completeness():
        context = get_current_context()
        dag_run = context["dag_run"]

        hook = PostgresHook(
            postgres_conn_id=CONNECTION_ID
        )

        connection = hook.get_conn()

        try:
            with connection:
                with connection.cursor() as cursor:

                    # Store an audit history for future dashboard use.
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS
                        source_completeness_audits (
                            audit_id BIGINT GENERATED ALWAYS AS IDENTITY
                                PRIMARY KEY,
                            dag_id VARCHAR(250) NOT NULL,
                            dag_run_id VARCHAR(250) NOT NULL,
                            business_date DATE NOT NULL,
                            overall_status VARCHAR(20) NOT NULL
                                CHECK (
                                    overall_status IN (
                                        'complete', 'incomplete'
                                    )
                                ),
                            checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            report JSONB NOT NULL DEFAULT '{}'::jsonb,
                            UNIQUE (dag_id, dag_run_id)
                        );
                    """)

                    source_results = []
                    issues = []

                    for source in EXPECTED_SOURCES:
                        cursor.execute(
                            """
                            SELECT
                                batch_id,
                                file_name,
                                status,
                                row_count,
                                loaded_count,
                                error_message,
                                completed_at
                            FROM ingestion_batches
                            WHERE source_system = %s
                              AND business_date = %s
                            ORDER BY batch_id DESC
                            LIMIT 1;
                            """,
                            (
                                source["source_system"],
                                BUSINESS_DATE,
                            ),
                        )

                        batch = cursor.fetchone()

                        result = {
                            "source_name": source["name"],
                            "source_system": source["source_system"],
                            "expected_file": source["file_name"],
                            "business_date": str(BUSINESS_DATE),
                            "expected_rows": source["expected_rows"],
                            "batch_id": None,
                            "batch_status": "missing",
                            "row_count": None,
                            "loaded_count": None,
                            "completed_at": None,
                            "error_message": None,
                        }

                        if batch is None:
                            result["audit_status"] = "missing_batch"
                            issues.append({
                                "source": source["name"],
                                "issue": "No ingestion batch exists "
                                         "for the expected business date.",
                            })

                        else:
                            (
                                batch_id,
                                file_name,
                                status,
                                row_count,
                                loaded_count,
                                error_message,
                                completed_at,
                            ) = batch

                            result.update({
                                "batch_id": batch_id,
                                "batch_status": status,
                                "actual_file": file_name,
                                "row_count": row_count,
                                "loaded_count": loaded_count,
                                "completed_at": (
                                    completed_at.isoformat()
                                    if completed_at else None
                                ),
                                "error_message": error_message,
                            })

                            if status != "loaded":
                                result["audit_status"] = "batch_not_loaded"

                                issues.append({
                                    "source": source["name"],
                                    "issue": f"Latest batch status is {status}.",
                                })

                            elif (
                                row_count != source["expected_rows"]
                                or loaded_count != source["expected_rows"]
                            ):
                                result["audit_status"] = "row_count_mismatch"

                                issues.append({
                                    "source": source["name"],
                                    "issue": (
                                        f"Expected {source['expected_rows']} "
                                        f"rows; observed row_count={row_count}, "
                                        f"loaded_count={loaded_count}."
                                    ),
                                })

                            elif loaded_count != row_count:
                                # Defensive check if ingestion logic changes.
                                result["audit_status"] = "count_inconsistent"

                                issues.append({
                                    "source": source["name"],
                                    "issue": "Loaded count differs from row count.",
                                })

                            else:
                                result["audit_status"] = "complete"

                        source_results.append(result)

                    overall_status = (
                        "complete" if not issues else "incomplete"
                    )

                    report = {
                        "business_date": str(BUSINESS_DATE),
                        "overall_status": overall_status,
                        "expected_source_count": len(EXPECTED_SOURCES),
                        "complete_source_count": sum(
                            1 for item in source_results
                            if item["audit_status"] == "complete"
                        ),
                        "issue_count": len(issues),
                        "sources": source_results,
                        "issues": issues,
                    }

                    cursor.execute(
                        """
                        INSERT INTO source_completeness_audits (
                            dag_id,
                            dag_run_id,
                            business_date,
                            overall_status,
                            report
                        )
                        VALUES (%s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (dag_id, dag_run_id)
                        DO UPDATE SET
                            business_date = EXCLUDED.business_date,
                            overall_status = EXCLUDED.overall_status,
                            checked_at = NOW(),
                            report = EXCLUDED.report
                        RETURNING audit_id;
                        """,
                        (
                            DAG_ID,
                            dag_run.run_id,
                            BUSINESS_DATE,
                            overall_status,
                            json.dumps(report, default=str),
                        ),
                    )

                    audit_id = cursor.fetchone()[0]

            print(f"Audit ID: {audit_id}")
            print(f"Business date: {BUSINESS_DATE}")
            print(f"Overall status: {overall_status}")
            print(
                f"Complete sources: {report['complete_source_count']}"
                f"/{report['expected_source_count']}"
            )
            print(f"Issues found: {len(issues)}")

            for item in source_results:
                print(
                    f"{item['source_name']}: "
                    f"{item['audit_status']}"
                )

            if issues:
                print("Source completeness issues:")
                for issue in issues:
                    print(f"- {issue['source']}: {issue['issue']}")

            # The DAG audits and records problems rather than failing
            # simply because a source is missing.
            print("Source completeness report saved.")

        finally:
            connection.close()

    audit_source_completeness()


source_completeness_audit()

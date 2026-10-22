"""Load the dated payments CSV from S3 into PostgreSQL."""

import pendulum
from airflow.sdk import dag, task

from s3_ingestion_common import run_source_ingestion


@dag(
    dag_id="revenue_assurance_demo_payments",
    description="Load and validate the S3 payments CSV for a business date.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "s3", "payments"],
)
def revenue_assurance_demo_payments():
    @task(task_id="load_demo_payments")
    def load_demo_payments():
        run_source_ingestion("payments")

    load_demo_payments()


revenue_assurance_demo_payments()

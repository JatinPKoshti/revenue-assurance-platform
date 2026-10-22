"""Load the dated orders CSV from S3 into PostgreSQL."""

import pendulum
from airflow.sdk import dag, task

from s3_ingestion_common import run_source_ingestion


@dag(
    dag_id="revenue_assurance_demo_orders",
    description="Load and validate the S3 orders CSV for a business date.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "s3", "orders"],
)
def revenue_assurance_demo_orders():
    @task(task_id="load_demo_orders")
    def load_demo_orders():
        run_source_ingestion("orders")

    load_demo_orders()


revenue_assurance_demo_orders()

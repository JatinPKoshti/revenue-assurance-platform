"""Load the dated shipment CSV from S3 into PostgreSQL."""

import pendulum
from airflow.sdk import dag, task

from s3_ingestion_common import run_source_ingestion


@dag(
    dag_id="revenue_assurance_shipments_ingestion",
    description="Load and validate the S3 shipments CSV for a business date.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "s3", "shipments"],
)
def revenue_assurance_shipments_ingestion():
    @task(task_id="load_shipments")
    def load_shipments():
        run_source_ingestion("shipments")

    load_shipments()


revenue_assurance_shipments_ingestion()

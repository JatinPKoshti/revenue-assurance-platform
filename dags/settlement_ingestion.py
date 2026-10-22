"""Load the dated settlement CSV from S3 into PostgreSQL."""

import pendulum
from airflow.sdk import dag, task

from s3_ingestion_common import run_source_ingestion


@dag(
    dag_id="revenue_assurance_settlement_ingestion",
    description="Ingest settlement files from AWS S3 into PostgreSQL.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "s3", "settlements"],
)
def settlement_ingestion():
    @task(task_id="ingest_settlement_file")
    def ingest_settlement_file():
        run_source_ingestion("settlements")

    ingest_settlement_file()


settlement_ingestion()

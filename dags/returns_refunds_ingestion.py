"""Load both the returns and refunds CSV files from S3."""

import pendulum
from airflow.sdk import dag, task

from s3_ingestion_common import run_source_ingestion


@dag(
    dag_id="revenue_assurance_returns_refunds_ingestion",
    description="Ingest the S3 returns and refunds files into PostgreSQL.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "s3", "returns", "refunds"],
)
def revenue_assurance_returns_refunds_ingestion():
    @task(task_id="load_returns")
    def load_returns():
        run_source_ingestion("returns")

    @task(task_id="load_refunds")
    def load_refunds():
        run_source_ingestion("refunds")

    # These feeds are independent input files and can be ingested in parallel.
    load_returns()
    load_refunds()


revenue_assurance_returns_refunds_ingestion()

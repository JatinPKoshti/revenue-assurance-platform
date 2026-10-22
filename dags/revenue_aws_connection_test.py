
"""Test the revenue_aws connection from an actual Airflow task."""

import pendulum

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


@dag(
    dag_id="revenue_aws_connection_test",
    description="Verify AWS credentials through an Airflow task.",
    schedule=None,
    start_date=pendulum.datetime(
        2026, 1, 1, tz="Asia/Kolkata"
    ),
    catchup=False,
    tags=["revenue-assurance", "connection-test"],
)
def revenue_aws_connection_test():

    @task
    def test_aws_authentication():
        client = AwsBaseHook(
            aws_conn_id="revenue_aws",
            client_type="sts",
            region_name="ap-south-1",
        ).get_conn()

        result = client.get_caller_identity()

        # Never print access keys or secret keys.
        print("AWS connection successful")
        print("Account:", result["Account"])
        print("Identity:", result["Arn"])

    test_aws_authentication()


revenue_aws_connection_test()

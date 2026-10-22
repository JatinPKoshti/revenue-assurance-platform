
import pendulum

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


AWS_CONNECTION_ID = "revenue_aws"

# Replace this with your actual S3 bucket name.
BUCKET_NAME = "BUCKET_NAME"

REQUIRED_KEYS = [
    "raw/settlements/settlements_2026-10-15.csv",
    "raw/shipments/shipments_2026-10-15.csv",
    "raw/returns/returns_2026-10-15.csv",
    "raw/refunds/refunds_2026-10-15.csv",
]


@dag(
    dag_id="revenue_assurance_s3_landing_zone_check",
    description="Verify access to the S3 raw landing zone",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "aws", "s3"],
)
def s3_landing_zone_check():

    @task
    def validate_s3_access():
        hook = S3Hook(aws_conn_id=AWS_CONNECTION_ID)
        client = hook.get_conn()

        response = client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix="raw/",
        )

        available_keys = {
            item["Key"]
            for item in response.get("Contents", [])
        }

        missing_keys = [
            key
            for key in REQUIRED_KEYS
            if key not in available_keys
        ]

        if missing_keys:
            raise ValueError(
                "These required S3 objects are missing: "
                + ", ".join(missing_keys)
            )

        # Prove Airflow can read an actual object.
        sample_key = REQUIRED_KEYS[0]

        result = client.get_object(
            Bucket=BUCKET_NAME,
            Key=sample_key,
        )

        try:
            header = (
                result["Body"]
                .read(4096)
                .decode("utf-8-sig")
                .splitlines()[0]
            )
        finally:
            result["Body"].close()

        expected_header = (
            "settlement_line_id,provider,"
            "provider_transaction_id,order_id,payment_id,"
            "payout_id,gross_amount,fee_amount,net_amount,"
            "settlement_date,currency"
        )

        if header.strip() != expected_header:
            raise ValueError(
                "Settlement CSV header did not match "
                "the expected columns."
            )

        print(f"S3 bucket: {BUCKET_NAME}")
        print(f"Raw objects found: {len(available_keys)}")
        print(f"Verified S3 object: {sample_key}")
        print("S3 listing and object read succeeded.")

    validate_s3_access()


s3_landing_zone_check()

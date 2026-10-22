"""Shared S3 CSV ingestion logic for the Revenue Assurance project."""

from __future__ import annotations

import csv
import hashlib
import io
import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg import sql

from business_date_utils import get_business_date, get_raw_s3_key


AWS_CONNECTION_ID = os.getenv(
    "REVENUE_AWS_CONNECTION_ID", "revenue_aws"
)
POSTGRES_CONNECTION_ID = os.getenv(
    "REVENUE_POSTGRES_CONNECTION_ID", "revenue_postgres"
)
BUCKET_NAME = os.getenv(
    "REVENUE_ASSURANCE_S3_BUCKET",
    "BUCKET_NAME",
)
DB_SCHEMA = os.getenv("REVENUE_ASSURANCE_DB_SCHEMA", "public")


@dataclass(frozen=True)
class FieldSpec:
    canonical: str
    aliases: tuple[str, ...]
    db_columns: tuple[str, ...]
    required: bool = False


@dataclass(frozen=True)
class SourceSpec:
    source: str
    source_system: str
    table: str
    primary_key: str
    fields: tuple[FieldSpec, ...]
    extra_required_headers: tuple[tuple[str, ...], ...] = ()
    validation: str | None = None


COMMON_CONFIGS: dict[str, SourceSpec] = {

    # ========================================================
    # ORDERS
    # ========================================================

    "orders": SourceSpec(
        source="orders",
        source_system="S3-DemoOrders",
        table="orders",
        primary_key="order_id",
        fields=(
            FieldSpec(
                "order_id",
                ("order_id",),
                ("order_id",),
                True,
            ),
            FieldSpec(
                "customer_id",
                ("customer_id",),
                ("customer_id",),
            ),
            FieldSpec(
                "order_timestamp",
                (
                    "order_timestamp",
                    "order_ts",
                    "order_date",
                    "created_at",
                ),
                (
                    "order_timestamp",
                    "order_ts",
                    "order_date",
                    "created_at",
                ),
                True,
            ),
            FieldSpec(
                "currency",
                ("currency",),
                ("currency",),
                True,
            ),
            FieldSpec(
                "gross_amount",
                (
                    "gross_amount",
                    "order_amount",
                    "total_amount",
                    "amount",
                ),
                (
                    "gross_amount",
                    "order_amount",
                    "total_amount",
                    "amount",
                ),
                True,
            ),
            FieldSpec(
                "order_status",
                ("order_status", "status"),
                ("order_status", "status"),
                True,
            ),
        ),
    ),

    # ========================================================
    # PAYMENTS
    # ========================================================

    "payments": SourceSpec(
        source="payments",
        source_system="S3-DemoPayments",
        table="payments",
        primary_key="payment_id",
        fields=(
            FieldSpec(
                "payment_id",
                ("payment_id",),
                ("payment_id",),
                True,
            ),
            FieldSpec(
                "order_id",
                ("order_id",),
                ("order_id",),
                True,
            ),
            FieldSpec(
                "provider",
                ("provider",),
                ("provider",),
                True,
            ),
            FieldSpec(
                "provider_transaction_id",
                (
                    "provider_transaction_id",
                    "transaction_id",
                ),
                (
                    "provider_transaction_id",
                    "transaction_id",
                ),
                True,
            ),
            FieldSpec(
                "payment_status",
                ("payment_status", "status"),
                ("payment_status", "status"),
                True,
            ),
            FieldSpec(
                "payment_amount",
                (
                    "payment_amount",
                    "captured_amount",
                    "amount",
                ),
                (
                    "payment_amount",
                    "captured_amount",
                    "amount",
                ),
                True,
            ),
            FieldSpec(
                "currency",
                ("currency",),
                ("currency",),
                True,
            ),
            FieldSpec(
                "payment_ts",
                (
                    "payment_ts",
                    "captured_at",
                    "payment_timestamp",
                ),
                (
                    "payment_ts",
                    "captured_at",
                    "payment_timestamp",
                ),
                True,
            ),
        ),
    ),

    # ========================================================
    # SETTLEMENTS
    # ========================================================

    "settlements": SourceSpec(
        source="settlements",
        source_system="S3-DemoPay",
        table="settlements",
        primary_key="settlement_line_id",
        fields=(
            FieldSpec(
                "settlement_line_id",
                ("settlement_line_id",),
                ("settlement_line_id",),
                True,
            ),
            FieldSpec(
                "provider",
                ("provider",),
                ("provider",),
                True,
            ),
            FieldSpec(
                "provider_transaction_id",
                ("provider_transaction_id",),
                ("provider_transaction_id",),
                True,
            ),
            FieldSpec(
                "order_id",
                ("order_id",),
                ("order_id",),
            ),
            FieldSpec(
                "payment_id",
                ("payment_id",),
                ("payment_id",),
            ),
            FieldSpec(
                "payout_id",
                ("payout_id",),
                ("payout_id",),
            ),
            FieldSpec(
                "gross_amount",
                ("gross_amount",),
                ("gross_amount",),
                True,
            ),
            FieldSpec(
                "fee_amount",
                ("fee_amount",),
                ("fee_amount",),
                True,
            ),
            FieldSpec(
                "net_amount",
                ("net_amount",),
                ("net_amount",),
                True,
            ),
            FieldSpec(
                "settlement_date",
                ("settlement_date",),
                ("settlement_date",),
                True,
            ),
            FieldSpec(
                "currency",
                ("currency",),
                ("currency",),
                True,
            ),
        ),
        validation="settlements",
    ),

    # ========================================================
    # SHIPMENTS
    # Supports:
    # shipment_status -> delivery_status
    # promised_delivery_date -> sla_due_at
    # ========================================================

    "shipments": SourceSpec(
        source="shipments",
        source_system="S3-DemoShipments",
        table="shipments",
        primary_key="shipment_id",
        fields=(
            FieldSpec(
                "shipment_id",
                ("shipment_id",),
                ("shipment_id",),
                True,
            ),
            FieldSpec(
                "order_id",
                ("order_id",),
                ("order_id",),
                True,
            ),
            FieldSpec(
                "shipped_at",
                ("shipped_at", "ship_timestamp"),
                ("shipped_at", "ship_timestamp"),
                True,
            ),
            FieldSpec(
                "delivered_at",
                ("delivered_at", "delivery_timestamp"),
                ("delivered_at", "delivery_timestamp"),
            ),
            FieldSpec(
                "delivery_status",
                (
                    "delivery_status",
                    "shipment_status",
                    "status",
                ),
                (
                    "delivery_status",
                    "shipment_status",
                    "status",
                ),
                True,
            ),
            FieldSpec(
                "sla_due_at",
                (
                    "sla_due_at",
                    "promised_delivery_at",
                    "expected_delivery_at",
                    "promised_delivery_date",
                ),
                (
                    "sla_due_at",
                    "promised_delivery_at",
                    "expected_delivery_at",
                    "promised_delivery_date",
                ),
                True,
            ),
        ),
    ),

    # ========================================================
    # RETURNS
    # Supports return_created_at and expected_refund_amount.
    # ========================================================

    "returns": SourceSpec(
        source="returns",
        source_system="S3-DemoReturns",
        table="returns",
        primary_key="return_id",
        fields=(
            FieldSpec(
                "return_id",
                ("return_id",),
                ("return_id",),
                True,
            ),
            FieldSpec(
                "order_id",
                ("order_id",),
                ("order_id",),
                True,
            ),
            FieldSpec(
                "requested_at",
                (
                    "requested_at",
                    "return_requested_at",
                    "return_created_at",
                ),
                (
                    "requested_at",
                    "return_requested_at",
                    "return_created_at",
                ),
                True,
            ),
            FieldSpec(
                "approved_at",
                ("approved_at", "return_approved_at"),
                ("approved_at", "return_approved_at"),
            ),
            FieldSpec(
                "return_status",
                ("return_status", "status"),
                ("return_status", "status"),
                True,
            ),
            FieldSpec(
                "reason_code",
                ("reason_code", "return_reason"),
                ("reason_code", "return_reason"),
            ),
            FieldSpec(
                "expected_refund_amount",
                (
                    "expected_refund_amount",
                    "refund_amount",
                    "amount",
                ),
                (
                    "expected_refund_amount",
                    "refund_amount",
                ),
                True,
            ),
            FieldSpec(
                "currency",
                ("currency",),
                ("currency",),
                True,
            ),
        ),
    ),

    # ========================================================
    # REFUNDS
    # Supports provider and refund_ts.
    # ========================================================

    "refunds": SourceSpec(
        source="refunds",
        source_system="S3-DemoRefunds",
        table="refunds",
        primary_key="refund_id",
        fields=(
            FieldSpec(
                "refund_id",
                ("refund_id",),
                ("refund_id",),
                True,
            ),
            FieldSpec(
                "order_id",
                ("order_id",),
                ("order_id",),
                True,
            ),
            FieldSpec(
                "payment_id",
                ("payment_id",),
                ("payment_id",),
            ),
            FieldSpec(
                "provider",
                ("provider",),
                ("provider",),
                True,
            ),
            FieldSpec(
                "refund_amount",
                ("refund_amount", "amount"),
                ("refund_amount", "amount"),
                True,
            ),
            FieldSpec(
                "refund_status",
                ("refund_status", "status"),
                ("refund_status", "status"),
                True,
            ),
            FieldSpec(
                "requested_at",
                (
                    "requested_at",
                    "refund_requested_at",
                    "refund_ts",
                ),
                (
                    "requested_at",
                    "refund_requested_at",
                    "refund_ts",
                ),
                True,
            ),
            FieldSpec(
                "processed_at",
                ("processed_at", "refund_processed_at"),
                ("processed_at", "refund_processed_at"),
            ),
            FieldSpec(
                "currency",
                ("currency",),
                ("currency",),
                True,
            ),
        ),
    ),
}


# ============================================================
# CSV READING
# ============================================================

def _normalise_header(value: str) -> str:
    return (
        (value or "")
        .strip()
        .lower()
        .replace(" ", "_")
    )


def _read_s3_csv(
    source: str,
    business_date: str,
) -> tuple[str, bytes, str, list[dict[str, str]], list[str]]:

    if not BUCKET_NAME or BUCKET_NAME in {
        "YOUR_BUCKET_NAME",
        "<your-bucket>",
    }:
        raise ValueError(
            "Set REVENUE_ASSURANCE_S3_BUCKET to the actual S3 bucket name."
        )

    object_key = get_raw_s3_key(
        source,
        business_date,
    )

    s3_hook = S3Hook(
        aws_conn_id=AWS_CONNECTION_ID,
    )

    s3_object = s3_hook.get_key(
        key=object_key,
        bucket_name=BUCKET_NAME,
    )

    if s3_object is None:
        raise FileNotFoundError(
            f"S3 object was not found: "
            f"s3://{BUCKET_NAME}/{object_key}"
        )

    response = s3_object.get()
    body = response["Body"]

    try:
        content = body.read()
    finally:
        body.close()

    if not content:
        raise ValueError(
            f"S3 CSV is empty: {object_key}"
        )

    checksum = hashlib.sha256(content).hexdigest()
    decoded = content.decode("utf-8-sig")

    reader = csv.DictReader(
        io.StringIO(decoded, newline="")
    )

    if not reader.fieldnames:
        raise ValueError(
            f"CSV has no header: {object_key}"
        )

    original_headers = list(reader.fieldnames)

    normalised_headers = [
        _normalise_header(header)
        for header in original_headers
    ]

    if len(normalised_headers) != len(set(normalised_headers)):
        raise ValueError(
            "CSV contains duplicate header names after trimming: "
            f"{object_key}"
        )

    rows: list[dict[str, str]] = []

    for row_number, raw_row in enumerate(reader, start=2):

        normalised_row = {
            _normalise_header(key): (
                value.strip()
                if isinstance(value, str)
                else value
            )
            for key, value in raw_row.items()
            if key is not None
        }

        if None in raw_row:
            raise ValueError(
                f"CSV row {row_number} has more values "
                "than header columns."
            )

        if not any(
            value not in (None, "")
            for value in normalised_row.values()
        ):
            continue

        rows.append(normalised_row)

    if not rows:
        raise ValueError(
            f"CSV has no data rows: {object_key}"
        )

    print(f"S3 bucket: {BUCKET_NAME}")
    print(f"S3 object: {object_key}")
    print(f"SHA-256: {checksum}")
    print(f"CSV rows: {len(rows)}")

    return (
        object_key,
        content,
        checksum,
        rows,
        normalised_headers,
    )


# ============================================================
# MAP CSV HEADERS
# ============================================================

def _field_header_map(
    spec: SourceSpec,
    headers: list[str],
) -> dict[str, str]:

    header_set = set(headers)
    selected: dict[str, str] = {}
    missing = []

    for field in spec.fields:
        match = next(
            (
                alias
                for alias in field.aliases
                if alias in header_set
            ),
            None,
        )

        if match is None:
            if field.required:
                missing.append(
                    f"{field.canonical} "
                    f"(one of: {', '.join(field.aliases)})"
                )
        else:
            selected[field.canonical] = match

    if missing:
        raise ValueError(
            f"{spec.source} CSV has missing required headers: "
            f"{missing}. Headers found: {headers}"
        )

    return selected


# ============================================================
# VALIDATION AND CANONICALISATION
# ============================================================

def _validate_and_canonicalise(
    spec: SourceSpec,
    rows: list[dict[str, str]],
    header_map: dict[str, str],
) -> list[dict[str, Any]]:

    canonical_rows: list[dict[str, Any]] = []
    ids = []

    settlement_providers: set[str] = set()
    settlement_dates: set[str] = set()

    for line_number, source_row in enumerate(rows, start=2):

        record: dict[str, Any] = {}

        for field in spec.fields:
            header = header_map.get(field.canonical)
            value = source_row.get(header) if header else None

            if isinstance(value, str):
                value = value.strip()

            if value == "":
                value = None

            record[field.canonical] = value

        pk_value = record.get(spec.primary_key)

        if pk_value is None:
            raise ValueError(
                f"Row {line_number}: "
                f"{spec.primary_key} cannot be empty."
            )

        ids.append(str(pk_value))

        for field in spec.fields:
            if (
                field.required
                and record.get(field.canonical) is None
            ):
                # These fields must have a header, but their values
                # may be blank for individual records.
                if field.canonical in {
                    "provider_transaction_id",
                    "sla_due_at",
                }:
                    continue

                raise ValueError(
                    f"Row {line_number}: "
                    f"{field.canonical} cannot be empty."
                )

        if spec.validation == "settlements":
            try:
                gross = Decimal(str(record["gross_amount"]))
                fee = Decimal(str(record["fee_amount"]))
                net = Decimal(str(record["net_amount"]))
            except (InvalidOperation, TypeError) as exc:
                raise ValueError(
                    f"Settlement row {line_number}: "
                    "amounts must be numeric."
                ) from exc

            if not all(
                amount.is_finite()
                for amount in (gross, fee, net)
            ):
                raise ValueError(
                    f"Settlement row {line_number}: "
                    "amounts must be finite."
                )

            if gross < 0 or fee < 0 or net < 0:
                raise ValueError(
                    f"Settlement row {line_number}: "
                    "amounts cannot be negative."
                )

            if gross != fee + net:
                raise ValueError(
                    f"Settlement row {line_number}: "
                    "gross_amount must equal fee_amount + net_amount."
                )

            record["gross_amount"] = gross
            record["fee_amount"] = fee
            record["net_amount"] = net

            settlement_providers.add(
                str(record.get("provider") or "")
            )
            settlement_dates.add(
                str(record.get("settlement_date") or "")
            )

        # Validate monetary values, including the expected refund
        # amount recorded on return records.
        for field_name in (
            "gross_amount",
            "payment_amount",
            "refund_amount",
            "expected_refund_amount",
        ):
            value = record.get(field_name)

            if value is not None:
                try:
                    number = Decimal(str(value))
                except InvalidOperation as exc:
                    raise ValueError(
                        f"Row {line_number}: "
                        f"{field_name} must be numeric."
                    ) from exc

                if not number.is_finite() or number < 0:
                    raise ValueError(
                        f"Row {line_number}: {field_name} "
                        "must be a finite non-negative number."
                    )

                record[field_name] = number

        if record.get("currency") is not None:
            record["currency"] = str(
                record["currency"]
            ).upper()

            if len(record["currency"]) != 3:
                raise ValueError(
                    f"Row {line_number}: "
                    "currency must contain three letters."
                )

        canonical_rows.append(record)

    if len(ids) != len(set(ids)):
        raise ValueError(
            f"{spec.source} CSV contains duplicate "
            f"{spec.primary_key} values."
        )

    if spec.validation == "settlements":
        if (
            len(settlement_providers) != 1
            or len(settlement_dates) != 1
        ):
            raise ValueError(
                "Settlement CSV must contain one provider "
                "and one settlement date per file."
            )

    return canonical_rows


# ============================================================
# READ TARGET TABLE METADATA
# ============================================================

def _get_table_metadata(
    connection,
    table_name: str,
) -> list[dict[str, Any]]:

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                column_name,
                is_nullable,
                column_default,
                data_type,
                udt_name,
                is_identity,
                is_generated
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (DB_SCHEMA, table_name),
        )

        rows = cursor.fetchall()

    if not rows:
        raise RuntimeError(
            f"Target table {DB_SCHEMA}.{table_name} "
            "does not exist or is not visible to the database user."
        )

    return [
        {
            "column_name": row[0],
            "is_nullable": row[1],
            "column_default": row[2],
            "data_type": row[3],
            "udt_name": row[4],
            "is_identity": row[5],
            "is_generated": row[6],
        }
        for row in rows
    ]


def _canonical_for_db_column(
    spec: SourceSpec,
    db_column: str,
) -> str | None:

    for field in spec.fields:
        if (
            db_column in field.db_columns
            or db_column == field.canonical
        ):
            return field.canonical

    return None


# ============================================================
# CONVERT VALUES TO DATABASE TYPES
# ============================================================

def _coerce_for_column(
    value: Any,
    column: dict[str, Any],
    canonical: str | None,
) -> Any:

    if value in (None, ""):
        return None

    data_type = column["data_type"]
    udt_name = column["udt_name"]
    column_name = column["column_name"].lower()

    if isinstance(value, Decimal):
        return value

    if udt_name in {"int2", "int4", "int8"}:
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Column {column_name} must be an integer; "
                f"received {value!r}."
            ) from exc

    numeric_types = {
        "numeric",
        "decimal",
        "real",
        "double precision",
    }

    if data_type in numeric_types or any(
        token in column_name
        for token in ("amount", "gross", "fee", "net")
    ):
        try:
            number = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(
                f"Column {column_name} must be numeric; "
                f"received {value!r}."
            ) from exc

        if not number.is_finite():
            raise ValueError(
                f"Column {column_name} must be finite."
            )

        return number

    if data_type == "date" or column_name.endswith("_date"):
        try:
            return pendulum.parse(str(value)).date()
        except Exception as exc:
            raise ValueError(
                f"Column {column_name} must be a valid date; "
                f"received {value!r}."
            ) from exc

    if (
        "timestamp" in data_type
        or data_type == "time without time zone"
        or data_type == "time with time zone"
    ):
        try:
            parsed = pendulum.parse(str(value))

            if (
                data_type == "timestamp without time zone"
                and parsed.tzinfo is not None
            ):
                parsed = parsed.in_timezone("UTC").naive()

            return parsed
        except Exception as exc:
            raise ValueError(
                f"Column {column_name} must be a valid timestamp; "
                f"received {value!r}."
            ) from exc

    if data_type == "boolean":
        lowered = str(value).strip().lower()

        if lowered in {"true", "t", "1", "yes", "y"}:
            return True

        if lowered in {"false", "f", "0", "no", "n"}:
            return False

        raise ValueError(
            f"Column {column_name} must be boolean; "
            f"received {value!r}."
        )

    return str(value)


# ============================================================
# BUILD EACH DATABASE ROW
# ============================================================

def _build_destination_row(
    spec: SourceSpec,
    canonical_row: dict[str, Any],
    metadata: list[dict[str, Any]],
    batch_id: int,
) -> tuple[list[str], list[Any]]:

    metadata_by_name = {
        column["column_name"]: column
        for column in metadata
    }

    values_by_name: dict[str, Any] = {}

    for column in metadata:
        name = column["column_name"]

        if name == "source_batch_id":
            values_by_name[name] = batch_id
            continue

        canonical = _canonical_for_db_column(spec, name)

        if canonical is None:
            continue

        value = canonical_row.get(canonical)

        if value is not None:
            values_by_name[name] = _coerce_for_column(
                value,
                column,
                canonical,
            )

    missing_required = []

    for column in metadata:
        name = column["column_name"]

        generated = column["is_generated"] not in (
            None,
            "NEVER",
        )
        identity = column["is_identity"] == "YES"

        if (
            column["is_nullable"] == "NO"
            and column["column_default"] is None
            and not identity
            and not generated
            and name not in values_by_name
        ):
            missing_required.append(name)

    if missing_required:
        raise ValueError(
            f"Cannot load into {DB_SCHEMA}.{spec.table}: "
            "required target columns have no mapped CSV value: "
            f"{missing_required}. Check the CSV headers "
            "and target table schema."
        )

    primary_key_dest = next(
        (
            name
            for name in metadata_by_name
            if name == spec.primary_key
        ),
        None,
    )

    if (
        primary_key_dest is None
        or primary_key_dest not in values_by_name
    ):
        raise ValueError(
            f"Target table {DB_SCHEMA}.{spec.table} does not "
            f"expose the expected primary key column "
            f"{spec.primary_key}."
        )

    ordered_columns = [
        column["column_name"]
        for column in metadata
        if column["column_name"] in values_by_name
    ]

    ordered_values = [
        values_by_name[name]
        for name in ordered_columns
    ]

    return ordered_columns, ordered_values


# ============================================================
# REGISTER THE INGESTION BATCH
# ============================================================

def _register_batch(
    connection,
    spec: SourceSpec,
    object_key: str,
    checksum: str,
    business_date: str,
    row_count: int,
) -> tuple[int, str]:

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ingestion_batches (
                source_system,
                file_name,
                file_checksum,
                business_date,
                status,
                row_count,
                loaded_count
            )
            VALUES (
                %s, %s, %s, %s, 'discovered', %s, 0
            )
            ON CONFLICT (source_system, file_checksum)
            DO UPDATE SET
                file_name = EXCLUDED.file_name
            RETURNING batch_id, status
            """,
            (
                spec.source_system,
                object_key,
                checksum,
                business_date,
                row_count,
            ),
        )

        row = cursor.fetchone()

        if row is None:
            raise RuntimeError(
                "PostgreSQL did not return the ingestion batch ID."
            )

        return int(row[0]), str(row[1])


# ============================================================
# UPSERT SOURCE ROWS
# Uses Psycopg 3 SQL composition objects.
# ============================================================

def _upsert_rows(
    connection,
    spec: SourceSpec,
    canonical_rows: list[dict[str, Any]],
    metadata: list[dict[str, Any]],
    batch_id: int,
) -> None:

    metadata_by_name = {
        column["column_name"]: column
        for column in metadata
    }

    if spec.primary_key not in metadata_by_name:
        raise RuntimeError(
            f"Primary key column {spec.primary_key} "
            f"is missing from {spec.table}."
        )

    for canonical_row in canonical_rows:
        columns, values = _build_destination_row(
            spec,
            canonical_row,
            metadata,
            batch_id,
        )

        if spec.primary_key not in columns:
            raise RuntimeError(
                f"Primary key {spec.primary_key} is not being "
                f"inserted into {spec.table}."
            )

        update_columns = [
            name
            for name in columns
            if name != spec.primary_key
        ]

        if update_columns:
            update_clause = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(name),
                    sql.Identifier(name),
                )
                for name in update_columns
            )

            conflict_clause = sql.SQL(
                "DO UPDATE SET {}"
            ).format(update_clause)
        else:
            conflict_clause = sql.SQL("DO NOTHING")

        query = sql.SQL(
            """
            INSERT INTO {}.{} ({})
            VALUES ({})
            ON CONFLICT ({})
            {}
            """
        ).format(
            sql.Identifier(DB_SCHEMA),
            sql.Identifier(spec.table),
            sql.SQL(", ").join(
                sql.Identifier(name)
                for name in columns
            ),
            sql.SQL(", ").join(
                sql.Placeholder()
                for _ in columns
            ),
            sql.Identifier(spec.primary_key),
            conflict_clause,
        )

        with connection.cursor() as cursor:
            cursor.execute(query, values)


# ============================================================
# MAIN INGESTION FUNCTION
# ============================================================

def run_source_ingestion(source: str) -> None:
    """Read a dated S3 CSV, validate it and upsert into PostgreSQL."""

    if source not in COMMON_CONFIGS:
        raise ValueError(
            f"Unknown source {source!r}."
        )

    spec = COMMON_CONFIGS[source]

    business_date = get_business_date()

    print(f"Business date: {business_date}")

    (
        object_key,
        _content,
        checksum,
        raw_rows,
        headers,
    ) = _read_s3_csv(
        source,
        business_date,
    )

    header_map = _field_header_map(
        spec,
        headers,
    )

    canonical_rows = _validate_and_canonicalise(
        spec,
        raw_rows,
        header_map,
    )

    postgres_hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONNECTION_ID,
    )

    connection = postgres_hook.get_conn()
    batch_id: int | None = None

    try:
        batch_id, batch_status = _register_batch(
            connection,
            spec,
            object_key,
            checksum,
            business_date,
            len(canonical_rows),
        )

        connection.commit()

        if batch_status == "loaded":
            print(
                f"File already loaded. Batch ID: {batch_id}. "
                "Skipping duplicate file."
            )
            return

        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ingestion_batches
                SET
                    status = 'discovered',
                    row_count = %s,
                    loaded_count = 0,
                    error_message = NULL,
                    completed_at = NULL
                WHERE batch_id = %s
                """,
                (
                    len(canonical_rows),
                    batch_id,
                ),
            )

        connection.commit()

        metadata = _get_table_metadata(
            connection,
            spec.table,
        )

        _upsert_rows(
            connection,
            spec,
            canonical_rows,
            metadata,
            batch_id,
        )

        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ingestion_batches
                SET
                    status = 'loaded',
                    row_count = %s,
                    loaded_count = %s,
                    completed_at = NOW(),
                    error_message = NULL
                WHERE batch_id = %s
                """,
                (
                    len(canonical_rows),
                    len(canonical_rows),
                    batch_id,
                ),
            )

        connection.commit()

        print(f"Ingestion batch ID: {batch_id}")
        print(f"Source system: {spec.source_system}")
        print(f"Target table: {DB_SCHEMA}.{spec.table}")
        print(f"Rows loaded/upserted: {len(canonical_rows)}")
        print(
            f"S3 ingestion for {source} completed successfully."
        )

    except Exception as exc:
        connection.rollback()

        if batch_id is not None:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE ingestion_batches
                        SET
                            status = 'failed',
                            error_message = %s,
                            completed_at = NOW()
                        WHERE batch_id = %s
                        """,
                        (
                            str(exc)[:2000],
                            batch_id,
                        ),
                    )

                connection.commit()

            except Exception:
                connection.rollback()

        raise

    finally:
        connection.close()
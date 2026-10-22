
import csv
import hashlib
import json
from pathlib import Path

import pendulum

from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "revenue_assurance_shipment_sla"
CONNECTION_ID = "revenue_postgres"

CSV_PATH = Path(
    "/opt/airflow/data/incoming/shipments/"
    "shipments_2026-10-15.csv"
)


@dag(
    dag_id=DAG_ID,
    description="Ingest shipments and monitor delivery SLAs",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "shipments", "sla"],
)
def shipment_sla_pipeline():

    @task
    def ingest_shipments():
        if not CSV_PATH.is_file():
            raise FileNotFoundError(
                f"Shipment file not found: {CSV_PATH}"
            )

        content = CSV_PATH.read_bytes()
        checksum = hashlib.sha256(content).hexdigest()

        required_columns = {
            "shipment_id",
            "order_id",
            "carrier",
            "shipment_status",
            "shipped_at",
            "delivered_at",
            "promised_delivery_date",
        }

        allowed_statuses = {
            "pending",
            "in_transit",
            "delivered",
            "cancelled",
            "returned",
        }

        rows = []

        with CSV_PATH.open(
            "r", encoding="utf-8-sig", newline=""
        ) as file:
            reader = csv.DictReader(file)

            if not reader.fieldnames:
                raise ValueError("Shipment CSV has no header.")

            missing_columns = required_columns - set(
                reader.fieldnames
            )

            if missing_columns:
                raise ValueError(
                    f"Missing columns: {sorted(missing_columns)}"
                )

            for line_number, row in enumerate(reader, start=2):
                try:
                    shipment_id = row["shipment_id"].strip()
                    order_id = row["order_id"].strip()
                    carrier = row["carrier"].strip()
                    status = row["shipment_status"].strip().lower()

                    if not shipment_id or not order_id:
                        raise ValueError(
                            "Shipment ID and order ID are required."
                        )

                    if status not in allowed_statuses:
                        raise ValueError(
                            f"Unsupported shipment status: {status}"
                        )

                    promised_text = row[
                        "promised_delivery_date"
                    ].strip()

                    if not promised_text:
                        raise ValueError(
                            "Promised delivery date is required."
                        )

                    promised_date = pendulum.parse(
                        promised_text
                    ).date()

                    shipped_at = (
                        row["shipped_at"].strip() or None
                    )
                    delivered_at = (
                        row["delivered_at"].strip() or None
                    )

                    if status == "delivered" and not delivered_at:
                        raise ValueError(
                            "Delivered shipment must have delivered_at."
                        )

                    if (
                        status == "in_transit"
                        and not shipped_at
                    ):
                        raise ValueError(
                            "In-transit shipment must have shipped_at."
                        )

                    rows.append(
                        (
                            shipment_id,
                            order_id,
                            carrier or None,
                            status,
                            shipped_at,
                            delivered_at,
                            promised_date,
                        )
                    )

                except Exception as exc:
                    raise ValueError(
                        f"Invalid CSV row {line_number}: {exc}"
                    ) from exc

        if not rows:
            raise ValueError("Shipment CSV contains no records.")

        shipment_ids = [row[0] for row in rows]

        if len(shipment_ids) != len(set(shipment_ids)):
            raise ValueError(
                "Duplicate shipment IDs in the input file."
            )

        # The filename identifies the report's business date.
        business_date = pendulum.parse(
            CSV_PATH.stem.removeprefix("shipments_")
        ).date()

        hook = PostgresHook(
            postgres_conn_id=CONNECTION_ID
        )
        connection = hook.get_conn()
        batch_id = None

        try:
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
                        'DemoCarrier', %s, %s, %s,
                        'discovered', %s, 0
                    )
                    ON CONFLICT (source_system, file_checksum)
                    DO UPDATE SET
                        file_name = EXCLUDED.file_name
                    RETURNING batch_id, status;
                    """,
                    (
                        CSV_PATH.name,
                        checksum,
                        business_date,
                        len(rows),
                    ),
                )

                batch_id, batch_status = cursor.fetchone()

                if batch_status == "loaded":
                    connection.commit()
                    print(
                        "Shipment file was already processed. "
                        "Skipping duplicate ingestion."
                    )
                    return batch_id

                cursor.execute(
                    """
                    UPDATE ingestion_batches
                    SET status = 'discovered',
                        row_count = %s,
                        error_message = NULL,
                        completed_at = NULL
                    WHERE batch_id = %s;
                    """,
                    (len(rows), batch_id),
                )

            connection.commit()

            with connection.cursor() as cursor:
                insert_sql = """
                    INSERT INTO shipments (
                        shipment_id,
                        order_id,
                        carrier,
                        shipment_status,
                        shipped_at,
                        delivered_at,
                        promised_delivery_date,
                        source_batch_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (shipment_id)
                    DO UPDATE SET
                        order_id = EXCLUDED.order_id,
                        carrier = EXCLUDED.carrier,
                        shipment_status = EXCLUDED.shipment_status,
                        shipped_at = EXCLUDED.shipped_at,
                        delivered_at = EXCLUDED.delivered_at,
                        promised_delivery_date =
                            EXCLUDED.promised_delivery_date,
                        source_batch_id = EXCLUDED.source_batch_id;
                """

                for row in rows:
                    cursor.execute(
                        insert_sql,
                        (*row, batch_id),
                    )

                cursor.execute(
                    """
                    UPDATE ingestion_batches
                    SET status = 'loaded',
                        row_count = %s,
                        loaded_count = %s,
                        completed_at = NOW(),
                        error_message = NULL
                    WHERE batch_id = %s;
                    """,
                    (len(rows), len(rows), batch_id),
                )

            connection.commit()

            print(f"Shipment batch ID: {batch_id}")
            print(f"Shipment rows processed: {len(rows)}")

            return batch_id

        except Exception as exc:
            connection.rollback()

            if batch_id is not None:
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE ingestion_batches
                            SET status = 'failed',
                                error_message = %s,
                                completed_at = NOW()
                            WHERE batch_id = %s;
                            """,
                            (str(exc)[:2000], batch_id),
                        )
                    connection.commit()
                except Exception:
                    connection.rollback()

            raise

        finally:
            connection.close()

    @task
    def evaluate_delivery_sla(batch_id):
        context = get_current_context()
        dag_run = context["dag_run"]
        logical_date = context.get("logical_date")

        if logical_date:
            business_date = logical_date.in_timezone(
                "Asia/Kolkata"
            ).date()
        else:
            business_date = pendulum.now(
                "Asia/Kolkata"
            ).date()

        hook = PostgresHook(
            postgres_conn_id=CONNECTION_ID
        )
        connection = hook.get_conn()

        try:
            with connection:
                with connection.cursor() as cursor:

                    cursor.execute(
                        """
                        INSERT INTO reconciliation_runs (
                            dag_id,
                            dag_run_id,
                            business_date,
                            run_status,
                            run_type,
                            started_at,
                            result_summary
                        )
                        VALUES (
                            %s, %s, %s,
                            'running', 'manual',
                            NOW(), '{}'::jsonb
                        )
                        ON CONFLICT (dag_id, dag_run_id)
                        DO UPDATE SET
                            business_date = EXCLUDED.business_date,
                            run_status = 'running',
                            started_at = NOW(),
                            completed_at = NULL,
                            result_summary = '{}'::jsonb
                        RETURNING run_id;
                        """,
                        (
                            DAG_ID,
                            dag_run.run_id,
                            business_date,
                        ),
                    )

                    run_id = cursor.fetchone()[0]

                    # Make task retries repeatable for this run.
                    cursor.execute(
                        """
                        DELETE FROM reconciliation_exceptions
                        WHERE run_id = %s;
                        """,
                        (run_id,),
                    )

                    findings_sql = """
                    SELECT
                        'SHIPMENT_WITHOUT_ORDER'::text,
                        'shipment'::text,
                        s.shipment_id::text,
                        'high'::text,
                        NULL::numeric,
                        NULL::numeric,
                        NULL::numeric,
                        jsonb_build_object(
                            'order_id', s.order_id,
                            'carrier', s.carrier,
                            'reason',
                                'Shipment references a missing order'
                        )::text
                    FROM shipments s
                    LEFT JOIN orders o
                        ON o.order_id = s.order_id
                    WHERE o.order_id IS NULL

                    UNION ALL

                    SELECT
                        'LATE_DELIVERY',
                        'shipment',
                        s.shipment_id::text,
                        'medium',
                        NULL::numeric,
                        NULL::numeric,
                        NULL::numeric,
                        jsonb_build_object(
                            'order_id', s.order_id,
                            'promised_date',
                                s.promised_delivery_date,
                            'actual_delivery_date',
                                (s.delivered_at AT TIME ZONE
                                    'Asia/Kolkata')::date,
                            'reason',
                                'Delivered after promised date'
                        )::text
                    FROM shipments s
                    WHERE LOWER(s.shipment_status) = 'delivered'
                      AND s.delivered_at IS NOT NULL
                      AND (
                          s.delivered_at AT TIME ZONE
                              'Asia/Kolkata'
                      )::date > s.promised_delivery_date

                    UNION ALL

                    SELECT
                        'OVERDUE_IN_TRANSIT',
                        'shipment',
                        s.shipment_id::text,
                        'high',
                        NULL::numeric,
                        NULL::numeric,
                        NULL::numeric,
                        jsonb_build_object(
                            'order_id', s.order_id,
                            'carrier', s.carrier,
                            'promised_date',
                                s.promised_delivery_date,
                            'checked_business_date', %s,
                            'reason',
                                'Shipment remains undelivered after promised date'
                        )::text
                    FROM shipments s
                    WHERE LOWER(s.shipment_status) NOT IN (
                        'delivered', 'cancelled', 'returned'
                    )
                      AND s.promised_delivery_date < %s

                    UNION ALL

                    SELECT
                        'MISSING_SHIPMENT_FOR_PAID_ORDER',
                        'order',
                        o.order_id::text,
                        'medium',
                        NULL::numeric,
                        NULL::numeric,
                        NULL::numeric,
                        jsonb_build_object(
                            'order_status', o.order_status,
                            'reason',
                                'Eligible order has no shipment record'
                        )::text
                    FROM orders o
                    LEFT JOIN shipments s
                        ON s.order_id = o.order_id
                    WHERE LOWER(o.order_status) IN (
                        'paid', 'completed', 'fulfilled',
                        'shipped', 'delivered'
                    )
                      AND s.shipment_id IS NULL

                    ORDER BY 1, 3;
                    """

                    cursor.execute(
                        findings_sql,
                        (business_date, business_date),
                    )

                    findings = cursor.fetchall()

                    insert_exception_sql = """
                        INSERT INTO reconciliation_exceptions (
                            run_id,
                            business_date,
                            exception_type,
                            entity_type,
                            entity_id,
                            severity,
                            exception_status,
                            expected_amount,
                            actual_amount,
                            difference_amount,
                            details
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s,
                            'open', %s, %s, %s, %s::jsonb
                        )
                        ON CONFLICT (
                            run_id,
                            exception_type,
                            entity_type,
                            entity_id
                        )
                        DO UPDATE SET
                            severity = EXCLUDED.severity,
                            exception_status = 'open',
                            details = EXCLUDED.details,
                            detected_at = NOW(),
                            resolved_at = NULL;
                    """

                    counts_by_type = {}

                    for finding in findings:
                        (
                            exception_type,
                            entity_type,
                            entity_id,
                            severity,
                            expected_amount,
                            actual_amount,
                            difference_amount,
                            details,
                        ) = finding

                        cursor.execute(
                            insert_exception_sql,
                            (
                                run_id,
                                business_date,
                                exception_type,
                                entity_type,
                                entity_id,
                                severity,
                                expected_amount,
                                actual_amount,
                                difference_amount,
                                details,
                            ),
                        )

                        counts_by_type[exception_type] = (
                            counts_by_type.get(exception_type, 0) + 1
                        )

                    summary = {
                        "total_exceptions": len(findings),
                        "by_type": counts_by_type,
                        "business_date": str(business_date),
                        "shipment_batch_id": batch_id,
                    }

                    cursor.execute(
                        """
                        UPDATE reconciliation_runs
                        SET run_status = 'succeeded',
                            completed_at = NOW(),
                            result_summary = %s::jsonb
                        WHERE run_id = %s;
                        """,
                        (json.dumps(summary), run_id),
                    )

            print(f"Reconciliation run ID: {run_id}")
            print(f"Business date: {business_date}")
            print(f"Shipment batch ID: {batch_id}")
            print(f"Total SLA exceptions: {len(findings)}")
            print(f"Exceptions by type: {counts_by_type}")

        finally:
            connection.close()

    batch_id = ingest_shipments()
    evaluate_delivery_sla(batch_id)


shipment_sla_pipeline()

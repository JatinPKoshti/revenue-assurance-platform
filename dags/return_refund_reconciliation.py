
import json

import pendulum

from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "revenue_assurance_return_refund_reconciliation"
CONNECTION_ID = "revenue_postgres"


@dag(
    dag_id=DAG_ID,
    description="Reconcile customer returns against successful refunds",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "returns", "refunds"],
)
def return_refund_reconciliation():

    @task
    def reconcile_returns_and_refunds():
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

                    # Create or reuse the run audit record.
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

                    # Make reruns of this same Airflow run repeatable.
                    cursor.execute(
                        """
                        DELETE FROM reconciliation_exceptions
                        WHERE run_id = %s;
                        """,
                        (run_id,),
                    )

                    findings_sql = """
                    WITH eligible_returns AS (
                        SELECT
                            r.return_id,
                            r.order_id,
                            UPPER(r.currency) AS currency,
                            r.expected_refund_amount
                        FROM returns r
                        WHERE LOWER(r.return_status) IN (
                            'approved', 'received', 'completed'
                        )
                    ),

                    successful_refunds AS (
                        SELECT
                            f.refund_id,
                            f.order_id,
                            f.payment_id,
                            UPPER(f.currency) AS currency,
                            f.refund_amount
                        FROM refunds f
                        WHERE LOWER(f.refund_status) IN (
                            'succeeded', 'processed', 'completed'
                        )
                    ),

                    return_totals AS (
                        SELECT
                            order_id,
                            currency,
                            SUM(expected_refund_amount)
                                AS expected_total,
                            ARRAY_AGG(return_id) AS return_ids
                        FROM eligible_returns
                        GROUP BY order_id, currency
                    ),

                    refund_totals AS (
                        SELECT
                            order_id,
                            currency,
                            SUM(refund_amount) AS actual_total,
                            ARRAY_AGG(refund_id) AS refund_ids
                        FROM successful_refunds
                        GROUP BY order_id, currency
                    ),

                    findings AS (

                        -- Rule 1: Return references a missing order.
                        SELECT
                            'RETURN_WITHOUT_ORDER'::text
                                AS exception_type,
                            'return'::text AS entity_type,
                            r.return_id::text AS entity_id,
                            'high'::text AS severity,
                            r.expected_refund_amount::numeric
                                AS expected_amount,
                            NULL::numeric AS actual_amount,
                            NULL::numeric AS difference_amount,
                            jsonb_build_object(
                                'order_id', r.order_id,
                                'currency', r.currency,
                                'reason',
                                    'Return references a nonexistent order'
                            )::text AS details
                        FROM eligible_returns r
                        LEFT JOIN orders o
                            ON o.order_id = r.order_id
                        WHERE o.order_id IS NULL

                        UNION ALL

                        -- Rule 2: Successful refund references
                        -- a missing order.
                        SELECT
                            'REFUND_WITHOUT_ORDER',
                            'refund',
                            f.refund_id::text,
                            'high',
                            NULL::numeric,
                            f.refund_amount,
                            NULL::numeric,
                            jsonb_build_object(
                                'order_id', f.order_id,
                                'payment_id', f.payment_id,
                                'currency', f.currency,
                                'reason',
                                    'Refund references a nonexistent order'
                            )::text
                        FROM successful_refunds f
                        LEFT JOIN orders o
                            ON o.order_id = f.order_id
                        WHERE o.order_id IS NULL

                        UNION ALL

                        -- Rule 3: Successful refund exists without
                        -- any eligible return for that order.
                        SELECT
                            'REFUND_WITHOUT_RETURN',
                            'refund',
                            f.refund_id::text,
                            'high',
                            NULL::numeric,
                            f.refund_amount,
                            NULL::numeric,
                            jsonb_build_object(
                                'order_id', f.order_id,
                                'payment_id', f.payment_id,
                                'currency', f.currency,
                                'reason',
                                    'No approved, received or completed return exists'
                            )::text
                        FROM successful_refunds f
                        JOIN orders o
                            ON o.order_id = f.order_id
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM eligible_returns r
                            WHERE r.order_id = f.order_id
                        )

                        UNION ALL

                        -- Rule 4: Return exists, but there is
                        -- no successful refund in its currency.
                        SELECT
                            'MISSING_REFUND_FOR_RETURN',
                            'return',
                            rt.order_id || ':' || rt.currency,
                            'high',
                            rt.expected_total,
                            NULL::numeric,
                            NULL::numeric,
                            jsonb_build_object(
                                'order_id', rt.order_id,
                                'return_ids', rt.return_ids,
                                'currency', rt.currency,
                                'reason',
                                    'No successful refund exists for this return amount currency'
                            )::text
                        FROM return_totals rt
                        JOIN orders o
                            ON o.order_id = rt.order_id
                        LEFT JOIN refund_totals ft
                            ON ft.order_id = rt.order_id
                           AND ft.currency = rt.currency
                        WHERE ft.order_id IS NULL

                        UNION ALL

                        -- Rule 5: Expected return refund differs
                        -- from total successful refunds.
                        SELECT
                            'REFUND_AMOUNT_MISMATCH',
                            'order',
                            rt.order_id || ':' || rt.currency,
                            'high',
                            rt.expected_total,
                            ft.actual_total,
                            ft.actual_total - rt.expected_total,
                            jsonb_build_object(
                                'order_id', rt.order_id,
                                'return_ids', rt.return_ids,
                                'refund_ids', ft.refund_ids,
                                'currency', rt.currency,
                                'reason',
                                    'Successful refund total differs from expected return refund'
                            )::text
                        FROM return_totals rt
                        JOIN orders o
                            ON o.order_id = rt.order_id
                        JOIN refund_totals ft
                            ON ft.order_id = rt.order_id
                           AND ft.currency = rt.currency
                        WHERE ft.actual_total <> rt.expected_total

                        UNION ALL

                        -- Rule 6: Refund currency differs from the
                        -- eligible return currency for that order.
                        SELECT
                            'REFUND_CURRENCY_MISMATCH',
                            'refund',
                            f.refund_id::text,
                            'high',
                            NULL::numeric,
                            f.refund_amount,
                            NULL::numeric,
                            jsonb_build_object(
                                'order_id', f.order_id,
                                'refund_currency', f.currency,
                                'return_currencies', (
                                    SELECT ARRAY_AGG(DISTINCT r.currency)
                                    FROM eligible_returns r
                                    WHERE r.order_id = f.order_id
                                ),
                                'reason',
                                    'Refund currency has no matching return currency'
                            )::text
                        FROM successful_refunds f
                        WHERE EXISTS (
                            SELECT 1
                            FROM eligible_returns r
                            WHERE r.order_id = f.order_id
                              AND r.currency <> f.currency
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM eligible_returns r
                            WHERE r.order_id = f.order_id
                              AND r.currency = f.currency
                        )
                    )

                    SELECT
                        exception_type,
                        entity_type,
                        entity_id,
                        severity,
                        expected_amount,
                        actual_amount,
                        difference_amount,
                        details
                    FROM findings
                    ORDER BY exception_type, entity_id;
                    """

                    cursor.execute(findings_sql)
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
                            expected_amount = EXCLUDED.expected_amount,
                            actual_amount = EXCLUDED.actual_amount,
                            difference_amount = EXCLUDED.difference_amount,
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
            print(f"Total exceptions: {len(findings)}")
            print(f"Exceptions by type: {counts_by_type}")

        finally:
            connection.close()

    reconcile_returns_and_refunds()


return_refund_reconciliation()


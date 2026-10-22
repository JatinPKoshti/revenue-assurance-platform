import json
import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "revenue_assurance_order_payment_reconciliation"
CONNECTION_ID = "revenue_postgres"


@dag(
    dag_id=DAG_ID,
    description="Reconcile e-commerce orders against captured payments",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "reconciliation"],
)
def order_payment_reconciliation():

    @task
    def reconcile_orders_and_payments():
        context = get_current_context()
        dag_run = context["dag_run"]

        dag_run_id = dag_run.run_id
        logical_date = context.get("logical_date")

        if logical_date:
            business_date = logical_date.astimezone(
                pendulum.timezone("Asia/Kolkata")
            ).date()
        else:
            business_date = pendulum.now("Asia/Kolkata").date()

        hook = PostgresHook(
            postgres_conn_id=CONNECTION_ID
        )

        connection = hook.get_conn()

        try:
            # Keep run tracking and exception writes in one transaction.
            with connection:
                with connection.cursor() as cursor:

                    # Create a run record, or reuse it on task retry.
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
                        (DAG_ID, dag_run_id, business_date),
                    )

                    run_id = cursor.fetchone()[0]

                    # Rebuild findings for this exact run so that
                    # reruns do not retain outdated exceptions.
                    cursor.execute(
                        """
                        DELETE FROM reconciliation_exceptions
                        WHERE run_id = %s;
                        """,
                        (run_id,),
                    )

                    # Treat captured/successful payments as money
                    # collected for reconciliation purposes.
                    findings_sql = """
                    WITH captured_totals AS (
                        SELECT
                            order_id,
                            UPPER(currency) AS currency,
                            SUM(payment_amount)::numeric(14,2)
                                AS captured_total,
                            ARRAY_AGG(
                                payment_id ORDER BY payment_id
                            ) AS payment_ids
                        FROM payments
                        WHERE LOWER(payment_status) = 'captured'
                        GROUP BY order_id, UPPER(currency)
                    )

                    -- Rule 1: Captured payment without an order.
                    SELECT
                        'PAYMENT_WITHOUT_ORDER',
                        'payment',
                        p.payment_id,
                        'high',
                        NULL::numeric,
                        p.payment_amount,
                        p.payment_amount,
                        jsonb_build_object(
                            'order_id', p.order_id,
                            'provider', p.provider,
                            'reason', 'No matching order exists'
                        )::text
                    FROM payments p
                    LEFT JOIN orders o
                        ON o.order_id = p.order_id
                    WHERE o.order_id IS NULL
                      AND LOWER(p.payment_status) = 'captured'

                    UNION ALL

                    -- Rule 2: Payment currency differs from the order.
                    SELECT
                        'PAYMENT_CURRENCY_MISMATCH',
                        'payment',
                        p.payment_id,
                        'high',
                        NULL::numeric,
                        p.payment_amount,
                        NULL::numeric,
                        jsonb_build_object(
                            'order_id', p.order_id,
                            'order_currency', o.currency,
                            'payment_currency', p.currency
                        )::text
                    FROM payments p
                    JOIN orders o
                        ON o.order_id = p.order_id
                    WHERE LOWER(p.payment_status) = 'captured'
                      AND UPPER(p.currency) <> UPPER(o.currency)

                    UNION ALL

                    -- Rule 3: Captured total differs from the order total.
                    SELECT
                        'PAYMENT_AMOUNT_MISMATCH',
                        'order',
                        o.order_id,
                        'high',
                        o.total_amount,
                        c.captured_total,
                        c.captured_total - o.total_amount,
                        jsonb_build_object(
                            'payment_ids', c.payment_ids,
                            'currency', o.currency,
                            'reason',
                                'Captured total differs from order total'
                        )::text
                    FROM orders o
                    JOIN captured_totals c
                        ON c.order_id = o.order_id
                       AND c.currency = UPPER(o.currency)
                    WHERE c.captured_total <> o.total_amount

                    UNION ALL

                    -- Rule 4: Captured payment conflicts with order status.
                    SELECT
                        'CAPTURED_PAYMENT_ORDER_STATUS_MISMATCH',
                        'payment',
                        p.payment_id,
                        'medium',
                        NULL::numeric,
                        p.payment_amount,
                        NULL::numeric,
                        jsonb_build_object(
                            'order_id', o.order_id,
                            'order_status', o.order_status,
                            'payment_status', p.payment_status,
                            'reason',
                                'Payment is captured but order is not in an active paid status'
                        )::text
                    FROM payments p
                    JOIN orders o
                        ON o.order_id = p.order_id
                    WHERE LOWER(p.payment_status) = 'captured'
                      AND LOWER(o.order_status) NOT IN (
                          'paid',
                          'completed',
                          'fulfilled',
                          'shipped',
                          'delivered'
                      )

                    UNION ALL

                    -- Rule 5: Paid/fulfilled order has no captured payment
                    -- in the order currency.
                    SELECT
                        'MISSING_CAPTURED_PAYMENT',
                        'order',
                        o.order_id,
                        'high',
                        o.total_amount,
                        NULL::numeric,
                        NULL::numeric,
                        jsonb_build_object(
                            'order_status', o.order_status,
                            'currency', o.currency,
                            'reason',
                                'No captured payment exists in the order currency'
                        )::text
                    FROM orders o
                    LEFT JOIN captured_totals c
                        ON c.order_id = o.order_id
                       AND c.currency = UPPER(o.currency)
                    WHERE LOWER(o.order_status) IN (
                        'paid',
                        'completed',
                        'fulfilled',
                        'shipped',
                        'delivered'
                    )
                      AND c.order_id IS NULL

                    ORDER BY 1, 2, 3;
                    """

                    cursor.execute(findings_sql)
                    findings = cursor.fetchall()

                    counts_by_type = {}

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

                    for finding in findings:
                        (
                            exception_type,
                            entity_type,
                            entity_id,
                            severity,
                            expected_amount,
                            actual_amount,
                            difference_amount,
                            details_json,
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
                                details_json,
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

                    # A succeeded run means the checks ran successfully.
                    # It does not mean that no business discrepancies exist.
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
            print(f"Total exceptions found: {len(findings)}")
            print(f"Exceptions by type: {counts_by_type}")

        finally:
            connection.close()

    reconcile_orders_and_payments()


order_payment_reconciliation()

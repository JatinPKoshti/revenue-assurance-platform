
import json
import pendulum

from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "revenue_assurance_settlement_reconciliation"
CONNECTION_ID = "revenue_postgres"


@dag(
    dag_id=DAG_ID,
    description="Reconcile provider settlements against captured payments",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["revenue-assurance", "settlements", "reconciliation"],
)
def settlement_reconciliation():

    @task
    def reconcile_settlements():
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

                    # Create or reuse this Airflow run's audit record.
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

                    # Remove this run's previous findings on retry.
                    cursor.execute(
                        """
                        DELETE FROM reconciliation_exceptions
                        WHERE run_id = %s;
                        """,
                        (run_id,),
                    )

                    findings_sql = """
                    WITH matched_lines AS (
                        SELECT
                            s.*,
                            p.payment_id AS matched_payment_id,
                            p.payment_status,
                            p.payment_amount,
                            p.currency AS payment_currency
                        FROM settlements s
                        JOIN payments p
                          ON (
                              s.payment_id = p.payment_id
                              OR (
                                  s.payment_id IS NULL
                                  AND s.provider = p.provider
                                  AND s.provider_transaction_id =
                                      p.provider_transaction_id
                                  AND s.provider_transaction_id IS NOT NULL
                              )
                          )
                    ),

                    findings AS (

                        -- Rule 1: Settlement has no matching payment.
                        SELECT
                            'SETTLEMENT_WITHOUT_PAYMENT'::text
                                AS exception_type,
                            'settlement'::text AS entity_type,
                            s.settlement_line_id::text AS entity_id,
                            'high'::text AS severity,
                            NULL::numeric AS expected_amount,
                            s.gross_amount AS actual_amount,
                            NULL::numeric AS difference_amount,
                            jsonb_build_object(
                                'payment_id', s.payment_id,
                                'provider', s.provider,
                                'provider_transaction_id',
                                    s.provider_transaction_id,
                                'payout_id', s.payout_id,
                                'reason',
                                    'No matching payment was found'
                            )::text AS details
                        FROM settlements s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM payments p
                            WHERE
                                s.payment_id = p.payment_id
                                OR (
                                    s.payment_id IS NULL
                                    AND s.provider = p.provider
                                    AND s.provider_transaction_id =
                                        p.provider_transaction_id
                                    AND s.provider_transaction_id IS NOT NULL
                                )
                        )

                        UNION ALL

                        -- Rule 2: Settlement uses a different currency.
                        SELECT
                            'SETTLEMENT_CURRENCY_MISMATCH',
                            'settlement',
                            m.settlement_line_id::text,
                            'high',
                            NULL::numeric,
                            NULL::numeric,
                            NULL::numeric,
                            jsonb_build_object(
                                'payment_id', m.matched_payment_id,
                                'settlement_currency', m.currency,
                                'payment_currency', m.payment_currency
                            )::text
                        FROM matched_lines m
                        WHERE UPPER(m.currency) <>
                              UPPER(m.payment_currency)

                        UNION ALL

                        -- Rule 3: Settlement exists for a payment
                        -- that is not marked captured.
                        SELECT
                            'SETTLEMENT_FOR_UNCAPTURED_PAYMENT',
                            'payment',
                            m.matched_payment_id::text,
                            'high',
                            NULL::numeric,
                            m.gross_amount,
                            NULL::numeric,
                            jsonb_build_object(
                                'settlement_line_id',
                                    m.settlement_line_id,
                                'payment_status', m.payment_status,
                                'reason',
                                    'Settlement references a non-captured payment'
                            )::text
                        FROM matched_lines m
                        WHERE LOWER(m.payment_status) <> 'captured'

                        UNION ALL

                        -- Rule 4: Net amount does not equal gross minus fees.
                        SELECT
                            'SETTLEMENT_FEE_MATH_INVALID',
                            'settlement',
                            s.settlement_line_id::text,
                            'high',
                            s.gross_amount,
                            s.fee_amount + s.net_amount,
                            (s.fee_amount + s.net_amount)
                                - s.gross_amount,
                            jsonb_build_object(
                                'gross_amount', s.gross_amount,
                                'fee_amount', s.fee_amount,
                                'net_amount', s.net_amount,
                                'reason',
                                    'Gross must equal fee plus net'
                            )::text
                        FROM settlements s
                        WHERE s.gross_amount <>
                              s.fee_amount + s.net_amount

                        UNION ALL

                        -- Rule 5: Total settlement gross differs from
                        -- the captured payment amount. Aggregate first
                        -- so split settlement lines are supported.
                        SELECT
                            'SETTLEMENT_GROSS_MISMATCH',
                            'payment',
                            (
                                m.matched_payment_id
                                || ':' || UPPER(m.currency)
                            )::text,
                            'high',
                            MAX(m.payment_amount),
                            SUM(m.gross_amount),
                            SUM(m.gross_amount)
                                - MAX(m.payment_amount),
                            jsonb_build_object(
                                'payment_id', m.matched_payment_id,
                                'settlement_currency', UPPER(m.currency),
                                'settlement_lines',
                                    ARRAY_AGG(m.settlement_line_id),
                                'reason',
                                    'Total settlement gross differs from payment amount'
                            )::text
                        FROM matched_lines m
                        WHERE LOWER(m.payment_status) = 'captured'
                          AND UPPER(m.currency) =
                              UPPER(m.payment_currency)
                        GROUP BY
                            m.matched_payment_id,
                            UPPER(m.currency)
                        HAVING SUM(m.gross_amount) <>
                               MAX(m.payment_amount)

                        UNION ALL

                        -- Rule 6: Captured payment has no settlement.
                        SELECT
                            'PAYMENT_WITHOUT_SETTLEMENT',
                            'payment',
                            p.payment_id::text,
                            'high',
                            p.payment_amount,
                            NULL::numeric,
                            NULL::numeric,
                            jsonb_build_object(
                                'provider', p.provider,
                                'provider_transaction_id',
                                    p.provider_transaction_id,
                                'currency', p.currency,
                                'reason',
                                    'No settlement references this payment'
                            )::text
                        FROM payments p
                        WHERE LOWER(p.payment_status) = 'captured'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM settlements s
                              WHERE
                                  s.payment_id = p.payment_id
                                  OR (
                                      s.payment_id IS NULL
                                      AND s.provider = p.provider
                                      AND s.provider_transaction_id =
                                          p.provider_transaction_id
                                      AND s.provider_transaction_id IS NOT NULL
                                  )
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

    reconcile_settlements()


settlement_reconciliation()

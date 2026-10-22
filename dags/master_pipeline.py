"""Master orchestration DAG for the Revenue Assurance project."""

import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

BUSINESS_DATE_TEMPLATE = "{{ (dag_run.conf or {}).get('business_date', '2026-10-15') }}"

CHILD_DAGS = [
    ("load_orders", "revenue_assurance_demo_orders"),
    ("load_payments", "revenue_assurance_demo_payments"),
    ("reconcile_orders_payments", "revenue_assurance_order_payment_reconciliation"),
    ("load_settlements", "revenue_assurance_settlement_ingestion"),
    ("reconcile_settlements", "revenue_assurance_settlement_reconciliation"),
    ("load_shipments", "revenue_assurance_shipments_ingestion"),
    ("check_shipment_sla", "revenue_assurance_shipment_sla"),
    ("load_returns_refunds", "revenue_assurance_returns_refunds_ingestion"),
    ("reconcile_returns_refunds", "revenue_assurance_return_refund_reconciliation"),
    ("audit_source_completeness", "revenue_assurance_source_completeness_audit"),
    ("audit_pipeline_health", "revenue_assurance_pipeline_health_audit"),
]


@dag(
    dag_id="revenue_assurance_master_pipeline",
    description="Runs ingestion, reconciliation and audit DAGs for one business date.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["revenue-assurance", "master", "business-date"],
)
def revenue_assurance_master_pipeline():
    previous_task = None
    for task_id, child_dag_id in CHILD_DAGS:
        trigger_child = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id=child_dag_id,
            conf={"business_date": BUSINESS_DATE_TEMPLATE},
            wait_for_completion=True,
            poke_interval=10,
            allowed_states=["success"],
            failed_states=["failed"],
            fail_when_dag_is_paused=True,
            deferrable=False,
        )
        if previous_task is not None:
            previous_task >> trigger_child
        previous_task = trigger_child


revenue_assurance_master_pipeline()

"""Shared business-date helpers for the Revenue Assurance Airflow project.

Call get_business_date() only from inside a running Airflow task, because it
reads the current DAG-run context. The fallback date preserves the project's
current synthetic batch until the user explicitly chooses another date.
"""

from datetime import date
from typing import Any

from airflow.sdk import get_current_context

DEFAULT_BUSINESS_DATE = "2026-10-15"
SUPPORTED_SOURCES = {
    "orders",
    "payments",
    "settlements",
    "shipments",
    "returns",
    "refunds",
}


def validate_business_date(value: Any) -> str:
    """Return value as YYYY-MM-DD, raising a clear error for invalid input."""
    if value is None:
        raise ValueError(
            "business_date is missing. Use YYYY-MM-DD, e.g. 2026-10-15."
        )

    value_text = str(value).strip()
    if not value_text:
        raise ValueError(
            "business_date is empty. Use YYYY-MM-DD, e.g. 2026-10-15."
        )

    try:
        parsed = date.fromisoformat(value_text)
    except ValueError as exc:
        raise ValueError(
            f"Invalid business_date {value_text!r}. "
            "Expected YYYY-MM-DD, e.g. 2026-10-15."
        ) from exc

    # Enforce the expected ISO representation, not other date formats.
    if parsed.isoformat() != value_text:
        raise ValueError(
            f"Invalid business_date {value_text!r}. "
            "Expected exactly YYYY-MM-DD."
        )

    return parsed.isoformat()


def get_business_date(default: str = DEFAULT_BUSINESS_DATE) -> str:
    """Read business_date from the active DAG run's conf, with a safe default.

    Use this function inside a task body, not while the DAG file is parsed.
    """
    context = get_current_context()
    dag_run = context.get("dag_run")
    run_conf = getattr(dag_run, "conf", None) or {}

    raw_value = run_conf.get("business_date", default)
    return validate_business_date(raw_value)


def get_raw_s3_key(source: str, business_date: str | None = None) -> str:
    """Build the project's dated raw CSV key for a supported source.

    Examples:
      orders -> raw/orders/orders_2026-10-15.csv
      refunds -> raw/refunds/refunds_2026-10-15.csv

    If business_date is omitted, the active Airflow DAG-run conf is used.
    """
    source_name = str(source).strip().lower()
    if source_name not in SUPPORTED_SOURCES:
        raise ValueError(
            f"Unsupported source {source!r}. Supported: "
            f"{', '.join(sorted(SUPPORTED_SOURCES))}."
        )

    selected_date = (
        validate_business_date(business_date)
        if business_date is not None
        else get_business_date()
    )
    return f"raw/{source_name}/{source_name}_{selected_date}.csv"

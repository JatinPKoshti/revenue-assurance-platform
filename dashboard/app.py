import os
from contextlib import contextmanager
from datetime import date

import pandas as pd
import psycopg2
from psycopg2 import sql
import streamlit as st


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Revenue Assurance Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# DATABASE CONFIGURATION
# These variables will come from Docker Compose.
# ============================================================

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "revenue_assurance")
DB_USER = os.getenv("DB_USER", "revenue_app")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")


EXPECTED_SOURCES = [
    ("S3-DemoOrders", "Orders", 5),
    ("S3-DemoPayments", "Payments", 5),
    ("S3-DemoPay", "Settlements", 5),
    ("S3-DemoShipments", "Shipments", 5),
    ("S3-DemoReturns", "Returns", 3),
    ("S3-DemoRefunds", "Refunds", 3),
]


TARGET_TABLES = {
    "Orders": ("orders", "S3-DemoOrders"),
    "Payments": ("payments", "S3-DemoPayments"),
    "Settlements": ("settlements", "S3-DemoPay"),
    "Shipments": ("shipments", "S3-DemoShipments"),
    "Returns": ("returns", "S3-DemoReturns"),
    "Refunds": ("refunds", "S3-DemoRefunds"),
}


AUDIT_TABLES = [
    "source_completeness_audits",
    "pipeline_health_audits",
]


# ============================================================
# DATABASE CONNECTION
# ============================================================

@contextmanager
def db_connection():
    """Open a database connection and always close it."""

    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
        connect_timeout=8,
    )

    try:
        yield connection
    finally:
        connection.close()


def run_query(query, params=None):
    """Run a read-only SELECT-style query and return a DataFrame."""

    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())

            if cursor.description is None:
                return pd.DataFrame()

            columns = [
                column[0]
                for column in cursor.description
            ]

            rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=columns)


# ============================================================
# DATABASE METADATA
# ============================================================

@st.cache_data(ttl=30, show_spinner=False)
def get_public_tables():
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY table_name;
    """

    df = run_query(query)

    if df.empty:
        return []

    return df["table_name"].tolist()


@st.cache_data(ttl=30, show_spinner=False)
def get_table_columns(table_name):
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        ORDER BY ordinal_position;
    """

    df = run_query(query, (table_name,))

    if df.empty:
        return []

    return df["column_name"].tolist()


# ============================================================
# INGESTION BATCH DATA
# ============================================================

@st.cache_data(ttl=30, show_spinner=False)
def get_ingestion_batches(start_date, end_date):
    query = """
        SELECT
            batch_id,
            source_system,
            file_name,
            business_date,
            status,
            row_count,
            loaded_count,
            error_message,
            completed_at
        FROM public.ingestion_batches
        WHERE business_date BETWEEN %s AND %s
        ORDER BY business_date DESC, source_system, batch_id;
    """

    return run_query(
        query,
        (start_date, end_date),
    )


def build_source_completeness(batches):
    """Compare source batches with the expected source checklist."""

    result = []

    for source_system, label, expected_rows in EXPECTED_SOURCES:

        source_batches = batches[
            batches["source_system"] == source_system
        ]

        if source_batches.empty:
            status = "MISSING"
            batch_count = 0
            source_rows = 0
            loaded_rows = 0
            failed_batches = 0

        else:
            batch_count = len(source_batches)

            source_rows = int(
                source_batches["row_count"]
                .fillna(0)
                .sum()
            )

            loaded_rows = int(
                source_batches["loaded_count"]
                .fillna(0)
                .sum()
            )

            failed_batches = int(
                (
                    source_batches["status"]
                    .astype(str)
                    .str.lower() == "failed"
                ).sum()
            )

            all_loaded = (
                source_batches["status"]
                .astype(str)
                .str.lower()
                .eq("loaded")
                .all()
            )

            if failed_batches:
                status = "FAILED"
            elif all_loaded:
                status = "LOADED"
            else:
                status = "IN PROGRESS"

        result.append(
            {
                "Source": label,
                "Source system": source_system,
                "Expected sample rows": expected_rows,
                "Batches": batch_count,
                "Source rows": source_rows,
                "Loaded rows": loaded_rows,
                "Failed batches": failed_batches,
                "Status": status,
            }
        )

    return pd.DataFrame(result)


# ============================================================
# TARGET TABLE PREVIEW
# ============================================================

def get_target_preview(
    table_name,
    source_system,
    start_date,
    end_date,
):
    available_tables = get_public_tables()

    if table_name not in available_tables:
        return pd.DataFrame()

    columns = get_table_columns(table_name)

    # Prefer a source-batch join so the date filter refers to
    # the correct ingestion batch.
    if "source_batch_id" in columns:

        query = sql.SQL(
            """
            SELECT t.*
            FROM public.{} AS t
            JOIN public.ingestion_batches AS b
              ON b.batch_id = t.source_batch_id
            WHERE b.business_date BETWEEN %s AND %s
              AND b.source_system = %s
            LIMIT 200;
            """
        ).format(sql.Identifier(table_name))

        with db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        start_date,
                        end_date,
                        source_system,
                    ),
                )

                names = [
                    column[0]
                    for column in cursor.description
                ]

                rows = cursor.fetchall()

        return pd.DataFrame(rows, columns=names)

    # If a source-batch column doesn't exist, do not invent one.
    return pd.DataFrame()


def get_read_only_preview(table_name):
    """Preview a known public table without modifying its data."""

    if table_name not in get_public_tables():
        return pd.DataFrame()

    query = sql.SQL(
        "SELECT * FROM public.{} LIMIT 100;"
    ).format(sql.Identifier(table_name))

    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

            if cursor.description is None:
                return pd.DataFrame()

            names = [
                column[0]
                for column in cursor.description
            ]

            rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=names)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("Revenue Assurance")

st.sidebar.caption(
    "E-Commerce Revenue Assurance & Refund Reconciliation"
)

start_date = st.sidebar.date_input(
    "Start business date",
    value=date(2026, 10, 15),
    key="start_business_date",
)

end_date = st.sidebar.date_input(
    "End business date",
    value=date(2026, 10, 15),
    key="end_business_date",
)

if start_date > end_date:
    st.sidebar.error(
        "Start date must be on or before end date."
    )
    st.stop()

st.sidebar.divider()

st.sidebar.caption("Database connection")
st.sidebar.text(f"Host: {DB_HOST}")
st.sidebar.text(f"Database: {DB_NAME}")

if st.sidebar.button(
    "Refresh dashboard",
    use_container_width=True,
):
    st.cache_data.clear()
    st.rerun()


# ============================================================
# LOAD DASHBOARD DATA
# ============================================================

st.title("E-Commerce Revenue Assurance")
st.caption(
    "Ingestion monitoring, source completeness, "
    "reconciliation data, and pipeline health."
)

st.caption(
    f"Business date range: {start_date} to {end_date}"
)

try:
    batches = get_ingestion_batches(
        start_date,
        end_date,
    )

    available_tables = get_public_tables()

except Exception as exc:
    st.error(
        "Unable to read the project database. "
        "Check the PostgreSQL service, dashboard environment "
        "variables, credentials, and database schema."
    )

    st.code(str(exc))
    st.stop()


if "ingestion_batches" not in available_tables:
    st.error(
        "The public.ingestion_batches table was not found "
        "in the configured database."
    )
    st.stop()


# ============================================================
# KPI CALCULATIONS
# ============================================================

total_batches = len(batches)

loaded_batches = int(
    batches["status"]
    .astype(str)
    .str.lower()
    .eq("loaded")
    .sum()
) if not batches.empty else 0

failed_batches = int(
    batches["status"]
    .astype(str)
    .str.lower()
    .eq("failed")
    .sum()
) if not batches.empty else 0

total_source_rows = int(
    batches["row_count"]
    .fillna(0)
    .sum()
) if not batches.empty else 0

total_loaded_rows = int(
    batches["loaded_count"]
    .fillna(0)
    .sum()
) if not batches.empty else 0


# ============================================================
# TABS
# ============================================================

tab_overview, tab_sources, tab_reconciliation, \
    tab_tables, tab_health = st.tabs(
        [
            "Overview",
            "Source completeness",
            "Reconciliation",
            "Data tables",
            "Pipeline health",
        ]
    )


# ============================================================
# TAB 1 — OVERVIEW
# ============================================================

with tab_overview:

    st.subheader("Pipeline overview")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Ingestion batches",
        f"{total_batches:,}",
    )

    col2.metric(
        "Loaded batches",
        f"{loaded_batches:,}",
    )

    col3.metric(
        "Failed batches",
        f"{failed_batches:,}",
    )

    col4.metric(
        "Rows loaded",
        f"{total_loaded_rows:,}",
    )

    col5, col6 = st.columns(2)

    col5.metric(
        "Source rows",
        f"{total_source_rows:,}",
    )

    load_percentage = (
        total_loaded_rows / total_source_rows * 100
        if total_source_rows
        else 0
    )

    col6.metric(
        "Rows loaded percentage",
        f"{load_percentage:.1f}%",
    )

    st.divider()

    if batches.empty:
        st.info(
            "No ingestion batches were found for this date "
            "range. Run the ingestion DAGs, then refresh."
        )

    else:
        st.subheader("Batches by source")

        source_summary = (
            batches.groupby("source_system", as_index=False)
            .agg(
                batches=("batch_id", "count"),
                source_rows=("row_count", "sum"),
                loaded_rows=("loaded_count", "sum"),
            )
        )

        st.bar_chart(
            source_summary.set_index("source_system")[
                ["source_rows", "loaded_rows"]
            ]
        )

        st.subheader("Recent ingestion batches")

        st.dataframe(
            batches,
            use_container_width=True,
            hide_index=True,
        )

        st.download_button(
            "Download ingestion batches as CSV",
            data=batches.to_csv(index=False).encode("utf-8"),
            file_name="ingestion_batches.csv",
            mime="text/csv",
        )


# ============================================================
# TAB 2 — SOURCE COMPLETENESS
# ============================================================

with tab_sources:

    st.subheader("Expected versus received sources")

    completeness = build_source_completeness(batches)

    st.dataframe(
        completeness,
        use_container_width=True,
        hide_index=True,
    )

    loaded_count = int(
        completeness["Status"].eq("LOADED").sum()
    )

    missing_count = int(
        completeness["Status"].eq("MISSING").sum()
    )

    failed_count = int(
        completeness["Status"].eq("FAILED").sum()
    )

    c1, c2, c3 = st.columns(3)

    c1.metric("Sources loaded", loaded_count)
    c2.metric("Sources missing", missing_count)
    c3.metric("Sources failed", failed_count)

    st.caption(
        "The expected row counts refer to the original sample "
        "data. They are not proof of the actual row counts. "
        "A source is marked loaded when all its batches in "
        "the selected date range are loaded."
    )


# ============================================================
# TAB 3 — RECONCILIATION
# ============================================================

with tab_reconciliation:

    st.subheader("Reconciliation and exception tables")

    reconciliation_tables = [
        name
        for name in available_tables
        if (
            "reconcil" in name.lower()
            or "exception" in name.lower()
        )
    ]

    if not reconciliation_tables:
        st.info(
            "No reconciliation or exception tables were "
            "discovered in the public schema. Complete the "
            "reconciliation DAGs and confirm their target "
            "table names before adding detailed views."
        )

    else:
        selected_recon_table = st.selectbox(
            "Select a reconciliation table",
            reconciliation_tables,
        )

        try:
            recon_df = get_read_only_preview(
                selected_recon_table
            )

            st.write(
                f"Preview: public.{selected_recon_table}"
            )

            st.dataframe(
                recon_df,
                use_container_width=True,
                hide_index=True,
            )

            if not recon_df.empty:
                st.download_button(
                    "Download this preview as CSV",
                    data=recon_df.to_csv(
                        index=False
                    ).encode("utf-8"),
                    file_name=(
                        f"{selected_recon_table}_preview.csv"
                    ),
                    mime="text/csv",
                )

        except Exception as exc:
            st.error(
                f"Could not read {selected_recon_table}."
            )
            st.code(str(exc))


# ============================================================
# TAB 4 — SOURCE DATA TABLES
# ============================================================

with tab_tables:

    st.subheader("Loaded source records")

    selected_source_label = st.selectbox(
        "Select source",
        list(TARGET_TABLES.keys()),
    )

    table_name, source_system = TARGET_TABLES[
        selected_source_label
    ]

    if table_name not in available_tables:
        st.warning(
            f"The public.{table_name} table does not exist "
            "in the configured database."
        )

    elif "source_batch_id" not in get_table_columns(
        table_name
    ):
        st.warning(
            f"public.{table_name} does not expose a "
            "source_batch_id column. A date-filtered preview "
            "cannot be constructed with the current code."
        )

    else:
        try:
            preview = get_target_preview(
                table_name,
                source_system,
                start_date,
                end_date,
            )

            st.caption(
                f"Table: public.{table_name} | "
                f"Source: {source_system}"
            )

            if preview.empty:
                st.info(
                    "No target records are associated with "
                    "this source and date range yet."
                )

            else:
                st.metric(
                    "Rows shown",
                    f"{len(preview):,}",
                )

                st.dataframe(
                    preview,
                    use_container_width=True,
                    hide_index=True,
                )

                st.download_button(
                    "Download preview as CSV",
                    data=preview.to_csv(
                        index=False
                    ).encode("utf-8"),
                    file_name=f"{table_name}_preview.csv",
                    mime="text/csv",
                )

        except Exception as exc:
            st.error(
                f"Could not load public.{table_name}."
            )
            st.code(str(exc))


# ============================================================
# TAB 5 — PIPELINE HEALTH
# ============================================================

with tab_health:

    st.subheader("Pipeline health audits")

    found_audit_tables = [
        name
        for name in AUDIT_TABLES
        if name in available_tables
    ]

    if not found_audit_tables:
        st.info(
            "Neither source_completeness_audits nor "
            "pipeline_health_audits was found in the public "
            "schema. Run the audit DAGs and confirm their "
            "actual target table names."
        )

    else:
        for audit_table in found_audit_tables:

            with st.expander(
                f"public.{audit_table}",
                expanded=True,
            ):
                try:
                    audit_df = get_read_only_preview(
                        audit_table
                    )

                    if audit_df.empty:
                        st.info(
                            "The audit table exists but "
                            "contains no rows."
                        )

                    else:
                        st.dataframe(
                            audit_df,
                            use_container_width=True,
                            hide_index=True,
                        )

                        st.download_button(
                            f"Download {audit_table}",
                            data=audit_df.to_csv(
                                index=False
                            ).encode("utf-8"),
                            file_name=f"{audit_table}.csv",
                            mime="text/csv",
                            key=f"download_{audit_table}",
                        )

                except Exception as exc:
                    st.error(
                        f"Could not read {audit_table}."
                    )
                    st.code(str(exc))


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Revenue Assurance Platform | Streamlit dashboard | "
    "Reads project PostgreSQL tables only; it does not "
    "modify ingestion or reconciliation records."
)
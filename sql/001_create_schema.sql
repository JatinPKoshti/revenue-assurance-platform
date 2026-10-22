
-- 1. Track every source file processed by the pipeline
CREATE TABLE IF NOT EXISTS ingestion_batches (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_system VARCHAR(80) NOT NULL,
    file_name TEXT NOT NULL,
    file_checksum VARCHAR(64) NOT NULL,
    business_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'discovered'
        CHECK (status IN (
            'discovered', 'validated', 'loaded',
            'failed', 'quarantined'
        )),
    row_count INTEGER NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    loaded_count INTEGER NOT NULL DEFAULT 0 CHECK (loaded_count >= 0),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_ingestion_source_checksum
        UNIQUE (source_system, file_checksum)
);

-- 2. Orders
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(100) PRIMARY KEY,
    order_ts TIMESTAMPTZ NOT NULL,
    customer_id VARCHAR(100),
    order_status VARCHAR(30) NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount >= 0),
    currency CHAR(3) NOT NULL DEFAULT 'INR',
    source_batch_id BIGINT REFERENCES ingestion_batches(batch_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Payments
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(120) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    provider VARCHAR(80) NOT NULL,
    provider_transaction_id VARCHAR(150),
    payment_status VARCHAR(30) NOT NULL,
    payment_amount NUMERIC(14,2) NOT NULL
        CHECK (payment_amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'INR',
    payment_ts TIMESTAMPTZ,
    source_batch_id BIGINT REFERENCES ingestion_batches(batch_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Settlement lines
CREATE TABLE IF NOT EXISTS settlements (
    settlement_line_id VARCHAR(150) PRIMARY KEY,
    provider VARCHAR(80) NOT NULL,
    provider_transaction_id VARCHAR(150),
    order_id VARCHAR(100),
    payment_id VARCHAR(120),
    payout_id VARCHAR(150),
    gross_amount NUMERIC(14,2) NOT NULL CHECK (gross_amount >= 0),
    fee_amount NUMERIC(14,2) NOT NULL CHECK (fee_amount >= 0),
    net_amount NUMERIC(14,2) NOT NULL CHECK (net_amount >= 0),
    settlement_date DATE NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'INR',
    source_batch_id BIGINT REFERENCES ingestion_batches(batch_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Shipments
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id VARCHAR(120) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    carrier VARCHAR(100),
    shipment_status VARCHAR(30) NOT NULL,
    shipped_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    promised_delivery_date DATE,
    source_batch_id BIGINT REFERENCES ingestion_batches(batch_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 6. Returns
CREATE TABLE IF NOT EXISTS returns (
    return_id VARCHAR(120) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    return_status VARCHAR(30) NOT NULL,
    return_created_at TIMESTAMPTZ,
    expected_refund_amount NUMERIC(14,2)
        CHECK (expected_refund_amount >= 0),
    currency CHAR(3) NOT NULL DEFAULT 'INR',
    source_batch_id BIGINT REFERENCES ingestion_batches(batch_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 7. Refund transactions
CREATE TABLE IF NOT EXISTS refunds (
    refund_id VARCHAR(120) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    payment_id VARCHAR(120),
    provider VARCHAR(80),
    refund_status VARCHAR(30) NOT NULL,
    refund_amount NUMERIC(14,2) NOT NULL
        CHECK (refund_amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'INR',
    refund_ts TIMESTAMPTZ,
    source_batch_id BIGINT REFERENCES ingestion_batches(batch_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 8. Record each reconciliation execution
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_id VARCHAR(250) NOT NULL,
    dag_run_id VARCHAR(250) NOT NULL,
    business_date DATE NOT NULL,
    run_status VARCHAR(20) NOT NULL DEFAULT 'running'
        CHECK (run_status IN (
            'running', 'succeeded', 'failed', 'partial'
        )),
    run_type VARCHAR(20) NOT NULL DEFAULT 'scheduled'
        CHECK (run_type IN ('scheduled', 'manual', 'backfill')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    result_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT uq_reconciliation_dag_run
        UNIQUE (dag_id, dag_run_id)
);

-- 9. Record actionable business discrepancies
CREATE TABLE IF NOT EXISTS reconciliation_exceptions (
    exception_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id BIGINT NOT NULL
        REFERENCES reconciliation_runs(run_id),
    business_date DATE NOT NULL,
    exception_type VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(150) NOT NULL,
    severity VARCHAR(20) NOT NULL
        CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    exception_status VARCHAR(20) NOT NULL DEFAULT 'open'
        CHECK (exception_status IN (
            'open', 'in_progress', 'resolved', 'false_positive'
        )),
    expected_amount NUMERIC(14,2),
    actual_amount NUMERIC(14,2),
    difference_amount NUMERIC(14,2),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    CONSTRAINT uq_run_exception
        UNIQUE (run_id, exception_type, entity_type, entity_id)
);

-- Helpful indexes for reconciliation queries
CREATE INDEX IF NOT EXISTS idx_payments_order_id
    ON payments(order_id);

CREATE INDEX IF NOT EXISTS idx_settlements_payment_id
    ON settlements(payment_id);

CREATE INDEX IF NOT EXISTS idx_shipments_order_id
    ON shipments(order_id);

CREATE INDEX IF NOT EXISTS idx_returns_order_id
    ON returns(order_id);

CREATE INDEX IF NOT EXISTS idx_refunds_order_id
    ON refunds(order_id);

CREATE INDEX IF NOT EXISTS idx_runs_business_date
    ON reconciliation_runs(business_date);

CREATE INDEX IF NOT EXISTS idx_exceptions_status_date
    ON reconciliation_exceptions(exception_status, business_date);

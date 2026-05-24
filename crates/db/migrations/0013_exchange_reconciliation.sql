CREATE TABLE IF NOT EXISTS exchange_reconciliation_runs (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    environment TEXT NOT NULL,
    status TEXT NOT NULL,
    checked_orders INTEGER NOT NULL DEFAULT 0,
    matched_orders INTEGER NOT NULL DEFAULT 0,
    mismatched_orders INTEGER NOT NULL DEFAULT 0,
    unknown_orders INTEGER NOT NULL DEFAULT 0,
    failed_reason TEXT,
    correlation_id UUID NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS exchange_reconciliation_mismatches (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES exchange_reconciliation_runs(id) ON DELETE CASCADE,
    client_order_id TEXT NOT NULL,
    local_status TEXT,
    exchange_status TEXT,
    mismatch_kind TEXT NOT NULL,
    action TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exchange_reconciliation_runs_environment_started_at
    ON exchange_reconciliation_runs (environment, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_exchange_reconciliation_mismatches_run_id
    ON exchange_reconciliation_mismatches (run_id);

CREATE INDEX IF NOT EXISTS idx_exchange_reconciliation_mismatches_client_order_id
    ON exchange_reconciliation_mismatches (client_order_id);

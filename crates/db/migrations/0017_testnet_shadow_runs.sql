CREATE TABLE IF NOT EXISTS testnet_shadow_runs (
    id UUID PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    decision TEXT NOT NULL,
    signal_id UUID NULL REFERENCES signals(id) ON DELETE SET NULL,
    risk_decision_id UUID NULL REFERENCES risk_decisions(id) ON DELETE SET NULL,
    would_submit_payload JSONB NULL,
    price_source TEXT NULL,
    resolved_price NUMERIC NULL,
    reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_testnet_shadow_runs_strategy_symbol_created_at
    ON testnet_shadow_runs (strategy_id, symbol, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_testnet_shadow_runs_decision_created_at
    ON testnet_shadow_runs (decision, created_at DESC);

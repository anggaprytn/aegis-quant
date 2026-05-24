CREATE TABLE IF NOT EXISTS testnet_shadow_promotions (
    id UUID PRIMARY KEY,
    shadow_run_id UUID NOT NULL REFERENCES testnet_shadow_runs(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    strategy_id TEXT,
    symbol TEXT,
    timeframe TEXT,
    signal_id UUID NULL REFERENCES signals(id) ON DELETE SET NULL,
    risk_decision_id UUID NULL REFERENCES risk_decisions(id) ON DELETE SET NULL,
    would_submit_payload JSONB NOT NULL,
    resolved_price NUMERIC NULL,
    price_source TEXT NULL,
    rejection_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    testnet_order_id UUID NULL REFERENCES exchange_testnet_orders(id) ON DELETE SET NULL,
    client_order_id TEXT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_by UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    submitted_by UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    submitted_at TIMESTAMPTZ NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_testnet_shadow_promotions_created_at
    ON testnet_shadow_promotions (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_testnet_shadow_promotions_shadow_run_id_created_at
    ON testnet_shadow_promotions (shadow_run_id, created_at DESC, id DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_testnet_shadow_promotions_active_shadow_run
    ON testnet_shadow_promotions (shadow_run_id)
    WHERE status IN ('PREVIEWED', 'SUBMITTED');

CREATE UNIQUE INDEX IF NOT EXISTS uq_testnet_shadow_promotions_submitted_order
    ON testnet_shadow_promotions (testnet_order_id)
    WHERE testnet_order_id IS NOT NULL;

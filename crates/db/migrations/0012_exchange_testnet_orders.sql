CREATE TABLE IF NOT EXISTS exchange_testnet_orders (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    environment TEXT NOT NULL,
    client_order_id TEXT NOT NULL UNIQUE,
    exchange_order_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    time_in_force TEXT,
    requested_qty NUMERIC,
    requested_notional NUMERIC,
    limit_price NUMERIC,
    status TEXT NOT NULL,
    ack_payload JSONB,
    latest_status_payload JSONB,
    risk_decision_id UUID REFERENCES risk_decisions(id) ON DELETE SET NULL,
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exchange_testnet_orders_created_at
    ON exchange_testnet_orders (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_exchange_testnet_orders_status
    ON exchange_testnet_orders (status);

CREATE INDEX IF NOT EXISTS idx_exchange_testnet_orders_symbol
    ON exchange_testnet_orders (symbol);

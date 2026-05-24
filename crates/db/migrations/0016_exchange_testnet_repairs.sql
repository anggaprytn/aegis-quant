CREATE TABLE IF NOT EXISTS exchange_testnet_repair_actions (
    id UUID PRIMARY KEY,
    client_order_id TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    previous_state TEXT NULL,
    next_state TEXT NULL,
    reason TEXT NULL,
    payload JSONB NULL,
    actor_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_exchange_testnet_repair_actions_client_order_id_created_at
    ON exchange_testnet_repair_actions (client_order_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_exchange_testnet_repair_actions_correlation_id
    ON exchange_testnet_repair_actions (correlation_id);

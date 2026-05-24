ALTER TABLE exchange_testnet_orders
    ADD COLUMN IF NOT EXISTS execution_state TEXT,
    ADD COLUMN IF NOT EXISTS last_transition_at TIMESTAMPTZ;

UPDATE exchange_testnet_orders
SET execution_state = COALESCE(
    execution_state,
    CASE
        WHEN status = 'SUBMIT_REQUESTED' THEN 'ORDER_SUBMIT_REQUESTED'
        WHEN status = 'NEW' THEN 'NEW'
        WHEN status = 'PARTIALLY_FILLED' THEN 'PARTIALLY_FILLED'
        WHEN status = 'FILLED' THEN 'FILLED'
        WHEN status IN ('CANCELED', 'CANCELLED') THEN 'CANCELLED'
        WHEN status = 'REJECTED' THEN 'REJECTED'
        WHEN status IN ('EXPIRED', 'EXPIRED_IN_MATCH') THEN 'EXPIRED'
        WHEN status = 'PENDING_CANCEL' THEN 'CANCEL_REQUESTED'
        ELSE 'FAILED'
    END
),
last_transition_at = COALESCE(last_transition_at, updated_at, created_at);

ALTER TABLE exchange_testnet_orders
    ALTER COLUMN execution_state SET NOT NULL;

CREATE TABLE IF NOT EXISTS exchange_testnet_order_lifecycle_events (
    id UUID PRIMARY KEY,
    order_id UUID REFERENCES exchange_testnet_orders(id) ON DELETE SET NULL,
    client_order_id TEXT NOT NULL,
    previous_state TEXT,
    next_state TEXT NOT NULL,
    transition_source TEXT NOT NULL,
    reason TEXT,
    payload JSONB,
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID
);

CREATE INDEX IF NOT EXISTS idx_exchange_testnet_order_lifecycle_events_client_order_id_created_at
    ON exchange_testnet_order_lifecycle_events (client_order_id, created_at);

CREATE INDEX IF NOT EXISTS idx_exchange_testnet_order_lifecycle_events_order_id_created_at
    ON exchange_testnet_order_lifecycle_events (order_id, created_at);

CREATE INDEX IF NOT EXISTS idx_exchange_testnet_orders_execution_state
    ON exchange_testnet_orders (execution_state);

CREATE TABLE IF NOT EXISTS exchange_private_stream_events (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    environment TEXT NOT NULL,
    event_type TEXT NOT NULL,
    symbol TEXT,
    client_order_id TEXT,
    exchange_order_id TEXT,
    execution_type TEXT,
    order_status TEXT,
    payload JSONB NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID
);

CREATE INDEX IF NOT EXISTS idx_exchange_private_stream_events_received_at
    ON exchange_private_stream_events (received_at DESC);

CREATE INDEX IF NOT EXISTS idx_exchange_private_stream_events_client_order_id
    ON exchange_private_stream_events (client_order_id);

CREATE INDEX IF NOT EXISTS idx_exchange_private_stream_events_event_type
    ON exchange_private_stream_events (event_type);

CREATE TABLE IF NOT EXISTS exchange_private_stream_state (
    exchange TEXT NOT NULL,
    environment TEXT NOT NULL,
    status TEXT NOT NULL,
    listen_key_hash TEXT,
    connected_at TIMESTAMPTZ,
    last_event_at TIMESTAMPTZ,
    last_error TEXT,
    reconnect_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, environment)
);

CREATE INDEX IF NOT EXISTS idx_exchange_private_stream_state_updated_at
    ON exchange_private_stream_state (updated_at DESC);

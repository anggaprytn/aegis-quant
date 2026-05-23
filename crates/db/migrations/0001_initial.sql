CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS symbols (
    id UUID PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    market_mode TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY,
    correlation_id UUID NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    strength NUMERIC(20, 8) NOT NULL,
    strategy_name TEXT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS risk_decisions (
    id UUID PRIMARY KEY,
    correlation_id UUID NOT NULL,
    signal_id UUID,
    decision TEXT NOT NULL,
    rationale TEXT NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    correlation_id UUID NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity NUMERIC(20, 8) NOT NULL,
    limit_price NUMERIC(20, 8),
    market_mode TEXT NOT NULL,
    status TEXT NOT NULL,
    execution_state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_events (
    id UUID PRIMARY KEY,
    correlation_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY,
    correlation_id UUID NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signals_correlation_id ON signals (correlation_id);
CREATE INDEX IF NOT EXISTS idx_risk_decisions_correlation_id ON risk_decisions (correlation_id);
CREATE INDEX IF NOT EXISTS idx_orders_correlation_id ON orders (correlation_id);
CREATE INDEX IF NOT EXISTS idx_system_events_correlation_id ON system_events (correlation_id);
CREATE INDEX IF NOT EXISTS idx_system_events_created_at_desc ON system_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_correlation_id ON audit_logs (correlation_id);

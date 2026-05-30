CREATE TABLE IF NOT EXISTS derivatives_funding_rates (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    funding_time TIMESTAMPTZ NOT NULL,
    funding_rate NUMERIC(30, 12) NOT NULL,
    mark_price NUMERIC(30, 12) NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_derivatives_funding_rates_exchange_symbol_time
        UNIQUE (exchange, symbol, funding_time)
);

CREATE INDEX IF NOT EXISTS idx_derivatives_funding_rates_symbol_time
    ON derivatives_funding_rates (exchange, symbol, funding_time DESC);

CREATE TABLE IF NOT EXISTS derivatives_open_interest_snapshots (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    period TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_interest NUMERIC(30, 12) NOT NULL,
    open_interest_value NUMERIC(30, 12) NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_derivatives_open_interest_exchange_symbol_period_time
        UNIQUE (exchange, symbol, period, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_derivatives_open_interest_symbol_time
    ON derivatives_open_interest_snapshots (exchange, symbol, period, timestamp DESC);

CREATE TABLE IF NOT EXISTS derivatives_positioning_snapshots (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    metric TEXT NOT NULL,
    period TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    long_short_ratio NUMERIC(30, 12) NULL,
    long_account NUMERIC(30, 12) NULL,
    short_account NUMERIC(30, 12) NULL,
    buy_sell_ratio NUMERIC(30, 12) NULL,
    buy_vol NUMERIC(30, 12) NULL,
    sell_vol NUMERIC(30, 12) NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_derivatives_positioning_exchange_symbol_metric_period_time
        UNIQUE (exchange, symbol, metric, period, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_derivatives_positioning_symbol_time
    ON derivatives_positioning_snapshots (exchange, symbol, metric, period, timestamp DESC);

CREATE TABLE IF NOT EXISTS market_ticks (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    quantity NUMERIC(20, 8) NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    raw_payload JSONB
);

CREATE TABLE IF NOT EXISTS candles (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    open NUMERIC(20, 8) NOT NULL,
    high NUMERIC(20, 8) NOT NULL,
    low NUMERIC(20, 8) NOT NULL,
    close NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    quote_volume NUMERIC(20, 8),
    trade_count INTEGER NOT NULL,
    is_closed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_candles_exchange_symbol_interval_open_time
        UNIQUE (exchange, symbol, interval, open_time)
);

CREATE TABLE IF NOT EXISTS market_feed_status (
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    status TEXT NOT NULL,
    freshness_status TEXT NOT NULL DEFAULT 'unknown',
    last_event_at TIMESTAMPTZ,
    last_error TEXT,
    reconnect_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange, symbol)
);

CREATE INDEX IF NOT EXISTS idx_market_ticks_exchange_symbol_trade_time
    ON market_ticks (exchange, symbol, trade_time DESC);

CREATE INDEX IF NOT EXISTS idx_candles_exchange_symbol_interval_open_time
    ON candles (exchange, symbol, interval, open_time DESC);

CREATE INDEX IF NOT EXISTS idx_market_feed_status_exchange_symbol
    ON market_feed_status (exchange, symbol);

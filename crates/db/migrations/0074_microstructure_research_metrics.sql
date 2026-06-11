CREATE TABLE IF NOT EXISTS microstructure_collector_runs (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbols TEXT[] NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    stopped_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL,
    config_json JSONB NOT NULL,
    error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_microstructure_collector_runs_started_at
    ON microstructure_collector_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS microstructure_spread_metrics (
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_seconds INTEGER NOT NULL,
    best_bid_price NUMERIC(30, 12) NOT NULL,
    best_ask_price NUMERIC(30, 12) NOT NULL,
    mid_price NUMERIC(30, 12) NOT NULL,
    spread_abs NUMERIC(30, 12) NOT NULL,
    spread_bps NUMERIC(30, 12) NOT NULL,
    spread_avg_bps NUMERIC(30, 12) NOT NULL,
    spread_high_bps NUMERIC(30, 12) NOT NULL,
    spread_low_bps NUMERIC(30, 12) NOT NULL,
    update_count INTEGER NOT NULL,
    locked_count INTEGER NOT NULL,
    crossed_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_microstructure_spread_metrics_bucket
        UNIQUE (exchange, market_type, symbol, bucket_start, bucket_seconds)
);

CREATE INDEX IF NOT EXISTS idx_microstructure_spread_metrics_symbol_bucket
    ON microstructure_spread_metrics (symbol, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_microstructure_spread_metrics_bucket
    ON microstructure_spread_metrics (bucket_start DESC);

CREATE TABLE IF NOT EXISTS microstructure_imbalance_metrics (
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_seconds INTEGER NOT NULL,
    depth_levels INTEGER NOT NULL,
    bid_qty NUMERIC(30, 12) NOT NULL,
    ask_qty NUMERIC(30, 12) NOT NULL,
    bid_notional NUMERIC(30, 12) NOT NULL,
    ask_notional NUMERIC(30, 12) NOT NULL,
    qty_imbalance NUMERIC(30, 12) NOT NULL,
    notional_imbalance NUMERIC(30, 12) NOT NULL,
    depth_skew_bps NUMERIC(30, 12) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_microstructure_imbalance_metrics_bucket
        UNIQUE (exchange, market_type, symbol, bucket_start, bucket_seconds)
);

CREATE INDEX IF NOT EXISTS idx_microstructure_imbalance_metrics_symbol_bucket
    ON microstructure_imbalance_metrics (symbol, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_microstructure_imbalance_metrics_bucket
    ON microstructure_imbalance_metrics (bucket_start DESC);

CREATE TABLE IF NOT EXISTS microstructure_liquidity_metrics (
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_seconds INTEGER NOT NULL,
    bid_notional_10bps NUMERIC(30, 12) NOT NULL,
    ask_notional_10bps NUMERIC(30, 12) NOT NULL,
    bid_notional_25bps NUMERIC(30, 12) NOT NULL,
    ask_notional_25bps NUMERIC(30, 12) NOT NULL,
    bid_notional_50bps NUMERIC(30, 12) NOT NULL,
    ask_notional_50bps NUMERIC(30, 12) NOT NULL,
    liquidity_vacuum_score NUMERIC(30, 12) NOT NULL,
    aggressive_buy_notional NUMERIC(30, 12) NOT NULL,
    aggressive_sell_notional NUMERIC(30, 12) NOT NULL,
    aggressive_buy_count INTEGER NOT NULL,
    aggressive_sell_count INTEGER NOT NULL,
    sweep_buy_count INTEGER NOT NULL,
    sweep_sell_count INTEGER NOT NULL,
    liquidation_buy_count INTEGER NOT NULL,
    liquidation_sell_count INTEGER NOT NULL,
    liquidation_buy_notional NUMERIC(30, 12) NOT NULL,
    liquidation_sell_notional NUMERIC(30, 12) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_microstructure_liquidity_metrics_bucket
        UNIQUE (exchange, market_type, symbol, bucket_start, bucket_seconds)
);

CREATE INDEX IF NOT EXISTS idx_microstructure_liquidity_metrics_symbol_bucket
    ON microstructure_liquidity_metrics (symbol, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_microstructure_liquidity_metrics_bucket
    ON microstructure_liquidity_metrics (bucket_start DESC);

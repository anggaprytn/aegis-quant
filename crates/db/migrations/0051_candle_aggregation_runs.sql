CREATE TABLE IF NOT EXISTS candle_aggregation_runs (
    id UUID PRIMARY KEY,
    symbol TEXT NOT NULL,
    source_interval TEXT NOT NULL,
    target_interval TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    source_candles INTEGER NOT NULL DEFAULT 0,
    inserted INTEGER NOT NULL DEFAULT 0,
    updated INTEGER NOT NULL DEFAULT 0,
    skipped_incomplete INTEGER NOT NULL DEFAULT 0,
    latest_source_closed_time TIMESTAMPTZ,
    latest_target_closed_time TIMESTAMPTZ,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_candle_aggregation_runs_symbol_interval
    ON candle_aggregation_runs (symbol, source_interval, target_interval, started_at DESC);

CREATE TABLE IF NOT EXISTS candle_backfill_runs (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    requested_candles_estimate INTEGER NOT NULL DEFAULT 0,
    fetched_candles INTEGER NOT NULL DEFAULT 0,
    inserted_candles INTEGER NOT NULL DEFAULT 0,
    updated_candles INTEGER NOT NULL DEFAULT 0,
    skipped_candles INTEGER NOT NULL DEFAULT 0,
    failed_reason TEXT,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    config JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_candle_backfill_runs_created_at
    ON candle_backfill_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_candle_backfill_runs_symbol_interval
    ON candle_backfill_runs (symbol, interval, created_at DESC);

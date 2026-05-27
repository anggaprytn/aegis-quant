CREATE TABLE IF NOT EXISTS market_data_repair_runs (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    before_quality_status TEXT NOT NULL,
    after_quality_status TEXT,
    gap_count_before BIGINT NOT NULL DEFAULT 0,
    gap_count_after BIGINT NOT NULL DEFAULT 0,
    inserted_candles INTEGER NOT NULL DEFAULT 0,
    updated_candles INTEGER NOT NULL DEFAULT 0,
    skipped_candles INTEGER NOT NULL DEFAULT 0,
    failed_ranges INTEGER NOT NULL DEFAULT 0,
    provider_attempts JSONB NOT NULL DEFAULT '[]'::JSONB,
    plan JSONB NOT NULL DEFAULT '{}'::JSONB,
    result JSONB NOT NULL DEFAULT '{}'::JSONB,
    correlation_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_market_data_repair_runs_created_at
    ON market_data_repair_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_data_repair_runs_symbol_interval
    ON market_data_repair_runs (symbol, interval, created_at DESC);

CREATE TABLE IF NOT EXISTS market_data_repair_ranges (
    id UUID PRIMARY KEY,
    repair_run_id UUID NOT NULL REFERENCES market_data_repair_runs(id) ON DELETE CASCADE,
    source_interval TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    missing_candle_count BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    inserted_candles INTEGER NOT NULL DEFAULT 0,
    updated_candles INTEGER NOT NULL DEFAULT 0,
    skipped_candles INTEGER NOT NULL DEFAULT 0,
    failed_reason TEXT,
    provider_attempts JSONB NOT NULL DEFAULT '[]'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_market_data_repair_ranges_run
    ON market_data_repair_ranges (repair_run_id, start_time ASC);

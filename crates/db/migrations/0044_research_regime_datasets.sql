CREATE TABLE IF NOT EXISTS research_regime_datasets (
    id UUID PRIMARY KEY,
    request JSONB NOT NULL,
    summary JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_regime_datasets_created_at
    ON research_regime_datasets (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_regime_datasets_status_created_at
    ON research_regime_datasets (status, created_at DESC);

CREATE TABLE IF NOT EXISTS research_regime_windows (
    id UUID PRIMARY KEY,
    dataset_id UUID NOT NULL REFERENCES research_regime_datasets(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    regime_label TEXT NOT NULL,
    return_pct NUMERIC(18, 8) NOT NULL,
    realized_volatility NUMERIC(18, 8) NOT NULL,
    avg_range_pct NUMERIC(18, 8) NOT NULL,
    trend_slope NUMERIC(18, 8) NOT NULL,
    choppiness_proxy NUMERIC(18, 8) NOT NULL,
    data_quality_status TEXT NOT NULL,
    candle_count INTEGER NOT NULL,
    score NUMERIC(18, 8) NOT NULL,
    confidence NUMERIC(18, 8) NOT NULL,
    metrics JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_regime_windows_dataset_order
    ON research_regime_windows (dataset_id, regime_label, confidence DESC, start_time ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_research_regime_windows_symbol_timeframe_regime
    ON research_regime_windows (symbol, timeframe, regime_label, start_time ASC);

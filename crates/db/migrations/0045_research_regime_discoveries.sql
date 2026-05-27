CREATE TABLE IF NOT EXISTS research_regime_discoveries (
    id UUID PRIMARY KEY,
    request JSONB NOT NULL,
    summary JSONB NOT NULL,
    status TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    scan_start TIMESTAMPTZ NOT NULL,
    scan_end TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_regime_discoveries_created_at
    ON research_regime_discoveries (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_regime_discoveries_symbol_timeframe
    ON research_regime_discoveries (symbol, timeframe, scan_start, scan_end);

CREATE TABLE IF NOT EXISTS research_regime_discovery_windows (
    id UUID PRIMARY KEY,
    discovery_id UUID NOT NULL REFERENCES research_regime_discoveries(id) ON DELETE CASCADE,
    regime_label TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    confidence NUMERIC(18, 8) NOT NULL,
    return_pct NUMERIC(18, 8) NOT NULL,
    realized_volatility NUMERIC(18, 8) NOT NULL,
    avg_range_pct NUMERIC(18, 8) NOT NULL,
    trend_slope NUMERIC(18, 8) NOT NULL,
    choppiness_proxy NUMERIC(18, 8) NOT NULL,
    data_quality_status TEXT NOT NULL,
    candle_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_regime_discovery_windows_order
    ON research_regime_discovery_windows (discovery_id, regime_label, confidence DESC, start_time ASC, id ASC);

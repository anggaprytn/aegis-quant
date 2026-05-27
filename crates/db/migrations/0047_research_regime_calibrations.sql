CREATE TABLE IF NOT EXISTS research_regime_calibrations (
    id UUID PRIMARY KEY,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    scan_start TIMESTAMPTZ NOT NULL,
    scan_end TIMESTAMPTZ NOT NULL,
    window_hours BIGINT NOT NULL,
    step_hours BIGINT NOT NULL,
    status TEXT NOT NULL,
    recommended_config JSONB,
    summary JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID
);

CREATE INDEX IF NOT EXISTS idx_research_regime_calibrations_created_at
    ON research_regime_calibrations (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_regime_calibrations_symbol_timeframe
    ON research_regime_calibrations (symbol, timeframe, scan_start, scan_end);

CREATE TABLE IF NOT EXISTS research_regime_calibration_candidates (
    id UUID PRIMARY KEY,
    calibration_id UUID NOT NULL REFERENCES research_regime_calibrations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    config JSONB NOT NULL,
    counts_by_regime JSONB NOT NULL,
    missing_regimes JSONB NOT NULL,
    score NUMERIC(18, 8) NOT NULL,
    rank INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_regime_calibration_candidates_order
    ON research_regime_calibration_candidates (calibration_id, rank ASC, score DESC, name ASC);

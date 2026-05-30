CREATE TABLE IF NOT EXISTS cross_asset_candidate_shadow_observations (
    id UUID PRIMARY KEY,
    candidate_id UUID NOT NULL REFERENCES research_candidates(id) ON DELETE CASCADE,
    package_id TEXT NOT NULL,
    evaluated_candle_time TIMESTAMPTZ NOT NULL,
    decision TEXT NOT NULL,
    status TEXT NOT NULL,
    selected_symbol TEXT NULL,
    rank_snapshot_json JSONB NOT NULL,
    reason TEXT NOT NULL,
    warnings_json JSONB NOT NULL DEFAULT '[]'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL,
    CONSTRAINT uq_cross_asset_candidate_shadow_observations_candidate_candle
        UNIQUE (candidate_id, evaluated_candle_time)
);

CREATE INDEX IF NOT EXISTS idx_cross_asset_candidate_shadow_observations_candidate_created
    ON cross_asset_candidate_shadow_observations (candidate_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_cross_asset_candidate_shadow_observations_decision_status
    ON cross_asset_candidate_shadow_observations (decision, status);

CREATE TABLE IF NOT EXISTS cross_asset_candidate_shadow_observation_rankings (
    id UUID PRIMARY KEY,
    observation_id UUID NOT NULL REFERENCES cross_asset_candidate_shadow_observations(id) ON DELETE CASCADE,
    candidate_id UUID NOT NULL REFERENCES research_candidates(id) ON DELETE CASCADE,
    evaluated_candle_time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    rank INT NOT NULL,
    score NUMERIC(30, 12) NOT NULL,
    return_pct NUMERIC(30, 12) NOT NULL,
    return_24h_pct NUMERIC(30, 12) NULL,
    realized_vol_24h_pct NUMERIC(30, 12) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (observation_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_cross_asset_candidate_shadow_observation_rankings_observation_rank
    ON cross_asset_candidate_shadow_observation_rankings (observation_id, rank ASC);

CREATE INDEX IF NOT EXISTS idx_cross_asset_candidate_shadow_observation_rankings_candidate_candle
    ON cross_asset_candidate_shadow_observation_rankings (candidate_id, evaluated_candle_time, rank ASC);

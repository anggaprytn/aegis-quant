CREATE TABLE IF NOT EXISTS strategy_candidate_observations (
    id UUID PRIMARY KEY,
    candidate_id UUID NOT NULL REFERENCES strategy_research_candidates(id) ON DELETE CASCADE,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    status TEXT NOT NULL,
    requirements JSONB NOT NULL,
    summary JSONB NOT NULL,
    decision TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    created_by UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_strategy_candidate_observations_candidate_evaluated_at
    ON strategy_candidate_observations (candidate_id, evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_candidate_observations_decision_status
    ON strategy_candidate_observations (decision, status);

CREATE TABLE IF NOT EXISTS strategy_candidate_observation_checks (
    id UUID PRIMARY KEY,
    observation_id UUID NOT NULL REFERENCES strategy_candidate_observations(id) ON DELETE CASCADE,
    finding_index INT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    blocking BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_candidate_observation_checks_observation
    ON strategy_candidate_observation_checks (observation_id, finding_index ASC);

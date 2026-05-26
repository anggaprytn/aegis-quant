CREATE TABLE IF NOT EXISTS research_candidate_qualification_evaluations (
    id UUID PRIMARY KEY,
    candidate_id UUID NOT NULL REFERENCES research_candidates(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    score INT NOT NULL,
    latest_readiness_status TEXT NULL,
    total_shadow_runs INT NOT NULL DEFAULT 0,
    would_submit_count INT NOT NULL DEFAULT 0,
    risk_rejection_rate_pct NUMERIC NULL,
    warnings JSONB NOT NULL DEFAULT '[]'::JSONB,
    blockers JSONB NOT NULL DEFAULT '[]'::JSONB,
    recommendations JSONB NOT NULL DEFAULT '[]'::JSONB,
    thresholds JSONB NOT NULL DEFAULT '{}'::JSONB,
    evaluated_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_candidate_qualification_evaluations_candidate_evaluated
    ON research_candidate_qualification_evaluations (candidate_id, evaluated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_candidate_qualification_evaluations_status_evaluated
    ON research_candidate_qualification_evaluations (status, evaluated_at DESC, id DESC);

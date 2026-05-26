CREATE TABLE IF NOT EXISTS research_candidate_shadow_runs (
    candidate_id UUID NOT NULL REFERENCES strategy_research_candidates(id) ON DELETE CASCADE,
    shadow_run_id UUID NOT NULL REFERENCES testnet_shadow_runs(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (candidate_id, shadow_run_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_research_candidate_shadow_runs_shadow_run
    ON research_candidate_shadow_runs (shadow_run_id);

CREATE INDEX IF NOT EXISTS idx_research_candidate_shadow_runs_candidate_created
    ON research_candidate_shadow_runs (candidate_id, created_at DESC, shadow_run_id DESC);

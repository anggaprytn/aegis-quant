CREATE TABLE IF NOT EXISTS research_candidate_walk_forward_runs (
    candidate_id UUID NOT NULL REFERENCES research_candidates(id) ON DELETE CASCADE,
    walk_forward_run_id UUID NOT NULL REFERENCES strategy_walk_forward_runs(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (candidate_id, walk_forward_run_id)
);

CREATE INDEX IF NOT EXISTS idx_research_candidate_walk_forward_runs_candidate_created
    ON research_candidate_walk_forward_runs (candidate_id, created_at DESC, walk_forward_run_id DESC);

CREATE INDEX IF NOT EXISTS idx_research_candidate_walk_forward_runs_run
    ON research_candidate_walk_forward_runs (walk_forward_run_id);

ALTER TABLE research_candidate_qualification_evaluations
    ADD COLUMN IF NOT EXISTS walk_forward_status TEXT NULL,
    ADD COLUMN IF NOT EXISTS walk_forward_run_id UUID NULL REFERENCES strategy_walk_forward_runs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS walk_forward_score NUMERIC(18, 8) NULL,
    ADD COLUMN IF NOT EXISTS walk_forward_consistency_score NUMERIC(18, 8) NULL,
    ADD COLUMN IF NOT EXISTS walk_forward_recommendation TEXT NULL,
    ADD COLUMN IF NOT EXISTS walk_forward_blockers JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS walk_forward_warnings JSONB NOT NULL DEFAULT '[]'::jsonb;

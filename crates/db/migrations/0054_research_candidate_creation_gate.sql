CREATE TABLE IF NOT EXISTS research_candidate_proposals (
    id UUID PRIMARY KEY,
    source_batch_id UUID NULL REFERENCES research_batches(id) ON DELETE SET NULL,
    experiment_run_id UUID NOT NULL REFERENCES strategy_experiment_runs(id) ON DELETE CASCADE,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    config JSONB NOT NULL,
    score NUMERIC(10, 2) NOT NULL,
    pnl_pct NUMERIC(20, 8) NOT NULL,
    triage_status TEXT NOT NULL,
    walk_forward_status TEXT NULL,
    gate_decision JSONB NOT NULL,
    reason TEXT NOT NULL,
    promoted_candidate_id UUID NULL REFERENCES research_candidates(id) ON DELETE SET NULL,
    promoted_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_candidate_proposals_created_at
    ON research_candidate_proposals (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_candidate_proposals_source_batch
    ON research_candidate_proposals (source_batch_id, created_at DESC)
    WHERE source_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_candidate_proposals_experiment_run
    ON research_candidate_proposals (experiment_run_id, created_at DESC);

ALTER TABLE research_campaign_batches
    ADD COLUMN IF NOT EXISTS candidates_blocked_by_gate INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS proposals_created INTEGER NOT NULL DEFAULT 0;

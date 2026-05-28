ALTER TABLE research_candidate_proposals
    ADD COLUMN IF NOT EXISTS source_experiment_run_id UUID NULL REFERENCES strategy_experiment_runs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS source_walk_forward_run_id UUID NULL REFERENCES strategy_walk_forward_runs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS source_robustness_matrix_run_id UUID NULL REFERENCES strategy_robustness_matrix_runs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS source_robustness_status TEXT NULL,
    ADD COLUMN IF NOT EXISTS normalized_strategy_config JSONB NULL,
    ADD COLUMN IF NOT EXISTS config_fingerprint TEXT NULL,
    ADD COLUMN IF NOT EXISTS evidence_status_summary JSONB NULL,
    ADD COLUMN IF NOT EXISTS gate_evidence_mismatch BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE research_candidate_proposals
SET source_experiment_run_id = experiment_run_id
WHERE source_experiment_run_id IS NULL;

UPDATE research_candidate_proposals proposal
SET source_walk_forward_run_id = (candidate.value->>'walk_forward_run_id')::UUID
FROM research_batches batch,
    LATERAL jsonb_array_elements(batch.summary->'top_candidates') AS candidate(value)
WHERE proposal.source_batch_id = batch.id
    AND proposal.source_walk_forward_run_id IS NULL
    AND candidate.value ? 'walk_forward_run_id'
    AND candidate.value->>'walk_forward_run_id' IS NOT NULL
    AND candidate.value->>'walk_forward_run_id' <> 'null'
    AND (candidate.value->>'experiment_run_id')::UUID = proposal.experiment_run_id;

CREATE INDEX IF NOT EXISTS idx_research_candidate_proposals_source_walk_forward
    ON research_candidate_proposals (source_walk_forward_run_id, created_at DESC)
    WHERE source_walk_forward_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_candidate_proposals_config_fingerprint
    ON research_candidate_proposals (strategy_id, symbol, timeframe, config_fingerprint, created_at DESC)
    WHERE config_fingerprint IS NOT NULL;

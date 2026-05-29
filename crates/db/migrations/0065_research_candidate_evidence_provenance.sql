ALTER TABLE research_candidates
    ADD COLUMN IF NOT EXISTS source_experiment_run_id UUID NULL,
    ADD COLUMN IF NOT EXISTS source_walk_forward_run_id UUID NULL,
    ADD COLUMN IF NOT EXISTS source_robustness_matrix_run_id UUID NULL,
    ADD COLUMN IF NOT EXISTS source_robustness_matrix_cell_id UUID NULL,
    ADD COLUMN IF NOT EXISTS source_batch_id UUID NULL,
    ADD COLUMN IF NOT EXISTS source_campaign_id UUID NULL,
    ADD COLUMN IF NOT EXISTS source_proposal_id UUID NULL,
    ADD COLUMN IF NOT EXISTS candidate_creation_mode TEXT NULL,
    ADD COLUMN IF NOT EXISTS gate_status TEXT NULL,
    ADD COLUMN IF NOT EXISTS config_fingerprint TEXT NULL,
    ADD COLUMN IF NOT EXISTS gate_decision JSONB NULL,
    ADD COLUMN IF NOT EXISTS evidence_status_summary JSONB NULL;

UPDATE research_candidates
SET source_experiment_run_id = experiment_run_id
WHERE source_experiment_run_id IS NULL
  AND experiment_run_id IS NOT NULL;

UPDATE research_candidates candidate
SET source_experiment_run_id = COALESCE(proposal.source_experiment_run_id, proposal.experiment_run_id),
    source_walk_forward_run_id = proposal.source_walk_forward_run_id,
    source_robustness_matrix_run_id = proposal.source_robustness_matrix_run_id,
    source_batch_id = proposal.source_batch_id,
    source_proposal_id = proposal.id,
    gate_status = proposal.triage_status,
    config_fingerprint = proposal.config_fingerprint,
    gate_decision = proposal.gate_decision,
    evidence_status_summary = proposal.evidence_status_summary
FROM research_candidate_proposals proposal
WHERE proposal.promoted_candidate_id = candidate.id;

CREATE INDEX IF NOT EXISTS idx_research_candidates_source_experiment
    ON research_candidates (source_experiment_run_id, created_at DESC)
    WHERE source_experiment_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_candidates_source_walk_forward
    ON research_candidates (source_walk_forward_run_id, created_at DESC)
    WHERE source_walk_forward_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_candidates_source_robustness_matrix
    ON research_candidates (source_robustness_matrix_run_id, created_at DESC)
    WHERE source_robustness_matrix_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_candidates_config_fingerprint
    ON research_candidates (strategy_id, symbol, timeframe, config_fingerprint, created_at DESC)
    WHERE config_fingerprint IS NOT NULL;

ALTER TABLE research_candidate_imports
    ADD COLUMN IF NOT EXISTS evidence_provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS evidence_artifacts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS evidence_completeness_json JSONB NOT NULL DEFAULT '{}'::jsonb;

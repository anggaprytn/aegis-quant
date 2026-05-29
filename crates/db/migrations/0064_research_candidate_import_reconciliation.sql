ALTER TABLE research_candidate_imports
    ADD COLUMN IF NOT EXISTS bundle_schema_version TEXT NOT NULL DEFAULT 'research_candidate_evidence_bundle.v1',
    ADD COLUMN IF NOT EXISTS reconciliation_status TEXT NOT NULL DEFAULT 'NOT_CHECKED',
    ADD COLUMN IF NOT EXISTS reconciliation_checked_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS local_validation_window_start TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS local_validation_window_end TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS local_walk_forward_status TEXT NULL,
    ADD COLUMN IF NOT EXISTS local_worst_window_pnl NUMERIC NULL,
    ADD COLUMN IF NOT EXISTS local_recommendation TEXT NULL,
    ADD COLUMN IF NOT EXISTS reconciliation_summary_json JSONB NOT NULL DEFAULT '{}'::JSONB,
    ADD COLUMN IF NOT EXISTS recommended_next_action TEXT NOT NULL DEFAULT 'RUN_LOCAL_VALIDATION_OR_OBSERVATION';

CREATE INDEX IF NOT EXISTS idx_research_candidate_imports_reconciliation
    ON research_candidate_imports (reconciliation_status, reconciliation_checked_at DESC);

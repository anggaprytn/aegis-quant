CREATE TABLE IF NOT EXISTS research_candidate_imports (
    id UUID PRIMARY KEY,
    candidate_id UUID NOT NULL REFERENCES research_candidates(id) ON DELETE CASCADE,
    bundle_fingerprint TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    source_candidate_id UUID NOT NULL,
    source_environment TEXT NULL,
    imported_status TEXT NOT NULL,
    evidence_summary_json JSONB NOT NULL DEFAULT '{}'::JSONB,
    warnings_json JSONB NOT NULL DEFAULT '[]'::JSONB,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    imported_by UUID NULL REFERENCES users(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_research_candidate_imports_bundle_fingerprint
    ON research_candidate_imports (bundle_fingerprint);

CREATE UNIQUE INDEX IF NOT EXISTS uq_research_candidate_imports_source_config
    ON research_candidate_imports (source_candidate_id, config_fingerprint);

CREATE INDEX IF NOT EXISTS idx_research_candidate_imports_candidate_imported
    ON research_candidate_imports (candidate_id, imported_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_candidate_imports_config
    ON research_candidate_imports (config_fingerprint, imported_at DESC);

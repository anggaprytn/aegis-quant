CREATE TABLE IF NOT EXISTS scheduled_research_jobs (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    interval_seconds BIGINT NOT NULL CHECK (interval_seconds > 0),
    request JSONB NOT NULL DEFAULT '{}'::jsonb,
    max_runs_per_tick INTEGER NOT NULL DEFAULT 1 CHECK (max_runs_per_tick > 0),
    last_run_at TIMESTAMPTZ NULL,
    next_run_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT scheduled_research_jobs_kind_safe CHECK (
        kind IN (
            'PROVIDER_HEALTH',
            'MARKET_DATA_QUALITY',
            'AGGREGATION_STATUS',
            'RESEARCH_BATCH',
            'RESEARCH_CAMPAIGN',
            'REGIME_DISCOVERY',
            'ROBUSTNESS_MATRIX',
            'OPERATOR_REPORT'
        )
    ),
    CONSTRAINT scheduled_research_jobs_status_valid CHECK (
        status IN ('DISABLED', 'ENABLED', 'PAUSED', 'RUNNING', 'ERROR')
    )
);

CREATE INDEX IF NOT EXISTS idx_scheduled_research_jobs_due
    ON scheduled_research_jobs (enabled, next_run_at)
    WHERE enabled = TRUE;

CREATE TABLE IF NOT EXISTS scheduled_research_job_runs (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES scheduled_research_jobs(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT NULL,
    created_artifact_type TEXT NULL,
    created_artifact_id UUID NULL,
    correlation_id UUID NULL,
    CONSTRAINT scheduled_research_job_runs_status_valid CHECK (
        status IN ('COMPLETED', 'FAILED', 'SKIPPED', 'PARTIAL_SUCCESS')
    )
);

CREATE INDEX IF NOT EXISTS idx_scheduled_research_job_runs_job_started
    ON scheduled_research_job_runs (job_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_scheduled_research_job_runs_status_started
    ON scheduled_research_job_runs (status, started_at DESC);

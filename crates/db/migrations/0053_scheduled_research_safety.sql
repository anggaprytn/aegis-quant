ALTER TABLE scheduled_research_jobs
    ADD COLUMN IF NOT EXISTS consecutive_failure_count INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failure_count >= 0),
    ADD COLUMN IF NOT EXISTS last_failure_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS last_failure_reason TEXT NULL,
    ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS backoff_until TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS auto_paused_reason TEXT NULL;

ALTER TABLE scheduled_research_jobs
    DROP CONSTRAINT IF EXISTS scheduled_research_jobs_status_valid;

ALTER TABLE scheduled_research_jobs
    ADD CONSTRAINT scheduled_research_jobs_status_valid CHECK (
        status IN ('DISABLED', 'ENABLED', 'PAUSED', 'RUNNING', 'BACKING_OFF', 'ERROR', 'AUTO_PAUSED')
    );

ALTER TABLE scheduled_research_job_runs
    DROP CONSTRAINT IF EXISTS scheduled_research_job_runs_status_valid;

ALTER TABLE scheduled_research_job_runs
    ADD CONSTRAINT scheduled_research_job_runs_status_valid CHECK (
        status IN ('COMPLETED', 'FAILED', 'SKIPPED', 'SKIPPED_OVERLAP', 'SKIPPED_BACKOFF', 'PARTIAL_SUCCESS')
    );

CREATE INDEX IF NOT EXISTS idx_scheduled_research_jobs_backoff
    ON scheduled_research_jobs (enabled, status, backoff_until)
    WHERE enabled = TRUE;

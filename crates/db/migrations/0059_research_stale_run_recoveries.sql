CREATE TABLE IF NOT EXISTS research_stale_run_recoveries (
    id UUID PRIMARY KEY,
    target_type TEXT NOT NULL,
    target_id UUID NOT NULL,
    previous_status TEXT NOT NULL,
    recovered_status TEXT NOT NULL,
    reason TEXT NOT NULL,
    age_minutes BIGINT NOT NULL,
    stale_threshold_minutes BIGINT NOT NULL,
    actor_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    correlation_id UUID NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_stale_run_recoveries_created
    ON research_stale_run_recoveries (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_stale_run_recoveries_target
    ON research_stale_run_recoveries (target_type, target_id, created_at DESC);

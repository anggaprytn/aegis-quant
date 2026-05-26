ALTER TABLE strategy_candidate_observations
    ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS observation_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS observation_max_age_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS observation_snapshot_hash TEXT,
    ADD COLUMN IF NOT EXISTS runner_config_snapshot JSONB,
    ADD COLUMN IF NOT EXISTS readiness_snapshot JSONB;

UPDATE strategy_candidate_observations
SET last_observed_at = COALESCE(last_observed_at, evaluated_at)
WHERE last_observed_at IS NULL;

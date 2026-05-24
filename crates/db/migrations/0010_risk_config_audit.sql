CREATE TABLE IF NOT EXISTS risk_configs (
    config_key TEXT PRIMARY KEY,
    config_id UUID NOT NULL UNIQUE,
    max_open_positions INTEGER NOT NULL,
    max_daily_loss_pct NUMERIC(20, 8) NOT NULL,
    max_weekly_loss_pct NUMERIC(20, 8) NOT NULL,
    max_position_notional NUMERIC(20, 8) NOT NULL,
    max_slippage_pct NUMERIC(20, 8) NOT NULL,
    max_consecutive_losses INTEGER NOT NULL,
    cooldown_seconds INTEGER NOT NULL,
    max_signal_age_ms BIGINT NOT NULL,
    stale_feed_threshold_seconds INTEGER NOT NULL,
    current_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS risk_config_versions (
    id UUID PRIMARY KEY,
    config_key TEXT NOT NULL REFERENCES risk_configs(config_key) ON DELETE CASCADE,
    config_id UUID NOT NULL,
    version INTEGER NOT NULL,
    config JSONB NOT NULL,
    actor_id UUID,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (config_key, version)
);

CREATE INDEX IF NOT EXISTS idx_risk_config_versions_created_at
    ON risk_config_versions (config_key, created_at DESC);

CREATE TABLE IF NOT EXISTS risk_config_audit (
    id UUID PRIMARY KEY,
    config_key TEXT NOT NULL REFERENCES risk_configs(config_key) ON DELETE CASCADE,
    config_id UUID NOT NULL,
    version INTEGER,
    old_config JSONB,
    new_config JSONB,
    validation_issues JSONB NOT NULL DEFAULT '[]'::jsonb,
    actor_id UUID,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_risk_config_audit_created_at
    ON risk_config_audit (config_key, created_at DESC);

CREATE TABLE IF NOT EXISTS testnet_shadow_runner_config (
    id UUID PRIMARY KEY,
    enabled BOOLEAN NOT NULL,
    interval_seconds INTEGER NOT NULL,
    strategies JSONB NOT NULL DEFAULT '[]'::jsonb,
    symbols JSONB NOT NULL DEFAULT '[]'::jsonb,
    timeframe TEXT NOT NULL,
    max_runs_per_tick INTEGER NOT NULL,
    stale_feed_policy TEXT NOT NULL,
    notes TEXT NULL,
    updated_by UUID NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS testnet_shadow_runner_state (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    last_tick_at TIMESTAMPTZ NULL,
    last_success_at TIMESTAMPTZ NULL,
    last_error TEXT NULL,
    total_ticks BIGINT NOT NULL DEFAULT 0,
    total_runs BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

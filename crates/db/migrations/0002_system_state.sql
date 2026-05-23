CREATE TABLE IF NOT EXISTS system_state (
    state_key TEXT PRIMARY KEY,
    kill_switch_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    kill_switch_reason TEXT,
    updated_by_actor TEXT NOT NULL,
    updated_by_actor_id UUID,
    last_correlation_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research_hypotheses (
    id UUID PRIMARY KEY,
    source_type TEXT NOT NULL,
    status TEXT NOT NULL,
    strategy_id TEXT NULL,
    symbol TEXT NULL,
    timeframe TEXT NULL,
    regime TEXT NULL,
    failure_reasons JSONB NOT NULL,
    evidence JSONB NOT NULL,
    recommendation JSONB NOT NULL,
    proposed_action TEXT NOT NULL,
    proposed_experiment_config JSONB NOT NULL,
    priority TEXT NOT NULL,
    expected_effect TEXT NOT NULL,
    risk TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ NULL,
    decided_by UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    decision_reason TEXT NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_hypotheses_created_at
    ON research_hypotheses (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_hypotheses_status_priority
    ON research_hypotheses (status, priority, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_hypotheses_strategy_symbol_timeframe
    ON research_hypotheses (strategy_id, symbol, timeframe, created_at DESC);

CREATE TABLE IF NOT EXISTS research_hypothesis_events (
    id UUID PRIMARY KEY,
    hypothesis_id UUID NOT NULL REFERENCES research_hypotheses(id) ON DELETE CASCADE,
    previous_status TEXT NULL,
    next_status TEXT NOT NULL,
    reason TEXT NULL,
    actor_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_hypothesis_events_hypothesis_created
    ON research_hypothesis_events (hypothesis_id, created_at ASC, id ASC);

CREATE TABLE IF NOT EXISTS research_experiment_plans (
    id UUID PRIMARY KEY,
    hypothesis_id UUID NOT NULL REFERENCES research_hypotheses(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    source_campaign_id UUID NULL REFERENCES research_campaigns(id) ON DELETE SET NULL,
    strategy_id TEXT NOT NULL,
    symbol TEXT NULL,
    timeframe TEXT NULL,
    proposed_request JSONB NOT NULL,
    plan_type TEXT NOT NULL,
    status TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    validation_issues JSONB NOT NULL,
    steps JSONB NOT NULL,
    recommendation JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_experiment_plans_created_at
    ON research_experiment_plans (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_experiment_plans_hypothesis
    ON research_experiment_plans (hypothesis_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_experiment_plans_status
    ON research_experiment_plans (status, validation_status, created_at DESC);

CREATE TABLE IF NOT EXISTS research_experiment_plan_events (
    id UUID PRIMARY KEY,
    plan_id UUID NOT NULL REFERENCES research_experiment_plans(id) ON DELETE CASCADE,
    previous_status TEXT NULL,
    next_status TEXT NOT NULL,
    event_type TEXT NOT NULL,
    reason TEXT NULL,
    actor_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_experiment_plan_events_plan_created
    ON research_experiment_plan_events (plan_id, created_at ASC, id ASC);

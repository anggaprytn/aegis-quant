CREATE TABLE IF NOT EXISTS research_campaigns (
    id UUID PRIMARY KEY,
    request JSONB NOT NULL,
    status TEXT NOT NULL,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    correlation_id UUID NULL,
    error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_research_campaigns_created_at
    ON research_campaigns (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_campaigns_status_created_at
    ON research_campaigns (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_campaigns_correlation_id
    ON research_campaigns (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS research_campaign_batches (
    id UUID PRIMARY KEY,
    campaign_id UUID NOT NULL REFERENCES research_campaigns(id) ON DELETE CASCADE,
    research_batch_id UUID NULL REFERENCES research_batches(id) ON DELETE SET NULL,
    plan_index INTEGER NOT NULL,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    triage_status TEXT NOT NULL,
    candidates_created INTEGER NOT NULL DEFAULT 0,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_research_campaign_batches_campaign_plan
    ON research_campaign_batches (campaign_id, plan_index ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_research_campaign_batches_research_batch
    ON research_campaign_batches (research_batch_id)
    WHERE research_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_campaign_batches_status_created
    ON research_campaign_batches (status, created_at DESC);

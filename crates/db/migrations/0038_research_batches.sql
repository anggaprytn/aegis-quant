CREATE TABLE IF NOT EXISTS research_batches (
    id UUID PRIMARY KEY,
    request JSONB NOT NULL,
    status TEXT NOT NULL,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_batches_created_at
    ON research_batches (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_batches_status_created_at
    ON research_batches (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_batches_correlation_id
    ON research_batches (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS research_batch_steps (
    id UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES research_batches(id) ON DELETE CASCADE,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_research_batch_steps_batch_started
    ON research_batch_steps (batch_id, started_at ASC, id ASC);

CREATE TABLE IF NOT EXISTS research_dataset_builds (
    id UUID PRIMARY KEY,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    requested_intervals JSONB NOT NULL DEFAULT '[]'::JSONB,
    status TEXT NOT NULL,
    coverage_before JSONB NOT NULL DEFAULT '{}'::JSONB,
    coverage_after JSONB NOT NULL DEFAULT '{}'::JSONB,
    failed_reason TEXT,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_research_dataset_builds_symbol_created_at
    ON research_dataset_builds (symbol, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_dataset_builds_status_created_at
    ON research_dataset_builds (status, created_at DESC);

CREATE TABLE IF NOT EXISTS research_dataset_build_steps (
    id UUID PRIMARY KEY,
    build_id UUID NOT NULL REFERENCES research_dataset_builds(id) ON DELETE CASCADE,
    step_index INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    details JSONB,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_research_dataset_build_steps_build_step_index
    ON research_dataset_build_steps (build_id, step_index);

CREATE INDEX IF NOT EXISTS idx_research_dataset_build_steps_build_id
    ON research_dataset_build_steps (build_id, step_index ASC);

CREATE TABLE IF NOT EXISTS execution_readiness_snapshots (
    id UUID PRIMARY KEY,
    target TEXT NOT NULL,
    status TEXT NOT NULL,
    score INTEGER NOT NULL,
    blocking_reasons JSONB NOT NULL,
    warnings JSONB NOT NULL,
    checks JSONB NOT NULL,
    recommendations JSONB NOT NULL,
    created_by UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_readiness_snapshots_created_at
    ON execution_readiness_snapshots (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_readiness_snapshots_target_created_at
    ON execution_readiness_snapshots (target, created_at DESC);

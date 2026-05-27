CREATE TABLE IF NOT EXISTS research_experiment_plan_runs (
    id UUID PRIMARY KEY,
    plan_id UUID NOT NULL REFERENCES research_experiment_plans(id) ON DELETE CASCADE,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    artifact_type TEXT NULL,
    artifact_id UUID NULL,
    request JSONB NOT NULL,
    result JSONB NOT NULL,
    error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_experiment_plan_runs_plan_created
    ON research_experiment_plan_runs (plan_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_experiment_plan_runs_status_created
    ON research_experiment_plan_runs (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_experiment_plan_runs_artifact
    ON research_experiment_plan_runs (artifact_type, artifact_id)
    WHERE artifact_type IS NOT NULL AND artifact_id IS NOT NULL;

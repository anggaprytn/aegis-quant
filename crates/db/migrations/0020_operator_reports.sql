CREATE TABLE IF NOT EXISTS operator_reports (
    id UUID PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    format TEXT NOT NULL,
    status TEXT NOT NULL,
    payload JSONB NOT NULL,
    markdown TEXT NULL,
    created_by UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_operator_reports_created_at
    ON operator_reports (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_operator_reports_window
    ON operator_reports (window_start DESC, window_end DESC);

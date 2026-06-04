CREATE TABLE IF NOT EXISTS candidate_review_events (
    id UUID PRIMARY KEY,
    candidate_id UUID NOT NULL REFERENCES research_candidates(id) ON DELETE CASCADE,
    decision TEXT NOT NULL,
    notes TEXT NULL,
    actor TEXT NOT NULL,
    actor_id UUID NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL,
    CONSTRAINT chk_candidate_review_events_decision
        CHECK (decision IN ('KEEP_RESEARCH_ONLY', 'EXTEND_OBSERVATION'))
);

CREATE INDEX IF NOT EXISTS idx_candidate_review_events_candidate_created_at
    ON candidate_review_events (candidate_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_review_events_decision_created_at
    ON candidate_review_events (decision, created_at DESC, id DESC);

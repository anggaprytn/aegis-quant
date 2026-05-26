CREATE TABLE IF NOT EXISTS research_candidate_reviews (
    id UUID PRIMARY KEY,
    candidate_id UUID NOT NULL REFERENCES research_candidates(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    previous_candidate_status TEXT NOT NULL,
    next_candidate_status TEXT NULL,
    reason TEXT NULL,
    notes TEXT NULL,
    actor_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL,
    qualification_evaluation_id UUID NULL REFERENCES research_candidate_qualification_evaluations(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_research_candidate_reviews_candidate_created_at
    ON research_candidate_reviews (candidate_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_candidate_reviews_action_created_at
    ON research_candidate_reviews (action, created_at DESC, id DESC);

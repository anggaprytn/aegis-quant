CREATE TABLE IF NOT EXISTS strategy_research_candidates (
    id UUID PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    config JSONB NOT NULL,
    source_type TEXT NOT NULL,
    source_id UUID NULL,
    evidence JSONB NOT NULL,
    score NUMERIC(10,2) NOT NULL,
    status TEXT NOT NULL,
    warnings JSONB NOT NULL DEFAULT '[]'::JSONB,
    created_by UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    promoted_at TIMESTAMPTZ NULL,
    promoted_by UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_strategy_research_candidates_strategy_created
    ON strategy_research_candidates (strategy_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_research_candidates_symbol_timeframe
    ON strategy_research_candidates (symbol, timeframe, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_research_candidates_status
    ON strategy_research_candidates (status, created_at DESC);

CREATE TABLE IF NOT EXISTS strategy_research_candidate_promotions (
    id UUID PRIMARY KEY,
    candidate_id UUID NOT NULL REFERENCES strategy_research_candidates(id) ON DELETE CASCADE,
    previous_config JSONB NULL,
    promoted_config JSONB NOT NULL,
    status TEXT NOT NULL,
    actor_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_research_candidate_promotions_candidate_status
    ON strategy_research_candidate_promotions (candidate_id, status)
    WHERE status = 'PROMOTED_TO_SHADOW_CONFIG';

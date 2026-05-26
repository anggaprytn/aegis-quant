CREATE TABLE IF NOT EXISTS research_candidates (
    id UUID PRIMARY KEY,
    experiment_id UUID NULL REFERENCES strategy_experiments(id) ON DELETE SET NULL,
    experiment_run_id UUID NULL REFERENCES strategy_experiment_runs(id) ON DELETE SET NULL,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    config JSONB NOT NULL,
    score NUMERIC(10, 2) NULL,
    pnl_pct NUMERIC(20, 8) NULL,
    max_drawdown_pct NUMERIC(20, 8) NULL,
    trade_count INTEGER NULL,
    win_rate NUMERIC(20, 8) NULL,
    fee_drag NUMERIC(20, 8) NULL,
    status TEXT NOT NULL,
    rejection_reason TEXT NULL,
    notes TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_candidates_status_created_at
    ON research_candidates (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_candidates_strategy_symbol_timeframe
    ON research_candidates (strategy_id, symbol, timeframe, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_candidates_experiment_run_id
    ON research_candidates (experiment_run_id);

CREATE TABLE IF NOT EXISTS research_candidate_events (
    id UUID PRIMARY KEY,
    candidate_id UUID NOT NULL REFERENCES research_candidates(id) ON DELETE CASCADE,
    previous_status TEXT NULL,
    next_status TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NULL,
    notes TEXT NULL,
    actor_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_research_candidate_events_candidate_created_at
    ON research_candidate_events (candidate_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_research_candidate_events_decision_created_at
    ON research_candidate_events (decision, created_at DESC);

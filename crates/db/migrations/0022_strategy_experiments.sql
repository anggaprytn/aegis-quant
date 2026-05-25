CREATE TABLE IF NOT EXISTS strategy_experiments (
    id UUID PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    initial_capital NUMERIC(28, 8) NOT NULL,
    fee_bps NUMERIC(18, 8) NOT NULL,
    slippage_bps NUMERIC(18, 8) NOT NULL,
    max_signal_age_ms BIGINT,
    max_runs INTEGER,
    status TEXT NOT NULL,
    comparison JSONB NOT NULL DEFAULT '{}'::jsonb,
    correlation_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_experiments_strategy_symbol_created_at
    ON strategy_experiments (strategy_id, symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS strategy_experiment_runs (
    id UUID PRIMARY KEY,
    experiment_id UUID NOT NULL REFERENCES strategy_experiments(id) ON DELETE CASCADE,
    rank INTEGER NOT NULL DEFAULT 0,
    candidate_config JSONB NOT NULL,
    final_equity NUMERIC(28, 8) NOT NULL,
    pnl NUMERIC(28, 8) NOT NULL,
    pnl_pct NUMERIC(18, 8) NOT NULL,
    max_drawdown_pct NUMERIC(18, 8) NOT NULL,
    win_rate NUMERIC(18, 8) NOT NULL,
    trade_count INTEGER NOT NULL,
    fee_paid NUMERIC(28, 8) NOT NULL,
    slippage_cost NUMERIC(28, 8) NOT NULL,
    fee_slippage_drag_pct NUMERIC(18, 8) NOT NULL,
    score NUMERIC(18, 8) NOT NULL,
    status TEXT NOT NULL,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_experiment_runs_experiment_rank
    ON strategy_experiment_runs (experiment_id, rank ASC, created_at ASC);

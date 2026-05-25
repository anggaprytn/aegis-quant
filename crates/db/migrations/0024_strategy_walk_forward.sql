CREATE TABLE IF NOT EXISTS strategy_walk_forward_runs (
    id UUID PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    request JSONB NOT NULL,
    status TEXT NOT NULL,
    total_windows INTEGER NOT NULL,
    completed_windows INTEGER NOT NULL,
    skipped_windows INTEGER NOT NULL,
    profitable_test_windows INTEGER NOT NULL,
    losing_test_windows INTEGER NOT NULL,
    avg_test_pnl_pct NUMERIC(18, 8) NOT NULL,
    median_test_pnl_pct NUMERIC(18, 8) NOT NULL,
    worst_test_pnl_pct NUMERIC(18, 8) NOT NULL,
    best_test_pnl_pct NUMERIC(18, 8) NOT NULL,
    avg_max_drawdown_pct NUMERIC(18, 8) NOT NULL,
    robustness_score NUMERIC(18, 8) NOT NULL,
    robustness_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id UUID
);

CREATE INDEX IF NOT EXISTS idx_strategy_walk_forward_runs_strategy_symbol_created_at
    ON strategy_walk_forward_runs (strategy_id, symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS strategy_walk_forward_windows (
    id UUID PRIMARY KEY,
    walk_forward_id UUID NOT NULL REFERENCES strategy_walk_forward_runs(id) ON DELETE CASCADE,
    window_index INTEGER NOT NULL,
    train_start TIMESTAMPTZ NOT NULL,
    train_end TIMESTAMPTZ NOT NULL,
    test_start TIMESTAMPTZ NOT NULL,
    test_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    skip_reason TEXT,
    trade_count INTEGER NOT NULL,
    pnl NUMERIC(28, 8) NOT NULL,
    pnl_pct NUMERIC(18, 8) NOT NULL,
    max_drawdown_pct NUMERIC(18, 8) NOT NULL,
    win_rate NUMERIC(18, 8) NOT NULL,
    fee_paid NUMERIC(28, 8) NOT NULL,
    slippage_cost NUMERIC(28, 8) NOT NULL,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_walk_forward_windows_walk_forward_window_index
    ON strategy_walk_forward_windows (walk_forward_id, window_index ASC, created_at ASC);

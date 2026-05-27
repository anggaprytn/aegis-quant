CREATE TABLE IF NOT EXISTS strategy_robustness_matrix_runs (
    id UUID PRIMARY KEY,
    request JSONB NOT NULL,
    summary JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_robustness_matrix_runs_created_at
    ON strategy_robustness_matrix_runs (created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS strategy_robustness_matrix_cells (
    id UUID PRIMARY KEY,
    matrix_run_id UUID NOT NULL REFERENCES strategy_robustness_matrix_runs(id) ON DELETE CASCADE,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    regime TEXT NOT NULL,
    data_quality_status TEXT NOT NULL,
    status TEXT NOT NULL,
    pnl_pct NUMERIC(18, 8) NOT NULL,
    trade_count INTEGER NOT NULL,
    raw_signal_count INTEGER NOT NULL,
    executed_trade_count INTEGER NOT NULL,
    cooldown_suppressed_count INTEGER NOT NULL,
    win_rate NUMERIC(18, 8) NOT NULL,
    max_drawdown_pct NUMERIC(18, 8) NOT NULL,
    fee_drag NUMERIC(28, 8) NOT NULL,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    findings JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_robustness_matrix_cells_run_order
    ON strategy_robustness_matrix_cells (
        matrix_run_id,
        strategy_id,
        symbol,
        timeframe,
        window_start ASC,
        id ASC
    );

CREATE INDEX IF NOT EXISTS idx_strategy_robustness_matrix_cells_strategy_created
    ON strategy_robustness_matrix_cells (strategy_id, created_at DESC);

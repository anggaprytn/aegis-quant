CREATE TABLE IF NOT EXISTS cross_asset_robustness_matrix_runs (
    id UUID PRIMARY KEY,
    strategy_kind TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    symbols JSONB NOT NULL,
    request JSONB NOT NULL,
    status TEXT NOT NULL,
    recommendation TEXT NOT NULL,
    rankings JSONB NOT NULL DEFAULT '[]'::jsonb,
    findings JSONB NOT NULL DEFAULT '[]'::jsonb,
    recommendations JSONB NOT NULL DEFAULT '[]'::jsonb,
    cell_count INTEGER NOT NULL DEFAULT 0,
    evaluated_config_count INTEGER NOT NULL DEFAULT 0,
    full_config_count INTEGER NOT NULL DEFAULT 0,
    skipped_config_count INTEGER NOT NULL DEFAULT 0,
    summary JSONB NOT NULL,
    error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_cross_asset_robustness_matrix_runs_created_at
    ON cross_asset_robustness_matrix_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_cross_asset_robustness_matrix_runs_strategy_created_at
    ON cross_asset_robustness_matrix_runs (strategy_kind, created_at DESC);

CREATE TABLE IF NOT EXISTS cross_asset_robustness_matrix_cells (
    id UUID PRIMARY KEY,
    matrix_run_id UUID NOT NULL REFERENCES cross_asset_robustness_matrix_runs(id) ON DELETE CASCADE,
    config_index INTEGER NOT NULL,
    config JSONB NOT NULL,
    window_label TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    total_trades INTEGER NOT NULL DEFAULT 0,
    compounded_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    avg_trade_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    median_trade_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    win_rate NUMERIC NOT NULL DEFAULT 0,
    max_drawdown_pct NUMERIC NOT NULL DEFAULT 0,
    worst_trade_pct NUMERIC NOT NULL DEFAULT 0,
    worst_window_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    fee_slippage_drag_pct NUMERIC NOT NULL DEFAULT 0,
    symbol_distribution JSONB NOT NULL DEFAULT '{}'::jsonb,
    max_symbol_concentration_pct NUMERIC NOT NULL DEFAULT 0,
    quarter_distribution JSONB NOT NULL DEFAULT '{}'::jsonb,
    max_quarter_concentration_pct NUMERIC NOT NULL DEFAULT 0,
    findings JSONB NOT NULL DEFAULT '[]'::jsonb,
    error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (matrix_run_id, config_index, window_label)
);

CREATE INDEX IF NOT EXISTS idx_cross_asset_robustness_matrix_cells_run_config
    ON cross_asset_robustness_matrix_cells (matrix_run_id, config_index ASC, window_start ASC);

CREATE INDEX IF NOT EXISTS idx_cross_asset_robustness_matrix_cells_run_status
    ON cross_asset_robustness_matrix_cells (matrix_run_id, status);

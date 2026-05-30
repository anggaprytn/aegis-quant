CREATE TABLE IF NOT EXISTS cross_asset_research_runs (
    id UUID PRIMARY KEY,
    strategy_kind TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    symbols JSONB NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    request JSONB NOT NULL,
    summary JSONB NOT NULL,
    status TEXT NOT NULL,
    portfolio_status TEXT NOT NULL,
    recommendation TEXT NOT NULL,
    total_trades INTEGER NOT NULL DEFAULT 0,
    compounded_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    max_drawdown_pct NUMERIC NOT NULL DEFAULT 0,
    max_symbol_concentration_pct NUMERIC NOT NULL DEFAULT 0,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    correlation_id UUID NULL
);

CREATE INDEX IF NOT EXISTS idx_cross_asset_research_runs_created_at
    ON cross_asset_research_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_cross_asset_research_runs_strategy_created_at
    ON cross_asset_research_runs (strategy_kind, created_at DESC);

CREATE TABLE IF NOT EXISTS cross_asset_research_trades (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES cross_asset_research_runs(id) ON DELETE CASCADE,
    trade_index INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    signal_time TIMESTAMPTZ NOT NULL,
    entry_time TIMESTAMPTZ NOT NULL,
    exit_time TIMESTAMPTZ NOT NULL,
    entry_price NUMERIC NOT NULL,
    exit_price NUMERIC NOT NULL,
    weight NUMERIC NOT NULL,
    gross_pnl_pct NUMERIC NOT NULL,
    net_pnl_pct NUMERIC NOT NULL,
    fee_slippage_drag_pct NUMERIC NOT NULL,
    exit_reason TEXT NOT NULL,
    ranking_snapshot JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, trade_index)
);

CREATE INDEX IF NOT EXISTS idx_cross_asset_research_trades_run_index
    ON cross_asset_research_trades (run_id, trade_index ASC);

CREATE INDEX IF NOT EXISTS idx_cross_asset_research_trades_run_entry
    ON cross_asset_research_trades (run_id, entry_time ASC);

CREATE TABLE IF NOT EXISTS cross_asset_research_windows (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES cross_asset_research_runs(id) ON DELETE CASCADE,
    window_index INTEGER NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    trade_count INTEGER NOT NULL DEFAULT 0,
    net_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    compounded_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    avg_trade_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    median_trade_pnl_pct NUMERIC NOT NULL DEFAULT 0,
    win_rate NUMERIC NOT NULL DEFAULT 0,
    max_drawdown_pct NUMERIC NOT NULL DEFAULT 0,
    worst_trade_pct NUMERIC NOT NULL DEFAULT 0,
    best_trade_pct NUMERIC NOT NULL DEFAULT 0,
    symbol_distribution JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, window_index)
);

CREATE INDEX IF NOT EXISTS idx_cross_asset_research_windows_run_index
    ON cross_asset_research_windows (run_id, window_index ASC);

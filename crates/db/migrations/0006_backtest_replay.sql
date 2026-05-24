CREATE TABLE IF NOT EXISTS backtest_runs (
    id UUID PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    initial_capital NUMERIC(20, 8) NOT NULL,
    final_equity NUMERIC(20, 8) NOT NULL,
    pnl NUMERIC(20, 8) NOT NULL,
    pnl_pct NUMERIC(20, 8) NOT NULL,
    max_drawdown_pct NUMERIC(20, 8) NOT NULL,
    win_rate NUMERIC(20, 8) NOT NULL,
    trade_count INTEGER NOT NULL,
    winning_trades INTEGER NOT NULL,
    losing_trades INTEGER NOT NULL,
    avg_win NUMERIC(20, 8) NOT NULL,
    avg_loss NUMERIC(20, 8) NOT NULL,
    fee_paid NUMERIC(20, 8) NOT NULL,
    slippage_cost NUMERIC(20, 8) NOT NULL,
    status TEXT NOT NULL,
    config JSONB NOT NULL,
    correlation_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_time TIMESTAMPTZ NOT NULL,
    entry_price NUMERIC(20, 8) NOT NULL,
    exit_time TIMESTAMPTZ,
    exit_price NUMERIC(20, 8),
    quantity NUMERIC(20, 8) NOT NULL,
    notional NUMERIC(20, 8) NOT NULL,
    fee_paid NUMERIC(20, 8) NOT NULL,
    slippage_cost NUMERIC(20, 8) NOT NULL,
    realized_pnl NUMERIC(20, 8) NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_equity_curve (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    equity NUMERIC(20, 8) NOT NULL,
    drawdown_pct NUMERIC(20, 8) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy_symbol_created_at
    ON backtest_runs (strategy_id, symbol, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_backtest_trades_run_id
    ON backtest_trades (run_id);

CREATE INDEX IF NOT EXISTS idx_backtest_equity_curve_run_id_timestamp
    ON backtest_equity_curve (run_id, timestamp);

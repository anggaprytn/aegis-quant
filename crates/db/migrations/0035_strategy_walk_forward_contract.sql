ALTER TABLE strategy_walk_forward_runs
    ADD COLUMN IF NOT EXISTS config JSONB,
    ADD COLUMN IF NOT EXISTS experiment_run_id UUID,
    ADD COLUMN IF NOT EXISTS start_time TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS end_time TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS train_window_hours INTEGER,
    ADD COLUMN IF NOT EXISTS test_window_hours INTEGER,
    ADD COLUMN IF NOT EXISTS step_hours INTEGER,
    ADD COLUMN IF NOT EXISTS initial_capital NUMERIC(28, 8),
    ADD COLUMN IF NOT EXISTS fee_bps NUMERIC(18, 8),
    ADD COLUMN IF NOT EXISTS slippage_bps NUMERIC(18, 8),
    ADD COLUMN IF NOT EXISTS failed_windows INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS avg_trade_count NUMERIC(18, 8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_drawdown_pct NUMERIC(18, 8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS consistency_score NUMERIC(18, 8) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS robustness_status TEXT NOT NULL DEFAULT 'INSUFFICIENT_DATA',
    ADD COLUMN IF NOT EXISTS recommendation JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE strategy_walk_forward_windows
    ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reason TEXT;

UPDATE strategy_walk_forward_windows
SET
    window_start = COALESCE(window_start, test_start),
    window_end = COALESCE(window_end, test_end),
    reason = COALESCE(reason, skip_reason)
WHERE window_start IS NULL
    OR window_end IS NULL
    OR reason IS NULL;

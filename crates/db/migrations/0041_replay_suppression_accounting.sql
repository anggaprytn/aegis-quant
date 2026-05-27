ALTER TABLE backtest_runs
    ADD COLUMN IF NOT EXISTS raw_signal_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cooldown_suppressed_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS open_position_suppressed_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS executed_trade_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS suppression_breakdown JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS last_signal_time TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS last_executed_entry_time TIMESTAMPTZ NULL;

UPDATE backtest_runs
SET executed_trade_count = trade_count
WHERE executed_trade_count = 0
  AND trade_count <> 0;

ALTER TABLE strategy_experiment_runs
    ADD COLUMN IF NOT EXISTS raw_signal_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cooldown_suppressed_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS open_position_suppressed_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS executed_trade_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS suppression_breakdown JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS last_signal_time TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS last_executed_entry_time TIMESTAMPTZ NULL;

UPDATE strategy_experiment_runs
SET executed_trade_count = trade_count
WHERE executed_trade_count = 0
  AND trade_count <> 0;

ALTER TABLE strategy_walk_forward_windows
    ADD COLUMN IF NOT EXISTS raw_signal_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cooldown_suppressed_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS open_position_suppressed_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS executed_trade_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS suppression_breakdown JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS last_signal_time TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS last_executed_entry_time TIMESTAMPTZ NULL;

UPDATE strategy_walk_forward_windows
SET executed_trade_count = trade_count
WHERE executed_trade_count = 0
  AND trade_count <> 0;

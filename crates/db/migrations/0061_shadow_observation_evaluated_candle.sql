ALTER TABLE testnet_shadow_runs
    ADD COLUMN IF NOT EXISTS evaluated_candle_open_time TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_testnet_shadow_runs_evaluated_candle
    ON testnet_shadow_runs (strategy_id, symbol, timeframe, evaluated_candle_open_time DESC)
    WHERE evaluated_candle_open_time IS NOT NULL;

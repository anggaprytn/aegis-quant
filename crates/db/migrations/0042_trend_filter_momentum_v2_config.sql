ALTER TABLE strategy_configs
    ADD COLUMN IF NOT EXISTS min_close_above_sma_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS max_close_above_sma_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_momentum_return_pct NUMERIC(20, 8);

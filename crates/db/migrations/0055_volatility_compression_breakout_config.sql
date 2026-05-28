ALTER TABLE strategy_configs
    ADD COLUMN IF NOT EXISTS compression_lookback_candles INTEGER,
    ADD COLUMN IF NOT EXISTS compression_percentile_threshold NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_breakout_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS max_breakout_extension_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_volume_expansion_ratio NUMERIC(20, 8);

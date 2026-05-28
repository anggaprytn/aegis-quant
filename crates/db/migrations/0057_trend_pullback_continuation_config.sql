ALTER TABLE strategy_configs
    ADD COLUMN IF NOT EXISTS pullback_lookback_candles INTEGER,
    ADD COLUMN IF NOT EXISTS pullback_sma_lookback_candles INTEGER,
    ADD COLUMN IF NOT EXISTS min_trend_return_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_trend_slope_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_pullback_depth_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS max_pullback_depth_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_reclaim_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_volume_ratio NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS max_choppiness NUMERIC(20, 8);

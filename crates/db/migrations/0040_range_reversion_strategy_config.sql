ALTER TABLE strategy_configs
    ADD COLUMN IF NOT EXISTS lower_band_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS upper_band_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_range_width_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS max_range_width_pct NUMERIC(20, 8);

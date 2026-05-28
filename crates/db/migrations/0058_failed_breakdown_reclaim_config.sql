ALTER TABLE strategy_configs
    ADD COLUMN IF NOT EXISTS min_breakdown_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_reclaim_close_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS min_lower_wick_pct NUMERIC(20, 8);

ALTER TABLE strategy_experiments
    ADD COLUMN IF NOT EXISTS experiment_group_id UUID;

ALTER TABLE strategy_experiments
    ADD COLUMN IF NOT EXISTS candle_count INTEGER;

ALTER TABLE strategy_experiments
    ADD COLUMN IF NOT EXISTS warnings JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE strategy_experiments
    ADD COLUMN IF NOT EXISTS skipped_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_strategy_experiments_group_created_at
    ON strategy_experiments (experiment_group_id, created_at ASC);

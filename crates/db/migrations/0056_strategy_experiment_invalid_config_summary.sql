ALTER TABLE strategy_experiments
    ADD COLUMN IF NOT EXISTS total_candidate_configs INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS skipped_invalid_config_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS executed_config_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS invalid_config_examples JSONB NOT NULL DEFAULT '[]'::jsonb;

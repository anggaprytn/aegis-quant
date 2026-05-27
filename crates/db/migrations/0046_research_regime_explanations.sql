ALTER TABLE research_regime_windows
    ADD COLUMN IF NOT EXISTS explanation JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE research_regime_discovery_windows
    ADD COLUMN IF NOT EXISTS explanation JSONB NOT NULL DEFAULT '{}'::jsonb;

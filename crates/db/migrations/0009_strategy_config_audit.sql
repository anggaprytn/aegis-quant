ALTER TABLE strategy_configs
    ADD COLUMN IF NOT EXISTS enabled BOOLEAN,
    ADD COLUMN IF NOT EXISTS lookback_candles INTEGER,
    ADD COLUMN IF NOT EXISTS max_signal_age_ms BIGINT,
    ADD COLUMN IF NOT EXISTS cooldown_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS confidence_floor NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS holding_candles INTEGER,
    ADD COLUMN IF NOT EXISTS notes TEXT,
    ADD COLUMN IF NOT EXISTS current_version INTEGER;

UPDATE strategy_configs
SET
    enabled = COALESCE(enabled, status = 'enabled'),
    lookback_candles = COALESCE(
        lookback_candles,
        CASE
            WHEN strategy_id = 'momentum_v1' THEN momentum_lookback_candles
            WHEN strategy_id = 'volatility_breakout_v1' THEN breakout_lookback_candles
            ELSE momentum_lookback_candles
        END
    ),
    max_signal_age_ms = COALESCE(max_signal_age_ms, 5000),
    cooldown_seconds = COALESCE(cooldown_seconds, 900),
    current_version = COALESCE(current_version, 1),
    mode = CASE
        WHEN mode = 'signal_only' THEN 'paper'
        ELSE mode
    END
WHERE
    enabled IS NULL
    OR lookback_candles IS NULL
    OR max_signal_age_ms IS NULL
    OR cooldown_seconds IS NULL
    OR current_version IS NULL
    OR mode = 'signal_only';

ALTER TABLE strategy_configs
    ALTER COLUMN enabled SET NOT NULL,
    ALTER COLUMN lookback_candles SET NOT NULL,
    ALTER COLUMN max_signal_age_ms SET NOT NULL,
    ALTER COLUMN cooldown_seconds SET NOT NULL,
    ALTER COLUMN current_version SET NOT NULL;

CREATE TABLE IF NOT EXISTS strategy_config_versions (
    id UUID PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES strategy_configs(strategy_id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    config JSONB NOT NULL,
    actor_id UUID,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (strategy_id, version)
);

CREATE INDEX IF NOT EXISTS idx_strategy_config_versions_strategy_created_at
    ON strategy_config_versions (strategy_id, created_at DESC);

CREATE TABLE IF NOT EXISTS strategy_config_audit (
    id UUID PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES strategy_configs(strategy_id) ON DELETE CASCADE,
    version INTEGER,
    old_config JSONB,
    new_config JSONB,
    validation_issues JSONB NOT NULL DEFAULT '[]'::jsonb,
    actor_id UUID,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategy_config_audit_strategy_created_at
    ON strategy_config_audit (strategy_id, created_at DESC);

INSERT INTO strategy_config_versions (
    id,
    strategy_id,
    version,
    config,
    actor_id,
    correlation_id,
    created_at
)
SELECT
    (
        substr(md5(strategy_id || ':' || current_version::text), 1, 8) || '-' ||
        substr(md5(strategy_id || ':' || current_version::text), 9, 4) || '-' ||
        substr(md5(strategy_id || ':' || current_version::text), 13, 4) || '-' ||
        substr(md5(strategy_id || ':' || current_version::text), 17, 4) || '-' ||
        substr(md5(strategy_id || ':' || current_version::text), 21, 12)
    )::uuid,
    strategy_id,
    current_version,
    jsonb_build_object(
        'strategy_id', strategy_id,
        'enabled', enabled,
        'mode', mode,
        'symbols', string_to_array(symbols, ','),
        'timeframe', timeframe,
        'suggested_notional', suggested_notional,
        'max_signal_age_ms', max_signal_age_ms,
        'cooldown_seconds', cooldown_seconds,
        'lookback_candles', lookback_candles,
        'confidence_floor', confidence_floor,
        'stop_loss_pct', stop_loss_pct,
        'take_profit_pct', take_profit_pct,
        'holding_candles', holding_candles,
        'notes', notes
    ),
    NULL,
    '00000000-0000-0000-0000-000000000001'::uuid,
    created_at
FROM strategy_configs
ON CONFLICT (strategy_id, version) DO NOTHING;

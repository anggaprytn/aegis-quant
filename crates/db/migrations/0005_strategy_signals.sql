CREATE TABLE IF NOT EXISTS strategy_configs (
    strategy_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    mode TEXT NOT NULL,
    symbols TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    suggested_notional NUMERIC(20, 8) NOT NULL,
    momentum_lookback_candles INTEGER NOT NULL,
    breakout_lookback_candles INTEGER NOT NULL,
    stop_loss_pct NUMERIC(20, 8),
    take_profit_pct NUMERIC(20, 8),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategy_state (
    strategy_id TEXT PRIMARY KEY REFERENCES strategy_configs(strategy_id) ON DELETE CASCADE,
    last_evaluated_at TIMESTAMPTZ,
    last_evaluation_reason TEXT,
    last_signal_id UUID,
    last_signal_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'signals' AND column_name = 'strength'
    ) THEN
        ALTER TABLE signals RENAME COLUMN strength TO confidence;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'signals' AND column_name = 'strategy_name'
    ) THEN
        ALTER TABLE signals RENAME COLUMN strategy_name TO strategy_id;
    END IF;
END $$;

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS timeframe TEXT,
    ADD COLUMN IF NOT EXISTS reason TEXT,
    ADD COLUMN IF NOT EXISTS suggested_notional NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS stop_loss_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS take_profit_pct NUMERIC(20, 8),
    ADD COLUMN IF NOT EXISTS source_candle_open_time TIMESTAMPTZ;

UPDATE signals
SET
    timeframe = COALESCE(timeframe, '1m'),
    reason = COALESCE(reason, 'conditions_not_met'),
    suggested_notional = COALESCE(suggested_notional, 0),
    source_candle_open_time = COALESCE(source_candle_open_time, generated_at);

ALTER TABLE signals
    ALTER COLUMN strategy_id SET NOT NULL,
    ALTER COLUMN confidence SET NOT NULL,
    ALTER COLUMN timeframe SET NOT NULL,
    ALTER COLUMN reason SET NOT NULL,
    ALTER COLUMN suggested_notional SET NOT NULL,
    ALTER COLUMN source_candle_open_time SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_signals_strategy_symbol_timeframe_candle_side_reason
    ON signals (
        strategy_id,
        symbol,
        timeframe,
        source_candle_open_time,
        side,
        reason
    );

CREATE INDEX IF NOT EXISTS idx_signals_strategy_created_at_desc
    ON signals (strategy_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_signals_symbol_created_at_desc
    ON signals (symbol, created_at DESC);

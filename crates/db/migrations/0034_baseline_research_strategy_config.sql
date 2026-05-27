ALTER TABLE strategy_configs
    ADD COLUMN IF NOT EXISTS trend_lookback_candles INTEGER,
    ADD COLUMN IF NOT EXISTS strategy_momentum_lookback_candles INTEGER,
    ADD COLUMN IF NOT EXISTS strategy_breakout_lookback_candles INTEGER;

UPDATE strategy_configs
SET
    trend_lookback_candles = COALESCE(trend_lookback_candles, lookback_candles),
    strategy_momentum_lookback_candles = COALESCE(strategy_momentum_lookback_candles, momentum_lookback_candles),
    strategy_breakout_lookback_candles = COALESCE(strategy_breakout_lookback_candles, breakout_lookback_candles)
WHERE strategy_id IN ('trend_filter_momentum_v1', 'volatility_breakout_v2');

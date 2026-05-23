use aegis_core::{
    Candle, CandleInterval, CoreError, SignalConfidence, SignalReason, SignalSide, StrategyConfig,
    StrategyEvaluationContext, StrategyEvaluationResult, StrategyId, StrategySignal,
    StrategyStatus,
};
use rust_decimal::Decimal;
use uuid::Uuid;

pub fn known_strategy_ids() -> [StrategyId; 2] {
    [StrategyId::MomentumV1, StrategyId::VolatilityBreakoutV1]
}

pub fn evaluate(context: StrategyEvaluationContext) -> Result<StrategyEvaluationResult, CoreError> {
    context.config.validate()?;

    if context.config.strategy_id != context.strategy_id {
        return Err(CoreError::UnsupportedStrategyId(format!(
            "strategy config does not match evaluation target: {} != {}",
            context.config.strategy_id, context.strategy_id
        )));
    }

    let candles = normalize_closed_candles(&context.candles);

    if context.config.status == StrategyStatus::Disabled {
        return Ok(no_signal_result(
            &context,
            context.config.timeframe,
            SignalReason::StrategyDisabled,
        ));
    }

    match context.strategy_id {
        StrategyId::MomentumV1 => evaluate_momentum(&context, candles),
        StrategyId::VolatilityBreakoutV1 => evaluate_breakout(&context, candles),
    }
}

fn evaluate_momentum(
    context: &StrategyEvaluationContext,
    candles: Vec<Candle>,
) -> Result<StrategyEvaluationResult, CoreError> {
    let lookback = context.config.momentum_lookback_candles as usize;
    let required = lookback + 1;
    if candles.len() < required {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::InsufficientHistory,
        ));
    }

    let recent = &candles[candles.len() - required..];
    let is_higher_closes = recent.windows(2).all(|pair| pair[1].close > pair[0].close);
    if !is_higher_closes {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }

    let reason = if lookback == 3 {
        SignalReason::ThreeConsecutiveHigherCloses
    } else {
        SignalReason::MomentumHigherCloses
    };

    Ok(generated_result(
        context,
        recent.last().expect("recent candles must be present"),
        reason,
        Decimal::new(65, 2),
    )?)
}

fn evaluate_breakout(
    context: &StrategyEvaluationContext,
    candles: Vec<Candle>,
) -> Result<StrategyEvaluationResult, CoreError> {
    let lookback = context.config.breakout_lookback_candles as usize;
    let required = lookback + 1;
    if candles.len() < required {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::InsufficientHistory,
        ));
    }

    let recent = &candles[candles.len() - required..];
    let latest = recent.last().expect("recent candles must be present");
    let previous_window = &recent[..recent.len() - 1];
    let recent_high = previous_window
        .iter()
        .map(|candle| candle.high)
        .max()
        .expect("previous window must be present");

    if latest.close <= recent_high {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }

    Ok(generated_result(
        context,
        latest,
        SignalReason::BreakoutAboveRecentHigh,
        Decimal::new(70, 2),
    )?)
}

fn generated_result(
    context: &StrategyEvaluationContext,
    source_candle: &Candle,
    reason: SignalReason,
    confidence: Decimal,
) -> Result<StrategyEvaluationResult, CoreError> {
    let signal = StrategySignal {
        signal_id: Uuid::new_v4(),
        strategy_id: context.strategy_id,
        symbol: context.symbol.clone(),
        side: SignalSide::Buy,
        confidence: SignalConfidence::new(confidence)?,
        timeframe: context.config.timeframe,
        reason,
        suggested_notional: context.config.suggested_notional,
        stop_loss_pct: context.config.stop_loss_pct,
        take_profit_pct: context.config.take_profit_pct,
        source_candle_open_time: source_candle.open_time,
        correlation_id: context.correlation_id,
        created_at: context.evaluated_at,
    };

    Ok(StrategyEvaluationResult {
        strategy_id: context.strategy_id,
        symbol: context.symbol.clone(),
        timeframe: context.config.timeframe,
        generated: true,
        reason,
        signal: Some(signal),
        correlation_id: context.correlation_id,
        evaluated_at: context.evaluated_at,
    })
}

fn no_signal_result(
    context: &StrategyEvaluationContext,
    timeframe: CandleInterval,
    reason: SignalReason,
) -> StrategyEvaluationResult {
    StrategyEvaluationResult {
        strategy_id: context.strategy_id,
        symbol: context.symbol.clone(),
        timeframe,
        generated: false,
        reason,
        signal: None,
        correlation_id: context.correlation_id,
        evaluated_at: context.evaluated_at,
    }
}

fn normalize_closed_candles(candles: &[Candle]) -> Vec<Candle> {
    let mut normalized = candles
        .iter()
        .filter(|candle| candle.is_closed)
        .cloned()
        .collect::<Vec<_>>();
    normalized.sort_by_key(|candle| candle.open_time);
    normalized
}

pub fn build_default_strategy_configs(
    symbols: Vec<aegis_core::Symbol>,
    timeframe: CandleInterval,
    suggested_notional: Decimal,
    momentum_lookback_candles: u32,
    breakout_lookback_candles: u32,
) -> Vec<StrategyConfig> {
    vec![
        StrategyConfig {
            strategy_id: StrategyId::MomentumV1,
            status: StrategyStatus::Enabled,
            mode: aegis_core::StrategyMode::SignalOnly,
            symbols: symbols.clone(),
            timeframe,
            suggested_notional,
            momentum_lookback_candles,
            breakout_lookback_candles,
            stop_loss_pct: None,
            take_profit_pct: None,
        },
        StrategyConfig {
            strategy_id: StrategyId::VolatilityBreakoutV1,
            status: StrategyStatus::Enabled,
            mode: aegis_core::StrategyMode::SignalOnly,
            symbols,
            timeframe,
            suggested_notional,
            momentum_lookback_candles,
            breakout_lookback_candles,
            stop_loss_pct: None,
            take_profit_pct: None,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::evaluate;
    use aegis_core::{
        Candle, CandleInterval, MarketDataSource, SignalReason, StrategyEvaluationContext,
        StrategyId, Symbol,
    };
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_candle(index: i64, close: i64, high: i64, is_closed: bool) -> Candle {
        let open_time =
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + Duration::minutes(index);
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            interval: CandleInterval::OneMinute,
            open_time,
            close_time: open_time + Duration::minutes(1),
            open: Decimal::new(close - 1, 0),
            high: Decimal::new(high, 0),
            low: Decimal::new(close - 2, 0),
            close: Decimal::new(close, 0),
            volume: Decimal::new(10, 0),
            quote_volume: Some(Decimal::new(1000, 0)),
            trade_count: 5,
            is_closed,
            created_at: open_time,
            updated_at: open_time,
        }
    }

    fn context(strategy_id: StrategyId, candles: Vec<Candle>) -> StrategyEvaluationContext {
        let configs = super::build_default_strategy_configs(
            vec![Symbol::new("BTCUSDT").expect("valid symbol")],
            CandleInterval::OneMinute,
            Decimal::new(100_000, 0),
            3,
            20,
        );
        let config = configs
            .into_iter()
            .find(|config| config.strategy_id == strategy_id)
            .expect("strategy config must exist");

        StrategyEvaluationContext {
            correlation_id: Uuid::new_v4(),
            strategy_id,
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            config,
            candles,
            evaluated_at: Utc::now(),
        }
    }

    #[test]
    fn momentum_emits_signal_after_consecutive_higher_closes() {
        let candles = vec![
            sample_candle(0, 100, 101, true),
            sample_candle(1, 101, 102, true),
            sample_candle(2, 103, 104, true),
            sample_candle(3, 106, 107, true),
        ];

        let result =
            evaluate(context(StrategyId::MomentumV1, candles)).expect("evaluation should succeed");

        assert!(result.generated);
        assert_eq!(result.reason, SignalReason::ThreeConsecutiveHigherCloses);
    }

    #[test]
    fn momentum_emits_no_signal_when_condition_fails() {
        let candles = vec![
            sample_candle(0, 100, 101, true),
            sample_candle(1, 101, 102, true),
            sample_candle(2, 99, 102, true),
            sample_candle(3, 100, 101, true),
        ];

        let result =
            evaluate(context(StrategyId::MomentumV1, candles)).expect("evaluation should succeed");

        assert!(!result.generated);
        assert_eq!(result.reason, SignalReason::ConditionsNotMet);
    }

    #[test]
    fn volatility_breakout_emits_signal_when_latest_close_breaks_lookback_high() {
        let mut candles = (0..20)
            .map(|index| sample_candle(index, 100 + index, 101 + index, true))
            .collect::<Vec<_>>();
        candles.push(sample_candle(20, 130, 131, true));

        let result = evaluate(context(StrategyId::VolatilityBreakoutV1, candles))
            .expect("evaluation should succeed");

        assert!(result.generated);
        assert_eq!(result.reason, SignalReason::BreakoutAboveRecentHigh);
    }

    #[test]
    fn volatility_breakout_emits_no_signal_when_latest_close_does_not_break_high() {
        let mut candles = (0..20)
            .map(|index| sample_candle(index, 100 + index, 120 + index, true))
            .collect::<Vec<_>>();
        candles.push(sample_candle(20, 120, 121, true));

        let result = evaluate(context(StrategyId::VolatilityBreakoutV1, candles))
            .expect("evaluation should succeed");

        assert!(!result.generated);
        assert_eq!(result.reason, SignalReason::ConditionsNotMet);
    }

    #[test]
    fn insufficient_candle_history_emits_no_signal() {
        let candles = vec![
            sample_candle(0, 100, 101, true),
            sample_candle(1, 101, 102, true),
            sample_candle(2, 102, 103, true),
        ];

        let result =
            evaluate(context(StrategyId::MomentumV1, candles)).expect("evaluation should succeed");

        assert!(!result.generated);
        assert_eq!(result.reason, SignalReason::InsufficientHistory);
    }

    #[test]
    fn open_unclosed_candle_is_ignored() {
        let candles = vec![
            sample_candle(0, 100, 101, true),
            sample_candle(1, 101, 102, true),
            sample_candle(2, 103, 104, true),
            sample_candle(3, 106, 107, false),
            sample_candle(4, 108, 109, true),
        ];

        let result =
            evaluate(context(StrategyId::MomentumV1, candles)).expect("evaluation should succeed");

        assert!(result.generated);
        let signal = result.signal.expect("signal should be generated");
        assert_eq!(
            signal.source_candle_open_time,
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 4, 0).unwrap()
        );
    }
}

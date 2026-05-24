use aegis_core::{
    Candle, CandleInterval, CoreError, SignalConfidence, SignalReason, SignalSide, StrategyConfig,
    StrategyConfigUpdateRequest, StrategyConfigValidationIssue, StrategyConfigValidationResult,
    StrategyConfigValidationSeverity, StrategyEvaluationContext, StrategyEvaluationResult,
    StrategyId, StrategyMode, StrategySignal,
};
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct StrategyValidationContext {
    pub supported_symbols: Vec<aegis_core::Symbol>,
    pub max_position_notional: Option<Decimal>,
}

pub fn known_strategy_ids() -> [StrategyId; 2] {
    [StrategyId::MomentumV1, StrategyId::VolatilityBreakoutV1]
}

pub fn validate_strategy_config(
    request: &StrategyConfigUpdateRequest,
    context: &StrategyValidationContext,
) -> StrategyConfigValidationResult {
    let validated_at = Utc::now();
    let mut issues = Vec::new();

    let strategy_id = match request.strategy_id.parse::<StrategyId>() {
        Ok(strategy_id) => strategy_id,
        Err(_) => {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "unknown_strategy",
                "strategy_id",
                "strategy_id must be one of momentum_v1 or volatility_breakout_v1",
            ));
            return StrategyConfigValidationResult {
                strategy_id: request.strategy_id.clone(),
                valid: false,
                issues,
                normalized_config: None,
                validated_at,
            };
        }
    };

    if request.mode == StrategyMode::Live {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "live_mode_blocked",
            "mode",
            "live mode is blocked; use paper, research, or shadow",
        ));
    }

    let supported_symbols = context
        .supported_symbols
        .iter()
        .map(|symbol| symbol.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut symbols = Vec::new();
    if request.symbols.is_empty() {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "symbols_empty",
            "symbols",
            "at least one symbol is required",
        ));
    } else {
        for raw_symbol in &request.symbols {
            let trimmed = raw_symbol.trim();
            if trimmed.is_empty() {
                issues.push(issue(
                    StrategyConfigValidationSeverity::Error,
                    "symbol_empty",
                    "symbols",
                    "symbols cannot contain empty entries",
                ));
                continue;
            }
            if trimmed != trimmed.to_ascii_uppercase() {
                issues.push(issue(
                    StrategyConfigValidationSeverity::Error,
                    "symbol_not_uppercase",
                    "symbols",
                    "symbols must be uppercase",
                ));
                continue;
            }
            match aegis_core::Symbol::new(trimmed) {
                Ok(symbol) => {
                    if !supported_symbols.is_empty() && !supported_symbols.contains(symbol.as_str())
                    {
                        issues.push(issue(
                            StrategyConfigValidationSeverity::Error,
                            "unsupported_symbol",
                            "symbols",
                            &format!("symbol {} is not supported", symbol.as_str()),
                        ));
                    }
                    symbols.push(symbol);
                }
                Err(err) => issues.push(issue(
                    StrategyConfigValidationSeverity::Error,
                    "invalid_symbol",
                    "symbols",
                    &err.to_string(),
                )),
            }
        }
    }

    let timeframe = match request.timeframe.parse::<CandleInterval>() {
        Ok(timeframe) => timeframe,
        Err(_) => {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "unsupported_timeframe",
                "timeframe",
                "timeframe must currently be 1m",
            ));
            CandleInterval::OneMinute
        }
    };

    if request.suggested_notional <= Decimal::ZERO {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_suggested_notional",
            "suggested_notional",
            "suggested_notional must be greater than zero",
        ));
    }
    if let Some(limit) = context.max_position_notional {
        if request.suggested_notional > limit {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "suggested_notional_above_risk_limit",
                "suggested_notional",
                &format!(
                    "suggested_notional exceeds risk max_position_notional {}",
                    limit
                ),
            ));
        }
    }

    if !(1_000..=300_000).contains(&request.max_signal_age_ms) {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_max_signal_age_ms",
            "max_signal_age_ms",
            "max_signal_age_ms must be between 1_000 and 300_000",
        ));
    }

    if request.cooldown_seconds > 86_400 {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_cooldown_seconds",
            "cooldown_seconds",
            "cooldown_seconds must be between 0 and 86_400",
        ));
    }

    let lookback_range = match strategy_id {
        StrategyId::MomentumV1 => 2..=50,
        StrategyId::VolatilityBreakoutV1 => 5..=500,
    };
    if !lookback_range.contains(&request.lookback_candles) {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_lookback_candles",
            "lookback_candles",
            &format!(
                "lookback_candles must be within {}..={} for {}",
                lookback_range.start(),
                lookback_range.end(),
                strategy_id
            ),
        ));
    }

    validate_optional_percent(
        request.stop_loss_pct,
        Decimal::ZERO,
        Decimal::new(20, 0),
        "stop_loss_pct",
        &mut issues,
    );
    validate_optional_percent(
        request.take_profit_pct,
        Decimal::ZERO,
        Decimal::new(50, 0),
        "take_profit_pct",
        &mut issues,
    );

    if let Some(holding_candles) = request.holding_candles {
        if !(1..=10_000).contains(&holding_candles) {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_holding_candles",
                "holding_candles",
                "holding_candles must be between 1 and 10_000",
            ));
        }
    }

    if let Some(confidence_floor) = request.confidence_floor {
        if !(Decimal::ZERO..=Decimal::ONE).contains(&confidence_floor) {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_confidence_floor",
                "confidence_floor",
                "confidence_floor must be between 0 and 1",
            ));
        } else {
            let max_confidence = max_strategy_confidence(strategy_id);
            if confidence_floor > max_confidence {
                issues.push(issue(
                    StrategyConfigValidationSeverity::Error,
                    "unreachable_confidence_floor",
                    "confidence_floor",
                    &format!(
                        "confidence_floor exceeds the maximum deterministic confidence {} for {}",
                        max_confidence, strategy_id
                    ),
                ));
            }
        }
    }

    if let (Some(stop_loss_pct), Some(take_profit_pct)) =
        (request.stop_loss_pct, request.take_profit_pct)
    {
        if stop_loss_pct >= take_profit_pct {
            issues.push(issue(
                StrategyConfigValidationSeverity::Warn,
                "stop_loss_not_below_take_profit",
                "stop_loss_pct",
                "stop_loss_pct is greater than or equal to take_profit_pct",
            ));
        }
    }

    let normalized_config = if has_error(&issues) {
        None
    } else {
        Some(StrategyConfig {
            strategy_id,
            enabled: request.enabled,
            mode: request.mode,
            symbols,
            timeframe,
            suggested_notional: request.suggested_notional,
            max_signal_age_ms: request.max_signal_age_ms,
            cooldown_seconds: request.cooldown_seconds,
            lookback_candles: request.lookback_candles,
            confidence_floor: request.confidence_floor,
            stop_loss_pct: request.stop_loss_pct,
            take_profit_pct: request.take_profit_pct,
            holding_candles: request.holding_candles,
            notes: normalize_notes(request.notes.clone()),
        })
    };

    StrategyConfigValidationResult {
        strategy_id: request.strategy_id.clone(),
        valid: !has_error(&issues),
        issues,
        normalized_config,
        validated_at,
    }
}

pub fn required_candle_count(config: &StrategyConfig) -> i64 {
    (config.lookback_candles as i64 + 1).max(2)
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

    if !context.config.enabled {
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
    let lookback = context.config.lookback_candles as usize;
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
    let lookback = context.config.lookback_candles as usize;
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
    if let Some(confidence_floor) = context.config.confidence_floor {
        if confidence < confidence_floor {
            return Ok(no_signal_result(
                context,
                context.config.timeframe,
                SignalReason::ConditionsNotMet,
            ));
        }
    }

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
            enabled: true,
            mode: StrategyMode::Paper,
            symbols: symbols.clone(),
            timeframe,
            suggested_notional,
            max_signal_age_ms: 5_000,
            cooldown_seconds: 900,
            lookback_candles: momentum_lookback_candles,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("Default momentum paper config".to_string()),
        },
        StrategyConfig {
            strategy_id: StrategyId::VolatilityBreakoutV1,
            enabled: true,
            mode: StrategyMode::Paper,
            symbols,
            timeframe,
            suggested_notional,
            max_signal_age_ms: 5_000,
            cooldown_seconds: 900,
            lookback_candles: breakout_lookback_candles,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("Default breakout paper config".to_string()),
        },
    ]
}

fn issue(
    severity: StrategyConfigValidationSeverity,
    code: &str,
    field: &str,
    message: &str,
) -> StrategyConfigValidationIssue {
    StrategyConfigValidationIssue {
        severity,
        code: code.to_string(),
        field: field.to_string(),
        message: message.to_string(),
    }
}

fn has_error(issues: &[StrategyConfigValidationIssue]) -> bool {
    issues
        .iter()
        .any(|issue| issue.severity == StrategyConfigValidationSeverity::Error)
}

fn validate_optional_percent(
    value: Option<Decimal>,
    min_exclusive: Decimal,
    max_inclusive: Decimal,
    field: &str,
    issues: &mut Vec<StrategyConfigValidationIssue>,
) {
    if let Some(value) = value {
        if value <= min_exclusive || value > max_inclusive {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                &format!("invalid_{field}"),
                field,
                &format!(
                    "{field} must be greater than 0 and less than or equal to {max_inclusive}"
                ),
            ));
        }
    }
}

fn max_strategy_confidence(strategy_id: StrategyId) -> Decimal {
    match strategy_id {
        StrategyId::MomentumV1 => Decimal::new(65, 2),
        StrategyId::VolatilityBreakoutV1 => Decimal::new(70, 2),
    }
}

fn normalize_notes(notes: Option<String>) -> Option<String> {
    notes.and_then(|notes| {
        let trimmed = notes.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        build_default_strategy_configs, evaluate, required_candle_count, validate_strategy_config,
        StrategyValidationContext,
    };
    use aegis_core::{
        Candle, CandleInterval, MarketDataSource, SignalReason, StrategyConfigUpdateRequest,
        StrategyEvaluationContext, StrategyId, StrategyMode, Symbol,
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
        let configs = build_default_strategy_configs(
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

    fn sample_request(strategy_id: &str) -> StrategyConfigUpdateRequest {
        StrategyConfigUpdateRequest {
            strategy_id: strategy_id.to_string(),
            enabled: true,
            mode: StrategyMode::Paper,
            symbols: vec!["BTCUSDT".to_string()],
            timeframe: "1m".to_string(),
            suggested_notional: Decimal::new(100_000, 0),
            max_signal_age_ms: 5_000,
            cooldown_seconds: 900,
            lookback_candles: 3,
            confidence_floor: None,
            stop_loss_pct: Some(Decimal::new(5, 0)),
            take_profit_pct: Some(Decimal::new(10, 0)),
            holding_candles: Some(3),
            notes: Some("test".to_string()),
        }
    }

    fn validation_context() -> StrategyValidationContext {
        StrategyValidationContext {
            supported_symbols: vec![Symbol::new("BTCUSDT").expect("valid symbol")],
            max_position_notional: Some(Decimal::new(150_000, 0)),
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
    fn required_candle_count_uses_shared_lookback() {
        let config = build_default_strategy_configs(
            vec![Symbol::new("BTCUSDT").expect("valid symbol")],
            CandleInterval::OneMinute,
            Decimal::new(100_000, 0),
            3,
            20,
        )
        .remove(0);
        assert_eq!(required_candle_count(&config), 4);
    }

    #[test]
    fn unknown_strategy_rejected() {
        let result = validate_strategy_config(&sample_request("nope"), &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn live_mode_rejected() {
        let mut request = sample_request("momentum_v1");
        request.mode = StrategyMode::Live;
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn empty_symbols_rejected() {
        let mut request = sample_request("momentum_v1");
        request.symbols.clear();
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn lowercase_symbol_rejected() {
        let mut request = sample_request("momentum_v1");
        request.symbols = vec!["btcusdt".to_string()];
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn invalid_timeframe_rejected() {
        let mut request = sample_request("momentum_v1");
        request.timeframe = "5m".to_string();
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn suggested_notional_must_be_positive() {
        let mut request = sample_request("momentum_v1");
        request.suggested_notional = Decimal::ZERO;
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn suggested_notional_above_risk_max_is_rejected() {
        let mut request = sample_request("momentum_v1");
        request.suggested_notional = Decimal::new(200_000, 0);
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn invalid_momentum_lookback_rejected() {
        let mut request = sample_request("momentum_v1");
        request.lookback_candles = 1;
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn invalid_breakout_lookback_rejected() {
        let mut request = sample_request("volatility_breakout_v1");
        request.lookback_candles = 4;
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }

    #[test]
    fn invalid_stop_loss_take_profit_rejected() {
        let mut request = sample_request("momentum_v1");
        request.stop_loss_pct = Some(Decimal::new(25, 0));
        request.take_profit_pct = Some(Decimal::new(51, 0));
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
    }
}

use aegis_core::{
    Candle, CandleInterval, CoreError, SignalConfidence, SignalReason, SignalSide, StrategyConfig,
    StrategyConfigUpdateRequest, StrategyConfigValidationIssue, StrategyConfigValidationResult,
    StrategyConfigValidationSeverity, StrategyDataHealth, StrategyDiagnosticCheck,
    StrategyDiagnosticSeverity, StrategyDiagnosticsDecision, StrategyDiagnosticsResult,
    StrategyEvaluationContext, StrategyEvaluationResult, StrategyId, StrategyMode,
    StrategyNoSignalReason, StrategySignal,
};
use chrono::Utc;
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct StrategyValidationContext {
    pub supported_symbols: Vec<aegis_core::Symbol>,
    pub max_position_notional: Option<Decimal>,
}

pub fn known_strategy_ids() -> [StrategyId; 5] {
    [
        StrategyId::MomentumV1,
        StrategyId::VolatilityBreakoutV1,
        StrategyId::TrendFilterMomentumV1,
        StrategyId::VolatilityBreakoutV2,
        StrategyId::RangeReversionV1,
    ]
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
                "strategy_id must be one of momentum_v1, volatility_breakout_v1, trend_filter_momentum_v1, volatility_breakout_v2, or range_reversion_v1",
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
                "timeframe must be one of 1m, 5m, 15m, or 1h",
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

    if request.max_signal_age_ms < 1_000 {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_max_signal_age_ms",
            "max_signal_age_ms",
            "max_signal_age_ms must be greater than or equal to 1_000",
        ));
    }
    let (recommended_min, recommended_max) = timeframe.recommended_max_signal_age_ms();
    if request.max_signal_age_ms < recommended_min {
        issues.push(issue(
            StrategyConfigValidationSeverity::Warn,
            &format!("max_signal_age_ms_too_low_for_{}", timeframe.as_str()),
            "max_signal_age_ms",
            &format!(
                "{} candle strategies should use at least {}ms; recommended range is {}-{}ms",
                timeframe.as_str(),
                recommended_min,
                recommended_min,
                recommended_max
            ),
        ));
    }
    if request.max_signal_age_ms > recommended_max {
        issues.push(issue(
            StrategyConfigValidationSeverity::Warn,
            &format!("max_signal_age_ms_high_for_{}", timeframe.as_str()),
            "max_signal_age_ms",
            &format!(
                "{} candle strategies typically use at most {}ms; recommended range is {}-{}ms",
                timeframe.as_str(),
                recommended_max,
                recommended_min,
                recommended_max
            ),
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
        StrategyId::TrendFilterMomentumV1 => 2..=500,
        StrategyId::VolatilityBreakoutV2 => 5..=500,
        StrategyId::RangeReversionV1 => 2..=500,
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

    if let Some(trend_lookback) = request.trend_lookback_candles {
        if trend_lookback == 0 {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_trend_lookback_candles",
                "trend_lookback_candles",
                "trend_lookback_candles must be greater than zero",
            ));
        }
    }
    if let Some(momentum_lookback) = request.momentum_lookback_candles {
        if momentum_lookback == 0 {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_momentum_lookback_candles",
                "momentum_lookback_candles",
                "momentum_lookback_candles must be greater than zero",
            ));
        }
    }
    if let Some(breakout_lookback) = request.breakout_lookback_candles {
        if breakout_lookback == 0 {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_breakout_lookback_candles",
                "breakout_lookback_candles",
                "breakout_lookback_candles must be greater than zero",
            ));
        }
    }

    let lower_band_pct = request.lower_band_pct.unwrap_or(Decimal::new(20, 0));
    let upper_band_pct = request.upper_band_pct.unwrap_or(Decimal::new(80, 0));
    let min_range_width_pct = request.min_range_width_pct.unwrap_or(Decimal::new(15, 2));
    let max_range_width_pct = request.max_range_width_pct.unwrap_or(Decimal::new(3, 0));
    if strategy_id == StrategyId::RangeReversionV1 {
        if lower_band_pct < Decimal::ZERO || lower_band_pct > Decimal::new(50, 0) {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_lower_band_pct",
                "lower_band_pct",
                "lower_band_pct must be between 0 and 50",
            ));
        }
        if upper_band_pct < Decimal::new(50, 0) || upper_band_pct > Decimal::new(100, 0) {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_upper_band_pct",
                "upper_band_pct",
                "upper_band_pct must be between 50 and 100",
            ));
        }
        if lower_band_pct >= upper_band_pct {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_range_band_order",
                "lower_band_pct",
                "lower_band_pct must be less than upper_band_pct",
            ));
        }
        if min_range_width_pct <= Decimal::ZERO {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_min_range_width_pct",
                "min_range_width_pct",
                "min_range_width_pct must be greater than 0",
            ));
        }
        if max_range_width_pct <= min_range_width_pct {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_max_range_width_pct",
                "max_range_width_pct",
                "max_range_width_pct must be greater than min_range_width_pct",
            ));
        }
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
            symbols: symbols.clone(),
            timeframe,
            suggested_notional: request.suggested_notional,
            max_signal_age_ms: request.max_signal_age_ms,
            cooldown_seconds: request.cooldown_seconds,
            lookback_candles: request.lookback_candles,
            trend_lookback_candles: request.trend_lookback_candles,
            momentum_lookback_candles: request.momentum_lookback_candles,
            breakout_lookback_candles: request.breakout_lookback_candles,
            lower_band_pct: request
                .lower_band_pct
                .or((strategy_id == StrategyId::RangeReversionV1).then_some(lower_band_pct)),
            upper_band_pct: request
                .upper_band_pct
                .or((strategy_id == StrategyId::RangeReversionV1).then_some(upper_band_pct)),
            min_range_width_pct: request
                .min_range_width_pct
                .or((strategy_id == StrategyId::RangeReversionV1).then_some(min_range_width_pct)),
            max_range_width_pct: request
                .max_range_width_pct
                .or((strategy_id == StrategyId::RangeReversionV1).then_some(max_range_width_pct)),
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
    match config.strategy_id {
        StrategyId::TrendFilterMomentumV1 => {
            let trend = trend_lookback(config) as i64 + 1;
            let momentum = momentum_lookback(config) as i64 + 1;
            trend.max(momentum).max(2)
        }
        StrategyId::VolatilityBreakoutV2 => (breakout_lookback(config) as i64 + 1).max(2),
        StrategyId::RangeReversionV1 => (config.lookback_candles as i64 + 1).max(2),
        _ => (config.lookback_candles as i64 + 1).max(2),
    }
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
        StrategyId::TrendFilterMomentumV1 => evaluate_trend_filter_momentum(&context, candles),
        StrategyId::VolatilityBreakoutV2 => evaluate_volume_breakout(&context, candles),
        StrategyId::RangeReversionV1 => evaluate_range_reversion(&context, candles),
    }
}

pub fn diagnose(
    context: StrategyEvaluationContext,
) -> Result<StrategyDiagnosticsResult, CoreError> {
    context.config.validate()?;

    if context.config.strategy_id != context.strategy_id {
        return Err(CoreError::UnsupportedStrategyId(format!(
            "strategy config does not match evaluation target: {} != {}",
            context.config.strategy_id, context.strategy_id
        )));
    }

    let candles = normalize_closed_candles(&context.candles);
    let required_closed_candles = required_candle_count(&context.config);
    let latest_closed_candle_time = candles.last().map(|candle| candle.close_time);
    let latest_closed_candle_age_ms = latest_closed_candle_time.map(|timestamp| {
        context
            .evaluated_at
            .signed_duration_since(timestamp)
            .num_milliseconds()
    });
    let stale = latest_closed_candle_age_ms
        .map(|age_ms| age_ms > context.config.max_signal_age_ms)
        .unwrap_or(false);
    let data_health = StrategyDataHealth {
        required_lookback_candles: context.config.lookback_candles,
        required_closed_candles,
        available_closed_candles: candles.len() as i64,
        latest_closed_candle_time,
        latest_closed_candle_age_ms,
        stale,
        latest_closes: candles
            .iter()
            .rev()
            .take(20)
            .map(|candle| format!("{} close={}", candle.close_time.to_rfc3339(), candle.close))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect(),
    };

    let mut condition_checks = Vec::new();
    condition_checks.push(StrategyDiagnosticCheck {
        name: "strategy_enabled".to_string(),
        passed: context.config.enabled,
        severity: if context.config.enabled {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: if context.config.enabled {
            "Strategy is enabled.".to_string()
        } else {
            "Strategy is disabled in configuration.".to_string()
        },
        actual: Some(context.config.enabled.to_string()),
        expected: Some("true".to_string()),
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "closed_candle_coverage".to_string(),
        passed: data_health.available_closed_candles >= data_health.required_closed_candles,
        severity: if data_health.available_closed_candles >= data_health.required_closed_candles {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!(
            "Found {} closed candles; strategy needs {}.",
            data_health.available_closed_candles, data_health.required_closed_candles
        ),
        actual: Some(data_health.available_closed_candles.to_string()),
        expected: Some(format!(">= {}", data_health.required_closed_candles)),
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "closed_candle_freshness".to_string(),
        passed: !stale,
        severity: if stale {
            StrategyDiagnosticSeverity::Warn
        } else {
            StrategyDiagnosticSeverity::Info
        },
        message: match latest_closed_candle_age_ms {
            Some(age_ms) => format!(
                "Latest closed candle age is {} ms; max signal age is {} ms.",
                age_ms, context.config.max_signal_age_ms
            ),
            None => "No closed candle is available to evaluate freshness.".to_string(),
        },
        actual: latest_closed_candle_age_ms.map(|value| value.to_string()),
        expected: Some(format!("<= {}", context.config.max_signal_age_ms)),
    });

    let final_result = if !context.config.enabled {
        DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::StrategyDisabled,
            no_signal_reason: Some(StrategyNoSignalReason::StrategyDisabled),
            summary: "Strategy is disabled, so it would not emit a signal.".to_string(),
            source_candle_open_time: None,
            confidence: None,
        }
    } else if data_health.available_closed_candles < data_health.required_closed_candles {
        DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::InsufficientData,
            no_signal_reason: Some(if context.strategy_id == StrategyId::RangeReversionV1 {
                StrategyNoSignalReason::InsufficientData
            } else {
                StrategyNoSignalReason::InsufficientCandles
            }),
            summary: format!(
                "Only {} closed candles are available, below the required {} candles.",
                data_health.available_closed_candles, data_health.required_closed_candles
            ),
            source_candle_open_time: None,
            confidence: None,
        }
    } else if stale {
        DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::StaleData,
            no_signal_reason: Some(StrategyNoSignalReason::StaleData),
            summary: match latest_closed_candle_age_ms {
                Some(age_ms) => format!(
                    "Latest closed candle is stale at {} ms old; max signal age is {} ms.",
                    age_ms, context.config.max_signal_age_ms
                ),
                None => "No closed candle is available, so data is stale.".to_string(),
            },
            source_candle_open_time: None,
            confidence: None,
        }
    } else {
        match context.strategy_id {
            StrategyId::MomentumV1 => diagnose_momentum(&context, &candles, &mut condition_checks),
            StrategyId::VolatilityBreakoutV1 => {
                diagnose_breakout(&context, &candles, &mut condition_checks)
            }
            StrategyId::TrendFilterMomentumV1 => {
                diagnose_trend_filter_momentum(&context, &candles, &mut condition_checks)
            }
            StrategyId::VolatilityBreakoutV2 => {
                diagnose_volume_breakout(&context, &candles, &mut condition_checks)
            }
            StrategyId::RangeReversionV1 => {
                diagnose_range_reversion(&context, &candles, &mut condition_checks)
            }
        }?
    };

    Ok(StrategyDiagnosticsResult {
        strategy_id: context.strategy_id.to_string(),
        symbol: context.symbol.as_str().to_string(),
        timeframe: context.config.timeframe.as_str().to_string(),
        strategy_enabled: context.config.enabled,
        config_valid: true,
        validation_issues: Vec::new(),
        data_health,
        condition_checks,
        final_decision: final_result.final_decision,
        no_signal_reason: final_result.no_signal_reason,
        summary: final_result.summary,
        source_candle_open_time: final_result.source_candle_open_time,
        confidence: final_result.confidence,
        correlation_id: context.correlation_id,
        evaluated_at: context.evaluated_at,
    })
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

fn evaluate_trend_filter_momentum(
    context: &StrategyEvaluationContext,
    candles: Vec<Candle>,
) -> Result<StrategyEvaluationResult, CoreError> {
    let required = required_candle_count(&context.config) as usize;
    if candles.len() < required {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::InsufficientHistory,
        ));
    }

    let latest = candles.last().expect("candles must be present");
    let previous = &candles[candles.len() - 2];
    let trend = trend_lookback(&context.config) as usize;
    let momentum = momentum_lookback(&context.config) as usize;
    let trend_window = &candles[candles.len() - trend - 1..candles.len() - 1];
    let sma = average_decimal(trend_window.iter().map(|candle| candle.close));
    let momentum_reference = &candles[candles.len() - momentum - 1];

    if latest.close <= sma
        || latest.close <= previous.close
        || latest.close <= momentum_reference.close
    {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }

    Ok(generated_result(
        context,
        latest,
        SignalReason::TrendFilterMomentum,
        Decimal::new(68, 2),
    )?)
}

fn evaluate_volume_breakout(
    context: &StrategyEvaluationContext,
    candles: Vec<Candle>,
) -> Result<StrategyEvaluationResult, CoreError> {
    let lookback = breakout_lookback(&context.config) as usize;
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
    let breakout_level = previous_window
        .iter()
        .map(|candle| candle.high)
        .max()
        .expect("previous window must be present");
    let average_volume = average_decimal(previous_window.iter().map(|candle| candle.volume));

    if latest.close <= breakout_level || latest.volume <= average_volume {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }

    Ok(generated_result(
        context,
        latest,
        SignalReason::VolumeConfirmedBreakout,
        Decimal::new(72, 2),
    )?)
}

fn evaluate_range_reversion(
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
    let previous = &recent[recent.len() - 2];
    let range_window = &recent[recent.len() - lookback..];
    let range = calculate_range_metrics(range_window);
    if range.range_width_pct < min_range_width_pct(&context.config) {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }
    if range.range_width_pct > max_range_width_pct(&context.config) {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }
    if range.range_position_pct > lower_band_pct(&context.config) {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }
    let reversal_confirmed = latest.close > previous.close || latest.close > latest.open;
    let falling_knife_avoided = latest.low >= previous.low;
    if !reversal_confirmed || !falling_knife_avoided {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }

    Ok(generated_result(
        context,
        latest,
        SignalReason::RangeReversion,
        Decimal::new(66, 2),
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

#[derive(Debug)]
struct DiagnosticOutcome {
    final_decision: StrategyDiagnosticsDecision,
    no_signal_reason: Option<StrategyNoSignalReason>,
    summary: String,
    source_candle_open_time: Option<chrono::DateTime<Utc>>,
    confidence: Option<Decimal>,
}

fn diagnose_momentum(
    context: &StrategyEvaluationContext,
    candles: &[Candle],
    condition_checks: &mut Vec<StrategyDiagnosticCheck>,
) -> Result<DiagnosticOutcome, CoreError> {
    let recent = &candles[candles.len() - required_candle_count(&context.config) as usize..];
    let is_higher_closes = recent.windows(2).all(|pair| pair[1].close > pair[0].close);
    condition_checks.push(StrategyDiagnosticCheck {
        name: "momentum_higher_closes".to_string(),
        passed: is_higher_closes,
        severity: if is_higher_closes {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: if is_higher_closes {
            "Each close in the lookback window is strictly higher than the previous close."
                .to_string()
        } else {
            "Momentum requires strictly higher closes across the lookback window, but the sequence breaks."
                .to_string()
        },
        actual: Some(
            recent
                .iter()
                .map(|candle| candle.close.to_string())
                .collect::<Vec<_>>()
                .join(" -> "),
        ),
        expected: Some("strictly increasing closes".to_string()),
    });

    if !is_higher_closes {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::MomentumNotStrictlyHigherCloses),
            summary: format!(
                "Momentum did not trigger because the latest {} closes are not strictly increasing.",
                recent.len()
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let confidence = Decimal::new(65, 2);
    let confidence_passed = context
        .config
        .confidence_floor
        .map(|floor| confidence >= floor)
        .unwrap_or(true);
    condition_checks.push(StrategyDiagnosticCheck {
        name: "confidence_floor".to_string(),
        passed: confidence_passed,
        severity: if confidence_passed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: match context.config.confidence_floor {
            Some(floor) if confidence_passed => {
                format!(
                    "Deterministic confidence {} meets configured floor {}.",
                    confidence, floor
                )
            }
            Some(floor) => {
                format!(
                    "Deterministic confidence {} is below configured floor {}.",
                    confidence, floor
                )
            }
            None => "No confidence floor is configured.".to_string(),
        },
        actual: Some(confidence.to_string()),
        expected: context
            .config
            .confidence_floor
            .map(|value| format!(">= {value}")),
    });

    if !confidence_passed {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::ConfidenceBelowFloor),
            summary: format!(
                "Momentum conditions passed, but confidence {} is below the configured floor.",
                confidence
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    Ok(DiagnosticOutcome {
        final_decision: StrategyDiagnosticsDecision::WouldSignal,
        no_signal_reason: None,
        summary: format!(
            "Momentum would signal because the latest {} closes are strictly increasing.",
            recent.len()
        ),
        source_candle_open_time: recent.last().map(|candle| candle.open_time),
        confidence: Some(confidence),
    })
}

fn diagnose_breakout(
    context: &StrategyEvaluationContext,
    candles: &[Candle],
    condition_checks: &mut Vec<StrategyDiagnosticCheck>,
) -> Result<DiagnosticOutcome, CoreError> {
    let recent = &candles[candles.len() - required_candle_count(&context.config) as usize..];
    let latest = recent.last().expect("recent candles must be present");
    let previous_window = &recent[..recent.len() - 1];
    let recent_high = previous_window
        .iter()
        .map(|candle| candle.high)
        .max()
        .expect("previous window must be present");
    let breakout = latest.close > recent_high;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "breakout_above_recent_high".to_string(),
        passed: breakout,
        severity: if breakout {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: if breakout {
            "Latest close is above the highest high in the prior lookback window.".to_string()
        } else {
            "Latest close is not above the highest high in the prior lookback window.".to_string()
        },
        actual: Some(latest.close.to_string()),
        expected: Some(format!("> {}", recent_high)),
    });

    if !breakout {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::BreakoutNotAboveRecentHigh),
            summary: format!(
                "Breakout did not trigger because latest close {} is not above recent high {}.",
                latest.close, recent_high
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let confidence = Decimal::new(70, 2);
    let confidence_passed = context
        .config
        .confidence_floor
        .map(|floor| confidence >= floor)
        .unwrap_or(true);
    condition_checks.push(StrategyDiagnosticCheck {
        name: "confidence_floor".to_string(),
        passed: confidence_passed,
        severity: if confidence_passed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: match context.config.confidence_floor {
            Some(floor) if confidence_passed => {
                format!(
                    "Deterministic confidence {} meets configured floor {}.",
                    confidence, floor
                )
            }
            Some(floor) => {
                format!(
                    "Deterministic confidence {} is below configured floor {}.",
                    confidence, floor
                )
            }
            None => "No confidence floor is configured.".to_string(),
        },
        actual: Some(confidence.to_string()),
        expected: context
            .config
            .confidence_floor
            .map(|value| format!(">= {value}")),
    });

    if !confidence_passed {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::ConfidenceBelowFloor),
            summary: format!(
                "Breakout conditions passed, but confidence {} is below the configured floor.",
                confidence
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    Ok(DiagnosticOutcome {
        final_decision: StrategyDiagnosticsDecision::WouldSignal,
        no_signal_reason: None,
        summary: format!(
            "Breakout would signal because latest close {} is above recent high {}.",
            latest.close, recent_high
        ),
        source_candle_open_time: Some(latest.open_time),
        confidence: Some(confidence),
    })
}

fn diagnose_trend_filter_momentum(
    context: &StrategyEvaluationContext,
    candles: &[Candle],
    condition_checks: &mut Vec<StrategyDiagnosticCheck>,
) -> Result<DiagnosticOutcome, CoreError> {
    let trend = trend_lookback(&context.config) as usize;
    let momentum = momentum_lookback(&context.config) as usize;
    let latest = candles.last().expect("candles must be present");
    let previous = &candles[candles.len() - 2];
    let trend_window = &candles[candles.len() - trend - 1..candles.len() - 1];
    let sma = average_decimal(trend_window.iter().map(|candle| candle.close));
    let momentum_reference = &candles[candles.len() - momentum - 1];

    let above_sma = latest.close > sma;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "trend_close_above_sma".to_string(),
        passed: above_sma,
        severity: if above_sma {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: if above_sma {
            format!(
                "Latest close {} is above SMA({}) {}.",
                latest.close, trend, sma
            )
        } else {
            format!(
                "Latest close {} is not above SMA({}) {}.",
                latest.close, trend, sma
            )
        },
        actual: Some(latest.close.to_string()),
        expected: Some(format!("> {sma}")),
    });
    if !above_sma {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::TrendCloseNotAboveSma),
            summary: format!(
                "Trend-filter momentum did not trigger because latest close {} is not above SMA({}) {}.",
                latest.close, trend, sma
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let latest_above_previous = latest.close > previous.close;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "latest_close_above_previous_close".to_string(),
        passed: latest_above_previous,
        severity: if latest_above_previous {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: if latest_above_previous {
            "Latest close is above the previous close.".to_string()
        } else {
            "Latest close is not above the previous close.".to_string()
        },
        actual: Some(latest.close.to_string()),
        expected: Some(format!("> {}", previous.close)),
    });
    if !latest_above_previous {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::TrendMomentumNotPositive),
            summary: format!(
                "Trend-filter momentum did not trigger because latest close {} is not above previous close {}.",
                latest.close, previous.close
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let momentum_passed = latest.close > momentum_reference.close;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "momentum_lookback_close_check".to_string(),
        passed: momentum_passed,
        severity: if momentum_passed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: if momentum_passed {
            format!(
                "Latest close is above the close from {} candles ago.",
                momentum
            )
        } else {
            format!(
                "Latest close is not above the close from {} candles ago.",
                momentum
            )
        },
        actual: Some(latest.close.to_string()),
        expected: Some(format!("> {}", momentum_reference.close)),
    });
    if !momentum_passed {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::TrendMomentumNotPositive),
            summary: format!(
                "Trend-filter momentum did not trigger because latest close {} is not above the {}-candle momentum reference {}.",
                latest.close, momentum, momentum_reference.close
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    confidence_outcome(
        context,
        latest,
        SignalReason::TrendFilterMomentum,
        Decimal::new(68, 2),
    )
}

fn diagnose_volume_breakout(
    context: &StrategyEvaluationContext,
    candles: &[Candle],
    condition_checks: &mut Vec<StrategyDiagnosticCheck>,
) -> Result<DiagnosticOutcome, CoreError> {
    let lookback = breakout_lookback(&context.config) as usize;
    let recent = &candles[candles.len() - lookback - 1..];
    let latest = recent.last().expect("recent candles must be present");
    let previous_window = &recent[..recent.len() - 1];
    let breakout_level = previous_window
        .iter()
        .map(|candle| candle.high)
        .max()
        .expect("previous window must be present");
    let breakout = latest.close > breakout_level;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "breakout_close_above_prior_high".to_string(),
        passed: breakout,
        severity: if breakout {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: if breakout {
            "Latest close is above the prior breakout high.".to_string()
        } else {
            "Latest close is not above the prior breakout high.".to_string()
        },
        actual: Some(latest.close.to_string()),
        expected: Some(format!("> {breakout_level}")),
    });
    if !breakout {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::BreakoutNotAboveRecentHigh),
            summary: format!(
                "Volatility breakout did not trigger because latest close {} is not above prior high {}.",
                latest.close, breakout_level
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let average_volume = average_decimal(previous_window.iter().map(|candle| candle.volume));
    let volume_confirmed = latest.volume > average_volume;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "breakout_volume_above_average".to_string(),
        passed: volume_confirmed,
        severity: if volume_confirmed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: if volume_confirmed {
            "Latest volume is above the lookback average volume.".to_string()
        } else {
            "Latest volume is not above the lookback average volume.".to_string()
        },
        actual: Some(latest.volume.to_string()),
        expected: Some(format!("> {average_volume}")),
    });
    if !volume_confirmed {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::BreakoutVolumeBelowAverage),
            summary: format!(
                "Volatility breakout did not trigger because latest volume {} is not above average volume {}.",
                latest.volume, average_volume
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    confidence_outcome(
        context,
        latest,
        SignalReason::VolumeConfirmedBreakout,
        Decimal::new(72, 2),
    )
}

fn diagnose_range_reversion(
    context: &StrategyEvaluationContext,
    candles: &[Candle],
    condition_checks: &mut Vec<StrategyDiagnosticCheck>,
) -> Result<DiagnosticOutcome, CoreError> {
    let lookback = context.config.lookback_candles as usize;
    let recent = &candles[candles.len() - lookback - 1..];
    let latest = recent.last().expect("recent candles must be present");
    let previous = &recent[recent.len() - 2];
    let range_window = &recent[recent.len() - lookback..];
    let range = calculate_range_metrics(range_window);

    condition_checks.push(StrategyDiagnosticCheck {
        name: "range_lookback_candles_required".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: format!(
            "Range reversion uses {} closed candles for range bounds.",
            lookback
        ),
        actual: Some(lookback.to_string()),
        expected: Some(context.config.lookback_candles.to_string()),
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "range_high".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Highest high over the range lookback.".to_string(),
        actual: Some(range.range_high.to_string()),
        expected: None,
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "range_low".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Lowest low over the range lookback.".to_string(),
        actual: Some(range.range_low.to_string()),
        expected: None,
    });

    let min_width = min_range_width_pct(&context.config);
    let width_above_min = range.range_width_pct >= min_width;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "range_width_above_minimum".to_string(),
        passed: width_above_min,
        severity: if width_above_min {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!("Range width is {}%.", range.range_width_pct),
        actual: Some(range.range_width_pct.to_string()),
        expected: Some(format!(">= {min_width}")),
    });
    if !width_above_min {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::RangeTooNarrow),
            summary: format!(
                "Range reversion did not trigger because range width {}% is below {}%.",
                range.range_width_pct, min_width
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let max_width = max_range_width_pct(&context.config);
    let width_below_max = range.range_width_pct <= max_width;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "range_width_below_maximum".to_string(),
        passed: width_below_max,
        severity: if width_below_max {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!("Range width is {}%.", range.range_width_pct),
        actual: Some(range.range_width_pct.to_string()),
        expected: Some(format!("<= {max_width}")),
    });
    if !width_below_max {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::RangeTooWide),
            summary: format!(
                "Range reversion did not trigger because range width {}% is above {}%.",
                range.range_width_pct, max_width
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let lower_band = lower_band_pct(&context.config);
    let near_lower_band = range.range_position_pct <= lower_band;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "range_position_near_lower_band".to_string(),
        passed: near_lower_band,
        severity: if near_lower_band {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!(
            "Latest close {} is at {}% of the range.",
            latest.close, range.range_position_pct
        ),
        actual: Some(range.range_position_pct.to_string()),
        expected: Some(format!("<= {lower_band}")),
    });
    if !near_lower_band {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::NotNearLowerBand),
            summary: format!(
                "Range reversion did not trigger because range position {}% is above lower band {}%.",
                range.range_position_pct, lower_band
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "latest_close".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Latest closed candle close.".to_string(),
        actual: Some(latest.close.to_string()),
        expected: None,
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "previous_close".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Previous closed candle close.".to_string(),
        actual: Some(previous.close.to_string()),
        expected: None,
    });

    let reversal_confirmed = latest.close > previous.close || latest.close > latest.open;
    let falling_knife_avoided = latest.low >= previous.low;
    let reversal_passed = reversal_confirmed && falling_knife_avoided;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "reversal_confirmation".to_string(),
        passed: reversal_passed,
        severity: if reversal_passed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Requires close uptick or green candle and latest low not below previous low."
            .to_string(),
        actual: Some(format!(
            "close>{}: {}, close>open: {}, low>={}: {}",
            previous.close,
            latest.close > previous.close,
            latest.close > latest.open,
            previous.low,
            latest.low >= previous.low
        )),
        expected: Some("reversal confirmation and no lower low".to_string()),
    });
    if !reversal_passed {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::NoReversalConfirmation),
            summary: "Range reversion did not trigger because reversal confirmation failed."
                .to_string(),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "final_decision".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Range reversion conditions passed.".to_string(),
        actual: Some("WOULD_SIGNAL".to_string()),
        expected: Some("WOULD_SIGNAL".to_string()),
    });

    confidence_outcome(
        context,
        latest,
        SignalReason::RangeReversion,
        Decimal::new(66, 2),
    )
}

fn confidence_outcome(
    context: &StrategyEvaluationContext,
    latest: &Candle,
    reason: SignalReason,
    confidence: Decimal,
) -> Result<DiagnosticOutcome, CoreError> {
    let confidence_passed = context
        .config
        .confidence_floor
        .map(|floor| confidence >= floor)
        .unwrap_or(true);
    if !confidence_passed {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::ConfidenceBelowFloor),
            summary: format!(
                "{} conditions passed, but confidence {} is below the configured floor.",
                reason.as_str(),
                confidence
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    Ok(DiagnosticOutcome {
        final_decision: StrategyDiagnosticsDecision::WouldSignal,
        no_signal_reason: None,
        summary: format!(
            "{} would signal on the latest closed candle.",
            reason.as_str()
        ),
        source_candle_open_time: Some(latest.open_time),
        confidence: Some(confidence),
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

fn trend_lookback(config: &StrategyConfig) -> u32 {
    config
        .trend_lookback_candles
        .unwrap_or(config.lookback_candles)
}

fn momentum_lookback(config: &StrategyConfig) -> u32 {
    config.momentum_lookback_candles.unwrap_or(3)
}

fn breakout_lookback(config: &StrategyConfig) -> u32 {
    config
        .breakout_lookback_candles
        .unwrap_or(config.lookback_candles)
}

fn lower_band_pct(config: &StrategyConfig) -> Decimal {
    config.lower_band_pct.unwrap_or(Decimal::new(20, 0))
}

fn min_range_width_pct(config: &StrategyConfig) -> Decimal {
    config.min_range_width_pct.unwrap_or(Decimal::new(15, 2))
}

fn max_range_width_pct(config: &StrategyConfig) -> Decimal {
    config.max_range_width_pct.unwrap_or(Decimal::new(3, 0))
}

#[derive(Debug)]
struct RangeMetrics {
    range_high: Decimal,
    range_low: Decimal,
    range_width_pct: Decimal,
    range_position_pct: Decimal,
}

fn calculate_range_metrics(candles: &[Candle]) -> RangeMetrics {
    let range_high = candles
        .iter()
        .map(|candle| candle.high)
        .max()
        .unwrap_or(Decimal::ZERO);
    let range_low = candles
        .iter()
        .map(|candle| candle.low)
        .min()
        .unwrap_or(Decimal::ZERO);
    let range_width = range_high - range_low;
    let latest_close = candles
        .last()
        .map(|candle| candle.close)
        .unwrap_or(Decimal::ZERO);
    let range_width_pct = pct_ratio(range_width, range_low);
    let range_position_pct = pct_ratio(latest_close - range_low, range_width);
    RangeMetrics {
        range_high,
        range_low,
        range_width_pct,
        range_position_pct,
    }
}

fn pct_ratio(numerator: Decimal, denominator: Decimal) -> Decimal {
    if denominator == Decimal::ZERO {
        Decimal::ZERO
    } else {
        (numerator / denominator) * Decimal::new(100, 0)
    }
}

fn average_decimal(values: impl Iterator<Item = Decimal>) -> Decimal {
    let values = values.collect::<Vec<_>>();
    if values.is_empty() {
        return Decimal::ZERO;
    }
    values.iter().copied().sum::<Decimal>() / Decimal::from(values.len() as u64)
}

pub fn build_default_strategy_configs(
    symbols: Vec<aegis_core::Symbol>,
    timeframe: CandleInterval,
    suggested_notional: Decimal,
    momentum_lookback_candles: u32,
    breakout_lookback_candles: u32,
) -> Vec<StrategyConfig> {
    let (recommended_signal_age_ms, _) = timeframe.recommended_max_signal_age_ms();
    let trend_timeframe = CandleInterval::FiveMinutes;
    let breakout_timeframe = CandleInterval::FifteenMinutes;
    vec![
        StrategyConfig {
            strategy_id: StrategyId::MomentumV1,
            enabled: true,
            mode: StrategyMode::Paper,
            symbols: symbols.clone(),
            timeframe,
            suggested_notional,
            max_signal_age_ms: recommended_signal_age_ms,
            cooldown_seconds: 900,
            lookback_candles: momentum_lookback_candles,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            breakout_lookback_candles: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
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
            symbols: symbols.clone(),
            timeframe,
            suggested_notional,
            max_signal_age_ms: recommended_signal_age_ms,
            cooldown_seconds: 900,
            lookback_candles: breakout_lookback_candles,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            breakout_lookback_candles: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("Default breakout paper config".to_string()),
        },
        StrategyConfig {
            strategy_id: StrategyId::TrendFilterMomentumV1,
            enabled: true,
            mode: StrategyMode::Research,
            symbols: symbols.clone(),
            timeframe: trend_timeframe,
            suggested_notional,
            max_signal_age_ms: 900_000,
            cooldown_seconds: 1_800,
            lookback_candles: 20,
            trend_lookback_candles: Some(20),
            momentum_lookback_candles: Some(3),
            breakout_lookback_candles: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("Research baseline trend-filter momentum config".to_string()),
        },
        StrategyConfig {
            strategy_id: StrategyId::VolatilityBreakoutV2,
            enabled: true,
            mode: StrategyMode::Research,
            symbols: symbols.clone(),
            timeframe: breakout_timeframe,
            suggested_notional,
            max_signal_age_ms: 2_700_000,
            cooldown_seconds: 1_800,
            lookback_candles: 20,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            breakout_lookback_candles: Some(20),
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("Research baseline volume-confirmed breakout config".to_string()),
        },
        StrategyConfig {
            strategy_id: StrategyId::RangeReversionV1,
            enabled: true,
            mode: StrategyMode::Research,
            symbols,
            timeframe: CandleInterval::FifteenMinutes,
            suggested_notional,
            max_signal_age_ms: 2_700_000,
            cooldown_seconds: 1_800,
            lookback_candles: 20,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            breakout_lookback_candles: None,
            lower_band_pct: Some(Decimal::new(20, 0)),
            upper_band_pct: Some(Decimal::new(80, 0)),
            min_range_width_pct: Some(Decimal::new(15, 2)),
            max_range_width_pct: Some(Decimal::new(3, 0)),
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(5),
            notes: Some("Research baseline range-reversion config".to_string()),
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
        StrategyId::TrendFilterMomentumV1 => Decimal::new(68, 2),
        StrategyId::VolatilityBreakoutV2 => Decimal::new(72, 2),
        StrategyId::RangeReversionV1 => Decimal::new(66, 2),
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
        build_default_strategy_configs, diagnose, evaluate, required_candle_count,
        validate_strategy_config, StrategyValidationContext,
    };
    use aegis_core::{
        Candle, CandleInterval, MarketDataSource, SignalReason, StrategyConfigUpdateRequest,
        StrategyConfigValidationSeverity, StrategyDiagnosticsDecision, StrategyEvaluationContext,
        StrategyId, StrategyMode, StrategyNoSignalReason, Symbol,
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

    fn range_candle(
        index: i64,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
    ) -> Candle {
        let open_time =
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + Duration::minutes(index);
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            interval: CandleInterval::FifteenMinutes,
            open_time,
            close_time: open_time + Duration::minutes(15),
            open,
            high,
            low,
            close,
            volume: Decimal::new(10, 0),
            quote_volume: Some(Decimal::new(1000, 0)),
            trade_count: 5,
            is_closed: true,
            created_at: open_time,
            updated_at: open_time,
        }
    }

    fn range_reversion_candles(latest_close: Decimal, latest_open: Decimal) -> Vec<Candle> {
        let mut candles = (0..19)
            .map(|index| {
                range_candle(
                    index,
                    Decimal::new(101, 0),
                    Decimal::new(102, 0),
                    Decimal::new(100, 0),
                    Decimal::new(101, 0),
                )
            })
            .collect::<Vec<_>>();
        candles.push(range_candle(
            19,
            Decimal::new(1005, 1),
            Decimal::new(102, 0),
            Decimal::new(100, 0),
            Decimal::new(1002, 1),
        ));
        candles.push(range_candle(
            20,
            latest_open,
            Decimal::new(102, 0),
            Decimal::new(100, 0),
            latest_close,
        ));
        candles
    }

    fn context(strategy_id: StrategyId, candles: Vec<Candle>) -> StrategyEvaluationContext {
        let evaluated_at = candles
            .last()
            .map(|candle| candle.close_time + Duration::seconds(1))
            .unwrap_or_else(Utc::now);
        context_at(strategy_id, candles, evaluated_at)
    }

    fn context_at(
        strategy_id: StrategyId,
        candles: Vec<Candle>,
        evaluated_at: chrono::DateTime<Utc>,
    ) -> StrategyEvaluationContext {
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
            evaluated_at,
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
            max_signal_age_ms: 180_000,
            cooldown_seconds: 900,
            lookback_candles: 3,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            breakout_lookback_candles: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
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
    fn momentum_diagnostics_explain_no_signal() {
        let candles = vec![
            sample_candle(0, 100, 101, true),
            sample_candle(1, 101, 102, true),
            sample_candle(2, 99, 102, true),
            sample_candle(3, 100, 101, true),
        ];

        let result =
            diagnose(context(StrategyId::MomentumV1, candles)).expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::MomentumNotStrictlyHigherCloses)
        );
        assert!(result.summary.contains("not strictly increasing"));
    }

    #[test]
    fn momentum_diagnostics_explain_would_signal() {
        let candles = vec![
            sample_candle(0, 100, 101, true),
            sample_candle(1, 101, 102, true),
            sample_candle(2, 103, 104, true),
            sample_candle(3, 106, 107, true),
        ];

        let result =
            diagnose(context(StrategyId::MomentumV1, candles)).expect("diagnostics should succeed");

        assert_eq!(
            result.final_decision,
            StrategyDiagnosticsDecision::WouldSignal
        );
        assert_eq!(result.no_signal_reason, None);
        assert!(result.summary.contains("would signal"));
    }

    #[test]
    fn diagnostics_explain_insufficient_candles() {
        let candles = vec![
            sample_candle(0, 100, 101, true),
            sample_candle(1, 101, 102, true),
        ];

        let result =
            diagnose(context(StrategyId::MomentumV1, candles)).expect("diagnostics should succeed");

        assert_eq!(
            result.final_decision,
            StrategyDiagnosticsDecision::InsufficientData
        );
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::InsufficientCandles)
        );
        assert!(result.summary.contains("below the required"));
    }

    #[test]
    fn breakout_diagnostics_explain_no_signal() {
        let mut candles = (0..20)
            .map(|index| sample_candle(index, 100 + index, 120 + index, true))
            .collect::<Vec<_>>();
        candles.push(sample_candle(20, 120, 121, true));

        let result = diagnose(context(StrategyId::VolatilityBreakoutV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::BreakoutNotAboveRecentHigh)
        );
        assert!(result.summary.contains("not above recent high"));
    }

    #[test]
    fn breakout_diagnostics_explain_would_signal() {
        let mut candles = (0..20)
            .map(|index| sample_candle(index, 100 + index, 101 + index, true))
            .collect::<Vec<_>>();
        candles.push(sample_candle(20, 130, 131, true));

        let result = diagnose(context(StrategyId::VolatilityBreakoutV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(
            result.final_decision,
            StrategyDiagnosticsDecision::WouldSignal
        );
        assert_eq!(result.no_signal_reason, None);
        assert!(result.summary.contains("would signal"));
    }

    #[test]
    fn trend_filter_momentum_emits_signal_when_close_above_sma_and_momentum_passes() {
        let mut candles = (0..20)
            .map(|index| sample_candle(index, 100 + index, 101 + index, true))
            .collect::<Vec<_>>();
        candles.push(sample_candle(20, 130, 131, true));

        let result = evaluate(context(StrategyId::TrendFilterMomentumV1, candles))
            .expect("evaluation should succeed");

        assert!(result.generated);
        assert_eq!(result.reason, SignalReason::TrendFilterMomentum);
    }

    #[test]
    fn trend_filter_momentum_no_signal_when_below_sma() {
        let mut candles = (0..20)
            .map(|index| sample_candle(index, 120, 121, true))
            .collect::<Vec<_>>();
        candles.push(sample_candle(20, 110, 121, true));

        let result = evaluate(context(StrategyId::TrendFilterMomentumV1, candles))
            .expect("evaluation should succeed");

        assert!(!result.generated);
        assert_eq!(result.reason, SignalReason::ConditionsNotMet);
    }

    #[test]
    fn trend_filter_momentum_no_signal_when_latest_close_not_above_previous() {
        let mut candles = (0..20)
            .map(|index| sample_candle(index, 100 + index, 101 + index, true))
            .collect::<Vec<_>>();
        candles.push(sample_candle(20, 119, 121, true));

        let result = evaluate(context(StrategyId::TrendFilterMomentumV1, candles))
            .expect("evaluation should succeed");

        assert!(!result.generated);
        assert_eq!(result.reason, SignalReason::ConditionsNotMet);
    }

    #[test]
    fn trend_filter_momentum_reports_insufficient_data() {
        let candles = (0..5)
            .map(|index| sample_candle(index, 100 + index, 101 + index, true))
            .collect::<Vec<_>>();

        let result = diagnose(context(StrategyId::TrendFilterMomentumV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(
            result.final_decision,
            StrategyDiagnosticsDecision::InsufficientData
        );
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::InsufficientCandles)
        );
    }

    #[test]
    fn trend_filter_momentum_reports_stale_data() {
        let candles = (0..21)
            .map(|index| sample_candle(index, 100 + index, 101 + index, true))
            .collect::<Vec<_>>();
        let evaluated_at = candles.last().unwrap().close_time + Duration::minutes(20);

        let result = diagnose(context_at(
            StrategyId::TrendFilterMomentumV1,
            candles,
            evaluated_at,
        ))
        .expect("diagnostics should succeed");

        assert_eq!(
            result.final_decision,
            StrategyDiagnosticsDecision::StaleData
        );
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::StaleData)
        );
    }

    #[test]
    fn trend_filter_momentum_diagnostics_explain_below_sma() {
        let mut candles = (0..20)
            .map(|index| sample_candle(index, 120, 121, true))
            .collect::<Vec<_>>();
        candles.push(sample_candle(20, 110, 121, true));

        let result = diagnose(context(StrategyId::TrendFilterMomentumV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::TrendCloseNotAboveSma)
        );
        assert!(result.summary.contains("not above SMA"));
    }

    #[test]
    fn trend_filter_momentum_validation_rejects_invalid_lookbacks() {
        let mut request = sample_request("trend_filter_momentum_v1");
        request.lookback_candles = 20;
        request.trend_lookback_candles = Some(0);
        request.momentum_lookback_candles = Some(0);

        let result = validate_strategy_config(&request, &validation_context());

        assert!(!result.valid);
        assert!(result.issues.iter().any(|issue| {
            issue.severity == StrategyConfigValidationSeverity::Error
                && issue.code == "invalid_trend_lookback_candles"
        }));
    }

    #[test]
    fn range_reversion_emits_buy_near_lower_range_with_reversal() {
        let candles = range_reversion_candles(Decimal::new(1004, 1), Decimal::new(1003, 1));

        let result = evaluate(context(StrategyId::RangeReversionV1, candles))
            .expect("evaluation should succeed");

        assert!(result.generated);
        assert_eq!(result.reason, SignalReason::RangeReversion);
    }

    #[test]
    fn range_reversion_no_signal_when_not_near_lower_band() {
        let candles = range_reversion_candles(Decimal::new(101, 0), Decimal::new(1005, 1));

        let result = diagnose(context(StrategyId::RangeReversionV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::NotNearLowerBand)
        );
    }

    #[test]
    fn range_reversion_no_signal_when_range_too_narrow() {
        let mut candles = range_reversion_candles(Decimal::new(10004, 2), Decimal::new(10003, 2));
        for candle in &mut candles {
            candle.high = Decimal::new(1001, 1);
            candle.low = Decimal::new(100, 0);
        }

        let result = diagnose(context(StrategyId::RangeReversionV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::RangeTooNarrow)
        );
    }

    #[test]
    fn range_reversion_no_signal_when_range_too_wide() {
        let mut candles = range_reversion_candles(Decimal::new(1004, 1), Decimal::new(1003, 1));
        for candle in &mut candles {
            candle.high = Decimal::new(110, 0);
            candle.low = Decimal::new(100, 0);
        }

        let result = diagnose(context(StrategyId::RangeReversionV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::RangeTooWide)
        );
    }

    #[test]
    fn range_reversion_no_signal_without_reversal_confirmation() {
        let candles = range_reversion_candles(Decimal::new(1001, 1), Decimal::new(1002, 1));

        let result = diagnose(context(StrategyId::RangeReversionV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::NoReversalConfirmation)
        );
    }

    #[test]
    fn range_reversion_reports_insufficient_data() {
        let candles = vec![range_candle(
            0,
            Decimal::new(100, 0),
            Decimal::new(101, 0),
            Decimal::new(99, 0),
            Decimal::new(100, 0),
        )];

        let result = diagnose(context(StrategyId::RangeReversionV1, candles))
            .expect("diagnostics should succeed");

        assert_eq!(
            result.final_decision,
            StrategyDiagnosticsDecision::InsufficientData
        );
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::InsufficientData)
        );
    }

    #[test]
    fn range_reversion_reports_stale_data() {
        let candles = range_reversion_candles(Decimal::new(1004, 1), Decimal::new(1003, 1));
        let evaluated_at = candles.last().unwrap().close_time + Duration::minutes(60);

        let result = diagnose(context_at(
            StrategyId::RangeReversionV1,
            candles,
            evaluated_at,
        ))
        .expect("diagnostics should succeed");

        assert_eq!(
            result.final_decision,
            StrategyDiagnosticsDecision::StaleData
        );
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::StaleData)
        );
    }

    #[test]
    fn range_reversion_validation_rejects_invalid_band_config() {
        let mut request = sample_request("range_reversion_v1");
        request.lookback_candles = 20;
        request.lower_band_pct = Some(Decimal::new(60, 0));
        request.upper_band_pct = Some(Decimal::new(50, 0));

        let result = validate_strategy_config(&request, &validation_context());

        assert!(!result.valid);
        assert!(result
            .issues
            .iter()
            .any(|issue| issue.code == "invalid_lower_band_pct"));
        assert!(result
            .issues
            .iter()
            .any(|issue| issue.code == "invalid_range_band_order"));
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
    fn default_strategy_configs_use_safer_1m_signal_age() {
        let configs = build_default_strategy_configs(
            vec![Symbol::new("BTCUSDT").expect("valid symbol")],
            CandleInterval::OneMinute,
            Decimal::new(100_000, 0),
            3,
            20,
        );
        assert!(configs
            .iter()
            .filter(|config| {
                matches!(
                    config.strategy_id,
                    StrategyId::MomentumV1 | StrategyId::VolatilityBreakoutV1
                )
            })
            .all(|config| config.max_signal_age_ms == 120_000));
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
    fn unsupported_timeframe_rejected() {
        let mut request = sample_request("momentum_v1");
        request.timeframe = "2m".to_string();
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

    #[test]
    fn one_minute_strategy_with_low_signal_age_emits_warning() {
        let mut request = sample_request("momentum_v1");
        request.max_signal_age_ms = 5_000;
        let result = validate_strategy_config(&request, &validation_context());
        assert!(result.valid);
        assert!(result.issues.iter().any(|issue| {
            issue.severity == StrategyConfigValidationSeverity::Warn
                && issue.code == "max_signal_age_ms_too_low_for_1m"
        }));
    }

    #[test]
    fn higher_timeframe_is_accepted_with_scaled_signal_age() {
        let mut request = sample_request("momentum_v1");
        request.timeframe = "5m".to_string();
        request.max_signal_age_ms = 600_000;
        let result = validate_strategy_config(&request, &validation_context());
        assert!(result.valid);
        assert!(result
            .issues
            .iter()
            .all(|issue| issue.code != "unsupported_timeframe"));
    }

    #[test]
    fn one_minute_strategy_with_180000_signal_age_has_no_warning() {
        let request = sample_request("momentum_v1");
        let result = validate_strategy_config(&request, &validation_context());
        assert!(result.valid);
        assert!(!result.issues.iter().any(|issue| {
            issue.severity == StrategyConfigValidationSeverity::Warn
                && issue.field == "max_signal_age_ms"
        }));
    }

    #[test]
    fn max_signal_age_outside_hard_bounds_is_still_rejected() {
        let mut request = sample_request("momentum_v1");
        request.max_signal_age_ms = 999;
        let result = validate_strategy_config(&request, &validation_context());
        assert!(!result.valid);
        assert!(result.issues.iter().any(|issue| {
            issue.severity == StrategyConfigValidationSeverity::Error
                && issue.code == "invalid_max_signal_age_ms"
        }));
    }

    #[test]
    fn high_signal_age_for_higher_timeframe_emits_warning() {
        let mut request = sample_request("momentum_v1");
        request.timeframe = "1h".to_string();
        request.max_signal_age_ms = 10_800_001;
        let result = validate_strategy_config(&request, &validation_context());
        assert!(result.valid);
        assert!(result.issues.iter().any(|issue| {
            issue.severity == StrategyConfigValidationSeverity::Warn
                && issue.code == "max_signal_age_ms_high_for_1h"
        }));
    }
}

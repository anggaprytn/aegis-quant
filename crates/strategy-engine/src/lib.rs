use aegis_core::{
    Candle, CandleInterval, CoreError, SignalConfidence, SignalReason, SignalSide, StrategyConfig,
    StrategyConfigUpdateRequest, StrategyConfigValidationIssue, StrategyConfigValidationResult,
    StrategyConfigValidationSeverity, StrategyDataHealth, StrategyDiagnosticCheck,
    StrategyDiagnosticSeverity, StrategyDiagnosticsDecision, StrategyDiagnosticsResult,
    StrategyEvaluationContext, StrategyEvaluationResult, StrategyId, StrategyMode,
    StrategyNoSignalReason, StrategyOpportunityAnalysisRequest, StrategyOpportunityAnalysisResult,
    StrategyOpportunityRecommendation, StrategyOpportunityStatus, StrategyOpportunityWindowExample,
    StrategySignal,
};
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::json;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct StrategyValidationContext {
    pub supported_symbols: Vec<aegis_core::Symbol>,
    pub max_position_notional: Option<Decimal>,
}

pub fn known_strategy_ids() -> [StrategyId; 8] {
    [
        StrategyId::MomentumV1,
        StrategyId::VolatilityBreakoutV1,
        StrategyId::TrendFilterMomentumV1,
        StrategyId::TrendFilterMomentumV2,
        StrategyId::VolatilityBreakoutV2,
        StrategyId::VolatilityCompressionBreakoutV1,
        StrategyId::RangeReversionV1,
        StrategyId::TrendPullbackContinuationV1,
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
                "strategy_id must be one of momentum_v1, volatility_breakout_v1, trend_filter_momentum_v2, trend_filter_momentum_v1, volatility_breakout_v2, volatility_compression_breakout_v1, range_reversion_v1, or trend_pullback_continuation_v1",
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
        StrategyId::TrendFilterMomentumV1 | StrategyId::TrendFilterMomentumV2 => 2..=500,
        StrategyId::VolatilityBreakoutV2 => 5..=500,
        StrategyId::VolatilityCompressionBreakoutV1 => 2..=500,
        StrategyId::RangeReversionV1 => 2..=500,
        StrategyId::TrendPullbackContinuationV1 => 2..=500,
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
    if let Some(compression_lookback) = request.compression_lookback_candles {
        if compression_lookback <= 1 {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_compression_lookback_candles",
                "compression_lookback_candles",
                "compression_lookback_candles must be greater than 1",
            ));
        }
    }
    if strategy_id == StrategyId::TrendFilterMomentumV2 {
        let min_close_above_sma_pct = request.min_close_above_sma_pct.unwrap_or(Decimal::ZERO);
        let max_close_above_sma_pct = request.max_close_above_sma_pct.unwrap_or(Decimal::ONE);
        let min_momentum_return_pct = request.min_momentum_return_pct.unwrap_or(Decimal::ZERO);
        if min_close_above_sma_pct < Decimal::ZERO {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_min_close_above_sma_pct",
                "min_close_above_sma_pct",
                "min_close_above_sma_pct must be greater than or equal to 0",
            ));
        }
        if max_close_above_sma_pct < min_close_above_sma_pct {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_close_above_sma_band",
                "max_close_above_sma_pct",
                "max_close_above_sma_pct must be greater than or equal to min_close_above_sma_pct",
            ));
        }
        if min_momentum_return_pct < Decimal::ZERO {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_min_momentum_return_pct",
                "min_momentum_return_pct",
                "min_momentum_return_pct must be greater than or equal to 0",
            ));
        }
    }
    if let Some(breakout_lookback) = request.breakout_lookback_candles {
        if breakout_lookback <= 1 {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_breakout_lookback_candles",
                "breakout_lookback_candles",
                "breakout_lookback_candles must be greater than 1",
            ));
        }
    }
    if let Some(pullback_lookback) = request.pullback_lookback_candles {
        if pullback_lookback <= 1 {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_pullback_lookback_candles",
                "pullback_lookback_candles",
                "pullback_lookback_candles must be greater than 1",
            ));
        }
    }
    if let Some(pullback_sma_lookback) = request.pullback_sma_lookback_candles {
        if pullback_sma_lookback <= 1 {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_pullback_sma_lookback_candles",
                "pullback_sma_lookback_candles",
                "pullback_sma_lookback_candles must be greater than 1",
            ));
        }
    }

    if strategy_id == StrategyId::TrendPullbackContinuationV1 {
        let trend_lookback = request.trend_lookback_candles.unwrap_or(50);
        let pullback_lookback = request.pullback_lookback_candles.unwrap_or(10);
        let pullback_sma_lookback = request.pullback_sma_lookback_candles.unwrap_or(20);
        let min_pullback_depth = request.min_pullback_depth_pct.unwrap_or(Decimal::new(3, 1));
        let max_pullback_depth = request.max_pullback_depth_pct.unwrap_or(Decimal::new(5, 0));
        let max_close_above_sma = request.max_close_above_sma_pct.unwrap_or(Decimal::ONE);
        let min_volume_ratio = request.min_volume_ratio.unwrap_or(Decimal::new(8, 1));
        let max_choppiness = request.max_choppiness.unwrap_or(Decimal::new(60, 0));

        if trend_lookback <= 1 || pullback_lookback <= 1 || pullback_sma_lookback <= 1 {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_pullback_lookbacks",
                "trend_lookback_candles",
                "trend, pullback, and pullback SMA lookbacks must be greater than 1",
            ));
        }
        if trend_lookback < pullback_lookback {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "trend_lookback_below_pullback_lookback",
                "trend_lookback_candles",
                "trend_lookback_candles must be greater than or equal to pullback_lookback_candles",
            ));
        }
        if max_pullback_depth <= min_pullback_depth {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_pullback_depth_bounds",
                "max_pullback_depth_pct",
                "max_pullback_depth_pct must be greater than min_pullback_depth_pct",
            ));
        }
        if max_close_above_sma < Decimal::ZERO {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_max_close_above_sma_pct",
                "max_close_above_sma_pct",
                "max_close_above_sma_pct must be greater than or equal to 0",
            ));
        }
        if min_volume_ratio < Decimal::ZERO {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_min_volume_ratio",
                "min_volume_ratio",
                "min_volume_ratio must be greater than or equal to 0",
            ));
        }
        if max_choppiness <= Decimal::ZERO {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_max_choppiness",
                "max_choppiness",
                "max_choppiness must be greater than 0",
            ));
        }
        if request.max_signal_age_ms > recommended_max {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                &format!("max_signal_age_ms_unreasonable_for_{}", timeframe.as_str()),
                "max_signal_age_ms",
                &format!(
                    "max_signal_age_ms must be at most {}ms for {} pullback continuation research",
                    recommended_max,
                    timeframe.as_str()
                ),
            ));
        }
    }

    if strategy_id == StrategyId::VolatilityCompressionBreakoutV1 {
        let compression_lookback = request.compression_lookback_candles.unwrap_or(20);
        let breakout_lookback = request.breakout_lookback_candles.unwrap_or(20);
        let compression_threshold = request
            .compression_percentile_threshold
            .unwrap_or(Decimal::new(25, 0));
        let min_breakout = request.min_breakout_pct.unwrap_or(Decimal::new(5, 2));
        let max_extension = request
            .max_breakout_extension_pct
            .unwrap_or(Decimal::new(15, 1));
        let min_volume_ratio = request
            .min_volume_expansion_ratio
            .unwrap_or(Decimal::new(11, 1));
        let min_width = request.min_range_width_pct.unwrap_or(Decimal::new(2, 1));
        let max_width = request.max_range_width_pct.unwrap_or(Decimal::new(5, 0));

        if compression_lookback > breakout_lookback {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "compression_lookback_above_breakout_lookback",
                "compression_lookback_candles",
                "compression_lookback_candles must be less than or equal to breakout_lookback_candles",
            ));
        }
        if compression_threshold <= Decimal::ZERO || compression_threshold > Decimal::new(100, 0) {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_compression_percentile_threshold",
                "compression_percentile_threshold",
                "compression_percentile_threshold must be greater than 0 and at most 100",
            ));
        }
        if min_breakout < Decimal::ZERO {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_min_breakout_pct",
                "min_breakout_pct",
                "min_breakout_pct must be greater than or equal to 0",
            ));
        }
        if max_extension <= min_breakout {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_max_breakout_extension_pct",
                "max_breakout_extension_pct",
                "max_breakout_extension_pct must be greater than min_breakout_pct",
            ));
        }
        if min_volume_ratio < Decimal::ONE {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_min_volume_expansion_ratio",
                "min_volume_expansion_ratio",
                "min_volume_expansion_ratio must be greater than or equal to 1",
            ));
        }
        if min_width <= Decimal::ZERO {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_min_range_width_pct",
                "min_range_width_pct",
                "min_range_width_pct must be greater than 0",
            ));
        }
        if max_width <= min_width {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                "invalid_max_range_width_pct",
                "max_range_width_pct",
                "max_range_width_pct must be greater than min_range_width_pct",
            ));
        }
        if request.max_signal_age_ms > recommended_max {
            issues.push(issue(
                StrategyConfigValidationSeverity::Error,
                &format!(
                    "max_signal_age_ms_unreasonable_for_{}",
                    timeframe.as_str()
                ),
                "max_signal_age_ms",
                &format!(
                    "max_signal_age_ms must be at most {}ms for {} compression breakout experiments",
                    recommended_max,
                    timeframe.as_str()
                ),
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
            compression_lookback_candles: request
                .compression_lookback_candles
                .or((strategy_id == StrategyId::VolatilityCompressionBreakoutV1).then_some(20)),
            breakout_lookback_candles: request
                .breakout_lookback_candles
                .or((strategy_id == StrategyId::VolatilityCompressionBreakoutV1).then_some(20)),
            pullback_lookback_candles: request
                .pullback_lookback_candles
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1).then_some(10)),
            pullback_sma_lookback_candles: request
                .pullback_sma_lookback_candles
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1).then_some(20)),
            compression_percentile_threshold: request
                .compression_percentile_threshold
                .or((strategy_id == StrategyId::VolatilityCompressionBreakoutV1)
                    .then_some(Decimal::new(25, 0))),
            min_breakout_pct: request
                .min_breakout_pct
                .or((strategy_id == StrategyId::VolatilityCompressionBreakoutV1)
                    .then_some(Decimal::new(5, 2))),
            max_breakout_extension_pct: request
                .max_breakout_extension_pct
                .or((strategy_id == StrategyId::VolatilityCompressionBreakoutV1)
                    .then_some(Decimal::new(15, 1))),
            min_volume_expansion_ratio: request
                .min_volume_expansion_ratio
                .or((strategy_id == StrategyId::VolatilityCompressionBreakoutV1)
                    .then_some(Decimal::new(11, 1))),
            lower_band_pct: request
                .lower_band_pct
                .or((strategy_id == StrategyId::RangeReversionV1).then_some(lower_band_pct)),
            upper_band_pct: request
                .upper_band_pct
                .or((strategy_id == StrategyId::RangeReversionV1).then_some(upper_band_pct)),
            min_range_width_pct: request
                .min_range_width_pct
                .or((strategy_id == StrategyId::RangeReversionV1).then_some(min_range_width_pct))
                .or((strategy_id == StrategyId::VolatilityCompressionBreakoutV1)
                    .then_some(Decimal::new(2, 1))),
            max_range_width_pct: request
                .max_range_width_pct
                .or((strategy_id == StrategyId::RangeReversionV1).then_some(max_range_width_pct))
                .or((strategy_id == StrategyId::VolatilityCompressionBreakoutV1)
                    .then_some(Decimal::new(5, 0))),
            min_close_above_sma_pct: request
                .min_close_above_sma_pct
                .or((strategy_id == StrategyId::TrendFilterMomentumV2).then_some(Decimal::ZERO)),
            max_close_above_sma_pct: request
                .max_close_above_sma_pct
                .or((strategy_id == StrategyId::TrendFilterMomentumV2).then_some(Decimal::ONE)),
            min_momentum_return_pct: request
                .min_momentum_return_pct
                .or((strategy_id == StrategyId::TrendFilterMomentumV2).then_some(Decimal::ZERO)),
            min_trend_return_pct: request
                .min_trend_return_pct
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1)
                    .then_some(Decimal::new(2, 0))),
            min_trend_slope_pct: request
                .min_trend_slope_pct
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1)
                    .then_some(Decimal::ZERO)),
            min_pullback_depth_pct: request
                .min_pullback_depth_pct
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1)
                    .then_some(Decimal::new(3, 1))),
            max_pullback_depth_pct: request
                .max_pullback_depth_pct
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1)
                    .then_some(Decimal::new(5, 0))),
            min_reclaim_pct: request
                .min_reclaim_pct
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1)
                    .then_some(Decimal::new(5, 2))),
            min_volume_ratio: request
                .min_volume_ratio
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1)
                    .then_some(Decimal::new(8, 1))),
            max_choppiness: request
                .max_choppiness
                .or((strategy_id == StrategyId::TrendPullbackContinuationV1)
                    .then_some(Decimal::new(60, 0))),
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
        StrategyId::TrendFilterMomentumV1 | StrategyId::TrendFilterMomentumV2 => {
            let trend = trend_lookback(config) as i64 + 1;
            let momentum = momentum_lookback(config) as i64 + 1;
            trend.max(momentum).max(2)
        }
        StrategyId::VolatilityBreakoutV2 => (breakout_lookback(config) as i64 + 1).max(2),
        StrategyId::VolatilityCompressionBreakoutV1 => {
            (compression_lookback(config) as i64 + breakout_lookback(config) as i64 + 1).max(2)
        }
        StrategyId::RangeReversionV1 => (config.lookback_candles as i64 + 1).max(2),
        StrategyId::TrendPullbackContinuationV1 => {
            let trend = trend_lookback(config) as i64 + 1;
            let pullback = pullback_lookback(config) as i64 + 1;
            let sma = pullback_sma_lookback(config) as i64;
            trend.max(pullback).max(sma).max(20).max(2)
        }
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
        StrategyId::TrendFilterMomentumV2 => evaluate_trend_filter_momentum_v2(&context, candles),
        StrategyId::VolatilityBreakoutV2 => evaluate_volume_breakout(&context, candles),
        StrategyId::VolatilityCompressionBreakoutV1 => {
            evaluate_volatility_compression_breakout(&context, candles)
        }
        StrategyId::RangeReversionV1 => evaluate_range_reversion(&context, candles),
        StrategyId::TrendPullbackContinuationV1 => {
            evaluate_trend_pullback_continuation(&context, candles)
        }
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
            StrategyId::TrendFilterMomentumV2 => {
                diagnose_trend_filter_momentum_v2(&context, &candles, &mut condition_checks)
            }
            StrategyId::VolatilityBreakoutV2 => {
                diagnose_volume_breakout(&context, &candles, &mut condition_checks)
            }
            StrategyId::VolatilityCompressionBreakoutV1 => {
                diagnose_volatility_compression_breakout(&context, &candles, &mut condition_checks)
            }
            StrategyId::RangeReversionV1 => {
                diagnose_range_reversion(&context, &candles, &mut condition_checks)
            }
            StrategyId::TrendPullbackContinuationV1 => {
                diagnose_trend_pullback_continuation(&context, &candles, &mut condition_checks)
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

pub fn analyze_opportunity(
    request: &StrategyOpportunityAnalysisRequest,
    config: &StrategyConfig,
    candles: &[Candle],
    analyzed_at: chrono::DateTime<Utc>,
) -> Result<StrategyOpportunityAnalysisResult, CoreError> {
    config.validate()?;
    let strategy_id = request.strategy_id.parse::<StrategyId>()?;
    if strategy_id != config.strategy_id {
        return Err(CoreError::UnsupportedStrategyId(format!(
            "strategy config does not match opportunity target: {} != {}",
            config.strategy_id, strategy_id
        )));
    }

    let candles = normalize_closed_candles(candles);
    let total_closed_candles = candles.len() as i64;
    let required = required_candle_count(config) as usize;
    let limit_samples = request.limit_samples.unwrap_or(5);
    let mut condition_stats = ConditionStats::default();
    let mut pass_examples = Vec::new();
    let mut fail_examples = Vec::new();
    let mut would_signal_count = 0_i64;
    let mut range_positions = Vec::new();
    let mut range_widths = Vec::new();
    let mut close_vs_low = Vec::new();
    let mut close_vs_high = Vec::new();
    let mut reversal_confirmation_count = 0_i64;
    let mut close_vs_sma_values = Vec::new();
    let mut compression_ratios = Vec::new();
    let mut breakout_pcts = Vec::new();
    let mut volume_ratios = Vec::new();

    if candles.len() >= required {
        for end in required..=candles.len() {
            let window = &candles[end - required..end];
            let outcome = match strategy_id {
                StrategyId::RangeReversionV1 => analyze_range_reversion_window(
                    config,
                    window,
                    &mut range_positions,
                    &mut range_widths,
                    &mut close_vs_low,
                    &mut close_vs_high,
                    &mut reversal_confirmation_count,
                ),
                StrategyId::TrendFilterMomentumV1 => analyze_trend_filter_window(config, window),
                StrategyId::TrendFilterMomentumV2 => {
                    analyze_trend_filter_v2_window(config, window, &mut close_vs_sma_values)
                }
                StrategyId::VolatilityCompressionBreakoutV1 => {
                    analyze_volatility_compression_breakout_window(
                        config,
                        window,
                        &mut compression_ratios,
                        &mut breakout_pcts,
                        &mut volume_ratios,
                    )
                }
                StrategyId::TrendPullbackContinuationV1 => {
                    analyze_trend_pullback_continuation_window(
                        config,
                        window,
                        &mut close_vs_sma_values,
                        &mut volume_ratios,
                    )
                }
                _ => analyze_generic_window(config, strategy_id, window)?,
            };

            for condition in &outcome.conditions {
                condition_stats.record(&condition.name, condition.passed);
            }
            if let Some(blocker) = outcome.blocking_condition.as_ref() {
                condition_stats.record_blocker(blocker);
            }
            if outcome.would_signal {
                would_signal_count += 1;
                if request.include_examples && pass_examples.len() < limit_samples {
                    pass_examples.push(outcome.example());
                }
            } else if request.include_examples && fail_examples.len() < limit_samples {
                fail_examples.push(outcome.example());
            }
        }
    }

    let evaluable_windows = if candles.len() >= required {
        (candles.len() - required + 1) as i64
    } else {
        0
    };
    let no_signal_count = evaluable_windows - would_signal_count;
    let signal_rate_pct = pct_i64(would_signal_count, evaluable_windows);
    let mut condition_pass_rates = condition_stats.pass_rates(evaluable_windows);
    condition_pass_rates.sort_by(|a, b| a.condition.cmp(&b.condition));
    let condition_failure_breakdown = condition_stats.failure_breakdown(evaluable_windows);
    let top_blocking_conditions = condition_stats.top_blockers(evaluable_windows);
    let data_quality_status = if total_closed_candles < required as i64 {
        StrategyOpportunityStatus::InsufficientData
    } else {
        StrategyOpportunityStatus::HealthyOpportunity
    };
    let recommendation = build_opportunity_recommendation(
        strategy_id,
        signal_rate_pct,
        evaluable_windows,
        &top_blocking_conditions,
        &condition_pass_rates,
    );

    Ok(StrategyOpportunityAnalysisResult {
        strategy_id: strategy_id.to_string(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        total_closed_candles,
        evaluable_windows,
        would_signal_count,
        no_signal_count,
        signal_rate_pct,
        top_blocking_conditions,
        condition_pass_rates,
        condition_failure_breakdown,
        example_pass_windows: pass_examples,
        example_fail_windows: fail_examples,
        distributions: json!({
            "range_position_pct": distribution_json(&range_positions),
            "range_width_pct": distribution_json(&range_widths),
            "latest_close_vs_range_low_pct": distribution_json(&close_vs_low),
            "latest_close_vs_range_high_pct": distribution_json(&close_vs_high),
            "reversal_confirmation_count": reversal_confirmation_count,
            "close_vs_sma_pct": distribution_json(&close_vs_sma_values),
            "compression_ratio": distribution_json(&compression_ratios),
            "breakout_pct": distribution_json(&breakout_pcts),
            "volume_ratio": distribution_json(&volume_ratios),
        }),
        recommendation,
        data_quality_status,
        analyzed_at,
    })
}

#[derive(Debug)]
struct ConditionOutcome {
    name: String,
    passed: bool,
}

#[derive(Debug)]
struct WindowOutcome {
    open_time: chrono::DateTime<Utc>,
    close_time: chrono::DateTime<Utc>,
    would_signal: bool,
    blocking_condition: Option<String>,
    conditions: Vec<ConditionOutcome>,
    details: serde_json::Value,
}

impl WindowOutcome {
    fn example(&self) -> StrategyOpportunityWindowExample {
        StrategyOpportunityWindowExample {
            source_candle_open_time: self.open_time,
            source_candle_close_time: self.close_time,
            would_signal: self.would_signal,
            blocking_condition: self.blocking_condition.clone(),
            details: self.details.clone(),
        }
    }
}

#[derive(Default)]
struct ConditionStats {
    passed: BTreeMap<String, i64>,
    failed: BTreeMap<String, i64>,
    blockers: BTreeMap<String, i64>,
}

impl ConditionStats {
    fn record(&mut self, condition: &str, passed: bool) {
        let target = if passed {
            &mut self.passed
        } else {
            &mut self.failed
        };
        *target.entry(condition.to_string()).or_insert(0) += 1;
    }

    fn record_blocker(&mut self, condition: &str) {
        *self.blockers.entry(condition.to_string()).or_insert(0) += 1;
    }

    fn pass_rates(&self, evaluable_windows: i64) -> Vec<aegis_core::StrategyConditionPassRate> {
        self.all_conditions()
            .into_iter()
            .map(|condition| {
                let passed_count = *self.passed.get(&condition).unwrap_or(&0);
                let failed_count = *self.failed.get(&condition).unwrap_or(&0);
                aegis_core::StrategyConditionPassRate {
                    condition,
                    passed_count,
                    failed_count,
                    pass_rate_pct: pct_i64(passed_count, evaluable_windows),
                }
            })
            .collect()
    }

    fn failure_breakdown(
        &self,
        evaluable_windows: i64,
    ) -> Vec<aegis_core::StrategyConditionFailureBreakdown> {
        let mut rows = self
            .all_conditions()
            .into_iter()
            .map(|condition| {
                let failed_count = *self.failed.get(&condition).unwrap_or(&0);
                aegis_core::StrategyConditionFailureBreakdown {
                    condition,
                    failed_count,
                    failure_rate_pct: pct_i64(failed_count, evaluable_windows),
                }
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.failed_count
                .cmp(&a.failed_count)
                .then_with(|| a.condition.cmp(&b.condition))
        });
        rows
    }

    fn top_blockers(
        &self,
        evaluable_windows: i64,
    ) -> Vec<aegis_core::StrategyConditionFailureBreakdown> {
        let mut rows = self
            .blockers
            .iter()
            .map(
                |(condition, failed_count)| aegis_core::StrategyConditionFailureBreakdown {
                    condition: condition.clone(),
                    failed_count: *failed_count,
                    failure_rate_pct: pct_i64(*failed_count, evaluable_windows),
                },
            )
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| {
            b.failed_count
                .cmp(&a.failed_count)
                .then_with(|| a.condition.cmp(&b.condition))
        });
        rows.truncate(5);
        rows
    }

    fn all_conditions(&self) -> Vec<String> {
        let mut conditions = self
            .passed
            .keys()
            .chain(self.failed.keys())
            .cloned()
            .collect::<Vec<_>>();
        conditions.sort();
        conditions.dedup();
        conditions
    }
}

fn analyze_range_reversion_window(
    config: &StrategyConfig,
    window: &[Candle],
    range_positions: &mut Vec<Decimal>,
    range_widths: &mut Vec<Decimal>,
    close_vs_low: &mut Vec<Decimal>,
    close_vs_high: &mut Vec<Decimal>,
    reversal_confirmation_count: &mut i64,
) -> WindowOutcome {
    let latest = window.last().expect("window must contain latest candle");
    let previous = &window[window.len() - 2];
    let lookback = config.lookback_candles as usize;
    let range_window = &window[window.len() - lookback..];
    let range = calculate_range_metrics(range_window);
    let min_width = min_range_width_pct(config);
    let max_width = max_range_width_pct(config);
    let lower_band = lower_band_pct(config);
    let width_within_bounds =
        range.range_width_pct >= min_width && range.range_width_pct <= max_width;
    let near_lower_band = range.range_position_pct <= lower_band;
    let reversal_confirmation = latest.close > previous.close || latest.close > latest.open;
    let latest_low_not_undercutting_previous_low = latest.low >= previous.low;
    let raw_would_signal = width_within_bounds
        && near_lower_band
        && reversal_confirmation
        && latest_low_not_undercutting_previous_low;
    let confidence = Decimal::new(66, 2);
    let confidence_floor = config.confidence_floor.unwrap_or(Decimal::ZERO);
    let confidence_floor_passed = confidence >= confidence_floor;
    let final_would_signal = raw_would_signal && confidence_floor_passed;

    range_positions.push(range.range_position_pct);
    range_widths.push(range.range_width_pct);
    close_vs_low.push(pct_ratio(latest.close - range.range_low, range.range_low));
    close_vs_high.push(pct_ratio(range.range_high - latest.close, range.range_high));
    if reversal_confirmation {
        *reversal_confirmation_count += 1;
    }

    let conditions = vec![
        condition("has_enough_data", true),
        condition("range_width_within_bounds", width_within_bounds),
        condition("near_lower_band", near_lower_band),
        condition("reversal_confirmation", reversal_confirmation),
        condition(
            "latest_low_not_undercutting_previous_low",
            latest_low_not_undercutting_previous_low,
        ),
        condition("confidence_floor", confidence_floor_passed),
        condition("freshness", true),
        condition("final_would_signal", final_would_signal),
    ];
    WindowOutcome {
        open_time: latest.open_time,
        close_time: latest.close_time,
        would_signal: final_would_signal,
        blocking_condition: first_failed(&conditions),
        conditions,
        details: json!({
            "range_position_pct": range.range_position_pct,
            "range_width_pct": range.range_width_pct,
            "latest_close_vs_range_low_pct": pct_ratio(latest.close - range.range_low, range.range_low),
            "latest_close_vs_range_high_pct": pct_ratio(range.range_high - latest.close, range.range_high),
            "range_low": range.range_low,
            "range_high": range.range_high,
            "latest_close": latest.close,
            "previous_close": previous.close,
            "latest_low": latest.low,
            "previous_low": previous.low,
            "confidence": confidence,
            "confidence_floor": config.confidence_floor,
        }),
    }
}

fn analyze_trend_filter_window(config: &StrategyConfig, window: &[Candle]) -> WindowOutcome {
    let latest = window.last().expect("window must contain latest candle");
    let previous = &window[window.len() - 2];
    let trend = trend_lookback(config) as usize;
    let momentum = momentum_lookback(config) as usize;
    let trend_window = &window[window.len() - trend - 1..window.len() - 1];
    let sma = average_decimal(trend_window.iter().map(|candle| candle.close));
    let momentum_reference = &window[window.len() - momentum - 1];
    let close_above_sma = latest.close > sma;
    let latest_close_above_previous_close = latest.close > previous.close;
    let momentum_condition = latest.close > momentum_reference.close;
    let raw_would_signal =
        close_above_sma && latest_close_above_previous_close && momentum_condition;
    let confidence = Decimal::new(68, 2);
    let confidence_floor = config.confidence_floor.unwrap_or(Decimal::ZERO);
    let confidence_floor_passed = confidence >= confidence_floor;
    let final_would_signal = raw_would_signal && confidence_floor_passed;
    let conditions = vec![
        condition("has_enough_data", true),
        condition("close_above_sma", close_above_sma),
        condition(
            "latest_close_above_previous_close",
            latest_close_above_previous_close,
        ),
        condition("momentum_condition", momentum_condition),
        condition("confidence_floor", confidence_floor_passed),
        condition("freshness", true),
        condition("final_would_signal", final_would_signal),
    ];
    WindowOutcome {
        open_time: latest.open_time,
        close_time: latest.close_time,
        would_signal: final_would_signal,
        blocking_condition: first_failed(&conditions),
        conditions,
        details: json!({
            "latest_close": latest.close,
            "previous_close": previous.close,
            "sma": sma,
            "momentum_reference_close": momentum_reference.close,
            "confidence": confidence,
            "confidence_floor": config.confidence_floor,
        }),
    }
}

fn analyze_trend_filter_v2_window(
    config: &StrategyConfig,
    window: &[Candle],
    close_vs_sma_values: &mut Vec<Decimal>,
) -> WindowOutcome {
    let latest = window.last().expect("window must contain latest candle");
    let trend = trend_lookback(config) as usize;
    let momentum = momentum_lookback(config) as usize;
    let trend_window = &window[window.len() - trend - 1..window.len() - 1];
    let sma = average_decimal(trend_window.iter().map(|candle| candle.close));
    let momentum_reference = &window[window.len() - momentum - 1];
    let close_vs_sma_pct = pct_ratio(latest.close - sma, sma);
    let momentum_return_pct = pct_ratio(
        latest.close - momentum_reference.close,
        momentum_reference.close,
    );
    close_vs_sma_values.push(close_vs_sma_pct);

    let min_band = min_close_above_sma_pct(config);
    let max_band = max_close_above_sma_pct(config);
    let min_momentum = min_momentum_return_pct(config);
    let valid_config = max_band >= min_band;
    let close_above_sma = latest.close > sma;
    let close_within_sma_band =
        close_vs_sma_pct >= min_band && close_vs_sma_pct <= max_band && valid_config;
    let momentum_confirmed =
        latest.close > momentum_reference.close && momentum_return_pct >= min_momentum;
    let raw_would_signal =
        valid_config && close_above_sma && close_within_sma_band && momentum_confirmed;
    let confidence = Decimal::new(68, 2);
    let confidence_floor = config.confidence_floor.unwrap_or(Decimal::ZERO);
    let confidence_floor_passed = confidence >= confidence_floor;
    let final_would_signal = raw_would_signal && confidence_floor_passed;
    let conditions = vec![
        condition("has_enough_data", true),
        condition("valid_config", valid_config),
        condition("close_above_sma", close_above_sma),
        condition("close_within_sma_band", close_within_sma_band),
        condition("momentum_confirmed", momentum_confirmed),
        condition("confidence_floor", confidence_floor_passed),
        condition("freshness", true),
        condition("final_would_signal", final_would_signal),
    ];
    WindowOutcome {
        open_time: latest.open_time,
        close_time: latest.close_time,
        would_signal: final_would_signal,
        blocking_condition: first_failed(&conditions),
        conditions,
        details: json!({
            "latest_close": latest.close,
            "sma": sma,
            "close_vs_sma_pct": close_vs_sma_pct,
            "min_close_above_sma_pct": min_band,
            "max_close_above_sma_pct": max_band,
            "momentum_reference_close": momentum_reference.close,
            "momentum_return_pct": momentum_return_pct,
            "min_momentum_return_pct": min_momentum,
            "confidence": confidence,
            "confidence_floor": config.confidence_floor,
        }),
    }
}

fn analyze_volatility_compression_breakout_window(
    config: &StrategyConfig,
    window: &[Candle],
    compression_ratios: &mut Vec<Decimal>,
    breakout_pcts: &mut Vec<Decimal>,
    volume_ratios: &mut Vec<Decimal>,
) -> WindowOutcome {
    let latest = window.last().expect("window must contain latest candle");
    let metrics = calculate_compression_breakout_metrics(config, window);

    let valid_config = validate_compression_breakout_config(config).is_none();
    let compression_passed = valid_config && metrics.compression_passed;
    let range_width_passed = valid_config && metrics.range_width_within_bounds;
    let breakout_passed = valid_config && latest.close > metrics.breakout_level;
    let breakout_large_enough = valid_config && metrics.breakout_pct >= min_breakout_pct(config);
    let breakout_not_overextended =
        valid_config && metrics.breakout_pct <= max_breakout_extension_pct(config);
    let volume_confirmed =
        valid_config && metrics.volume_ratio >= min_volume_expansion_ratio(config);
    let bullish_close = latest.close > latest.open;
    let final_would_signal = compression_passed
        && range_width_passed
        && breakout_passed
        && breakout_large_enough
        && breakout_not_overextended
        && volume_confirmed
        && bullish_close;

    compression_ratios.push(metrics.compression_ratio);
    breakout_pcts.push(metrics.breakout_pct);
    volume_ratios.push(metrics.volume_ratio);

    let conditions = vec![
        condition("has_enough_data", true),
        condition("valid_config", valid_config),
        condition("compression_passed", compression_passed),
        condition("range_width_within_bounds", range_width_passed),
        condition("breakout_passed", breakout_passed),
        condition("breakout_large_enough", breakout_large_enough),
        condition("breakout_not_overextended", breakout_not_overextended),
        condition("volume_confirmed", volume_confirmed),
        condition("bullish_close", bullish_close),
        condition("freshness", true),
        condition("final_would_signal", final_would_signal),
    ];
    WindowOutcome {
        open_time: latest.open_time,
        close_time: latest.close_time,
        would_signal: final_would_signal,
        blocking_condition: first_failed(&conditions),
        conditions,
        details: json!({
            "compression_lookback": compression_lookback(config),
            "breakout_lookback": breakout_lookback(config),
            "recent_avg_range_pct": metrics.recent_avg_range_pct,
            "baseline_avg_range_pct": metrics.baseline_avg_range_pct,
            "compression_ratio": metrics.compression_ratio,
            "compression_passed": compression_passed,
            "breakout_level": metrics.breakout_level,
            "latest_close": latest.close,
            "breakout_pct": metrics.breakout_pct,
            "breakout_passed": breakout_passed,
            "breakout_not_overextended": breakout_not_overextended,
            "volume_ratio": metrics.volume_ratio,
            "volume_confirmed": volume_confirmed,
            "range_width_pct": metrics.range_width_pct,
            "final_would_signal": final_would_signal,
        }),
    }
}

fn analyze_trend_pullback_continuation_window(
    config: &StrategyConfig,
    window: &[Candle],
    close_vs_sma_values: &mut Vec<Decimal>,
    volume_ratios: &mut Vec<Decimal>,
) -> WindowOutcome {
    let latest = window.last().expect("window must contain latest candle");
    let metrics = calculate_trend_pullback_metrics(config, window);
    close_vs_sma_values.push(metrics.close_vs_sma_pct);
    volume_ratios.push(metrics.volume_ratio);

    let valid_config = validate_trend_pullback_config(config).is_none();
    let trend_confirmed = valid_config
        && metrics.trend_return_pct >= min_trend_return_pct(config)
        && metrics.trend_slope_pct >= min_trend_slope_pct(config);
    let pullback_depth_valid = valid_config
        && metrics.pullback_depth_pct >= min_pullback_depth_pct(config)
        && metrics.pullback_depth_pct <= max_pullback_depth_pct(config);
    let close_near_sma = valid_config
        && metrics.close_vs_sma_pct >= Decimal::ZERO
        && metrics.close_vs_sma_pct <= max_close_above_sma_pct(config);
    let reclaim_confirmed = valid_config && metrics.reclaim_confirmed;
    let volume_confirmed = valid_config && metrics.volume_ratio >= min_volume_ratio(config);
    let choppiness_valid = valid_config && metrics.choppiness <= max_choppiness(config);
    let final_would_signal = trend_confirmed
        && pullback_depth_valid
        && close_near_sma
        && reclaim_confirmed
        && volume_confirmed
        && choppiness_valid;
    let conditions = vec![
        condition("has_enough_data", true),
        condition("valid_config", valid_config),
        condition("trend_confirmed", trend_confirmed),
        condition("pullback_depth_valid", pullback_depth_valid),
        condition("close_near_sma", close_near_sma),
        condition("reclaim_confirmed", reclaim_confirmed),
        condition("volume_confirmed", volume_confirmed),
        condition("choppiness_valid", choppiness_valid),
        condition("freshness", true),
        condition("final_would_signal", final_would_signal),
    ];
    WindowOutcome {
        open_time: latest.open_time,
        close_time: latest.close_time,
        would_signal: final_would_signal,
        blocking_condition: first_failed(&conditions),
        conditions,
        details: json!({
            "trend_return_pct": metrics.trend_return_pct,
            "trend_slope_pct": metrics.trend_slope_pct,
            "recent_high": metrics.recent_high,
            "pullback_depth_pct": metrics.pullback_depth_pct,
            "pullback_sma": metrics.pullback_sma,
            "close_vs_sma_pct": metrics.close_vs_sma_pct,
            "reclaim_confirmed": metrics.reclaim_confirmed,
            "volume_ratio": metrics.volume_ratio,
            "choppiness": metrics.choppiness,
            "final_would_signal": final_would_signal,
        }),
    }
}

fn analyze_generic_window(
    config: &StrategyConfig,
    strategy_id: StrategyId,
    window: &[Candle],
) -> Result<WindowOutcome, CoreError> {
    let latest = window.last().expect("window must contain latest candle");
    let result = evaluate(StrategyEvaluationContext {
        correlation_id: Uuid::new_v4(),
        strategy_id,
        symbol: latest.symbol.clone(),
        config: StrategyConfig {
            max_signal_age_ms: i64::MAX,
            ..config.clone()
        },
        candles: window.to_vec(),
        evaluated_at: latest.close_time,
    })?;
    let final_would_signal = result.generated;
    let conditions = vec![
        condition("has_enough_data", true),
        condition("freshness", true),
        condition("final_would_signal", final_would_signal),
    ];
    Ok(WindowOutcome {
        open_time: latest.open_time,
        close_time: latest.close_time,
        would_signal: final_would_signal,
        blocking_condition: first_failed(&conditions),
        conditions,
        details: json!({
            "reason": result.reason.as_str(),
        }),
    })
}

fn condition(name: &str, passed: bool) -> ConditionOutcome {
    ConditionOutcome {
        name: name.to_string(),
        passed,
    }
}

fn first_failed(conditions: &[ConditionOutcome]) -> Option<String> {
    conditions
        .iter()
        .find(|condition| !condition.passed && condition.name != "final_would_signal")
        .map(|condition| condition.name.clone())
}

fn pct_i64(numerator: i64, denominator: i64) -> Decimal {
    if denominator <= 0 {
        Decimal::ZERO
    } else {
        (Decimal::from(numerator) / Decimal::from(denominator)) * Decimal::new(100, 0)
    }
}

fn distribution_json(values: &[Decimal]) -> serde_json::Value {
    if values.is_empty() {
        return json!({
            "min": null,
            "median": null,
            "p90": null,
        });
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    json!({
        "min": sorted[0],
        "median": percentile(&sorted, Decimal::new(50, 0)),
        "p90": percentile(&sorted, Decimal::new(90, 0)),
    })
}

fn percentile(sorted: &[Decimal], percentile: Decimal) -> Decimal {
    if sorted.is_empty() {
        return Decimal::ZERO;
    }
    let max_index = sorted.len() - 1;
    let rank = (Decimal::from(max_index as u64) * percentile / Decimal::new(100, 0))
        .round_dp(0)
        .to_usize()
        .unwrap_or(max_index)
        .min(max_index);
    sorted[rank]
}

fn build_opportunity_recommendation(
    strategy_id: StrategyId,
    signal_rate_pct: Decimal,
    evaluable_windows: i64,
    top_blocking_conditions: &[aegis_core::StrategyConditionFailureBreakdown],
    pass_rates: &[aegis_core::StrategyConditionPassRate],
) -> StrategyOpportunityRecommendation {
    if evaluable_windows == 0 {
        return StrategyOpportunityRecommendation {
            status: StrategyOpportunityStatus::InsufficientData,
            messages: vec![
                "Not enough closed candles to evaluate strategy opportunity.".to_string(),
            ],
        };
    }

    let mut status = if signal_rate_pct < Decimal::new(5, 1) {
        StrategyOpportunityStatus::TooRestrictive
    } else if signal_rate_pct > Decimal::new(20, 0) {
        StrategyOpportunityStatus::TooLoose
    } else {
        StrategyOpportunityStatus::HealthyOpportunity
    };
    let mut messages = Vec::new();
    for blocker in top_blocking_conditions {
        match blocker.condition.as_str() {
            "near_lower_band" if strategy_id == StrategyId::RangeReversionV1 => messages.push(
                "Strategy may be too restrictive near lower band; test lower_band_pct candidates 20,30,40."
                    .to_string(),
            ),
            "range_width_within_bounds" if strategy_id == StrategyId::RangeReversionV1 => {
                messages.push("Range width threshold may be too strict.".to_string())
            }
            "reversal_confirmation" if strategy_id == StrategyId::RangeReversionV1 => {
                messages.push("Reversal confirmation may be too restrictive.".to_string())
            }
            "close_above_sma"
                if strategy_id == StrategyId::TrendFilterMomentumV1
                    && status == StrategyOpportunityStatus::TooLoose =>
            {
                messages.push("Signal rate is too high; test longer trend lookbacks or add a trend-strength filter.".to_string())
            }
            "close_above_sma" if strategy_id == StrategyId::TrendFilterMomentumV1 => {
                messages.push("SMA trend filter blocks many windows; test shorter trend lookbacks only if empirical replay confirms a lower-quality under-signal problem.".to_string())
            }
            "momentum_condition"
                if strategy_id == StrategyId::TrendFilterMomentumV1
                    && status == StrategyOpportunityStatus::TooLoose =>
            {
                messages.push("Signal rate is too high; test stronger momentum confirmation, longer momentum lookbacks, and increased cooldown.".to_string())
            }
            "momentum_condition" if strategy_id == StrategyId::TrendFilterMomentumV1 => {
                messages.push("Momentum lookback may be too strict; test shorter momentum lookbacks only when replay confirms too few enterable signals.".to_string())
            }
            "close_within_sma_band" if strategy_id == StrategyId::TrendFilterMomentumV2 => {
                messages.push("SMA band blocks many windows; inspect CLOSE_TOO_EXTENDED_ABOVE_SMA versus close-too-close failures before widening the band.".to_string())
            }
            "momentum_confirmed" if strategy_id == StrategyId::TrendFilterMomentumV2 => {
                messages.push("Momentum confirmation blocks many windows; test lower min_momentum_return_pct only if replay does not overtrade.".to_string())
            }
            _ => {}
        }
    }
    if (strategy_id == StrategyId::TrendFilterMomentumV1
        || strategy_id == StrategyId::TrendFilterMomentumV2)
        && status == StrategyOpportunityStatus::TooLoose
    {
        messages.push(
            "Signal rate is above 20%; tighten before promotion by testing longer trend lookbacks, stronger momentum confirmation, increased cooldown, or a volatility/trend-strength filter."
                .to_string(),
        );
    }
    if messages.is_empty() {
        messages.push(match status {
            StrategyOpportunityStatus::TooRestrictive => {
                "Signal rate is below 0.5%; inspect top blocking conditions before changing parameters.".to_string()
            }
            StrategyOpportunityStatus::TooLoose => {
                "Signal rate is above 20%; strategy may be too loose for execution research.".to_string()
            }
            _ => "Opportunity rate is within the initial review band.".to_string(),
        });
    }
    if pass_rates
        .iter()
        .any(|rate| rate.condition == "final_would_signal" && rate.passed_count == 0)
        && status == StrategyOpportunityStatus::HealthyOpportunity
    {
        status = StrategyOpportunityStatus::TooRestrictive;
    }

    messages.sort();
    messages.dedup();
    StrategyOpportunityRecommendation { status, messages }
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

fn evaluate_trend_filter_momentum_v2(
    context: &StrategyEvaluationContext,
    candles: Vec<Candle>,
) -> Result<StrategyEvaluationResult, CoreError> {
    let min_band = min_close_above_sma_pct(&context.config);
    let max_band = max_close_above_sma_pct(&context.config);
    if max_band < min_band {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }

    let required = required_candle_count(&context.config) as usize;
    if candles.len() < required {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::InsufficientHistory,
        ));
    }

    let latest = candles.last().expect("candles must be present");
    let trend = trend_lookback(&context.config) as usize;
    let momentum = momentum_lookback(&context.config) as usize;
    let trend_window = &candles[candles.len() - trend - 1..candles.len() - 1];
    let sma = average_decimal(trend_window.iter().map(|candle| candle.close));
    let momentum_reference = &candles[candles.len() - momentum - 1];
    let close_vs_sma_pct = pct_ratio(latest.close - sma, sma);
    let momentum_return_pct = pct_ratio(
        latest.close - momentum_reference.close,
        momentum_reference.close,
    );

    if latest.close <= sma
        || close_vs_sma_pct < min_band
        || close_vs_sma_pct > max_band
        || latest.close <= momentum_reference.close
        || momentum_return_pct < min_momentum_return_pct(&context.config)
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

fn evaluate_volatility_compression_breakout(
    context: &StrategyEvaluationContext,
    candles: Vec<Candle>,
) -> Result<StrategyEvaluationResult, CoreError> {
    if validate_compression_breakout_config(&context.config).is_some() {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }

    let required = required_candle_count(&context.config) as usize;
    if candles.len() < required {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::InsufficientHistory,
        ));
    }

    let recent = &candles[candles.len() - required..];
    let latest = recent.last().expect("recent candles must be present");
    let metrics = calculate_compression_breakout_metrics(&context.config, recent);

    if !metrics.compression_passed
        || !metrics.range_width_within_bounds
        || latest.close <= metrics.breakout_level
        || metrics.breakout_pct < min_breakout_pct(&context.config)
        || metrics.breakout_pct > max_breakout_extension_pct(&context.config)
        || metrics.volume_ratio < min_volume_expansion_ratio(&context.config)
        || latest.close <= latest.open
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
        SignalReason::VolatilityCompressionBreakout,
        Decimal::new(71, 2),
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

fn evaluate_trend_pullback_continuation(
    context: &StrategyEvaluationContext,
    candles: Vec<Candle>,
) -> Result<StrategyEvaluationResult, CoreError> {
    if validate_trend_pullback_config(&context.config).is_some() {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::ConditionsNotMet,
        ));
    }

    let required = required_candle_count(&context.config) as usize;
    if candles.len() < required {
        return Ok(no_signal_result(
            context,
            context.config.timeframe,
            SignalReason::InsufficientHistory,
        ));
    }

    let recent = &candles[candles.len() - required..];
    let latest = recent.last().expect("recent candles must be present");
    let metrics = calculate_trend_pullback_metrics(&context.config, recent);
    if metrics.trend_return_pct < min_trend_return_pct(&context.config)
        || metrics.trend_slope_pct < min_trend_slope_pct(&context.config)
        || metrics.pullback_depth_pct < min_pullback_depth_pct(&context.config)
        || metrics.pullback_depth_pct > max_pullback_depth_pct(&context.config)
        || metrics.close_vs_sma_pct < Decimal::ZERO
        || metrics.close_vs_sma_pct > max_close_above_sma_pct(&context.config)
        || !metrics.reclaim_confirmed
        || metrics.volume_ratio < min_volume_ratio(&context.config)
        || metrics.choppiness > max_choppiness(&context.config)
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
        SignalReason::TrendPullbackContinuation,
        Decimal::new(69, 2),
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

fn diagnose_trend_filter_momentum_v2(
    context: &StrategyEvaluationContext,
    candles: &[Candle],
    condition_checks: &mut Vec<StrategyDiagnosticCheck>,
) -> Result<DiagnosticOutcome, CoreError> {
    let min_band = min_close_above_sma_pct(&context.config);
    let max_band = max_close_above_sma_pct(&context.config);
    if max_band < min_band {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::InvalidConfig),
            summary: format!(
                "Invalid trend-filter momentum v2 config: max_close_above_sma_pct {} is below min_close_above_sma_pct {}.",
                max_band, min_band
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let trend = trend_lookback(&context.config) as usize;
    let momentum = momentum_lookback(&context.config) as usize;
    let latest = candles.last().expect("candles must be present");
    let trend_window = &candles[candles.len() - trend - 1..candles.len() - 1];
    let sma = average_decimal(trend_window.iter().map(|candle| candle.close));
    let momentum_reference = &candles[candles.len() - momentum - 1];
    let close_vs_sma_pct = pct_ratio(latest.close - sma, sma);
    let momentum_return_pct = pct_ratio(
        latest.close - momentum_reference.close,
        momentum_reference.close,
    );
    let min_momentum = min_momentum_return_pct(&context.config);

    let close_above_sma = latest.close > sma;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "close_above_sma".to_string(),
        passed: close_above_sma,
        severity: if close_above_sma {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!(
            "Latest close {} compared with SMA({}) {} gives close_vs_sma_pct {}.",
            latest.close, trend, sma, close_vs_sma_pct
        ),
        actual: Some(latest.close.to_string()),
        expected: Some(format!("> {sma}")),
    });
    if !close_above_sma {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::CloseBelowSma),
            summary: format!(
                "Trend-filter momentum v2 did not trigger because latest close {} is not above SMA({}) {}.",
                latest.close, trend, sma
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let above_min = close_vs_sma_pct >= min_band;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "close_above_sma_min_band".to_string(),
        passed: above_min,
        severity: if above_min {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!(
            "close_vs_sma_pct {} must be at least configured min {}.",
            close_vs_sma_pct, min_band
        ),
        actual: Some(close_vs_sma_pct.to_string()),
        expected: Some(format!(">= {min_band}")),
    });
    if !above_min {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::CloseTooCloseToSma),
            summary: format!(
                "Trend-filter momentum v2 did not trigger because close_vs_sma_pct {} is below min band {}.",
                close_vs_sma_pct, min_band
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let below_max = close_vs_sma_pct <= max_band;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "close_below_sma_max_band".to_string(),
        passed: below_max,
        severity: if below_max {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!(
            "close_vs_sma_pct {} must be at most configured max {}.",
            close_vs_sma_pct, max_band
        ),
        actual: Some(close_vs_sma_pct.to_string()),
        expected: Some(format!("<= {max_band}")),
    });
    if !below_max {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::CloseTooExtendedAboveSma),
            summary: format!(
                "Trend-filter momentum v2 did not trigger because close_vs_sma_pct {} is above max band {}.",
                close_vs_sma_pct, max_band
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let momentum_confirmed =
        latest.close > momentum_reference.close && momentum_return_pct >= min_momentum;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "momentum_confirmed".to_string(),
        passed: momentum_confirmed,
        severity: if momentum_confirmed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!(
            "Latest close {} versus {}-candle reference {} gives momentum_return_pct {}; min is {}.",
            latest.close, momentum, momentum_reference.close, momentum_return_pct, min_momentum
        ),
        actual: Some(momentum_return_pct.to_string()),
        expected: Some(format!(">= {min_momentum} and close > {}", momentum_reference.close)),
    });
    if !momentum_confirmed {
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::NoSignal,
            no_signal_reason: Some(StrategyNoSignalReason::MomentumNotConfirmed),
            summary: format!(
                "Trend-filter momentum v2 did not trigger because momentum_return_pct {} is not confirmed.",
                momentum_return_pct
            ),
            source_candle_open_time: None,
            confidence: None,
        });
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "close_within_sma_band".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: format!(
            "close_vs_sma_pct {} is within configured band {} to {}.",
            close_vs_sma_pct, min_band, max_band
        ),
        actual: Some(close_vs_sma_pct.to_string()),
        expected: Some(format!("{min_band}..={max_band}")),
    });

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

fn diagnose_volatility_compression_breakout(
    context: &StrategyEvaluationContext,
    candles: &[Candle],
    condition_checks: &mut Vec<StrategyDiagnosticCheck>,
) -> Result<DiagnosticOutcome, CoreError> {
    if let Some(message) = validate_compression_breakout_config(&context.config) {
        condition_checks.push(StrategyDiagnosticCheck {
            name: "valid_config".to_string(),
            passed: false,
            severity: StrategyDiagnosticSeverity::Error,
            message: message.clone(),
            actual: Some("INVALID_CONFIG".to_string()),
            expected: Some("valid compression breakout config".to_string()),
        });
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::InvalidConfig,
            no_signal_reason: Some(StrategyNoSignalReason::InvalidConfig),
            summary: message,
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let required = required_candle_count(&context.config) as usize;
    let recent = &candles[candles.len() - required..];
    let latest = recent.last().expect("recent candles must be present");
    let metrics = calculate_compression_breakout_metrics(&context.config, recent);
    let min_width = min_range_width_pct(&context.config);
    let max_width = max_range_width_pct(&context.config);
    let min_breakout = min_breakout_pct(&context.config);
    let max_extension = max_breakout_extension_pct(&context.config);
    let min_volume_ratio = min_volume_expansion_ratio(&context.config);

    condition_checks.push(StrategyDiagnosticCheck {
        name: "compression_lookback".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Compression lookback closed candles.".to_string(),
        actual: Some(compression_lookback(&context.config).to_string()),
        expected: None,
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "breakout_lookback".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Breakout lookback closed candles excluding latest candle.".to_string(),
        actual: Some(breakout_lookback(&context.config).to_string()),
        expected: None,
    });

    let compression_passed = metrics.compression_passed;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "compression_passed".to_string(),
        passed: compression_passed,
        severity: if compression_passed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: format!(
            "recent_avg_range_pct {} versus baseline {}th percentile range {} gives compression_ratio {}.",
            metrics.recent_avg_range_pct,
            compression_percentile_threshold(&context.config),
            metrics.compression_threshold_range_pct,
            metrics.compression_ratio
        ),
        actual: Some(metrics.recent_avg_range_pct.to_string()),
        expected: Some(format!(
            "<= {}",
            metrics.compression_threshold_range_pct
        )),
    });
    if !compression_passed {
        return compression_no_signal_outcome(
            StrategyNoSignalReason::NoCompression,
            "NO_COMPRESSION",
            latest,
            &metrics,
        );
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "range_width_pct".to_string(),
        passed: metrics.range_width_within_bounds,
        severity: if metrics.range_width_within_bounds {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Prior breakout range width must remain within configured bounds.".to_string(),
        actual: Some(metrics.range_width_pct.to_string()),
        expected: Some(format!("{min_width}..={max_width}")),
    });
    if metrics.range_width_pct < min_width {
        return compression_no_signal_outcome(
            StrategyNoSignalReason::RangeTooNarrow,
            "RANGE_TOO_NARROW",
            latest,
            &metrics,
        );
    }
    if metrics.range_width_pct > max_width {
        return compression_no_signal_outcome(
            StrategyNoSignalReason::RangeTooWide,
            "RANGE_TOO_WIDE",
            latest,
            &metrics,
        );
    }

    let breakout_passed = latest.close > metrics.breakout_level;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "breakout_passed".to_string(),
        passed: breakout_passed,
        severity: if breakout_passed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Latest close must break above prior breakout level.".to_string(),
        actual: Some(latest.close.to_string()),
        expected: Some(format!("> {}", metrics.breakout_level)),
    });
    if !breakout_passed {
        return compression_no_signal_outcome(
            StrategyNoSignalReason::NoBreakout,
            "NO_BREAKOUT",
            latest,
            &metrics,
        );
    }

    let breakout_large_enough = metrics.breakout_pct >= min_breakout;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "breakout_large_enough".to_string(),
        passed: breakout_large_enough,
        severity: if breakout_large_enough {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Breakout percent must meet the configured minimum.".to_string(),
        actual: Some(metrics.breakout_pct.to_string()),
        expected: Some(format!(">= {min_breakout}")),
    });
    if !breakout_large_enough {
        return compression_no_signal_outcome(
            StrategyNoSignalReason::BreakoutTooSmall,
            "BREAKOUT_TOO_SMALL",
            latest,
            &metrics,
        );
    }

    let breakout_not_overextended = metrics.breakout_pct <= max_extension;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "breakout_not_overextended".to_string(),
        passed: breakout_not_overextended,
        severity: if breakout_not_overextended {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Breakout percent must not exceed the configured extension cap.".to_string(),
        actual: Some(metrics.breakout_pct.to_string()),
        expected: Some(format!("<= {max_extension}")),
    });
    if !breakout_not_overextended {
        return compression_no_signal_outcome(
            StrategyNoSignalReason::BreakoutTooExtended,
            "BREAKOUT_TOO_EXTENDED",
            latest,
            &metrics,
        );
    }

    let volume_confirmed = metrics.volume_ratio >= min_volume_ratio;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "volume_confirmed".to_string(),
        passed: volume_confirmed,
        severity: if volume_confirmed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Latest volume must expand versus prior average volume.".to_string(),
        actual: Some(metrics.volume_ratio.to_string()),
        expected: Some(format!(">= {min_volume_ratio}")),
    });
    if !volume_confirmed {
        return compression_no_signal_outcome(
            StrategyNoSignalReason::VolumeNotConfirmed,
            "VOLUME_NOT_CONFIRMED",
            latest,
            &metrics,
        );
    }

    let bullish_close = latest.close > latest.open;
    condition_checks.push(StrategyDiagnosticCheck {
        name: "bullish_close".to_string(),
        passed: bullish_close,
        severity: if bullish_close {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Latest close must be above latest open.".to_string(),
        actual: Some(format!("close={} open={}", latest.close, latest.open)),
        expected: Some("close > open".to_string()),
    });
    if !bullish_close {
        return compression_no_signal_outcome(
            StrategyNoSignalReason::NoBreakout,
            "NO_BREAKOUT",
            latest,
            &metrics,
        );
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "final_decision".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Volatility compression breakout conditions passed.".to_string(),
        actual: Some("WOULD_SIGNAL".to_string()),
        expected: Some("WOULD_SIGNAL".to_string()),
    });

    confidence_outcome(
        context,
        latest,
        SignalReason::VolatilityCompressionBreakout,
        Decimal::new(71, 2),
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

fn diagnose_trend_pullback_continuation(
    context: &StrategyEvaluationContext,
    candles: &[Candle],
    condition_checks: &mut Vec<StrategyDiagnosticCheck>,
) -> Result<DiagnosticOutcome, CoreError> {
    if let Some(message) = validate_trend_pullback_config(&context.config) {
        condition_checks.push(StrategyDiagnosticCheck {
            name: "valid_config".to_string(),
            passed: false,
            severity: StrategyDiagnosticSeverity::Error,
            message: message.clone(),
            actual: Some("INVALID_CONFIG".to_string()),
            expected: Some("valid trend pullback continuation config".to_string()),
        });
        return Ok(DiagnosticOutcome {
            final_decision: StrategyDiagnosticsDecision::InvalidConfig,
            no_signal_reason: Some(StrategyNoSignalReason::InvalidConfig),
            summary: message,
            source_candle_open_time: None,
            confidence: None,
        });
    }

    let required = required_candle_count(&context.config) as usize;
    let recent = &candles[candles.len() - required..];
    let latest = recent.last().expect("recent candles must be present");
    let metrics = calculate_trend_pullback_metrics(&context.config, recent);

    condition_checks.push(StrategyDiagnosticCheck {
        name: "recent_high".to_string(),
        passed: metrics.recent_high > Decimal::ZERO,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Recent high over the configured pullback lookback.".to_string(),
        actual: Some(metrics.recent_high.to_string()),
        expected: Some("> 0".to_string()),
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "pullback_sma".to_string(),
        passed: metrics.pullback_sma > Decimal::ZERO,
        severity: StrategyDiagnosticSeverity::Info,
        message: "SMA over the configured pullback SMA lookback.".to_string(),
        actual: Some(metrics.pullback_sma.to_string()),
        expected: Some("> 0".to_string()),
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "trend_return_pct".to_string(),
        passed: metrics.trend_return_pct >= min_trend_return_pct(&context.config),
        severity: if metrics.trend_return_pct >= min_trend_return_pct(&context.config) {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Trend return over the configured lookback.".to_string(),
        actual: Some(metrics.trend_return_pct.to_string()),
        expected: Some(format!(">= {}", min_trend_return_pct(&context.config))),
    });
    condition_checks.push(StrategyDiagnosticCheck {
        name: "trend_slope_pct".to_string(),
        passed: metrics.trend_slope_pct >= min_trend_slope_pct(&context.config),
        severity: if metrics.trend_slope_pct >= min_trend_slope_pct(&context.config) {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Trend slope proxy uses net trend return over lookback.".to_string(),
        actual: Some(metrics.trend_slope_pct.to_string()),
        expected: Some(format!(">= {}", min_trend_slope_pct(&context.config))),
    });
    if metrics.trend_return_pct < min_trend_return_pct(&context.config)
        || metrics.trend_slope_pct < min_trend_slope_pct(&context.config)
    {
        return pullback_no_signal_outcome(
            StrategyNoSignalReason::TrendNotConfirmed,
            "TREND_NOT_CONFIRMED",
            &metrics,
        );
    }

    let min_depth = min_pullback_depth_pct(&context.config);
    let max_depth = max_pullback_depth_pct(&context.config);
    condition_checks.push(StrategyDiagnosticCheck {
        name: "pullback_depth_pct".to_string(),
        passed: metrics.pullback_depth_pct >= min_depth && metrics.pullback_depth_pct <= max_depth,
        severity: if metrics.pullback_depth_pct >= min_depth
            && metrics.pullback_depth_pct <= max_depth
        {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Pullback depth from recent high to latest low/close.".to_string(),
        actual: Some(metrics.pullback_depth_pct.to_string()),
        expected: Some(format!("{min_depth}..={max_depth}")),
    });
    if metrics.pullback_depth_pct < min_depth {
        return pullback_no_signal_outcome(
            StrategyNoSignalReason::PullbackTooShallow,
            "PULLBACK_TOO_SHALLOW",
            &metrics,
        );
    }
    if metrics.pullback_depth_pct > max_depth {
        return pullback_no_signal_outcome(
            StrategyNoSignalReason::PullbackTooDeep,
            "PULLBACK_TOO_DEEP",
            &metrics,
        );
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "close_vs_sma_pct".to_string(),
        passed: metrics.close_vs_sma_pct >= Decimal::ZERO
            && metrics.close_vs_sma_pct <= max_close_above_sma_pct(&context.config),
        severity: if metrics.close_vs_sma_pct >= Decimal::ZERO
            && metrics.close_vs_sma_pct <= max_close_above_sma_pct(&context.config)
        {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Latest close distance from pullback SMA.".to_string(),
        actual: Some(metrics.close_vs_sma_pct.to_string()),
        expected: Some(format!("0..={}", max_close_above_sma_pct(&context.config))),
    });
    if metrics.close_vs_sma_pct < Decimal::ZERO {
        return pullback_no_signal_outcome(
            StrategyNoSignalReason::CloseBelowSma,
            "CLOSE_BELOW_SMA",
            &metrics,
        );
    }
    if metrics.close_vs_sma_pct > max_close_above_sma_pct(&context.config) {
        return pullback_no_signal_outcome(
            StrategyNoSignalReason::CloseTooExtendedAboveSma,
            "CLOSE_TOO_EXTENDED_ABOVE_SMA",
            &metrics,
        );
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "reclaim_confirmed".to_string(),
        passed: metrics.reclaim_confirmed,
        severity: if metrics.reclaim_confirmed {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Latest candle must close above previous close and above open.".to_string(),
        actual: Some(metrics.reclaim_confirmed.to_string()),
        expected: Some("true".to_string()),
    });
    if !metrics.reclaim_confirmed {
        return pullback_no_signal_outcome(
            StrategyNoSignalReason::ReclaimNotConfirmed,
            "RECLAIM_NOT_CONFIRMED",
            &metrics,
        );
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "volume_ratio".to_string(),
        passed: metrics.volume_ratio >= min_volume_ratio(&context.config),
        severity: if metrics.volume_ratio >= min_volume_ratio(&context.config) {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Latest volume versus prior pullback-window average volume.".to_string(),
        actual: Some(metrics.volume_ratio.to_string()),
        expected: Some(format!(">= {}", min_volume_ratio(&context.config))),
    });
    if metrics.volume_ratio < min_volume_ratio(&context.config) {
        return pullback_no_signal_outcome(
            StrategyNoSignalReason::VolumeNotConfirmed,
            "VOLUME_NOT_CONFIRMED",
            &metrics,
        );
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "choppiness".to_string(),
        passed: metrics.choppiness <= max_choppiness(&context.config),
        severity: if metrics.choppiness <= max_choppiness(&context.config) {
            StrategyDiagnosticSeverity::Info
        } else {
            StrategyDiagnosticSeverity::Warn
        },
        message: "Choppiness proxy must stay below configured maximum.".to_string(),
        actual: Some(metrics.choppiness.to_string()),
        expected: Some(format!("<= {}", max_choppiness(&context.config))),
    });
    if metrics.choppiness > max_choppiness(&context.config) {
        return pullback_no_signal_outcome(
            StrategyNoSignalReason::TooChoppy,
            "TOO_CHOPPY",
            &metrics,
        );
    }

    condition_checks.push(StrategyDiagnosticCheck {
        name: "final_decision".to_string(),
        passed: true,
        severity: StrategyDiagnosticSeverity::Info,
        message: "Trend pullback continuation conditions passed.".to_string(),
        actual: Some("WOULD_SIGNAL".to_string()),
        expected: Some("WOULD_SIGNAL".to_string()),
    });

    confidence_outcome(
        context,
        latest,
        SignalReason::TrendPullbackContinuation,
        Decimal::new(69, 2),
    )
}

fn pullback_no_signal_outcome(
    reason: StrategyNoSignalReason,
    reason_label: &str,
    metrics: &TrendPullbackMetrics,
) -> Result<DiagnosticOutcome, CoreError> {
    Ok(DiagnosticOutcome {
        final_decision: StrategyDiagnosticsDecision::NoSignal,
        no_signal_reason: Some(reason),
        summary: format!(
            "Trend pullback continuation did not trigger: {reason_label}; trend_return_pct={}, trend_slope_pct={}, recent_high={}, pullback_depth_pct={}, pullback_sma={}, close_vs_sma_pct={}, reclaim_confirmed={}, volume_ratio={}, choppiness={}, final_decision=NO_SIGNAL.",
            metrics.trend_return_pct,
            metrics.trend_slope_pct,
            metrics.recent_high,
            metrics.pullback_depth_pct,
            metrics.pullback_sma,
            metrics.close_vs_sma_pct,
            metrics.reclaim_confirmed,
            metrics.volume_ratio,
            metrics.choppiness
        ),
        source_candle_open_time: None,
        confidence: None,
    })
}

fn compression_no_signal_outcome(
    reason: StrategyNoSignalReason,
    reason_label: &str,
    latest: &Candle,
    metrics: &CompressionBreakoutMetrics,
) -> Result<DiagnosticOutcome, CoreError> {
    Ok(DiagnosticOutcome {
        final_decision: StrategyDiagnosticsDecision::NoSignal,
        no_signal_reason: Some(reason),
        summary: format!(
            "Volatility compression breakout did not trigger: {reason_label}; compression_ratio={}, breakout_level={}, latest_close={}, breakout_pct={}, volume_ratio={}, range_width_pct={}.",
            metrics.compression_ratio,
            metrics.breakout_level,
            latest.close,
            metrics.breakout_pct,
            metrics.volume_ratio,
            metrics.range_width_pct
        ),
        source_candle_open_time: None,
        confidence: None,
    })
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

fn compression_lookback(config: &StrategyConfig) -> u32 {
    config
        .compression_lookback_candles
        .unwrap_or(config.lookback_candles)
}

fn breakout_lookback(config: &StrategyConfig) -> u32 {
    config
        .breakout_lookback_candles
        .unwrap_or(config.lookback_candles)
}

fn pullback_lookback(config: &StrategyConfig) -> u32 {
    config.pullback_lookback_candles.unwrap_or(10)
}

fn pullback_sma_lookback(config: &StrategyConfig) -> u32 {
    config.pullback_sma_lookback_candles.unwrap_or(20)
}

fn compression_percentile_threshold(config: &StrategyConfig) -> Decimal {
    config
        .compression_percentile_threshold
        .unwrap_or(Decimal::new(25, 0))
}

fn min_breakout_pct(config: &StrategyConfig) -> Decimal {
    config.min_breakout_pct.unwrap_or(Decimal::new(5, 2))
}

fn max_breakout_extension_pct(config: &StrategyConfig) -> Decimal {
    config
        .max_breakout_extension_pct
        .unwrap_or(Decimal::new(15, 1))
}

fn min_volume_expansion_ratio(config: &StrategyConfig) -> Decimal {
    config
        .min_volume_expansion_ratio
        .unwrap_or(Decimal::new(11, 1))
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

fn min_close_above_sma_pct(config: &StrategyConfig) -> Decimal {
    config.min_close_above_sma_pct.unwrap_or(Decimal::ZERO)
}

fn max_close_above_sma_pct(config: &StrategyConfig) -> Decimal {
    config.max_close_above_sma_pct.unwrap_or(Decimal::ONE)
}

fn min_momentum_return_pct(config: &StrategyConfig) -> Decimal {
    config.min_momentum_return_pct.unwrap_or(Decimal::ZERO)
}

fn min_trend_return_pct(config: &StrategyConfig) -> Decimal {
    config.min_trend_return_pct.unwrap_or(Decimal::new(2, 0))
}

fn min_trend_slope_pct(config: &StrategyConfig) -> Decimal {
    config.min_trend_slope_pct.unwrap_or(Decimal::ZERO)
}

fn min_pullback_depth_pct(config: &StrategyConfig) -> Decimal {
    config.min_pullback_depth_pct.unwrap_or(Decimal::new(3, 1))
}

fn max_pullback_depth_pct(config: &StrategyConfig) -> Decimal {
    config.max_pullback_depth_pct.unwrap_or(Decimal::new(5, 0))
}

fn min_reclaim_pct(config: &StrategyConfig) -> Decimal {
    config.min_reclaim_pct.unwrap_or(Decimal::new(5, 2))
}

fn min_volume_ratio(config: &StrategyConfig) -> Decimal {
    config.min_volume_ratio.unwrap_or(Decimal::new(8, 1))
}

fn max_choppiness(config: &StrategyConfig) -> Decimal {
    config.max_choppiness.unwrap_or(Decimal::new(60, 0))
}

fn validate_trend_pullback_config(config: &StrategyConfig) -> Option<String> {
    let trend = trend_lookback(config);
    let pullback = pullback_lookback(config);
    let sma = pullback_sma_lookback(config);
    if trend <= 1 || pullback <= 1 || sma <= 1 {
        return Some(
            "Invalid trend pullback continuation config: lookbacks must be greater than 1."
                .to_string(),
        );
    }
    if trend < pullback {
        return Some(
            "Invalid trend pullback continuation config: trend_lookback_candles must be greater than or equal to pullback_lookback_candles."
                .to_string(),
        );
    }
    if max_pullback_depth_pct(config) <= min_pullback_depth_pct(config) {
        return Some(
            "Invalid trend pullback continuation config: max_pullback_depth_pct must be greater than min_pullback_depth_pct."
                .to_string(),
        );
    }
    if max_close_above_sma_pct(config) < Decimal::ZERO
        || min_volume_ratio(config) < Decimal::ZERO
        || max_choppiness(config) <= Decimal::ZERO
    {
        return Some(
            "Invalid trend pullback continuation config: SMA, volume, and choppiness thresholds are invalid."
                .to_string(),
        );
    }
    None
}

#[derive(Debug, Clone)]
struct TrendPullbackMetrics {
    trend_return_pct: Decimal,
    trend_slope_pct: Decimal,
    recent_high: Decimal,
    pullback_depth_pct: Decimal,
    pullback_sma: Decimal,
    close_vs_sma_pct: Decimal,
    reclaim_confirmed: bool,
    volume_ratio: Decimal,
    choppiness: Decimal,
}

fn calculate_trend_pullback_metrics(
    config: &StrategyConfig,
    window: &[Candle],
) -> TrendPullbackMetrics {
    let latest = window.last().expect("window must contain latest candle");
    let previous = &window[window.len() - 2];
    let trend = trend_lookback(config) as usize;
    let pullback = pullback_lookback(config) as usize;
    let sma_lookback = pullback_sma_lookback(config) as usize;
    let trend_reference = &window[window.len() - trend - 1];
    let trend_return_pct = pct_ratio(latest.close - trend_reference.close, trend_reference.close);
    let trend_slope_pct = trend_return_pct;
    let pullback_window = &window[window.len() - pullback..];
    let recent_high = pullback_window
        .iter()
        .map(|candle| candle.high)
        .max()
        .unwrap_or(latest.high);
    let pullback_reference = latest.low.min(latest.close);
    let pullback_depth_pct = pct_ratio(recent_high - pullback_reference, recent_high);
    let sma_window = &window[window.len() - sma_lookback..];
    let pullback_sma = average_decimal(sma_window.iter().map(|candle| candle.close));
    let close_vs_sma_pct = pct_ratio(latest.close - pullback_sma, pullback_sma);
    let reclaim_return_pct = pct_ratio(latest.close - previous.close, previous.close);
    let reclaim_confirmed = latest.close > previous.close
        && latest.close > latest.open
        && reclaim_return_pct >= min_reclaim_pct(config);
    let volume_window_start = window.len().saturating_sub(pullback + 1);
    let volume_window = &window[volume_window_start..window.len() - 1];
    let average_volume = average_decimal(volume_window.iter().map(|candle| candle.volume));
    let volume_ratio = if average_volume == Decimal::ZERO {
        Decimal::ZERO
    } else {
        latest.volume / average_volume
    };
    let chop_lookback = 20usize.min(window.len());
    let chop_window = &window[window.len() - chop_lookback..];
    let range_sum = chop_window.iter().fold(Decimal::ZERO, |sum, candle| {
        sum + (candle.high - candle.low)
    });
    let first_close = chop_window
        .first()
        .map(|candle| candle.close)
        .unwrap_or(latest.close);
    let directional_move = (latest.close - first_close).abs();
    let trend_efficiency = if range_sum == Decimal::ZERO {
        Decimal::ZERO
    } else {
        (directional_move / range_sum * Decimal::new(100, 0))
            .clamp(Decimal::ZERO, Decimal::new(100, 0))
    };
    let choppiness = Decimal::new(100, 0) - trend_efficiency;

    TrendPullbackMetrics {
        trend_return_pct,
        trend_slope_pct,
        recent_high,
        pullback_depth_pct,
        pullback_sma,
        close_vs_sma_pct,
        reclaim_confirmed,
        volume_ratio,
        choppiness,
    }
}

fn validate_compression_breakout_config(config: &StrategyConfig) -> Option<String> {
    let compression = compression_lookback(config);
    let breakout = breakout_lookback(config);
    let min_breakout = min_breakout_pct(config);
    let max_extension = max_breakout_extension_pct(config);
    let min_width = min_range_width_pct(config);
    let max_width = max_range_width_pct(config);
    if compression <= 1 || breakout <= 1 {
        return Some(
            "Invalid volatility compression breakout config: lookbacks must be greater than 1."
                .to_string(),
        );
    }
    if compression > breakout {
        return Some(
            "Invalid volatility compression breakout config: compression_lookback_candles must be less than or equal to breakout_lookback_candles."
                .to_string(),
        );
    }
    if min_breakout < Decimal::ZERO || max_extension <= min_breakout {
        return Some(
            "Invalid volatility compression breakout config: max_breakout_extension_pct must be greater than min_breakout_pct, and min_breakout_pct must be non-negative."
                .to_string(),
        );
    }
    if min_volume_expansion_ratio(config) < Decimal::ONE {
        return Some(
            "Invalid volatility compression breakout config: min_volume_expansion_ratio must be at least 1."
                .to_string(),
        );
    }
    if min_width <= Decimal::ZERO || max_width <= min_width {
        return Some(
            "Invalid volatility compression breakout config: range width bounds are invalid."
                .to_string(),
        );
    }
    let (_, recommended_max_signal_age_ms) = config.timeframe.recommended_max_signal_age_ms();
    if config.max_signal_age_ms > recommended_max_signal_age_ms {
        return Some(
            "Invalid volatility compression breakout config: max_signal_age_ms is unreasonable for timeframe."
                .to_string(),
        );
    }
    None
}

#[derive(Debug, Clone)]
struct CompressionBreakoutMetrics {
    recent_avg_range_pct: Decimal,
    baseline_avg_range_pct: Decimal,
    compression_threshold_range_pct: Decimal,
    compression_ratio: Decimal,
    compression_passed: bool,
    breakout_level: Decimal,
    breakout_pct: Decimal,
    volume_ratio: Decimal,
    range_width_pct: Decimal,
    range_width_within_bounds: bool,
}

fn calculate_compression_breakout_metrics(
    config: &StrategyConfig,
    window: &[Candle],
) -> CompressionBreakoutMetrics {
    let compression = compression_lookback(config) as usize;
    let breakout = breakout_lookback(config) as usize;
    let latest_index = window.len().saturating_sub(1);
    let latest = &window[latest_index];
    let recent_start = latest_index.saturating_sub(compression);
    let recent_window = &window[recent_start..latest_index];
    let baseline_end = recent_start;
    let baseline_start = baseline_end.saturating_sub(breakout);
    let baseline_window = if baseline_end > baseline_start {
        &window[baseline_start..baseline_end]
    } else {
        recent_window
    };
    let breakout_start = latest_index.saturating_sub(breakout);
    let breakout_window = &window[breakout_start..latest_index];

    let recent_avg_range_pct = average_decimal(recent_window.iter().map(candle_range_pct));
    let mut baseline_range_pcts = baseline_window
        .iter()
        .map(candle_range_pct)
        .collect::<Vec<_>>();
    baseline_range_pcts.sort();
    let baseline_avg_range_pct = average_decimal(baseline_range_pcts.iter().copied());
    let compression_threshold_range_pct = percentile(
        &baseline_range_pcts,
        compression_percentile_threshold(config),
    );
    let compression_ratio = pct_ratio(recent_avg_range_pct, baseline_avg_range_pct);
    let breakout_level = breakout_window
        .iter()
        .map(|candle| candle.high)
        .max()
        .unwrap_or(latest.high);
    let breakout_pct = pct_ratio(latest.close - breakout_level, breakout_level);
    let average_volume = average_decimal(breakout_window.iter().map(|candle| candle.volume));
    let volume_ratio = if average_volume == Decimal::ZERO {
        Decimal::ZERO
    } else {
        latest.volume / average_volume
    };
    let range_high = breakout_window
        .iter()
        .map(|candle| candle.high)
        .max()
        .unwrap_or(latest.high);
    let range_low = breakout_window
        .iter()
        .map(|candle| candle.low)
        .min()
        .unwrap_or(latest.low);
    let range_width_pct = pct_ratio(range_high - range_low, range_low);
    let min_width = min_range_width_pct(config);
    let max_width = max_range_width_pct(config);

    CompressionBreakoutMetrics {
        recent_avg_range_pct,
        baseline_avg_range_pct,
        compression_threshold_range_pct,
        compression_ratio,
        compression_passed: compression_threshold_range_pct > Decimal::ZERO
            && recent_avg_range_pct <= compression_threshold_range_pct,
        breakout_level,
        breakout_pct,
        volume_ratio,
        range_width_pct,
        range_width_within_bounds: range_width_pct >= min_width && range_width_pct <= max_width,
    }
}

fn candle_range_pct(candle: &Candle) -> Decimal {
    if candle.open > Decimal::ZERO {
        ((candle.high - candle.low) / candle.open) * Decimal::new(100, 0)
    } else {
        Decimal::ZERO
    }
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
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
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
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
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
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("Research baseline trend-filter momentum config".to_string()),
        },
        StrategyConfig {
            strategy_id: StrategyId::TrendFilterMomentumV2,
            enabled: true,
            mode: StrategyMode::Research,
            symbols: symbols.clone(),
            timeframe: CandleInterval::FifteenMinutes,
            suggested_notional,
            max_signal_age_ms: 2_700_000,
            cooldown_seconds: 1_800,
            lookback_candles: 20,
            trend_lookback_candles: Some(20),
            momentum_lookback_candles: Some(3),
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: Some(Decimal::ZERO),
            max_close_above_sma_pct: Some(Decimal::ONE),
            min_momentum_return_pct: Some(Decimal::ZERO),
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("Research baseline feature-filtered trend momentum config".to_string()),
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
            compression_lookback_candles: None,
            breakout_lookback_candles: Some(20),
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: Some("Research baseline volume-confirmed breakout config".to_string()),
        },
        StrategyConfig {
            strategy_id: StrategyId::VolatilityCompressionBreakoutV1,
            enabled: true,
            mode: StrategyMode::Research,
            symbols: symbols.clone(),
            timeframe: CandleInterval::OneHour,
            suggested_notional,
            max_signal_age_ms: 7_200_000,
            cooldown_seconds: 14_400,
            lookback_candles: 20,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            compression_lookback_candles: Some(20),
            breakout_lookback_candles: Some(20),
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: Some(Decimal::new(25, 0)),
            min_breakout_pct: Some(Decimal::new(5, 2)),
            max_breakout_extension_pct: Some(Decimal::new(15, 1)),
            min_volume_expansion_ratio: Some(Decimal::new(11, 1)),
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: Some(Decimal::new(2, 1)),
            max_range_width_pct: Some(Decimal::new(5, 0)),
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(5),
            notes: Some("Research baseline volatility compression breakout config".to_string()),
        },
        StrategyConfig {
            strategy_id: StrategyId::RangeReversionV1,
            enabled: true,
            mode: StrategyMode::Research,
            symbols: symbols.clone(),
            timeframe: CandleInterval::FifteenMinutes,
            suggested_notional,
            max_signal_age_ms: 2_700_000,
            cooldown_seconds: 1_800,
            lookback_candles: 20,
            trend_lookback_candles: None,
            momentum_lookback_candles: None,
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: Some(Decimal::new(20, 0)),
            upper_band_pct: Some(Decimal::new(80, 0)),
            min_range_width_pct: Some(Decimal::new(15, 2)),
            max_range_width_pct: Some(Decimal::new(3, 0)),
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(5),
            notes: Some("Research baseline range-reversion config".to_string()),
        },
        StrategyConfig {
            strategy_id: StrategyId::TrendPullbackContinuationV1,
            enabled: true,
            mode: StrategyMode::Research,
            symbols,
            timeframe: CandleInterval::OneHour,
            suggested_notional,
            max_signal_age_ms: 7_200_000,
            cooldown_seconds: 14_400,
            lookback_candles: 20,
            trend_lookback_candles: Some(50),
            momentum_lookback_candles: None,
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: Some(10),
            pullback_sma_lookback_candles: Some(20),
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: Some(Decimal::ZERO),
            max_close_above_sma_pct: Some(Decimal::ONE),
            min_momentum_return_pct: None,
            min_trend_return_pct: Some(Decimal::new(2, 0)),
            min_trend_slope_pct: Some(Decimal::ZERO),
            min_pullback_depth_pct: Some(Decimal::new(3, 1)),
            max_pullback_depth_pct: Some(Decimal::new(5, 0)),
            min_reclaim_pct: Some(Decimal::new(5, 2)),
            min_volume_ratio: Some(Decimal::new(8, 1)),
            max_choppiness: Some(Decimal::new(60, 0)),
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(20),
            notes: Some("Research baseline trend pullback continuation config".to_string()),
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
        StrategyId::TrendFilterMomentumV1 | StrategyId::TrendFilterMomentumV2 => {
            Decimal::new(68, 2)
        }
        StrategyId::VolatilityBreakoutV2 => Decimal::new(72, 2),
        StrategyId::VolatilityCompressionBreakoutV1 => Decimal::new(71, 2),
        StrategyId::RangeReversionV1 => Decimal::new(66, 2),
        StrategyId::TrendPullbackContinuationV1 => Decimal::new(69, 2),
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
        analyze_opportunity, build_default_strategy_configs, diagnose, evaluate,
        required_candle_count, validate_strategy_config, StrategyValidationContext,
    };
    use aegis_core::{
        Candle, CandleInterval, MarketDataSource, SignalReason, StrategyConfigUpdateRequest,
        StrategyConfigValidationSeverity, StrategyDiagnosticsDecision, StrategyEvaluationContext,
        StrategyId, StrategyMode, StrategyNoSignalReason, StrategyOpportunityAnalysisRequest,
        StrategyOpportunityStatus, Symbol,
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

    fn compression_candle(
        index: i64,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
    ) -> Candle {
        let mut candle = range_candle(index, open, high, low, close);
        candle.interval = CandleInterval::OneHour;
        candle.close_time = candle.open_time + Duration::hours(1);
        candle.volume = volume;
        candle
    }

    fn compression_breakout_candles() -> Vec<Candle> {
        let mut candles = Vec::new();
        for index in 0..20 {
            candles.push(compression_candle(
                index,
                Decimal::new(100, 0),
                Decimal::new(110, 0),
                Decimal::new(90, 0),
                Decimal::new(100, 0),
                Decimal::new(10, 0),
            ));
        }
        for index in 20..40 {
            candles.push(compression_candle(
                index,
                Decimal::new(100, 0),
                Decimal::new(101, 0),
                Decimal::new(99, 0),
                Decimal::new(100, 0),
                Decimal::new(10, 0),
            ));
        }
        candles.push(compression_candle(
            40,
            Decimal::new(100, 0),
            Decimal::new(102, 0),
            Decimal::new(100, 0),
            Decimal::new(1012, 1),
            Decimal::new(15, 0),
        ));
        candles
    }

    fn compression_context(candles: Vec<Candle>) -> StrategyEvaluationContext {
        let mut context = context(StrategyId::VolatilityCompressionBreakoutV1, candles);
        context.config.timeframe = CandleInterval::OneHour;
        context.config.lookback_candles = 20;
        context.config.compression_lookback_candles = Some(20);
        context.config.breakout_lookback_candles = Some(20);
        context.config.compression_percentile_threshold = Some(Decimal::new(25, 0));
        context.config.min_breakout_pct = Some(Decimal::new(5, 2));
        context.config.max_breakout_extension_pct = Some(Decimal::new(15, 1));
        context.config.min_volume_expansion_ratio = Some(Decimal::new(11, 1));
        context.config.min_range_width_pct = Some(Decimal::new(2, 1));
        context.config.max_range_width_pct = Some(Decimal::new(5, 0));
        context
    }

    fn pullback_candle(
        index: i64,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
    ) -> Candle {
        let open_time = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + Duration::hours(index);
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            interval: CandleInterval::OneHour,
            open_time,
            close_time: open_time + Duration::hours(1) - Duration::milliseconds(1),
            open,
            high,
            low,
            close,
            volume,
            quote_volume: Some(Decimal::new(1000, 0)),
            trade_count: 5,
            is_closed: true,
            created_at: open_time,
            updated_at: open_time,
        }
    }

    fn pullback_candles(latest_close: Decimal, latest_open: Decimal) -> Vec<Candle> {
        let mut candles = Vec::new();
        for index in 0..50 {
            let close = Decimal::new(1000 + index * 6, 1);
            candles.push(pullback_candle(
                index,
                close - Decimal::new(1, 1),
                close + Decimal::new(15, 1),
                close - Decimal::new(15, 1),
                close,
                Decimal::new(100, 0),
            ));
        }
        for offset in 0..9 {
            let index = 50 + offset;
            let close = Decimal::new(1300 - offset * 4, 1);
            candles.push(pullback_candle(
                index,
                close + Decimal::new(5, 1),
                Decimal::new(132, 0),
                close - Decimal::new(10, 1),
                close,
                Decimal::new(100, 0),
            ));
        }
        candles.push(pullback_candle(
            59,
            latest_open,
            Decimal::new(1305, 1),
            Decimal::new(1260, 1),
            latest_close,
            Decimal::new(120, 0),
        ));
        candles
    }

    fn pullback_context(candles: Vec<Candle>) -> StrategyEvaluationContext {
        let mut context = context(StrategyId::TrendPullbackContinuationV1, candles);
        context.config.timeframe = CandleInterval::OneHour;
        context.config.max_signal_age_ms = 7_200_000;
        context.config.cooldown_seconds = 14_400;
        context.config.lookback_candles = 20;
        context.config.trend_lookback_candles = Some(50);
        context.config.pullback_lookback_candles = Some(10);
        context.config.pullback_sma_lookback_candles = Some(20);
        context.config.min_trend_return_pct = Some(Decimal::new(2, 0));
        context.config.min_trend_slope_pct = Some(Decimal::ZERO);
        context.config.min_pullback_depth_pct = Some(Decimal::new(3, 1));
        context.config.max_pullback_depth_pct = Some(Decimal::new(5, 0));
        context.config.max_close_above_sma_pct = Some(Decimal::new(5, 0));
        context.config.min_reclaim_pct = Some(Decimal::ZERO);
        context.config.min_volume_ratio = Some(Decimal::new(8, 1));
        context.config.max_choppiness = Some(Decimal::new(100, 0));
        context
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
            compression_lookback_candles: None,
            breakout_lookback_candles: None,
            pullback_lookback_candles: None,
            pullback_sma_lookback_candles: None,
            compression_percentile_threshold: None,
            min_breakout_pct: None,
            max_breakout_extension_pct: None,
            min_volume_expansion_ratio: None,
            lower_band_pct: None,
            upper_band_pct: None,
            min_range_width_pct: None,
            max_range_width_pct: None,
            min_close_above_sma_pct: None,
            max_close_above_sma_pct: None,
            min_momentum_return_pct: None,
            min_trend_return_pct: None,
            min_trend_slope_pct: None,
            min_pullback_depth_pct: None,
            max_pullback_depth_pct: None,
            min_reclaim_pct: None,
            min_volume_ratio: None,
            max_choppiness: None,
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

    fn opportunity_request(strategy_id: StrategyId) -> StrategyOpportunityAnalysisRequest {
        StrategyOpportunityAnalysisRequest {
            strategy_id: strategy_id.to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "15m".to_string(),
            start_time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap(),
            config: None,
            limit_samples: Some(3),
            include_examples: true,
        }
    }

    fn default_config(
        strategy_id: StrategyId,
        timeframe: CandleInterval,
    ) -> aegis_core::StrategyConfig {
        build_default_strategy_configs(
            vec![Symbol::new("BTCUSDT").expect("valid symbol")],
            timeframe,
            Decimal::new(100_000, 0),
            3,
            20,
        )
        .into_iter()
        .find(|config| config.strategy_id == strategy_id)
        .expect("strategy config must exist")
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

    fn v2_candles(latest_close: Decimal, momentum_reference_close: Decimal) -> Vec<Candle> {
        let mut candles = (0..20)
            .map(|index| {
                let close = if index == 17 {
                    momentum_reference_close
                } else {
                    Decimal::new(100, 0)
                };
                range_candle(
                    index,
                    close - Decimal::ONE,
                    close + Decimal::ONE,
                    close - Decimal::new(2, 0),
                    close,
                )
            })
            .collect::<Vec<_>>();
        candles.push(range_candle(
            20,
            latest_close - Decimal::ONE,
            latest_close + Decimal::ONE,
            latest_close - Decimal::new(2, 0),
            latest_close,
        ));
        candles
    }

    #[test]
    fn trend_filter_momentum_v2_emits_buy_within_sma_band_and_momentum() {
        let result = evaluate(context(
            StrategyId::TrendFilterMomentumV2,
            v2_candles(Decimal::new(1005, 1), Decimal::new(100, 0)),
        ))
        .expect("evaluation should succeed");

        assert!(result.generated);
        assert_eq!(result.reason, SignalReason::TrendFilterMomentum);
    }

    #[test]
    fn trend_filter_momentum_v2_no_signal_when_close_below_sma() {
        let result = diagnose(context(
            StrategyId::TrendFilterMomentumV2,
            v2_candles(Decimal::new(995, 1), Decimal::new(100, 0)),
        ))
        .expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::CloseBelowSma)
        );
    }

    #[test]
    fn trend_filter_momentum_v2_no_signal_when_close_too_extended() {
        let result = diagnose(context(
            StrategyId::TrendFilterMomentumV2,
            v2_candles(Decimal::new(102, 0), Decimal::new(100, 0)),
        ))
        .expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::CloseTooExtendedAboveSma)
        );
        assert!(result.summary.contains("above max band"));
    }

    #[test]
    fn trend_filter_momentum_v2_no_signal_when_momentum_not_confirmed() {
        let result = diagnose(context(
            StrategyId::TrendFilterMomentumV2,
            v2_candles(Decimal::new(1005, 1), Decimal::new(101, 0)),
        ))
        .expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::MomentumNotConfirmed)
        );
    }

    #[test]
    fn trend_filter_momentum_v2_validation_rejects_inverted_sma_band() {
        let mut request = sample_request("trend_filter_momentum_v2");
        request.lookback_candles = 20;
        request.trend_lookback_candles = Some(20);
        request.momentum_lookback_candles = Some(3);
        request.min_close_above_sma_pct = Some(Decimal::ONE);
        request.max_close_above_sma_pct = Some(Decimal::ZERO);

        let result = validate_strategy_config(&request, &validation_context());

        assert!(!result.valid);
        assert!(result.issues.iter().any(|issue| {
            issue.severity == StrategyConfigValidationSeverity::Error
                && issue.code == "invalid_close_above_sma_band"
        }));
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
    fn volatility_compression_breakout_emits_buy_after_compression_breakout_and_volume() {
        let result = evaluate(compression_context(compression_breakout_candles()))
            .expect("evaluation should succeed");

        assert!(result.generated);
        assert_eq!(result.reason, SignalReason::VolatilityCompressionBreakout);
    }

    #[test]
    fn volatility_compression_breakout_no_signal_without_compression() {
        let mut candles = compression_breakout_candles();
        for candle in candles.iter_mut().take(40).skip(20) {
            candle.high = Decimal::new(112, 0);
            candle.low = Decimal::new(88, 0);
        }

        let result = diagnose(compression_context(candles)).expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::NoCompression)
        );
    }

    #[test]
    fn volatility_compression_breakout_no_signal_without_breakout() {
        let mut candles = compression_breakout_candles();
        let latest = candles.last_mut().expect("latest candle");
        latest.close = Decimal::new(1005, 1);

        let result = diagnose(compression_context(candles)).expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::NoBreakout)
        );
    }

    #[test]
    fn volatility_compression_breakout_no_signal_when_breakout_too_extended() {
        let mut candles = compression_breakout_candles();
        let latest = candles.last_mut().expect("latest candle");
        latest.close = Decimal::new(103, 0);
        latest.high = Decimal::new(104, 0);

        let result = diagnose(compression_context(candles)).expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::BreakoutTooExtended)
        );
    }

    #[test]
    fn volatility_compression_breakout_no_signal_without_volume_confirmation() {
        let mut candles = compression_breakout_candles();
        candles.last_mut().expect("latest candle").volume = Decimal::new(10, 0);

        let result = diagnose(compression_context(candles)).expect("diagnostics should succeed");

        assert_eq!(result.final_decision, StrategyDiagnosticsDecision::NoSignal);
        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::VolumeNotConfirmed)
        );
    }

    #[test]
    fn volatility_compression_breakout_validation_rejects_invalid_config() {
        let mut request = sample_request("volatility_compression_breakout_v1");
        request.lookback_candles = 20;
        request.compression_lookback_candles = Some(40);
        request.breakout_lookback_candles = Some(20);

        let result = validate_strategy_config(&request, &validation_context());

        assert!(!result.valid);
        assert!(result.issues.iter().any(|issue| {
            issue.severity == StrategyConfigValidationSeverity::Error
                && issue.code == "compression_lookback_above_breakout_lookback"
        }));
    }

    #[test]
    fn volatility_compression_breakout_diagnostics_include_exact_no_signal_reason() {
        let mut candles = compression_breakout_candles();
        candles.last_mut().expect("latest candle").volume = Decimal::new(10, 0);

        let result = diagnose(compression_context(candles)).expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::VolumeNotConfirmed)
        );
        assert!(result
            .condition_checks
            .iter()
            .any(|check| check.name == "volume_confirmed" && !check.passed));
    }

    #[test]
    fn volatility_compression_breakout_opportunity_counts_blockers() {
        let mut candles = compression_breakout_candles();
        candles.extend(compression_breakout_candles().into_iter().enumerate().map(
            |(offset, mut candle)| {
                candle.open_time += Duration::hours(100 + offset as i64);
                candle.close_time += Duration::hours(100 + offset as i64);
                candle
            },
        ));
        let context = compression_context(candles.clone());
        let request = opportunity_request(StrategyId::VolatilityCompressionBreakoutV1);

        let result = analyze_opportunity(&request, &context.config, &candles, Utc::now())
            .expect("opportunity should succeed");

        assert!(result.evaluable_windows > 0);
        assert!(result
            .condition_pass_rates
            .iter()
            .any(|rate| rate.condition == "compression_passed"));
        assert!(result
            .condition_pass_rates
            .iter()
            .any(|rate| rate.condition == "final_would_signal"));
    }

    #[test]
    fn trend_pullback_continuation_emits_buy_when_conditions_pass() {
        let result = evaluate(pullback_context(pullback_candles(
            Decimal::new(1282, 1),
            Decimal::new(1275, 1),
        )))
        .expect("evaluation should succeed");

        assert!(result.generated);
        assert_eq!(result.reason, SignalReason::TrendPullbackContinuation);
    }

    #[test]
    fn trend_pullback_continuation_no_signal_when_pullback_too_shallow() {
        let mut candles = pullback_candles(Decimal::new(1318, 1), Decimal::new(1310, 1));
        candles.last_mut().expect("latest candle").low = Decimal::new(1318, 1);
        let result = diagnose(pullback_context(candles)).expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::PullbackTooShallow)
        );
    }

    #[test]
    fn trend_pullback_continuation_no_signal_when_trend_not_confirmed() {
        let mut context = pullback_context(pullback_candles(
            Decimal::new(1282, 1),
            Decimal::new(1275, 1),
        ));
        context.config.min_trend_return_pct = Some(Decimal::new(50, 0));

        let result = diagnose(context).expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::TrendNotConfirmed)
        );
        assert!(result
            .condition_checks
            .iter()
            .any(|check| check.name == "recent_high"));
        assert!(result.summary.contains("final_decision=NO_SIGNAL"));
    }

    #[test]
    fn trend_pullback_continuation_no_signal_when_pullback_too_deep() {
        let result = diagnose(pullback_context(pullback_candles(
            Decimal::new(1200, 1),
            Decimal::new(1195, 1),
        )))
        .expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::PullbackTooDeep)
        );
    }

    #[test]
    fn trend_pullback_continuation_no_signal_when_close_too_extended_above_sma() {
        let mut context = pullback_context(pullback_candles(
            Decimal::new(1298, 1),
            Decimal::new(1290, 1),
        ));
        context.config.max_close_above_sma_pct = Some(Decimal::new(1, 1));

        let result = diagnose(context).expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::CloseTooExtendedAboveSma)
        );
    }

    #[test]
    fn trend_pullback_continuation_no_signal_when_reclaim_fails() {
        let result = diagnose(pullback_context(pullback_candles(
            Decimal::new(1282, 1),
            Decimal::new(1290, 1),
        )))
        .expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::ReclaimNotConfirmed)
        );
    }

    #[test]
    fn trend_pullback_continuation_no_signal_when_too_choppy() {
        let mut context = pullback_context(pullback_candles(
            Decimal::new(1282, 1),
            Decimal::new(1275, 1),
        ));
        context.config.max_choppiness = Some(Decimal::new(1, 0));

        let result = diagnose(context).expect("diagnostics should succeed");

        assert_eq!(
            result.no_signal_reason,
            Some(StrategyNoSignalReason::TooChoppy)
        );
    }

    #[test]
    fn trend_pullback_continuation_validation_rejects_invalid_config() {
        let mut request = sample_request("trend_pullback_continuation_v1");
        request.timeframe = "1h".to_string();
        request.lookback_candles = 20;
        request.trend_lookback_candles = Some(5);
        request.pullback_lookback_candles = Some(10);

        let result = validate_strategy_config(&request, &validation_context());

        assert!(!result.valid);
        assert!(result
            .issues
            .iter()
            .any(|issue| issue.code == "trend_lookback_below_pullback_lookback"));
    }

    #[test]
    fn trend_pullback_continuation_opportunity_counts_blockers() {
        let mut candles = pullback_candles(Decimal::new(1318, 1), Decimal::new(1310, 1));
        candles.last_mut().expect("latest candle").low = Decimal::new(1318, 1);
        let config = pullback_context(candles.clone()).config;
        let request = opportunity_request(StrategyId::TrendPullbackContinuationV1);

        let result =
            analyze_opportunity(&request, &config, &candles, Utc::now()).expect("opportunity");

        assert!(result
            .top_blocking_conditions
            .iter()
            .any(|row| row.condition == "pullback_depth_valid"));
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

    #[test]
    fn range_reversion_opportunity_identifies_not_near_lower_band_blocker() {
        let config = default_config(StrategyId::RangeReversionV1, CandleInterval::FifteenMinutes);
        let candles = (0..30)
            .map(|index| {
                range_candle(
                    index,
                    Decimal::new(101, 0),
                    Decimal::new(102, 0),
                    Decimal::new(100, 0),
                    Decimal::new(1018, 1),
                )
            })
            .collect::<Vec<_>>();
        let result = analyze_opportunity(
            &opportunity_request(StrategyId::RangeReversionV1),
            &config,
            &candles,
            Utc::now(),
        )
        .expect("analysis should succeed");

        assert_eq!(result.would_signal_count, 0);
        assert_eq!(
            result
                .top_blocking_conditions
                .first()
                .map(|row| row.condition.as_str()),
            Some("near_lower_band")
        );
        assert_eq!(
            result.recommendation.status,
            StrategyOpportunityStatus::TooRestrictive
        );
    }

    #[test]
    fn trend_filter_opportunity_counts_close_above_sma_passes_and_failures() {
        let mut config = default_config(
            StrategyId::TrendFilterMomentumV1,
            CandleInterval::FifteenMinutes,
        );
        config.trend_lookback_candles = Some(3);
        config.momentum_lookback_candles = Some(2);
        let closes = [100, 101, 102, 99, 103, 104, 98, 105];
        let candles = closes
            .iter()
            .enumerate()
            .map(|(index, close)| {
                range_candle(
                    index as i64,
                    Decimal::new(*close - 1, 0),
                    Decimal::new(*close + 1, 0),
                    Decimal::new(*close - 2, 0),
                    Decimal::new(*close, 0),
                )
            })
            .collect::<Vec<_>>();
        let result = analyze_opportunity(
            &opportunity_request(StrategyId::TrendFilterMomentumV1),
            &config,
            &candles,
            Utc::now(),
        )
        .expect("analysis should succeed");
        let close_above_sma = result
            .condition_pass_rates
            .iter()
            .find(|row| row.condition == "close_above_sma")
            .expect("close_above_sma row");

        assert!(close_above_sma.passed_count > 0);
        assert!(close_above_sma.failed_count > 0);
    }

    #[test]
    fn trend_filter_v2_opportunity_counts_band_and_momentum_conditions() {
        let mut config = default_config(
            StrategyId::TrendFilterMomentumV2,
            CandleInterval::FifteenMinutes,
        );
        config.trend_lookback_candles = Some(3);
        config.momentum_lookback_candles = Some(2);
        config.max_close_above_sma_pct = Some(Decimal::ONE);
        let closes = [100, 100, 100, 1005, 100, 101, 100, 1020];
        let candles = closes
            .iter()
            .enumerate()
            .map(|(index, close)| {
                let close = Decimal::new(*close, 1);
                range_candle(
                    index as i64,
                    close - Decimal::ONE,
                    close + Decimal::ONE,
                    close - Decimal::new(2, 0),
                    close,
                )
            })
            .collect::<Vec<_>>();
        let result = analyze_opportunity(
            &opportunity_request(StrategyId::TrendFilterMomentumV2),
            &config,
            &candles,
            Utc::now(),
        )
        .expect("analysis should succeed");

        for condition in [
            "close_above_sma",
            "close_within_sma_band",
            "momentum_confirmed",
            "final_would_signal",
        ] {
            assert!(
                result
                    .condition_pass_rates
                    .iter()
                    .any(|row| row.condition == condition),
                "missing condition {condition}"
            );
        }
    }

    #[test]
    fn range_reversion_opportunity_count_matches_evaluator_signal_count() {
        let config = default_config(StrategyId::RangeReversionV1, CandleInterval::FifteenMinutes);
        let candles = (0..30)
            .map(|index| {
                range_candle(
                    index,
                    Decimal::new(1003, 1),
                    Decimal::new(102, 0),
                    Decimal::new(100, 0),
                    Decimal::new(1004, 1),
                )
            })
            .collect::<Vec<_>>();

        let result = analyze_opportunity(
            &opportunity_request(StrategyId::RangeReversionV1),
            &config,
            &candles,
            Utc::now(),
        )
        .expect("analysis should succeed");
        let evaluator_count = (required_candle_count(&config) as usize..=candles.len())
            .filter(|end| {
                let latest = &candles[*end - 1];
                evaluate(StrategyEvaluationContext {
                    correlation_id: Uuid::new_v4(),
                    strategy_id: StrategyId::RangeReversionV1,
                    symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
                    config: config.clone(),
                    candles: candles[..*end].to_vec(),
                    evaluated_at: latest.close_time,
                })
                .expect("evaluation should succeed")
                .generated
            })
            .count() as i64;

        assert_eq!(result.would_signal_count, evaluator_count);
        assert!(result.would_signal_count > 0);
    }

    #[test]
    fn range_reversion_opportunity_honors_confidence_floor_like_evaluator() {
        let mut config =
            default_config(StrategyId::RangeReversionV1, CandleInterval::FifteenMinutes);
        config.confidence_floor = Some(Decimal::new(67, 2));
        let candles = range_reversion_candles(Decimal::new(1004, 1), Decimal::new(1003, 1));

        let result = analyze_opportunity(
            &opportunity_request(StrategyId::RangeReversionV1),
            &config,
            &candles,
            Utc::now(),
        )
        .expect("analysis should succeed");
        let evaluation = evaluate(StrategyEvaluationContext {
            correlation_id: Uuid::new_v4(),
            strategy_id: StrategyId::RangeReversionV1,
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            config,
            candles,
            evaluated_at: Utc::now(),
        })
        .expect("evaluation should succeed");

        assert_eq!(result.would_signal_count, 0);
        assert!(!evaluation.generated);
    }

    #[test]
    fn trend_filter_too_loose_recommendation_tightens_strategy() {
        let mut config = default_config(
            StrategyId::TrendFilterMomentumV1,
            CandleInterval::FifteenMinutes,
        );
        config.trend_lookback_candles = Some(3);
        config.momentum_lookback_candles = Some(2);
        let candles = (0..30)
            .map(|index| {
                let close = 100 + index;
                range_candle(
                    index,
                    Decimal::new(close - 1, 0),
                    Decimal::new(close + 1, 0),
                    Decimal::new(close - 2, 0),
                    Decimal::new(close, 0),
                )
            })
            .collect::<Vec<_>>();

        let result = analyze_opportunity(
            &opportunity_request(StrategyId::TrendFilterMomentumV1),
            &config,
            &candles,
            Utc::now(),
        )
        .expect("analysis should succeed");
        let joined = result.recommendation.messages.join(" ");

        assert_eq!(
            result.recommendation.status,
            StrategyOpportunityStatus::TooLoose
        );
        assert!(joined.contains("longer trend lookbacks"));
        assert!(!joined.contains("test shorter trend lookbacks"));
    }

    #[test]
    fn opportunity_recommendation_ordering_is_deterministic() {
        let config = default_config(StrategyId::RangeReversionV1, CandleInterval::FifteenMinutes);
        let candles = (0..30)
            .map(|index| {
                range_candle(
                    index,
                    Decimal::new(101, 0),
                    Decimal::new(102, 0),
                    Decimal::new(100, 0),
                    Decimal::new(1018, 1),
                )
            })
            .collect::<Vec<_>>();
        let first = analyze_opportunity(
            &opportunity_request(StrategyId::RangeReversionV1),
            &config,
            &candles,
            Utc::now(),
        )
        .expect("analysis should succeed");
        let second = analyze_opportunity(
            &opportunity_request(StrategyId::RangeReversionV1),
            &config,
            &candles,
            Utc::now(),
        )
        .expect("analysis should succeed");

        assert_eq!(
            first.top_blocking_conditions,
            second.top_blocking_conditions
        );
        assert_eq!(
            first.recommendation.messages,
            second.recommendation.messages
        );
    }

    #[test]
    fn opportunity_handles_insufficient_data() {
        let config = default_config(StrategyId::RangeReversionV1, CandleInterval::FifteenMinutes);
        let result = analyze_opportunity(
            &opportunity_request(StrategyId::RangeReversionV1),
            &config,
            &[],
            Utc::now(),
        )
        .expect("analysis should succeed");

        assert_eq!(result.evaluable_windows, 0);
        assert_eq!(
            result.data_quality_status,
            StrategyOpportunityStatus::InsufficientData
        );
        assert_eq!(
            result.recommendation.status,
            StrategyOpportunityStatus::InsufficientData
        );
    }
}

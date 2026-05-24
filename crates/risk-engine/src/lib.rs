use aegis_core::{
    RiskCheckContext, RiskConfig, RiskConfigValidationIssue, RiskConfigValidationResult,
    RiskEvaluationDecision, RiskEvaluationResult, RiskRejectionReason, RiskRuleDecision,
    RiskRuleResult, StrategyConfigValidationSeverity,
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct RiskStateSnapshot {
    pub kill_switch_enabled: bool,
    pub kill_switch_reason: Option<String>,
    pub open_positions_count: Option<u32>,
    pub daily_loss_pct: Option<Decimal>,
    pub weekly_loss_pct: Option<Decimal>,
    pub consecutive_losses: Option<u32>,
    pub latest_market_data_at: Option<DateTime<Utc>>,
    pub last_trade_at: Option<DateTime<Utc>>,
}

pub fn validate_risk_config(request: &RiskConfig) -> RiskConfigValidationResult {
    let validated_at = Utc::now();
    let mut issues = Vec::new();

    if !(1..=50).contains(&request.max_open_positions) {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_max_open_positions",
            "max_open_positions",
            "max_open_positions must be between 1 and 50",
        ));
    }
    validate_decimal_range(
        request.max_daily_loss_pct,
        Decimal::new(1, 2),
        Decimal::new(20, 0),
        "max_daily_loss_pct",
        &mut issues,
    );
    validate_decimal_range(
        request.max_weekly_loss_pct,
        Decimal::new(1, 2),
        Decimal::new(50, 0),
        "max_weekly_loss_pct",
        &mut issues,
    );
    if request.max_weekly_loss_pct < request.max_daily_loss_pct {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "weekly_loss_below_daily_loss",
            "max_weekly_loss_pct",
            "max_weekly_loss_pct must be greater than or equal to max_daily_loss_pct",
        ));
    }
    if request.max_position_notional <= Decimal::ZERO {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_max_position_notional",
            "max_position_notional",
            "max_position_notional must be greater than zero",
        ));
    }
    validate_decimal_range(
        request.max_slippage_pct,
        Decimal::ZERO,
        Decimal::new(5, 0),
        "max_slippage_pct",
        &mut issues,
    );
    if !(1..=20).contains(&request.max_consecutive_losses) {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_max_consecutive_losses",
            "max_consecutive_losses",
            "max_consecutive_losses must be between 1 and 20",
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
    if !(1_000..=300_000).contains(&request.max_signal_age_ms) {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_max_signal_age_ms",
            "max_signal_age_ms",
            "max_signal_age_ms must be between 1_000 and 300_000",
        ));
    }
    if !(1..=3_600).contains(&request.stale_feed_threshold_seconds) {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            "invalid_stale_feed_threshold_seconds",
            "stale_feed_threshold_seconds",
            "stale_feed_threshold_seconds must be between 1 and 3_600",
        ));
    }

    let valid = !issues
        .iter()
        .any(|issue| issue.severity == StrategyConfigValidationSeverity::Error);

    RiskConfigValidationResult {
        valid,
        issues,
        normalized_config: valid.then_some(request.clone()),
        validated_at,
    }
}

pub trait RiskRule: Send + Sync {
    fn evaluate(
        &self,
        context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult;
}

#[derive(Debug, Default)]
pub struct KillSwitchRule;

impl RiskRule for KillSwitchRule {
    fn evaluate(
        &self,
        _context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
        _config: &RiskConfig,
    ) -> RiskRuleResult {
        if snapshot.kill_switch_enabled {
            return RiskRuleResult {
                rule_name: "kill_switch".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::KillSwitchActive),
                message: snapshot
                    .kill_switch_reason
                    .clone()
                    .or_else(|| Some("persistent kill switch is enabled".to_string())),
            };
        }

        RiskRuleResult {
            rule_name: "kill_switch".to_string(),
            decision: RiskRuleDecision::Pass,
            reason: None,
            message: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxOpenPositionsRule;

impl RiskRule for MaxOpenPositionsRule {
    fn evaluate(
        &self,
        _context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult {
        match snapshot.open_positions_count {
            Some(count) if count >= config.max_open_positions => RiskRuleResult {
                rule_name: "max_open_positions".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::MaxOpenPositionsExceeded),
                message: Some(format!(
                    "open positions {count} exceeds configured limit {}",
                    config.max_open_positions
                )),
            },
            Some(_) => RiskRuleResult {
                rule_name: "max_open_positions".to_string(),
                decision: RiskRuleDecision::Pass,
                reason: None,
                message: None,
            },
            None => RiskRuleResult {
                rule_name: "max_open_positions".to_string(),
                decision: RiskRuleDecision::Warn,
                reason: None,
                message: Some("TODO: open position query is not wired yet".to_string()),
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxDailyLossRule;

impl RiskRule for MaxDailyLossRule {
    fn evaluate(
        &self,
        _context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult {
        match snapshot.daily_loss_pct {
            Some(loss_pct) if loss_pct >= config.max_daily_loss_pct => RiskRuleResult {
                rule_name: "max_daily_loss".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::MaxWeeklyLossExceeded),
                message: Some(format!(
                    "daily loss pct {loss_pct} exceeds configured limit {}",
                    config.max_daily_loss_pct
                )),
            },
            Some(_) => RiskRuleResult {
                rule_name: "max_daily_loss".to_string(),
                decision: RiskRuleDecision::Pass,
                reason: None,
                message: None,
            },
            None => RiskRuleResult {
                rule_name: "max_daily_loss".to_string(),
                decision: RiskRuleDecision::Warn,
                reason: None,
                message: Some("TODO: daily loss query is not wired yet".to_string()),
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxSignalAgeRule;

impl RiskRule for MaxSignalAgeRule {
    fn evaluate(
        &self,
        context: &RiskCheckContext,
        _snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult {
        let signal_age = context.evaluated_at - context.signal_created_at;
        let max_signal_age = Duration::milliseconds(config.max_signal_age_ms);

        if signal_age > max_signal_age {
            return RiskRuleResult {
                rule_name: "max_signal_age".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::SignalTooOld),
                message: Some(format!(
                    "signal age {}ms exceeds configured limit {}ms",
                    signal_age.num_milliseconds(),
                    config.max_signal_age_ms
                )),
            };
        }

        RiskRuleResult {
            rule_name: "max_signal_age".to_string(),
            decision: RiskRuleDecision::Pass,
            reason: None,
            message: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxPositionNotionalRule;

impl RiskRule for MaxPositionNotionalRule {
    fn evaluate(
        &self,
        context: &RiskCheckContext,
        _snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult {
        if context.suggested_notional > config.max_position_notional {
            return RiskRuleResult {
                rule_name: "max_position_notional".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::PositionNotionalExceeded),
                message: Some(format!(
                    "suggested notional {} exceeds configured limit {}",
                    context.suggested_notional, config.max_position_notional
                )),
            };
        }

        RiskRuleResult {
            rule_name: "max_position_notional".to_string(),
            decision: RiskRuleDecision::Pass,
            reason: None,
            message: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct DuplicateOrderRule;

impl RiskRule for DuplicateOrderRule {
    fn evaluate(
        &self,
        _context: &RiskCheckContext,
        _snapshot: &RiskStateSnapshot,
        _config: &RiskConfig,
    ) -> RiskRuleResult {
        RiskRuleResult {
            rule_name: "duplicate_order".to_string(),
            decision: RiskRuleDecision::Warn,
            reason: None,
            message: Some(
                "TODO: duplicate order detection requires persisted order-intent lookup"
                    .to_string(),
            ),
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxWeeklyLossRule;

impl RiskRule for MaxWeeklyLossRule {
    fn evaluate(
        &self,
        _context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult {
        match snapshot.weekly_loss_pct {
            Some(loss_pct) if loss_pct >= config.max_weekly_loss_pct => RiskRuleResult {
                rule_name: "max_weekly_loss".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::MaxDailyLossExceeded),
                message: Some(format!(
                    "weekly loss pct {loss_pct} exceeds configured limit {}",
                    config.max_weekly_loss_pct
                )),
            },
            Some(_) => RiskRuleResult {
                rule_name: "max_weekly_loss".to_string(),
                decision: RiskRuleDecision::Pass,
                reason: None,
                message: None,
            },
            None => RiskRuleResult {
                rule_name: "max_weekly_loss".to_string(),
                decision: RiskRuleDecision::Warn,
                reason: None,
                message: Some("weekly loss pct is unavailable".to_string()),
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxConsecutiveLossesRule;

impl RiskRule for MaxConsecutiveLossesRule {
    fn evaluate(
        &self,
        _context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult {
        match snapshot.consecutive_losses {
            Some(losses) if losses >= config.max_consecutive_losses => RiskRuleResult {
                rule_name: "max_consecutive_losses".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::MaxConsecutiveLossesExceeded),
                message: Some(format!(
                    "consecutive losses {losses} exceeds configured limit {}",
                    config.max_consecutive_losses
                )),
            },
            Some(_) => RiskRuleResult {
                rule_name: "max_consecutive_losses".to_string(),
                decision: RiskRuleDecision::Pass,
                reason: None,
                message: None,
            },
            None => RiskRuleResult {
                rule_name: "max_consecutive_losses".to_string(),
                decision: RiskRuleDecision::Warn,
                reason: None,
                message: Some("consecutive losses are unavailable".to_string()),
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct CooldownRule;

impl RiskRule for CooldownRule {
    fn evaluate(
        &self,
        context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult {
        match snapshot.last_trade_at {
            Some(last_trade_at)
                if context.evaluated_at - last_trade_at
                    < Duration::seconds(config.cooldown_seconds.into()) =>
            {
                RiskRuleResult {
                    rule_name: "cooldown".to_string(),
                    decision: RiskRuleDecision::Reject,
                    reason: Some(RiskRejectionReason::CooldownActive),
                    message: Some(format!(
                        "cooldown active until {}",
                        (last_trade_at + Duration::seconds(config.cooldown_seconds.into()))
                            .to_rfc3339()
                    )),
                }
            }
            Some(_) => RiskRuleResult {
                rule_name: "cooldown".to_string(),
                decision: RiskRuleDecision::Pass,
                reason: None,
                message: None,
            },
            None => RiskRuleResult {
                rule_name: "cooldown".to_string(),
                decision: RiskRuleDecision::Warn,
                reason: None,
                message: Some("latest trade timestamp is unavailable".to_string()),
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct DataFreshnessRule;

impl RiskRule for DataFreshnessRule {
    fn evaluate(
        &self,
        context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
        config: &RiskConfig,
    ) -> RiskRuleResult {
        match snapshot.latest_market_data_at {
            Some(last_update)
                if context.evaluated_at - last_update
                    > Duration::seconds(config.stale_feed_threshold_seconds.into()) =>
            {
                RiskRuleResult {
                    rule_name: "data_freshness".to_string(),
                    decision: RiskRuleDecision::Reject,
                    reason: Some(RiskRejectionReason::DataStale),
                    message: Some("latest market data is stale".to_string()),
                }
            }
            Some(_) => RiskRuleResult {
                rule_name: "data_freshness".to_string(),
                decision: RiskRuleDecision::Pass,
                reason: None,
                message: None,
            },
            None => RiskRuleResult {
                rule_name: "data_freshness".to_string(),
                decision: RiskRuleDecision::Warn,
                reason: None,
                message: Some("TODO: market data freshness source is not wired yet".to_string()),
            },
        }
    }
}

pub struct RiskEvaluator {
    config: RiskConfig,
    rules: Vec<Box<dyn RiskRule>>,
}

impl RiskEvaluator {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            rules: vec![
                Box::new(KillSwitchRule),
                Box::new(MaxOpenPositionsRule),
                Box::new(MaxDailyLossRule),
                Box::new(MaxWeeklyLossRule),
                Box::new(MaxConsecutiveLossesRule),
                Box::new(CooldownRule),
                Box::new(MaxSignalAgeRule),
                Box::new(MaxPositionNotionalRule),
                Box::new(DuplicateOrderRule),
                Box::new(DataFreshnessRule),
            ],
        }
    }

    pub fn evaluate(
        &self,
        context: &RiskCheckContext,
        snapshot: &RiskStateSnapshot,
    ) -> RiskEvaluationResult {
        let rule_results: Vec<RiskRuleResult> = self
            .rules
            .iter()
            .map(|rule| rule.evaluate(context, snapshot, &self.config))
            .collect();

        let reasons: Vec<RiskRejectionReason> = rule_results
            .iter()
            .filter_map(|result| {
                (result.decision == RiskRuleDecision::Reject)
                    .then_some(result.reason)
                    .flatten()
            })
            .collect();

        let rejected = !reasons.is_empty();

        RiskEvaluationResult {
            risk_decision_id: Uuid::new_v4(),
            decision: if rejected {
                RiskEvaluationDecision::Rejected
            } else {
                RiskEvaluationDecision::Approved
            },
            approved_notional: (!rejected).then_some(context.suggested_notional),
            risk_score: if rejected {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
            reasons,
            rule_results,
            correlation_id: context.correlation_id,
        }
    }
}

fn validate_decimal_range(
    value: Decimal,
    min: Decimal,
    max: Decimal,
    field: &str,
    issues: &mut Vec<RiskConfigValidationIssue>,
) {
    if value < min || value > max {
        issues.push(issue(
            StrategyConfigValidationSeverity::Error,
            &format!("invalid_{field}"),
            field,
            &format!("{field} must be between {min} and {max}"),
        ));
    }
}

fn issue(
    severity: StrategyConfigValidationSeverity,
    code: &str,
    field: &str,
    message: &str,
) -> RiskConfigValidationIssue {
    RiskConfigValidationIssue {
        severity,
        code: code.to_string(),
        field: field.to_string(),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{RiskEvaluator, RiskStateSnapshot};
    use aegis_core::{
        RiskCheckContext, RiskConfig, RiskEvaluationDecision, RiskRejectionReason, Side, Symbol,
    };
    use chrono::{Duration, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_context() -> RiskCheckContext {
        let now = Utc::now();

        RiskCheckContext {
            signal_id: Uuid::new_v4(),
            correlation_id: Uuid::new_v4(),
            strategy_id: "momentum_v1".to_string(),
            symbol: Symbol::new("BTCUSDT").expect("valid symbol"),
            side: Side::Buy,
            suggested_notional: Decimal::new(100_000, 0),
            signal_created_at: now,
            evaluated_at: now,
        }
    }

    #[test]
    fn kill_switch_active_rejects() {
        let evaluator = RiskEvaluator::new(RiskConfig::default());
        let context = sample_context();
        let snapshot = RiskStateSnapshot {
            kill_switch_enabled: true,
            kill_switch_reason: Some("manual stop".to_string()),
            ..RiskStateSnapshot::default()
        };

        let result = evaluator.evaluate(&context, &snapshot);

        assert_eq!(result.decision, RiskEvaluationDecision::Rejected);
        assert!(result
            .reasons
            .contains(&RiskRejectionReason::KillSwitchActive));
    }

    #[test]
    fn kill_switch_inactive_allows_evaluation_to_continue() {
        let evaluator = RiskEvaluator::new(RiskConfig::default());
        let context = sample_context();
        let snapshot = RiskStateSnapshot::default();

        let result = evaluator.evaluate(&context, &snapshot);

        assert_eq!(result.decision, RiskEvaluationDecision::Approved);
        assert!(result.reasons.is_empty());
        assert_eq!(result.approved_notional, Some(context.suggested_notional));
    }

    #[test]
    fn stale_signal_rejects() {
        let evaluator = RiskEvaluator::new(RiskConfig {
            max_signal_age_ms: 5_000,
            ..RiskConfig::default()
        });
        let mut context = sample_context();
        context.signal_created_at = context.evaluated_at - Duration::seconds(10);

        let result = evaluator.evaluate(&context, &RiskStateSnapshot::default());

        assert_eq!(result.decision, RiskEvaluationDecision::Rejected);
        assert!(result.reasons.contains(&RiskRejectionReason::SignalTooOld));
    }

    #[test]
    fn oversized_notional_rejects() {
        let evaluator = RiskEvaluator::new(RiskConfig {
            max_position_notional: Decimal::new(50_000, 0),
            ..RiskConfig::default()
        });
        let context = sample_context();

        let result = evaluator.evaluate(&context, &RiskStateSnapshot::default());

        assert_eq!(result.decision, RiskEvaluationDecision::Rejected);
        assert!(result
            .reasons
            .contains(&RiskRejectionReason::PositionNotionalExceeded));
    }
}

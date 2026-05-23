use aegis_core::{
    RiskCheckContext, RiskConfig, RiskEvaluationDecision, RiskEvaluationResult,
    RiskRejectionReason, RiskRuleDecision, RiskRuleResult,
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct RiskStateSnapshot {
    pub kill_switch_enabled: bool,
    pub kill_switch_reason: Option<String>,
    pub open_positions_count: Option<u32>,
    pub daily_loss: Option<Decimal>,
    pub latest_market_data_at: Option<DateTime<Utc>>,
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
        match snapshot.daily_loss {
            Some(loss) if loss >= config.max_daily_loss => RiskRuleResult {
                rule_name: "max_daily_loss".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::MaxDailyLossExceeded),
                message: Some(format!(
                    "daily loss {loss} exceeds configured limit {}",
                    config.max_daily_loss
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
        let max_signal_age = Duration::seconds(config.max_signal_age_secs);

        if signal_age > max_signal_age {
            return RiskRuleResult {
                rule_name: "max_signal_age".to_string(),
                decision: RiskRuleDecision::Reject,
                reason: Some(RiskRejectionReason::SignalTooOld),
                message: Some(format!(
                    "signal age {}s exceeds configured limit {}s",
                    signal_age.num_seconds(),
                    config.max_signal_age_secs
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
                    > Duration::seconds(config.max_signal_age_secs) =>
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
            max_signal_age_secs: 5,
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

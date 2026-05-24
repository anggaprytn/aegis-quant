use crate::{ensure_strategy_config, persist_paper_fill_accounting, AppState};
use aegis_core::{
    Candle, CandleInterval, DataFreshnessStatus, EventEnvelope, OrderIntentSource,
    PaperTradingPipelineRequest, PaperTradingPipelineResult, PipelineDecision,
    PipelineRejectionReason, PipelineStepStatus, RiskCheckContext, RiskEvaluationDecision, Side,
    SignalReason, SignalSide, StrategyEvaluationContext, StrategyRiskExecutionTrace,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use db::{
    create_paper_order, get_order_by_idempotency_key, get_recent_closed_candles, get_risk_config,
    insert_risk_decision, insert_signal_deduped, insert_system_event, list_market_feed_statuses,
    load_risk_state_snapshot, risk_config_from_record, update_strategy_state, CreateOrderError,
};
use risk_engine::RiskEvaluator;
use rust_decimal::Decimal;
use serde_json::json;
use strategy_engine::{evaluate as evaluate_strategy, required_candle_count};
use telemetry::telemetry;
use uuid::Uuid;

pub async fn run_paper_pipeline(
    state: &AppState,
    request: PaperTradingPipelineRequest,
) -> Result<PaperTradingPipelineResult> {
    let strategy_id = request.strategy_id.parse().context("invalid strategy_id")?;
    let symbol = aegis_core::Symbol::new(request.symbol.clone()).context("invalid symbol")?;
    let timeframe: CandleInterval = request.timeframe.parse().context("invalid timeframe")?;
    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);

    insert_pipeline_event(
        state,
        correlation_id,
        "paper_pipeline.started",
        json!({
            "strategy_id": request.strategy_id,
            "symbol": request.symbol,
            "timeframe": request.timeframe,
        }),
    )
    .await?;

    let config = ensure_strategy_config(state, strategy_id).await?;
    if !config.enabled {
        telemetry().inc_strategy_disabled(request.strategy_id.as_str());
        telemetry().inc_paper_pipeline_run(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            "strategy_disabled",
        );
        let result = terminal_result(
            PipelineDecision::StrategyDisabled,
            &request,
            correlation_id,
            vec![PipelineRejectionReason::StrategyDisabled],
            StrategyRiskExecutionTrace {
                strategy_evaluation: PipelineStepStatus::Skipped,
                signal: PipelineStepStatus::Skipped,
                risk_evaluation: PipelineStepStatus::Skipped,
                paper_order: PipelineStepStatus::Skipped,
                order_intent_source: None,
            },
        );
        insert_pipeline_event(
            state,
            correlation_id,
            "paper_pipeline.strategy_disabled",
            json!({ "strategy_id": request.strategy_id, "symbol": request.symbol }),
        )
        .await?;
        return Ok(result);
    }

    if config.timeframe != timeframe {
        telemetry().inc_paper_pipeline_run(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            "safety_stopped",
        );
        let result = terminal_result(
            PipelineDecision::SafetyStopped,
            &request,
            correlation_id,
            vec![PipelineRejectionReason::UnsupportedTimeframe],
            StrategyRiskExecutionTrace {
                strategy_evaluation: PipelineStepStatus::Skipped,
                signal: PipelineStepStatus::Skipped,
                risk_evaluation: PipelineStepStatus::Skipped,
                paper_order: PipelineStepStatus::Skipped,
                order_intent_source: None,
            },
        );
        insert_pipeline_event(
            state,
            correlation_id,
            "paper_pipeline.safety_stopped",
            json!({
                "reason": PipelineRejectionReason::UnsupportedTimeframe.as_str(),
                "strategy_id": request.strategy_id,
                "symbol": request.symbol,
                "timeframe": request.timeframe,
            }),
        )
        .await?;
        return Ok(result);
    }

    if let Some(reason) = feed_stop_reason(state, &symbol).await? {
        telemetry().inc_paper_pipeline_run(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            "safety_stopped",
        );
        let result = terminal_result(
            PipelineDecision::SafetyStopped,
            &request,
            correlation_id,
            vec![reason],
            StrategyRiskExecutionTrace {
                strategy_evaluation: PipelineStepStatus::Skipped,
                signal: PipelineStepStatus::Skipped,
                risk_evaluation: PipelineStepStatus::Skipped,
                paper_order: PipelineStepStatus::Skipped,
                order_intent_source: None,
            },
        );
        insert_pipeline_event(
            state,
            correlation_id,
            "paper_pipeline.safety_stopped",
            json!({
                "reason": reason.as_str(),
                "strategy_id": request.strategy_id,
                "symbol": request.symbol,
                "timeframe": request.timeframe,
            }),
        )
        .await?;
        return Ok(result);
    }

    let required_candles = required_candle_count(&config);
    let candles = get_recent_closed_candles(&state.db_pool, &symbol, timeframe, required_candles)
        .await
        .context("failed to query closed candles")?;

    let evaluation = evaluate_strategy(StrategyEvaluationContext {
        correlation_id,
        strategy_id,
        symbol: symbol.clone(),
        config,
        candles: candles.clone(),
        evaluated_at: Utc::now(),
    })
    .context("failed to evaluate strategy")?;

    if let Some(signal) = evaluation.signal.clone() {
        telemetry().inc_strategy_evaluation(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            "signal_generated",
        );
        let signal_outcome = insert_signal_deduped(&state.db_pool, &signal)
            .await
            .context("failed to persist signal")?;
        if signal_outcome.inserted {
            telemetry().inc_strategy_signal(
                request.strategy_id.as_str(),
                request.symbol.as_str(),
                signal.side.as_str(),
            );
        }

        update_strategy_state(
            &state.db_pool,
            strategy_id,
            evaluation.evaluated_at,
            evaluation.reason,
            Some(signal_outcome.signal.id),
            Some(signal_outcome.signal.created_at),
        )
        .await
        .context("failed to update strategy state after signal generation")?;

        if signal_outcome.inserted {
            insert_pipeline_event(
                state,
                signal_outcome.signal.correlation_id,
                "signal.generated",
                json!({
                    "signal_id": signal_outcome.signal.id,
                    "strategy_id": signal_outcome.signal.strategy_id,
                    "symbol": signal_outcome.signal.symbol,
                    "side": signal_outcome.signal.side,
                    "confidence": signal_outcome.signal.confidence,
                    "timeframe": signal_outcome.signal.timeframe,
                    "reason": signal_outcome.signal.reason,
                    "suggested_notional": signal_outcome.signal.suggested_notional,
                    "source_candle_open_time": signal_outcome.signal.source_candle_open_time,
                    "correlation_id": signal_outcome.signal.correlation_id,
                }),
            )
            .await?;
        }

        let risk_context = RiskCheckContext {
            signal_id: signal_outcome.signal.id,
            correlation_id,
            strategy_id: signal_outcome.signal.strategy_id.clone(),
            symbol: symbol.clone(),
            side: parse_signal_side(&signal_outcome.signal.side)?,
            suggested_notional: signal_outcome.signal.suggested_notional,
            signal_created_at: signal_outcome.signal.created_at,
            evaluated_at: Utc::now(),
        };
        let snapshot = load_risk_state_snapshot(&state.db_pool)
            .await
            .context("failed to load risk state snapshot")?;
        let risk_config = get_risk_config(&state.db_pool)
            .await
            .context("failed to load persisted risk config")?
            .map(|record| risk_config_from_record(&record))
            .transpose()
            .context("persisted risk config is invalid")?
            .unwrap_or_default();
        let evaluator = RiskEvaluator::new(risk_config);
        let risk_evaluation = evaluator.evaluate(&risk_context, &snapshot);
        let persisted_risk = insert_risk_decision(
            &state.db_pool,
            &state.config.app_name,
            &risk_context,
            &risk_evaluation,
        )
        .await
        .context("failed to persist risk decision")?;
        let decision_label = match risk_evaluation.decision {
            RiskEvaluationDecision::Approved => "approved",
            RiskEvaluationDecision::Rejected => "rejected",
        };
        telemetry().inc_risk_decision(
            decision_label,
            primary_risk_reason_label(&risk_evaluation.reasons),
        );

        if risk_evaluation.decision == RiskEvaluationDecision::Rejected {
            for reason in &risk_evaluation.reasons {
                telemetry().inc_risk_rejection(crate::reason_code(*reason));
            }
            telemetry().inc_paper_pipeline_run(
                request.strategy_id.as_str(),
                request.symbol.as_str(),
                "risk_rejected",
            );
            let result = PaperTradingPipelineResult {
                pipeline_decision: PipelineDecision::RiskRejected,
                strategy_id: request.strategy_id,
                symbol: request.symbol,
                timeframe: request.timeframe,
                signal_generated: signal_outcome.inserted,
                signal_reused: !signal_outcome.inserted,
                signal_id: Some(signal_outcome.signal.id),
                risk_decision_id: Some(persisted_risk.risk_decision_id),
                paper_order_id: None,
                execution_state: None,
                reasons: risk_evaluation
                    .reasons
                    .iter()
                    .map(|reason| crate::reason_code(*reason).to_string())
                    .collect(),
                correlation_id,
                trace: StrategyRiskExecutionTrace {
                    strategy_evaluation: PipelineStepStatus::Completed,
                    signal: if signal_outcome.inserted {
                        PipelineStepStatus::Completed
                    } else {
                        PipelineStepStatus::Reused
                    },
                    risk_evaluation: PipelineStepStatus::Rejected,
                    paper_order: PipelineStepStatus::Skipped,
                    order_intent_source: Some(OrderIntentSource::StrategySignal),
                },
            };
            insert_pipeline_event(
                state,
                correlation_id,
                "paper_pipeline.risk_rejected",
                json!({
                    "strategy_id": result.strategy_id,
                    "symbol": result.symbol,
                    "signal_id": result.signal_id,
                    "risk_decision_id": result.risk_decision_id,
                    "reasons": result.reasons,
                }),
            )
            .await?;
            return Ok(result);
        }

        let latest_candle = candles
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("closed candle history missing latest candle"))?;
        let idempotency_key = build_idempotency_key(
            &request.strategy_id,
            signal_outcome.signal.id,
            persisted_risk.risk_decision_id,
            &request.symbol,
            parse_signal_side(&signal_outcome.signal.side)?,
            signal_outcome
                .signal
                .source_candle_open_time
                .timestamp_millis(),
        );

        if let Some(existing) = get_order_by_idempotency_key(&state.db_pool, &idempotency_key)
            .await
            .context("failed to query existing paper order by idempotency key")?
        {
            telemetry().inc_paper_pipeline_run(
                request.strategy_id.as_str(),
                request.symbol.as_str(),
                "paper_order_reused",
            );
            let result = PaperTradingPipelineResult {
                pipeline_decision: PipelineDecision::PaperOrderReused,
                strategy_id: request.strategy_id,
                symbol: request.symbol,
                timeframe: request.timeframe,
                signal_generated: signal_outcome.inserted,
                signal_reused: !signal_outcome.inserted,
                signal_id: Some(signal_outcome.signal.id),
                risk_decision_id: Some(persisted_risk.risk_decision_id),
                paper_order_id: Some(existing.order_id),
                execution_state: Some(existing.execution_state),
                reasons: Vec::new(),
                correlation_id,
                trace: StrategyRiskExecutionTrace {
                    strategy_evaluation: PipelineStepStatus::Completed,
                    signal: if signal_outcome.inserted {
                        PipelineStepStatus::Completed
                    } else {
                        PipelineStepStatus::Reused
                    },
                    risk_evaluation: PipelineStepStatus::Completed,
                    paper_order: PipelineStepStatus::Reused,
                    order_intent_source: Some(OrderIntentSource::StrategySignal),
                },
            };
            insert_pipeline_event(
                state,
                correlation_id,
                "paper_pipeline.paper_order_reused",
                json!({
                    "strategy_id": result.strategy_id,
                    "symbol": result.symbol,
                    "signal_id": result.signal_id,
                    "risk_decision_id": result.risk_decision_id,
                    "paper_order_id": result.paper_order_id,
                }),
            )
            .await?;
            return Ok(result);
        }

        let order_intent = build_order_intent(
            correlation_id,
            persisted_risk.risk_decision_id,
            &idempotency_key,
            &symbol,
            parse_signal_side(&signal_outcome.signal.side)?,
            risk_evaluation
                .approved_notional
                .unwrap_or(signal_outcome.signal.suggested_notional),
            &latest_candle,
        )?;

        let order_outcome = match create_paper_order(
            &state.db_pool,
            &state.config.app_name,
            &db::StateActor::system("paper-pipeline"),
            order_intent,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(CreateOrderError::DuplicateIdempotencyKey) => {
                let existing = get_order_by_idempotency_key(&state.db_pool, &idempotency_key)
                    .await
                    .context("failed to load duplicate-safe paper order")?
                    .ok_or_else(|| anyhow!("duplicate idempotency key without existing order"))?;
                telemetry().inc_paper_pipeline_run(
                    request.strategy_id.as_str(),
                    request.symbol.as_str(),
                    "paper_order_reused",
                );
                let result = PaperTradingPipelineResult {
                    pipeline_decision: PipelineDecision::PaperOrderReused,
                    strategy_id: request.strategy_id,
                    symbol: request.symbol,
                    timeframe: request.timeframe,
                    signal_generated: signal_outcome.inserted,
                    signal_reused: !signal_outcome.inserted,
                    signal_id: Some(signal_outcome.signal.id),
                    risk_decision_id: Some(persisted_risk.risk_decision_id),
                    paper_order_id: Some(existing.order_id),
                    execution_state: Some(existing.execution_state),
                    reasons: Vec::new(),
                    correlation_id,
                    trace: StrategyRiskExecutionTrace {
                        strategy_evaluation: PipelineStepStatus::Completed,
                        signal: if signal_outcome.inserted {
                            PipelineStepStatus::Completed
                        } else {
                            PipelineStepStatus::Reused
                        },
                        risk_evaluation: PipelineStepStatus::Completed,
                        paper_order: PipelineStepStatus::Reused,
                        order_intent_source: Some(OrderIntentSource::StrategySignal),
                    },
                };
                insert_pipeline_event(
                    state,
                    correlation_id,
                    "paper_pipeline.paper_order_reused",
                    json!({
                        "strategy_id": result.strategy_id,
                        "symbol": result.symbol,
                        "signal_id": result.signal_id,
                        "risk_decision_id": result.risk_decision_id,
                        "paper_order_id": result.paper_order_id,
                    }),
                )
                .await?;
                return Ok(result);
            }
            Err(err) => return Err(err.into()),
        };
        telemetry().inc_paper_pipeline_run(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            "paper_order_created",
        );
        telemetry().inc_paper_order(
            request.symbol.as_str(),
            order_outcome.order.status.to_ascii_lowercase().as_str(),
        );
        telemetry().inc_paper_fill(request.symbol.as_str(), "buy");

        let result = PaperTradingPipelineResult {
            pipeline_decision: PipelineDecision::PaperOrderCreated,
            strategy_id: request.strategy_id,
            symbol: request.symbol,
            timeframe: request.timeframe,
            signal_generated: signal_outcome.inserted,
            signal_reused: !signal_outcome.inserted,
            signal_id: Some(signal_outcome.signal.id),
            risk_decision_id: Some(persisted_risk.risk_decision_id),
            paper_order_id: Some(order_outcome.order.order_id),
            execution_state: Some(order_outcome.order.execution_state.clone()),
            reasons: Vec::new(),
            correlation_id,
            trace: StrategyRiskExecutionTrace {
                strategy_evaluation: PipelineStepStatus::Completed,
                signal: if signal_outcome.inserted {
                    PipelineStepStatus::Completed
                } else {
                    PipelineStepStatus::Reused
                },
                risk_evaluation: PipelineStepStatus::Completed,
                paper_order: PipelineStepStatus::Completed,
                order_intent_source: Some(OrderIntentSource::StrategySignal),
            },
        };
        let paper_account = persist_paper_fill_accounting(&state.db_pool, &order_outcome.order)
            .await
            .context("failed to persist paper accounting artifacts")?;
        if let Some(account) = paper_account {
            insert_pipeline_event(
                state,
                correlation_id,
                "paper.fill.created",
                json!({
                    "account_id": account.id,
                    "order_id": order_outcome.order.order_id,
                    "symbol": order_outcome.order.symbol,
                }),
            )
            .await?;
            insert_pipeline_event(
                state,
                correlation_id,
                "paper.position.opened",
                json!({
                    "account_id": account.id,
                    "order_id": order_outcome.order.order_id,
                    "symbol": order_outcome.order.symbol,
                }),
            )
            .await?;
            insert_pipeline_event(
                state,
                correlation_id,
                "paper.equity.updated",
                json!({
                    "account_id": account.id,
                    "equity": account.current_equity,
                    "realized_pnl": account.realized_pnl,
                    "unrealized_pnl": account.unrealized_pnl,
                }),
            )
            .await?;
        }
        insert_pipeline_event(
            state,
            correlation_id,
            "paper_pipeline.paper_order_created",
            json!({
                "strategy_id": result.strategy_id,
                "symbol": result.symbol,
                "signal_id": result.signal_id,
                "risk_decision_id": result.risk_decision_id,
                "paper_order_id": result.paper_order_id,
                "execution_state": result.execution_state,
            }),
        )
        .await?;
        return Ok(result);
    }
    telemetry().inc_strategy_evaluation(
        request.strategy_id.as_str(),
        request.symbol.as_str(),
        "no_signal",
    );
    telemetry().inc_paper_pipeline_run(
        request.strategy_id.as_str(),
        request.symbol.as_str(),
        "no_signal",
    );

    update_strategy_state(
        &state.db_pool,
        strategy_id,
        evaluation.evaluated_at,
        evaluation.reason,
        None,
        None,
    )
    .await
    .context("failed to update strategy state after no-signal evaluation")?;

    let result = terminal_result(
        PipelineDecision::NoSignal,
        &request,
        correlation_id,
        vec![signal_reason_to_pipeline_reason(evaluation.reason)],
        StrategyRiskExecutionTrace {
            strategy_evaluation: PipelineStepStatus::Completed,
            signal: PipelineStepStatus::Skipped,
            risk_evaluation: PipelineStepStatus::Skipped,
            paper_order: PipelineStepStatus::Skipped,
            order_intent_source: None,
        },
    );
    insert_pipeline_event(
        state,
        correlation_id,
        "paper_pipeline.no_signal",
        json!({
            "strategy_id": result.strategy_id,
            "symbol": result.symbol,
            "reasons": result.reasons,
        }),
    )
    .await?;
    Ok(result)
}

fn terminal_result(
    decision: PipelineDecision,
    request: &PaperTradingPipelineRequest,
    correlation_id: Uuid,
    reasons: Vec<PipelineRejectionReason>,
    trace: StrategyRiskExecutionTrace,
) -> PaperTradingPipelineResult {
    PaperTradingPipelineResult {
        pipeline_decision: decision,
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        signal_generated: false,
        signal_reused: false,
        signal_id: None,
        risk_decision_id: None,
        paper_order_id: None,
        execution_state: None,
        reasons: reasons
            .into_iter()
            .map(|reason| reason.as_str().to_string())
            .collect(),
        correlation_id,
        trace,
    }
}

async fn feed_stop_reason(
    state: &AppState,
    symbol: &aegis_core::Symbol,
) -> Result<Option<PipelineRejectionReason>> {
    let feed = list_market_feed_statuses(&state.db_pool)
        .await
        .context("failed to query market feed status")?
        .into_iter()
        .find(|feed| {
            feed.exchange == state.market_config.exchange.as_str() && feed.symbol == symbol.as_str()
        });

    let Some(feed) = feed else {
        return Ok(Some(PipelineRejectionReason::MarketFeedUnavailable));
    };

    if feed.freshness_status != DataFreshnessStatus::Fresh {
        return Ok(Some(PipelineRejectionReason::DataStale));
    }

    if feed.status != "connected" {
        return Ok(Some(PipelineRejectionReason::MarketFeedDegraded));
    }

    Ok(None)
}

fn signal_reason_to_pipeline_reason(reason: SignalReason) -> PipelineRejectionReason {
    match reason {
        SignalReason::ConditionsNotMet => PipelineRejectionReason::ConditionsNotMet,
        SignalReason::InsufficientHistory => PipelineRejectionReason::InsufficientHistory,
        SignalReason::StrategyDisabled => PipelineRejectionReason::StrategyDisabled,
        _ => PipelineRejectionReason::UnsupportedState,
    }
}

fn primary_risk_reason_label(reasons: &[aegis_core::RiskRejectionReason]) -> &'static str {
    reasons
        .first()
        .map(|reason| crate::reason_code(*reason))
        .unwrap_or("none")
}

fn parse_signal_side(value: &str) -> Result<Side> {
    let side: SignalSide = value.parse().context("invalid persisted signal side")?;
    Ok(side.into())
}

fn build_order_intent(
    correlation_id: Uuid,
    risk_decision_id: Uuid,
    idempotency_key: &str,
    symbol: &aegis_core::Symbol,
    side: Side,
    approved_notional: Decimal,
    latest_candle: &Candle,
) -> Result<aegis_core::OrderIntent> {
    if latest_candle.close <= Decimal::ZERO {
        return Err(anyhow!("latest candle close must be greater than zero"));
    }

    Ok(aegis_core::OrderIntent {
        order_id: Uuid::new_v4(),
        correlation_id,
        risk_decision_id,
        idempotency_key: idempotency_key.to_string(),
        symbol: symbol.clone(),
        side,
        quantity: approved_notional / latest_candle.close,
        limit_price: Some(latest_candle.close),
        created_at: Utc::now(),
        expires_at: None,
    })
}

fn build_idempotency_key(
    strategy_id: &str,
    signal_id: Uuid,
    risk_decision_id: Uuid,
    symbol: &str,
    side: Side,
    source_candle_open_time_ms: i64,
) -> String {
    let side = match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    };
    format!(
        "{strategy_id}:{signal_id}:{risk_decision_id}:{symbol}:{side}:{source_candle_open_time_ms}"
    )
}

async fn insert_pipeline_event(
    state: &AppState,
    correlation_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<()> {
    insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            event_type,
            correlation_id,
            state.config.app_name.clone(),
            payload,
        ),
    )
    .await
    .map(|_| ())
    .context("failed to persist system event")
}

#[cfg(test)]
mod tests {
    use super::{
        build_idempotency_key, build_order_intent, signal_reason_to_pipeline_reason,
        terminal_result,
    };
    use aegis_core::{
        Candle, CandleInterval, MarketDataSource, PipelineDecision, PipelineRejectionReason,
        PipelineStepStatus, Side, StrategyRiskExecutionTrace,
    };
    use chrono::Utc;
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_candle() -> Candle {
        let now = Utc::now();
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: aegis_core::Symbol::new("BTCUSDT").expect("valid symbol"),
            interval: CandleInterval::OneMinute,
            open_time: now,
            close_time: now,
            open: Decimal::new(100_000, 0),
            high: Decimal::new(100_000, 0),
            low: Decimal::new(100_000, 0),
            close: Decimal::new(100_000, 0),
            volume: Decimal::ONE,
            quote_volume: Some(Decimal::new(100_000, 0)),
            trade_count: 1,
            is_closed: true,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn no_signal_result_skips_risk_and_order() {
        let request = aegis_core::PaperTradingPipelineRequest {
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            correlation_id: None,
        };
        let result = terminal_result(
            PipelineDecision::NoSignal,
            &request,
            Uuid::new_v4(),
            vec![PipelineRejectionReason::ConditionsNotMet],
            StrategyRiskExecutionTrace {
                strategy_evaluation: PipelineStepStatus::Completed,
                signal: PipelineStepStatus::Skipped,
                risk_evaluation: PipelineStepStatus::Skipped,
                paper_order: PipelineStepStatus::Skipped,
                order_intent_source: None,
            },
        );

        assert_eq!(result.pipeline_decision, PipelineDecision::NoSignal);
        assert_eq!(result.trace.risk_evaluation, PipelineStepStatus::Skipped);
        assert_eq!(result.trace.paper_order, PipelineStepStatus::Skipped);
    }

    #[test]
    fn signal_too_old_maps_to_machine_readable_reason() {
        let reason = crate::reason_code(aegis_core::RiskRejectionReason::SignalTooOld);
        assert_eq!(reason, "signal_too_old");
    }

    #[test]
    fn disabled_strategy_maps_to_pipeline_reason() {
        assert_eq!(
            signal_reason_to_pipeline_reason(aegis_core::SignalReason::StrategyDisabled),
            PipelineRejectionReason::StrategyDisabled
        );
    }

    #[test]
    fn approved_risk_builds_paper_order_intent() {
        let candle = sample_candle();
        let intent = build_order_intent(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "pipeline-order-1",
            &candle.symbol,
            Side::Buy,
            Decimal::new(200_000, 0),
            &candle,
        )
        .expect("intent should build");

        assert_eq!(intent.limit_price, Some(Decimal::new(100_000, 0)));
        assert_eq!(intent.quantity, Decimal::new(2, 0));
    }

    #[test]
    fn idempotency_key_is_deterministic() {
        let signal_id = Uuid::nil();
        let risk_decision_id = Uuid::from_u128(1);
        let first = build_idempotency_key(
            "momentum_v1",
            signal_id,
            risk_decision_id,
            "BTCUSDT",
            Side::Buy,
            1_717_171_717,
        );
        let second = build_idempotency_key(
            "momentum_v1",
            signal_id,
            risk_decision_id,
            "BTCUSDT",
            Side::Buy,
            1_717_171_717,
        );

        assert_eq!(first, second);
    }
}

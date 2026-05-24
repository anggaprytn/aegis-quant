use aegis_core::{
    CoreError, EventEnvelope, ExchangeEnvironment, ExchangeError, ExchangeName, ExchangeOrderState,
    ExchangeOrderStatus, ExchangeReconciliationAction, ExchangeReconciliationMismatch,
    ExchangeReconciliationMismatchKind, ExchangeReconciliationRequest,
    ExchangeReconciliationResult, ExchangeReconciliationRun, ExchangeReconciliationStatus,
    ExchangeReconciliationSummary,
};
use anyhow::Context;
use chrono::Utc;
use db::{
    complete_exchange_reconciliation_run, fail_exchange_reconciliation_run, insert_audit_log,
    insert_exchange_reconciliation_mismatch, insert_exchange_reconciliation_run,
    insert_system_event, list_exchange_testnet_orders_for_reconciliation,
    update_exchange_testnet_order_status, ExchangeReconciliationMismatchRecord,
    ExchangeReconciliationRunRecord, ExchangeTestnetOrderRecord, PgPool, StateActor,
};
use exchange::ExchangeAdapter;
use serde_json::{json, Value};
use telemetry::telemetry;
use uuid::Uuid;

#[derive(Debug)]
pub enum ReconcileTestnetOrdersError {
    Validation(CoreError),
    Failed {
        run_id: Uuid,
        correlation_id: Uuid,
        reason: String,
    },
    Unexpected(anyhow::Error),
}

impl From<anyhow::Error> for ReconcileTestnetOrdersError {
    fn from(value: anyhow::Error) -> Self {
        Self::Unexpected(value)
    }
}

impl From<CoreError> for ReconcileTestnetOrdersError {
    fn from(value: CoreError) -> Self {
        Self::Unexpected(anyhow::Error::new(value))
    }
}

#[derive(Debug, Clone)]
pub struct ReconciliationRunDetails {
    pub run: ExchangeReconciliationRun,
}

pub async fn reconcile_testnet_orders<A: ExchangeAdapter>(
    pool: &PgPool,
    adapter: &A,
    app_name: &str,
    actor: &StateActor,
    request: &ExchangeReconciliationRequest,
) -> Result<ReconciliationRunDetails, ReconcileTestnetOrdersError> {
    request
        .validate()
        .map_err(ReconcileTestnetOrdersError::Validation)?;
    if request.exchange != ExchangeName::Binance {
        return Err(ReconcileTestnetOrdersError::Validation(
            CoreError::UnsupportedExchangeName(request.exchange.as_str().to_string()),
        ));
    }
    if request.environment != ExchangeEnvironment::Testnet {
        return Err(ReconcileTestnetOrdersError::Validation(
            CoreError::LiveExchangeEnvironmentRejected,
        ));
    }

    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
    let started_at = Utc::now();
    let run_record = insert_exchange_reconciliation_run(
        pool,
        &ExchangeReconciliationRunRecord {
            id: Uuid::new_v4(),
            exchange: request.exchange.as_str().to_string(),
            environment: request.environment.as_str().to_string(),
            status: ExchangeReconciliationStatus::Running.as_str().to_string(),
            checked_orders: 0,
            matched_orders: 0,
            mismatched_orders: 0,
            unknown_orders: 0,
            failed_reason: None,
            correlation_id,
            started_at,
            completed_at: None,
        },
    )
    .await
    .context("insert reconciliation run")?;

    let _ = insert_audit_log(
        pool,
        correlation_id,
        actor,
        "exchange.testnet.reconciliation.requested",
        &run_record.id.to_string(),
        &json!({
            "exchange": request.exchange.as_str(),
            "environment": request.environment.as_str(),
            "limit": request.limit,
            "status_filter": &request.status_filter,
        }),
    )
    .await;
    let _ = insert_system_event(
        pool,
        &EventEnvelope::new(
            "exchange.reconciliation.started",
            correlation_id,
            app_name,
            json!({
                "run_id": run_record.id,
                "exchange": request.exchange.as_str(),
                "environment": request.environment.as_str(),
                "limit": request.limit,
                "status_filter": &request.status_filter,
            }),
        ),
    )
    .await;

    let orders = list_exchange_testnet_orders_for_reconciliation(
        pool,
        request.limit,
        &request.status_filter,
    )
    .await
    .context("list testnet orders for reconciliation")?;
    let mut summary = ExchangeReconciliationSummary {
        checked_orders: 0,
        matched_orders: 0,
        mismatched_orders: 0,
        unknown_orders: 0,
    };
    let mut mismatches = Vec::new();

    for order in orders {
        match adapter.get_order_status(&order.client_order_id).await {
            Ok(exchange_status) => {
                summary.checked_orders += 1;
                telemetry()
                    .inc_exchange_reconciliation_checked_orders(request.environment.as_str(), 1);
                let evaluation = evaluate_order_reconciliation(&order, &exchange_status);

                let persisted_status = evaluation
                    .canonical_local_status
                    .unwrap_or(order.status.as_str());
                update_exchange_testnet_order_status(
                    pool,
                    &order.client_order_id,
                    exchange_status.exchange_order_id.as_deref(),
                    persisted_status,
                    &exchange_status.raw_payload,
                )
                .await
                .with_context(|| {
                    format!(
                        "update local exchange testnet order status for {}",
                        order.client_order_id
                    )
                })?;

                if let Some((kind, action, payload)) = evaluation.mismatch {
                    summary.mismatched_orders += 1;
                    if kind == ExchangeReconciliationMismatchKind::ExchangeOrderMissing
                        || kind == ExchangeReconciliationMismatchKind::UnknownExchangeState
                    {
                        summary.unknown_orders += 1;
                    }

                    let mismatch = insert_exchange_reconciliation_mismatch(
                        pool,
                        &ExchangeReconciliationMismatchRecord {
                            id: Uuid::new_v4(),
                            run_id: run_record.id,
                            client_order_id: order.client_order_id.clone(),
                            local_status: Some(order.status.clone()),
                            exchange_status: Some(exchange_status.status.as_str().to_string()),
                            mismatch_kind: kind.as_str().to_string(),
                            action: action.as_str().to_string(),
                            payload,
                            created_at: Utc::now(),
                        },
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "insert exchange reconciliation mismatch for {}",
                            order.client_order_id
                        )
                    })?;

                    telemetry().inc_exchange_reconciliation_mismatch(
                        request.environment.as_str(),
                        kind.as_str(),
                    );
                    let _ = insert_system_event(
                        pool,
                        &EventEnvelope::new(
                            "exchange.reconciliation.mismatch",
                            correlation_id,
                            app_name,
                            json!({
                                "run_id": run_record.id,
                                "client_order_id": order.client_order_id,
                                "mismatch_kind": kind.as_str(),
                                "action": action.as_str(),
                            }),
                        ),
                    )
                    .await;
                    mismatches.push(mismatch_from_record(&mismatch)?);
                } else {
                    summary.matched_orders += 1;
                }
            }
            Err(err) => {
                if is_exchange_order_missing(&err) {
                    summary.checked_orders += 1;
                    telemetry().inc_exchange_reconciliation_checked_orders(
                        request.environment.as_str(),
                        1,
                    );
                    summary.mismatched_orders += 1;
                    summary.unknown_orders += 1;
                    let mismatch = insert_exchange_reconciliation_mismatch(
                        pool,
                        &ExchangeReconciliationMismatchRecord {
                            id: Uuid::new_v4(),
                            run_id: run_record.id,
                            client_order_id: order.client_order_id.clone(),
                            local_status: Some(order.status.clone()),
                            exchange_status: None,
                            mismatch_kind: ExchangeReconciliationMismatchKind::ExchangeOrderMissing
                                .as_str()
                                .to_string(),
                            action: ExchangeReconciliationAction::Alert.as_str().to_string(),
                            payload: json!({
                                "symbol": order.symbol,
                                "reason": err.to_string(),
                            }),
                            created_at: Utc::now(),
                        },
                    )
                    .await
                    .context("insert exchange missing-order mismatch")?;
                    telemetry().inc_exchange_reconciliation_mismatch(
                        request.environment.as_str(),
                        ExchangeReconciliationMismatchKind::ExchangeOrderMissing.as_str(),
                    );
                    let _ = insert_system_event(
                        pool,
                        &EventEnvelope::new(
                            "exchange.reconciliation.mismatch",
                            correlation_id,
                            app_name,
                            json!({
                                "run_id": run_record.id,
                                "client_order_id": order.client_order_id,
                                "mismatch_kind": ExchangeReconciliationMismatchKind::ExchangeOrderMissing.as_str(),
                                "action": ExchangeReconciliationAction::Alert.as_str(),
                            }),
                        ),
                    )
                    .await;
                    mismatches.push(mismatch_from_record(&mismatch)?);
                    continue;
                }

                let reason = format!("failed to fetch testnet order status: {err}");
                let failed_record =
                    fail_exchange_reconciliation_run(pool, run_record.id, &summary, &reason)
                        .await
                        .context("mark reconciliation run failed")?
                        .unwrap_or(ExchangeReconciliationRunRecord {
                            id: run_record.id,
                            exchange: run_record.exchange.clone(),
                            environment: run_record.environment.clone(),
                            status: ExchangeReconciliationStatus::Failed.as_str().to_string(),
                            checked_orders: summary.checked_orders,
                            matched_orders: summary.matched_orders,
                            mismatched_orders: summary.mismatched_orders,
                            unknown_orders: summary.unknown_orders,
                            failed_reason: Some(reason.clone()),
                            correlation_id,
                            started_at: run_record.started_at,
                            completed_at: Some(Utc::now()),
                        });
                telemetry().inc_exchange_reconciliation_run(
                    request.environment.as_str(),
                    ExchangeReconciliationStatus::Failed.as_str(),
                );
                let _ = insert_system_event(
                    pool,
                    &EventEnvelope::new(
                        "exchange.reconciliation.failed",
                        correlation_id,
                        app_name,
                        json!({
                            "run_id": run_record.id,
                            "reason": reason,
                            "checked_orders": summary.checked_orders,
                            "matched_orders": summary.matched_orders,
                            "mismatched_orders": summary.mismatched_orders,
                            "unknown_orders": summary.unknown_orders,
                        }),
                    ),
                )
                .await;

                return Err(ReconcileTestnetOrdersError::Failed {
                    run_id: failed_record.id,
                    correlation_id,
                    reason,
                });
            }
        }
    }

    let completed_record = complete_exchange_reconciliation_run(pool, run_record.id, &summary)
        .await
        .context("complete reconciliation run")?
        .unwrap_or(ExchangeReconciliationRunRecord {
            id: run_record.id,
            exchange: run_record.exchange.clone(),
            environment: run_record.environment.clone(),
            status: ExchangeReconciliationStatus::Completed.as_str().to_string(),
            checked_orders: summary.checked_orders,
            matched_orders: summary.matched_orders,
            mismatched_orders: summary.mismatched_orders,
            unknown_orders: summary.unknown_orders,
            failed_reason: None,
            correlation_id,
            started_at: run_record.started_at,
            completed_at: Some(Utc::now()),
        });
    telemetry().inc_exchange_reconciliation_run(
        request.environment.as_str(),
        ExchangeReconciliationStatus::Completed.as_str(),
    );
    let _ = insert_system_event(
        pool,
        &EventEnvelope::new(
            "exchange.reconciliation.completed",
            correlation_id,
            app_name,
            json!({
                "run_id": run_record.id,
                "checked_orders": summary.checked_orders,
                "matched_orders": summary.matched_orders,
                "mismatched_orders": summary.mismatched_orders,
                "unknown_orders": summary.unknown_orders,
            }),
        ),
    )
    .await;

    Ok(ReconciliationRunDetails {
        run: run_from_record(&completed_record)?,
    })
}

pub fn local_testnet_status_from_exchange_state(state: ExchangeOrderState) -> Option<&'static str> {
    match state {
        ExchangeOrderState::New => Some("NEW"),
        ExchangeOrderState::PartiallyFilled => Some("PARTIALLY_FILLED"),
        ExchangeOrderState::Filled => Some("FILLED"),
        ExchangeOrderState::Canceled => Some("CANCELLED"),
        ExchangeOrderState::Rejected => Some("REJECTED"),
        ExchangeOrderState::Expired => Some("EXPIRED"),
        ExchangeOrderState::PendingCancel => None,
    }
}

pub fn run_result_from_run(run: &ExchangeReconciliationRun) -> ExchangeReconciliationResult {
    ExchangeReconciliationResult {
        run_id: run.id,
        status: run.status,
        checked_orders: run.checked_orders,
        matched_orders: run.matched_orders,
        mismatched_orders: run.mismatched_orders,
        unknown_orders: run.unknown_orders,
        correlation_id: run.correlation_id,
    }
}

pub fn run_from_record(
    record: &ExchangeReconciliationRunRecord,
) -> Result<ExchangeReconciliationRun, CoreError> {
    Ok(ExchangeReconciliationRun {
        id: record.id,
        exchange: record.exchange.parse()?,
        environment: record.environment.parse()?,
        status: record.status.parse()?,
        checked_orders: record.checked_orders,
        matched_orders: record.matched_orders,
        mismatched_orders: record.mismatched_orders,
        unknown_orders: record.unknown_orders,
        failed_reason: record.failed_reason.clone(),
        started_at: record.started_at,
        completed_at: record.completed_at,
        correlation_id: record.correlation_id,
    })
}

pub fn mismatch_from_record(
    record: &ExchangeReconciliationMismatchRecord,
) -> Result<ExchangeReconciliationMismatch, CoreError> {
    Ok(ExchangeReconciliationMismatch {
        id: record.id,
        run_id: record.run_id,
        client_order_id: record.client_order_id.clone(),
        local_status: record.local_status.clone(),
        exchange_status: record.exchange_status.clone(),
        mismatch_kind: record.mismatch_kind.parse()?,
        action: record.action.parse()?,
        payload: record.payload.clone(),
        created_at: record.created_at,
    })
}

#[derive(Debug)]
struct EvaluatedOrderReconciliation {
    canonical_local_status: Option<&'static str>,
    mismatch: Option<(
        ExchangeReconciliationMismatchKind,
        ExchangeReconciliationAction,
        Value,
    )>,
}

fn evaluate_order_reconciliation(
    local_order: &ExchangeTestnetOrderRecord,
    exchange_status: &ExchangeOrderStatus,
) -> EvaluatedOrderReconciliation {
    let local_normalized = normalize_local_status(&local_order.status);
    let exchange_local_status = local_testnet_status_from_exchange_state(exchange_status.status);

    if exchange_local_status.is_none() {
        return EvaluatedOrderReconciliation {
            canonical_local_status: None,
            mismatch: Some((
                ExchangeReconciliationMismatchKind::UnknownExchangeState,
                ExchangeReconciliationAction::Alert,
                mismatch_payload(
                    local_order,
                    exchange_status,
                    "exchange status could not be mapped safely",
                ),
            )),
        };
    }

    let exchange_local_status = exchange_local_status.expect("checked some");
    let exchange_normalized = normalize_local_status(exchange_local_status);

    if local_order.status == "SUBMIT_REQUESTED" && local_order.ack_payload.is_some() {
        return EvaluatedOrderReconciliation {
            canonical_local_status: Some(exchange_local_status),
            mismatch: Some((
                ExchangeReconciliationMismatchKind::AckWithoutStatus,
                ExchangeReconciliationAction::UpdateLocalStatus,
                mismatch_payload(
                    local_order,
                    exchange_status,
                    "ack payload exists but local status was not refreshed",
                ),
            )),
        };
    }

    if matches!(local_normalized, Some(NormalizedLocalStatus::Cancelled))
        && !matches!(exchange_normalized, Some(NormalizedLocalStatus::Cancelled))
    {
        return EvaluatedOrderReconciliation {
            canonical_local_status: Some(exchange_local_status),
            mismatch: Some((
                ExchangeReconciliationMismatchKind::CancelNotConfirmed,
                ExchangeReconciliationAction::Alert,
                mismatch_payload(
                    local_order,
                    exchange_status,
                    "local cancel was not confirmed by exchange",
                ),
            )),
        };
    }

    if local_normalized == exchange_normalized {
        return EvaluatedOrderReconciliation {
            canonical_local_status: Some(exchange_local_status),
            mismatch: None,
        };
    }

    EvaluatedOrderReconciliation {
        canonical_local_status: Some(exchange_local_status),
        mismatch: Some((
            ExchangeReconciliationMismatchKind::StatusMismatch,
            ExchangeReconciliationAction::UpdateLocalStatus,
            mismatch_payload(
                local_order,
                exchange_status,
                "local and exchange statuses differ",
            ),
        )),
    }
}

fn mismatch_payload(
    local_order: &ExchangeTestnetOrderRecord,
    exchange_status: &ExchangeOrderStatus,
    reason: &str,
) -> Value {
    json!({
        "reason": reason,
        "symbol": local_order.symbol,
        "exchange_order_id": exchange_status.exchange_order_id,
        "local_status": local_order.status,
        "exchange_status": exchange_status.status.as_str(),
        "ack_payload_present": local_order.ack_payload.is_some(),
        "latest_status_payload_present": local_order.latest_status_payload.is_some(),
        "exchange_payload": exchange_status.raw_payload,
    })
}

fn is_exchange_order_missing(err: &ExchangeError) -> bool {
    match err {
        ExchangeError::Api(message) => {
            let message = message.to_ascii_lowercase();
            message.contains("order does not exist") || message.contains("-2013")
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NormalizedLocalStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
}

fn normalize_local_status(value: &str) -> Option<NormalizedLocalStatus> {
    match value.trim().to_ascii_uppercase().as_str() {
        "ACKED" | "NEW" => Some(NormalizedLocalStatus::New),
        "PARTIALLY_FILLED" => Some(NormalizedLocalStatus::PartiallyFilled),
        "FILLED" => Some(NormalizedLocalStatus::Filled),
        "CANCELED" | "CANCELLED" => Some(NormalizedLocalStatus::Cancelled),
        "REJECTED" => Some(NormalizedLocalStatus::Rejected),
        "EXPIRED" => Some(NormalizedLocalStatus::Expired),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate_order_reconciliation, is_exchange_order_missing,
        local_testnet_status_from_exchange_state, ReconcileTestnetOrdersError,
    };
    use aegis_core::{
        ExchangeEnvironment, ExchangeError, ExchangeName, ExchangeOrderSide, ExchangeOrderState,
        ExchangeOrderStatus, ExchangeOrderType, ExchangeReconciliationRequest,
    };
    use chrono::Utc;
    use db::ExchangeTestnetOrderRecord;
    use rust_decimal::Decimal;
    use serde_json::json;

    fn sample_local_order(status: &str) -> ExchangeTestnetOrderRecord {
        ExchangeTestnetOrderRecord {
            id: uuid::Uuid::new_v4(),
            exchange: "binance".to_string(),
            environment: "testnet".to_string(),
            client_order_id: "client-1".to_string(),
            exchange_order_id: Some("123".to_string()),
            symbol: "BTCUSDT".to_string(),
            side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            time_in_force: Some("GTC".to_string()),
            requested_qty: Some(Decimal::ONE),
            requested_notional: None,
            limit_price: Some(Decimal::new(100_000, 0)),
            status: status.to_string(),
            ack_payload: Some(json!({"status":"NEW"})),
            latest_status_payload: None,
            risk_decision_id: None,
            created_by: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn sample_exchange_status(status: ExchangeOrderState) -> ExchangeOrderStatus {
        ExchangeOrderStatus {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "BTCUSDT".to_string(),
            client_order_id: "client-1".to_string(),
            exchange_order_id: Some("123".to_string()),
            status,
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Limit,
            time_in_force: None,
            original_qty: Some(Decimal::ONE),
            executed_qty: Decimal::ZERO,
            cumulative_quote_qty: Decimal::ZERO,
            limit_price: Some(Decimal::new(100_000, 0)),
            updated_at: Utc::now(),
            raw_payload: json!({"status": status.as_str()}),
        }
    }

    #[test]
    fn binance_status_mapping_is_safe_and_explicit() {
        assert_eq!(
            local_testnet_status_from_exchange_state(ExchangeOrderState::New),
            Some("NEW")
        );
        assert_eq!(
            local_testnet_status_from_exchange_state(ExchangeOrderState::PartiallyFilled),
            Some("PARTIALLY_FILLED")
        );
        assert_eq!(
            local_testnet_status_from_exchange_state(ExchangeOrderState::Filled),
            Some("FILLED")
        );
        assert_eq!(
            local_testnet_status_from_exchange_state(ExchangeOrderState::Canceled),
            Some("CANCELLED")
        );
        assert_eq!(
            local_testnet_status_from_exchange_state(ExchangeOrderState::Rejected),
            Some("REJECTED")
        );
        assert_eq!(
            local_testnet_status_from_exchange_state(ExchangeOrderState::Expired),
            Some("EXPIRED")
        );
        assert_eq!(
            local_testnet_status_from_exchange_state(ExchangeOrderState::PendingCancel),
            None
        );
    }

    #[test]
    fn matched_statuses_are_not_flagged() {
        let result = evaluate_order_reconciliation(
            &sample_local_order("ACKED"),
            &sample_exchange_status(ExchangeOrderState::New),
        );
        assert!(result.mismatch.is_none());
    }

    #[test]
    fn status_mismatch_is_detected() {
        let result = evaluate_order_reconciliation(
            &sample_local_order("NEW"),
            &sample_exchange_status(ExchangeOrderState::Filled),
        );
        let (kind, _, _) = result.mismatch.expect("mismatch");
        assert_eq!(kind.as_str(), "STATUS_MISMATCH");
        assert_eq!(result.canonical_local_status, Some("FILLED"));
    }

    #[test]
    fn unknown_exchange_state_is_detected() {
        let result = evaluate_order_reconciliation(
            &sample_local_order("NEW"),
            &sample_exchange_status(ExchangeOrderState::PendingCancel),
        );
        let (kind, _, _) = result.mismatch.expect("mismatch");
        assert_eq!(kind.as_str(), "UNKNOWN_EXCHANGE_STATE");
    }

    #[test]
    fn exchange_missing_order_detection_matches_binance_error() {
        assert!(is_exchange_order_missing(&ExchangeError::Api(
            "{\"code\":-2013,\"msg\":\"Order does not exist.\"}".to_string()
        )));
    }

    #[test]
    fn live_environment_is_rejected() {
        let err = ExchangeReconciliationRequest {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Live,
            limit: 10,
            status_filter: vec!["NEW".to_string()],
            correlation_id: None,
        }
        .validate()
        .expect_err("live should be rejected");
        assert!(matches!(
            err,
            aegis_core::CoreError::LiveExchangeEnvironmentRejected
        ));
    }

    #[test]
    fn reconciliation_request_limit_is_validated() {
        let err = ExchangeReconciliationRequest {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            limit: 0,
            status_filter: vec!["NEW".to_string()],
            correlation_id: None,
        }
        .validate()
        .expect_err("invalid limit");
        assert!(matches!(
            err,
            aegis_core::CoreError::InvalidExchangeReconciliationLimit(0)
        ));
    }

    #[test]
    fn failed_error_carries_run_context() {
        let err = ReconcileTestnetOrdersError::Failed {
            run_id: uuid::Uuid::nil(),
            correlation_id: uuid::Uuid::nil(),
            reason: "boom".to_string(),
        };
        match err {
            ReconcileTestnetOrdersError::Failed { reason, .. } => assert_eq!(reason, "boom"),
            _ => panic!("unexpected variant"),
        }
    }
}

use anyhow::Result;
use chrono::Utc;
use uuid::Uuid;

use aegis_core::{
    EventEnvelope, ExecutionReadinessRequest, ExecutionReadinessTarget,
    StrategyCandidateObservationDecision, StrategyCandidateObservationFinding,
    StrategyCandidateObservationRequest, StrategyCandidateObservationResult,
    StrategyCandidateObservationStatus, StrategyCandidateObservationSummary,
    StrategyResearchCandidateStatus, TestnetShadowDecision,
};
use db::{
    get_strategy_research_candidate, insert_strategy_candidate_observation, insert_system_event,
    list_testnet_shadow_runs_in_window, strategy_candidate_observation_result_from_record,
    strategy_research_candidate_from_record,
};
use telemetry::telemetry;

use crate::{readiness::compute_execution_readiness, AppState};

pub async fn evaluate_candidate_observation(
    state: &AppState,
    request: &StrategyCandidateObservationRequest,
    created_by: Option<Uuid>,
) -> Result<StrategyCandidateObservationResult> {
    let evaluated_at = Utc::now();
    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
    let candidate_record = get_strategy_research_candidate(&state.db_pool, request.candidate_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("research candidate was not found"))?;
    let candidate = strategy_research_candidate_from_record(&candidate_record)?;
    let requirements = request.to_requirement(
        candidate.strategy_id.clone(),
        candidate.symbol.clone(),
        candidate.timeframe.clone(),
    );

    let observation = if candidate.status != StrategyResearchCandidateStatus::PromotedToShadowConfig
        || candidate.promoted_at.is_none()
    {
        let window_start = evaluated_at;
        let summary = StrategyCandidateObservationSummary {
            candidate_id: candidate.id,
            window_start,
            window_end: evaluated_at,
            shadow_runs: 0,
            would_submit_count: 0,
            no_signal_count: 0,
            risk_rejected_count: 0,
            skipped_count: 0,
            risk_rejection_rate: rust_decimal::Decimal::ZERO,
            no_signal_rate: rust_decimal::Decimal::ZERO,
            latest_readiness_status: None,
            latest_readiness_score: None,
            decision: StrategyCandidateObservationDecision::InsufficientData,
            findings: vec![StrategyCandidateObservationFinding {
                code: "candidate_not_promoted_to_shadow".to_string(),
                message: "Candidate has not been promoted to shadow config.".to_string(),
                blocking: true,
            }],
            created_at: evaluated_at,
        };
        StrategyCandidateObservationResult {
            observation_id: Uuid::new_v4(),
            candidate_id: candidate.id,
            strategy_id: candidate.strategy_id.clone(),
            symbol: candidate.symbol.clone(),
            timeframe: candidate.timeframe.clone(),
            status: StrategyCandidateObservationStatus::InsufficientData,
            requirements,
            summary,
            decision: StrategyCandidateObservationDecision::InsufficientData,
            started_at: window_start,
            evaluated_at,
            created_by,
            correlation_id: Some(correlation_id),
        }
    } else {
        let window_start = request.start_time.unwrap_or(candidate.promoted_at.unwrap());
        let shadow_runs = list_testnet_shadow_runs_in_window(
            &state.db_pool,
            &candidate.strategy_id,
            &candidate.symbol,
            &candidate.timeframe,
            window_start,
            evaluated_at,
        )
        .await?;
        let would_submit_count = shadow_runs
            .iter()
            .filter(|run| run.decision == TestnetShadowDecision::WouldSubmit.as_str())
            .count() as i64;
        let no_signal_count = shadow_runs
            .iter()
            .filter(|run| run.decision == TestnetShadowDecision::NoSignal.as_str())
            .count() as i64;
        let risk_rejected_count = shadow_runs
            .iter()
            .filter(|run| run.decision == TestnetShadowDecision::RiskRejected.as_str())
            .count() as i64;
        let skipped_count = shadow_runs
            .iter()
            .filter(|run| run.decision.starts_with("SKIPPED_"))
            .count() as i64;

        let readiness = compute_execution_readiness(
            state,
            &ExecutionReadinessRequest {
                target: ExecutionReadinessTarget::TestnetShadow,
                symbol: Some(candidate.symbol.clone()),
                strategy_id: Some(candidate.strategy_id.clone()),
                timeframe: Some(candidate.timeframe.clone()),
                promotion_id: None,
                risk_decision_id: None,
                start_time: Some(window_start),
                end_time: Some(evaluated_at),
                persist: false,
                correlation_id: Some(correlation_id),
            },
            None,
        )
        .await?;
        let summary = aegis_core::evaluate_strategy_candidate_observation(
            &requirements,
            window_start,
            evaluated_at,
            shadow_runs.len() as i64,
            would_submit_count,
            no_signal_count,
            risk_rejected_count,
            skipped_count,
            Some(readiness.status),
            Some(readiness.score),
            evaluated_at,
        );
        StrategyCandidateObservationResult {
            observation_id: Uuid::new_v4(),
            candidate_id: candidate.id,
            strategy_id: candidate.strategy_id.clone(),
            symbol: candidate.symbol.clone(),
            timeframe: candidate.timeframe.clone(),
            status: match summary.decision {
                StrategyCandidateObservationDecision::Pass => {
                    StrategyCandidateObservationStatus::ReadyForReview
                }
                StrategyCandidateObservationDecision::Fail => {
                    StrategyCandidateObservationStatus::Failed
                }
                StrategyCandidateObservationDecision::ContinueObserving => {
                    StrategyCandidateObservationStatus::Observing
                }
                StrategyCandidateObservationDecision::InsufficientData => {
                    StrategyCandidateObservationStatus::InsufficientData
                }
            },
            requirements,
            decision: summary.decision,
            summary,
            started_at: window_start,
            evaluated_at,
            created_by,
            correlation_id: Some(correlation_id),
        }
    };

    let record = insert_strategy_candidate_observation(&state.db_pool, &observation).await?;
    let persisted = strategy_candidate_observation_result_from_record(&record)?;
    emit_observation_events(state, &persisted, correlation_id).await;
    telemetry()
        .inc_research_candidate_observation(persisted.decision.as_str(), persisted.status.as_str());
    Ok(persisted)
}

async fn emit_observation_events(
    state: &AppState,
    observation: &StrategyCandidateObservationResult,
    correlation_id: Uuid,
) {
    let payload = serde_json::json!({
        "observation_id": observation.observation_id,
        "candidate_id": observation.candidate_id,
        "strategy_id": observation.strategy_id,
        "symbol": observation.symbol,
        "timeframe": observation.timeframe,
        "status": observation.status.as_str(),
        "decision": observation.decision.as_str(),
        "shadow_runs": observation.summary.shadow_runs,
        "would_submit_count": observation.summary.would_submit_count,
        "risk_rejection_rate": observation.summary.risk_rejection_rate,
        "no_signal_rate": observation.summary.no_signal_rate,
    });
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "research.candidate.observation.evaluated",
            correlation_id,
            state.config.app_name.clone(),
            payload.clone(),
        ),
    )
    .await;

    let event_type = match observation.decision {
        StrategyCandidateObservationDecision::Pass => Some("research.candidate.observation.passed"),
        StrategyCandidateObservationDecision::Fail => Some("research.candidate.observation.failed"),
        _ => None,
    };
    if let Some(event_type) = event_type {
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                event_type,
                correlation_id,
                state.config.app_name.clone(),
                payload,
            ),
        )
        .await;
    }
}

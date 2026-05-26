use anyhow::Result;
use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use aegis_core::{
    EventEnvelope, ExecutionReadinessRequest, ExecutionReadinessTarget, ResearchCandidateDecision,
    ResearchCandidateLifecycleEvent, ResearchCandidateStatus, StrategyCandidateObservationDecision,
    StrategyCandidateObservationRequest, StrategyCandidateObservationResult,
    StrategyCandidateObservationStatus, StrategyCandidateRunnerAlignment, TestnetShadowDecision,
    TestnetShadowRunnerConfig, TestnetShadowRunnerState,
};
use db::{
    append_research_candidate_event, ensure_testnet_shadow_runner_config,
    ensure_testnet_shadow_runner_state, get_research_candidate,
    insert_strategy_candidate_observation, insert_system_event, list_testnet_shadow_runs_in_window,
    research_candidate_from_record, strategy_candidate_observation_result_from_record,
    testnet_shadow_runner_config_from_record, testnet_shadow_runner_state_from_record,
    update_research_candidate_status,
};
use telemetry::telemetry;

use crate::{readiness::compute_execution_readiness, AppState};

struct TestnetShadowRunnerSnapshot {
    config: TestnetShadowRunnerConfig,
    state: TestnetShadowRunnerState,
}

async fn load_testnet_shadow_runner_snapshot(
    state: &AppState,
) -> Result<TestnetShadowRunnerSnapshot> {
    Ok(TestnetShadowRunnerSnapshot {
        config: testnet_shadow_runner_config_from_record(
            &ensure_testnet_shadow_runner_config(&state.db_pool).await?,
        )?,
        state: testnet_shadow_runner_state_from_record(
            &ensure_testnet_shadow_runner_state(&state.db_pool).await?,
        )?,
    })
}

fn evaluate_runner_alignment(
    strategy_id: &str,
    symbol: &str,
    timeframe: &str,
    snapshot: &TestnetShadowRunnerSnapshot,
) -> StrategyCandidateRunnerAlignment {
    let normalized_strategy = strategy_id.trim().to_ascii_lowercase();
    let normalized_symbol = symbol.trim().to_ascii_uppercase();
    let normalized_timeframe = timeframe.trim().to_ascii_lowercase();
    let mut mismatch_reasons = Vec::new();

    if snapshot.config.timeframe.trim().to_ascii_lowercase() != normalized_timeframe {
        mismatch_reasons.push(format!(
            "runner timeframe {} does not include candidate timeframe {}",
            snapshot.config.timeframe, timeframe
        ));
    }
    if !snapshot
        .config
        .symbols
        .iter()
        .any(|value| value.trim().eq_ignore_ascii_case(&normalized_symbol))
    {
        mismatch_reasons.push(format!(
            "runner symbols [{}] do not include candidate symbol {}",
            snapshot.config.symbols.join(", "),
            symbol
        ));
    }
    if !snapshot
        .config
        .strategies
        .iter()
        .any(|value| value.trim().eq_ignore_ascii_case(&normalized_strategy))
    {
        mismatch_reasons.push(format!(
            "runner strategies [{}] do not include candidate strategy {}",
            snapshot.config.strategies.join(", "),
            strategy_id
        ));
    }

    StrategyCandidateRunnerAlignment {
        strategy_config_matches_runner: mismatch_reasons.is_empty(),
        runner_enabled: snapshot.config.enabled,
        runner_status: snapshot.state.status.as_str().to_string(),
        runner_timeframe: snapshot.config.timeframe.clone(),
        runner_symbols: snapshot.config.symbols.clone(),
        runner_strategies: snapshot.config.strategies.clone(),
        mismatch_reasons,
    }
}

fn runner_config_snapshot_hash(snapshot: &serde_json::Value) -> Result<String> {
    let bytes = serde_json::to_vec(snapshot)?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub async fn evaluate_candidate_observation(
    state: &AppState,
    request: &StrategyCandidateObservationRequest,
    created_by: Option<Uuid>,
    mark_observing: bool,
) -> Result<StrategyCandidateObservationResult> {
    let evaluated_at = Utc::now();
    let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
    let candidate_record = get_research_candidate(&state.db_pool, request.candidate_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("research candidate was not found"))?;
    let candidate = research_candidate_from_record(&candidate_record)?;
    let requirements = request.to_requirement(
        candidate.strategy_id.clone(),
        candidate.symbol.clone(),
        candidate.timeframe.clone(),
    );
    let runner_snapshot = load_testnet_shadow_runner_snapshot(state).await?;
    let runner_alignment = evaluate_runner_alignment(
        &candidate.strategy_id,
        &candidate.symbol,
        &candidate.timeframe,
        &runner_snapshot,
    );

    let window_start = request.start_time.unwrap_or(candidate.created_at);
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
    let runner_config_snapshot = serde_json::to_value(&runner_snapshot.config)?;
    let observation_snapshot_hash = runner_config_snapshot_hash(&runner_config_snapshot)?;
    let readiness_snapshot = serde_json::to_value(&readiness)?;
    let observation_max_age_seconds =
        Some(state.config.research_candidate_observation_max_age_seconds);
    let observation_expires_at = Some(
        evaluated_at
            + Duration::seconds(state.config.research_candidate_observation_max_age_seconds),
    );
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
        runner_alignment.clone(),
        evaluated_at,
    );
    let observation = StrategyCandidateObservationResult {
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
        runner_alignment,
        decision: summary.decision,
        summary,
        started_at: window_start,
        evaluated_at,
        last_observed_at: evaluated_at,
        observation_expires_at,
        observation_max_age_seconds,
        observation_snapshot_hash: Some(observation_snapshot_hash),
        runner_config_snapshot: Some(runner_config_snapshot),
        readiness_snapshot: Some(readiness_snapshot),
        created_by,
        correlation_id: Some(correlation_id),
    };

    let record = insert_strategy_candidate_observation(&state.db_pool, &observation).await?;
    let persisted = strategy_candidate_observation_result_from_record(&record)?;

    if mark_observing && candidate.status == ResearchCandidateStatus::Discovered {
        let _ = update_research_candidate_status(
            &state.db_pool,
            candidate.id,
            ResearchCandidateStatus::Observing,
            None,
            Some("Observation explicitly requested."),
            evaluated_at,
            Some(correlation_id),
        )
        .await;
        let _ = append_research_candidate_event(
            &state.db_pool,
            &ResearchCandidateLifecycleEvent {
                id: Uuid::new_v4(),
                candidate_id: candidate.id,
                previous_status: Some(ResearchCandidateStatus::Discovered),
                next_status: ResearchCandidateStatus::Observing,
                decision: ResearchCandidateDecision::Reopen,
                reason: Some("observation_requested".to_string()),
                notes: Some(
                    "Candidate moved into OBSERVING after explicit observation request."
                        .to_string(),
                ),
                actor_id: created_by,
                payload: serde_json::json!({
                    "observation_id": persisted.observation_id,
                    "decision": persisted.decision.as_str(),
                }),
                created_at: evaluated_at,
                correlation_id: Some(correlation_id),
            },
        )
        .await;
    }

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
        "runner_alignment": observation.runner_alignment,
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

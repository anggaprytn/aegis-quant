use crate::{testnet_shadow::run_testnet_shadow_once, AppState};
use aegis_core::{
    CandleInterval, EventEnvelope, Symbol, TestnetShadowRunnerConfig,
    TestnetShadowRunnerConfigInput, TestnetShadowRunnerControlAction, TestnetShadowRunnerState,
    TestnetShadowRunnerStatus, TestnetShadowRunnerTickResult, TestnetShadowRunnerTickStatus,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use db::{
    ensure_testnet_shadow_runner_config, ensure_testnet_shadow_runner_state, insert_system_event,
    testnet_shadow_runner_config_from_record, testnet_shadow_runner_state_from_record,
    upsert_testnet_shadow_runner_config, upsert_testnet_shadow_runner_state, StateActor,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use telemetry::telemetry;
use tracing::error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestnetShadowRunnerConfigValidation {
    pub valid: bool,
    pub issues: Vec<String>,
    pub normalized_config: Option<TestnetShadowRunnerConfigInput>,
}

#[derive(Debug, Clone)]
pub struct TestnetShadowRunnerSnapshot {
    pub config: TestnetShadowRunnerConfig,
    pub state: TestnetShadowRunnerState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerTickMode {
    Scheduled,
    ManualRunOnce,
}

impl RunnerTickMode {
    fn scheduled(self) -> bool {
        matches!(self, Self::Scheduled)
    }
}

pub fn validate_testnet_shadow_runner_config(
    input: &TestnetShadowRunnerConfigInput,
) -> TestnetShadowRunnerConfigValidation {
    let mut issues = Vec::new();

    if input.interval_seconds <= 0 {
        issues.push("interval_seconds must be greater than 0".to_string());
    }
    if input.max_runs_per_tick <= 0 {
        issues.push("max_runs_per_tick must be greater than 0".to_string());
    }

    let strategies = input
        .strategies
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if strategies.is_empty() {
        issues.push("at least one strategy is required".to_string());
    }

    let mut symbols = Vec::new();
    for value in &input.symbols {
        match Symbol::new(value.clone()) {
            Ok(symbol) => symbols.push(symbol.to_string()),
            Err(err) => issues.push(format!("invalid symbol {value:?}: {err}")),
        }
    }
    if symbols.is_empty() {
        issues.push("at least one symbol is required".to_string());
    }

    let timeframe = input.timeframe.trim().to_ascii_lowercase();
    if timeframe.parse::<CandleInterval>().is_err() {
        issues.push("timeframe must be a supported candle interval".to_string());
    }

    if issues.is_empty() {
        TestnetShadowRunnerConfigValidation {
            valid: true,
            issues,
            normalized_config: Some(TestnetShadowRunnerConfigInput {
                enabled: input.enabled,
                interval_seconds: input.interval_seconds,
                strategies,
                symbols,
                timeframe,
                max_runs_per_tick: input.max_runs_per_tick,
                stale_feed_policy: input.stale_feed_policy,
                notes: input
                    .notes
                    .as_ref()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
            }),
        }
    } else {
        TestnetShadowRunnerConfigValidation {
            valid: false,
            issues,
            normalized_config: None,
        }
    }
}

pub async fn load_testnet_shadow_runner_snapshot(
    state: &AppState,
) -> Result<TestnetShadowRunnerSnapshot> {
    let config = testnet_shadow_runner_config_from_record(
        &ensure_testnet_shadow_runner_config(&state.db_pool).await?,
    )?;
    let runner_state = testnet_shadow_runner_state_from_record(
        &ensure_testnet_shadow_runner_state(&state.db_pool).await?,
    )?;
    telemetry().set_exchange_testnet_shadow_runner_status(runner_state.status.as_str());
    telemetry().set_exchange_testnet_shadow_runner_last_tick_age_seconds(
        runner_state
            .last_tick_at
            .map(|value| Utc::now().signed_duration_since(value).num_seconds().max(0) as f64)
            .unwrap_or(0.0),
    );
    Ok(TestnetShadowRunnerSnapshot {
        config,
        state: runner_state,
    })
}

pub async fn persist_testnet_shadow_runner_config(
    state: &AppState,
    input: &TestnetShadowRunnerConfigInput,
    updated_by: Option<Uuid>,
) -> Result<TestnetShadowRunnerConfig> {
    let normalized = validate_testnet_shadow_runner_config(input);
    if !normalized.valid {
        return Err(anyhow!(normalized.issues.join("; ")));
    }

    let normalized = normalized
        .normalized_config
        .expect("valid runner config should be normalized");
    let record = upsert_testnet_shadow_runner_config(
        &state.db_pool,
        &TestnetShadowRunnerConfig {
            id: db::TESTNET_SHADOW_RUNNER_CONFIG_ID,
            enabled: normalized.enabled,
            interval_seconds: normalized.interval_seconds,
            strategies: normalized.strategies,
            symbols: normalized.symbols,
            timeframe: normalized.timeframe,
            max_runs_per_tick: normalized.max_runs_per_tick,
            stale_feed_policy: normalized.stale_feed_policy,
            notes: normalized.notes,
            updated_by,
            updated_at: Utc::now(),
        },
    )
    .await?;

    testnet_shadow_runner_config_from_record(&record)
}

pub async fn apply_testnet_shadow_runner_control_action(
    state: &AppState,
    actor: Option<&StateActor>,
    action: TestnetShadowRunnerControlAction,
    correlation_id: Uuid,
) -> Result<(
    TestnetShadowRunnerState,
    Option<TestnetShadowRunnerTickResult>,
)> {
    let snapshot = load_testnet_shadow_runner_snapshot(state).await?;
    match action {
        TestnetShadowRunnerControlAction::RunOnce => {
            let tick = run_shadow_runner_tick(
                state,
                actor,
                Some(correlation_id),
                RunnerTickMode::ManualRunOnce,
            )
            .await?;
            let updated_state = testnet_shadow_runner_state_from_record(
                &ensure_testnet_shadow_runner_state(&state.db_pool).await?,
            )?;
            Ok((updated_state, Some(tick)))
        }
        TestnetShadowRunnerControlAction::Start => {
            let updated = persist_runner_state(
                state,
                TestnetShadowRunnerState {
                    status: TestnetShadowRunnerStatus::Running,
                    last_error: None,
                    updated_at: Utc::now(),
                    ..snapshot.state
                },
            )
            .await?;
            Ok((updated, None))
        }
        TestnetShadowRunnerControlAction::Stop => {
            let updated = persist_runner_state(
                state,
                TestnetShadowRunnerState {
                    status: TestnetShadowRunnerStatus::Stopped,
                    updated_at: Utc::now(),
                    ..snapshot.state
                },
            )
            .await?;
            Ok((updated, None))
        }
        TestnetShadowRunnerControlAction::Pause => {
            if snapshot.state.status != TestnetShadowRunnerStatus::Running {
                return Err(anyhow!("runner can only be paused from RUNNING"));
            }
            let updated = persist_runner_state(
                state,
                TestnetShadowRunnerState {
                    status: TestnetShadowRunnerStatus::Paused,
                    updated_at: Utc::now(),
                    ..snapshot.state
                },
            )
            .await?;
            Ok((updated, None))
        }
        TestnetShadowRunnerControlAction::Resume => {
            if !matches!(
                snapshot.state.status,
                TestnetShadowRunnerStatus::Paused | TestnetShadowRunnerStatus::Error
            ) {
                return Err(anyhow!("runner can only be resumed from PAUSED or ERROR"));
            }
            let updated = persist_runner_state(
                state,
                TestnetShadowRunnerState {
                    status: TestnetShadowRunnerStatus::Running,
                    last_error: None,
                    updated_at: Utc::now(),
                    ..snapshot.state
                },
            )
            .await?;
            Ok((updated, None))
        }
    }
}

pub async fn run_shadow_runner_tick(
    state: &AppState,
    actor: Option<&StateActor>,
    correlation_id: Option<Uuid>,
    mode: RunnerTickMode,
) -> Result<TestnetShadowRunnerTickResult> {
    let correlation_id = correlation_id.unwrap_or_else(Uuid::new_v4);
    let started_at = Utc::now();
    let snapshot = load_testnet_shadow_runner_snapshot(state).await?;
    let validation = validate_testnet_shadow_runner_config(&TestnetShadowRunnerConfigInput {
        enabled: snapshot.config.enabled,
        interval_seconds: snapshot.config.interval_seconds,
        strategies: snapshot.config.strategies.clone(),
        symbols: snapshot.config.symbols.clone(),
        timeframe: snapshot.config.timeframe.clone(),
        max_runs_per_tick: snapshot.config.max_runs_per_tick,
        stale_feed_policy: snapshot.config.stale_feed_policy,
        notes: snapshot.config.notes.clone(),
    });

    if !validation.valid {
        let message = validation.issues.join("; ");
        let failed_state = persist_runner_state(
            state,
            TestnetShadowRunnerState {
                status: TestnetShadowRunnerStatus::Error,
                last_error: Some(message.clone()),
                updated_at: Utc::now(),
                ..snapshot.state
            },
        )
        .await?;
        let _ = insert_system_event(
            &state.db_pool,
            &EventEnvelope::new(
                "exchange.testnet.shadow_runner.tick_failed",
                correlation_id,
                &state.config.app_name,
                json!({
                    "scheduled": mode.scheduled(),
                    "error": message,
                    "status": failed_state.status.as_str(),
                }),
            ),
        )
        .await;
        telemetry().inc_exchange_testnet_shadow_runner_tick("failed");
        return Ok(TestnetShadowRunnerTickResult {
            status: TestnetShadowRunnerTickStatus::Failed,
            started_at,
            completed_at: Utc::now(),
            scheduled: mode.scheduled(),
            attempted_runs: 0,
            completed_runs: 0,
            failed_runs: 0,
            correlation_id,
            message: Some("persisted runner config is invalid".to_string()),
        });
    }

    if let Some(reason) = scheduled_tick_noop_reason(&snapshot.config, &snapshot.state, mode) {
        telemetry().inc_exchange_testnet_shadow_runner_tick("no_op");
        return Ok(TestnetShadowRunnerTickResult {
            status: TestnetShadowRunnerTickStatus::NoOp,
            started_at,
            completed_at: Utc::now(),
            scheduled: mode.scheduled(),
            attempted_runs: 0,
            completed_runs: 0,
            failed_runs: 0,
            correlation_id,
            message: Some(reason),
        });
    }

    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            "exchange.testnet.shadow_runner.tick_started",
            correlation_id,
            &state.config.app_name,
            json!({
                "scheduled": mode.scheduled(),
                "status": snapshot.state.status.as_str(),
                "strategies": snapshot.config.strategies,
                "symbols": snapshot.config.symbols,
                "timeframe": snapshot.config.timeframe,
                "max_runs_per_tick": snapshot.config.max_runs_per_tick,
            }),
        ),
    )
    .await;

    let mut attempted_runs = 0_i32;
    let mut completed_runs = 0_i32;
    let mut failed_runs = 0_i32;
    let mut error_messages = Vec::new();

    for (strategy_id, symbol) in bounded_strategy_symbol_pairs(&snapshot.config) {
        attempted_runs += 1;
        match run_testnet_shadow_once(
            state,
            actor,
            aegis_core::TestnetShadowRunRequest {
                strategy_id: strategy_id.clone(),
                symbol: symbol.clone(),
                timeframe: snapshot.config.timeframe.clone(),
                correlation_id: Some(correlation_id),
            },
        )
        .await
        {
            Ok(run) => {
                completed_runs += 1;
                telemetry().inc_exchange_testnet_shadow_runner_run(run.decision.as_str());
            }
            Err(err) => {
                failed_runs += 1;
                error!(strategy_id = %strategy_id, symbol = %symbol, error = %err, "shadow runner pair failed");
                error_messages.push(format!("{strategy_id}/{symbol}: {err}"));
            }
        }
    }

    let status = if failed_runs == 0 {
        TestnetShadowRunnerTickStatus::Completed
    } else if completed_runs > 0 {
        TestnetShadowRunnerTickStatus::PartialFailure
    } else {
        TestnetShadowRunnerTickStatus::Failed
    };
    let message = if error_messages.is_empty() {
        None
    } else {
        Some(error_messages.join(" | "))
    };

    let next_runner_status =
        if matches!(snapshot.state.status, TestnetShadowRunnerStatus::Error) && mode.scheduled() {
            TestnetShadowRunnerStatus::Error
        } else {
            snapshot.state.status
        };

    let persisted_state = persist_runner_state(
        state,
        TestnetShadowRunnerState {
            status: next_runner_status,
            last_tick_at: Some(started_at),
            last_success_at: if status != TestnetShadowRunnerTickStatus::Failed {
                Some(Utc::now())
            } else {
                snapshot.state.last_success_at
            },
            last_error: message.clone(),
            total_ticks: snapshot.state.total_ticks + 1,
            total_runs: snapshot.state.total_runs + i64::from(completed_runs),
            updated_at: Utc::now(),
            ..snapshot.state
        },
    )
    .await
    .context("failed to persist runner state")?;

    let event_type = if status == TestnetShadowRunnerTickStatus::Failed {
        "exchange.testnet.shadow_runner.tick_failed"
    } else {
        "exchange.testnet.shadow_runner.tick_completed"
    };
    let _ = insert_system_event(
        &state.db_pool,
        &EventEnvelope::new(
            event_type,
            correlation_id,
            &state.config.app_name,
            json!({
                "scheduled": mode.scheduled(),
                "tick_status": status.as_str(),
                "attempted_runs": attempted_runs,
                "completed_runs": completed_runs,
                "failed_runs": failed_runs,
                "runner_status": persisted_state.status.as_str(),
                "message": message,
            }),
        ),
    )
    .await;

    telemetry().inc_exchange_testnet_shadow_runner_tick(match status {
        TestnetShadowRunnerTickStatus::NoOp => "no_op",
        TestnetShadowRunnerTickStatus::Completed => "completed",
        TestnetShadowRunnerTickStatus::PartialFailure => "partial_failure",
        TestnetShadowRunnerTickStatus::Failed => "failed",
    });

    Ok(TestnetShadowRunnerTickResult {
        status,
        started_at,
        completed_at: Utc::now(),
        scheduled: mode.scheduled(),
        attempted_runs,
        completed_runs,
        failed_runs,
        correlation_id,
        message,
    })
}

fn scheduled_tick_noop_reason(
    config: &TestnetShadowRunnerConfig,
    state: &TestnetShadowRunnerState,
    mode: RunnerTickMode,
) -> Option<String> {
    if !mode.scheduled() {
        return None;
    }
    if !config.enabled {
        return Some("runner config is disabled".to_string());
    }
    if matches!(
        state.status,
        TestnetShadowRunnerStatus::Paused | TestnetShadowRunnerStatus::Stopped
    ) {
        return Some(format!("runner state is {}", state.status.as_str()));
    }
    None
}

fn bounded_strategy_symbol_pairs(config: &TestnetShadowRunnerConfig) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    'outer: for strategy_id in &config.strategies {
        for symbol in &config.symbols {
            if pairs.len() >= config.max_runs_per_tick as usize {
                break 'outer;
            }
            pairs.push((strategy_id.clone(), symbol.clone()));
        }
    }
    pairs
}

async fn persist_runner_state(
    state: &AppState,
    runner_state: TestnetShadowRunnerState,
) -> Result<TestnetShadowRunnerState> {
    let record = upsert_testnet_shadow_runner_state(&state.db_pool, &runner_state).await?;
    let mapped = testnet_shadow_runner_state_from_record(&record)?;
    telemetry().set_exchange_testnet_shadow_runner_status(mapped.status.as_str());
    telemetry().set_exchange_testnet_shadow_runner_last_tick_age_seconds(
        mapped
            .last_tick_at
            .map(|value| Utc::now().signed_duration_since(value).num_seconds().max(0) as f64)
            .unwrap_or(0.0),
    );
    Ok(mapped)
}

#[cfg(test)]
mod tests {
    use super::{
        bounded_strategy_symbol_pairs, scheduled_tick_noop_reason,
        validate_testnet_shadow_runner_config, RunnerTickMode,
    };
    use aegis_core::{
        TestnetShadowRunnerConfig, TestnetShadowRunnerConfigInput,
        TestnetShadowRunnerControlAction, TestnetShadowRunnerStaleFeedPolicy,
        TestnetShadowRunnerState, TestnetShadowRunnerStatus,
    };
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn sample_config() -> TestnetShadowRunnerConfigInput {
        TestnetShadowRunnerConfigInput {
            enabled: true,
            interval_seconds: 60,
            strategies: vec!["momentum_v1".to_string()],
            symbols: vec!["btcusdt".to_string()],
            timeframe: "1m".to_string(),
            max_runs_per_tick: 2,
            stale_feed_policy: TestnetShadowRunnerStaleFeedPolicy::Skip,
            notes: Some("  test  ".to_string()),
        }
    }

    fn persisted_config() -> TestnetShadowRunnerConfig {
        TestnetShadowRunnerConfig {
            id: Uuid::from_u128(1),
            enabled: true,
            interval_seconds: 60,
            strategies: vec![
                "momentum_v1".to_string(),
                "volatility_breakout_v1".to_string(),
            ],
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            timeframe: "1m".to_string(),
            max_runs_per_tick: 3,
            stale_feed_policy: TestnetShadowRunnerStaleFeedPolicy::Skip,
            notes: None,
            updated_by: None,
            updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    fn runner_state(status: TestnetShadowRunnerStatus) -> TestnetShadowRunnerState {
        TestnetShadowRunnerState {
            id: Uuid::from_u128(2),
            status,
            last_tick_at: None,
            last_success_at: None,
            last_error: None,
            total_ticks: 0,
            total_runs: 0,
            updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn runner_config_validation_normalizes_values() {
        let validation = validate_testnet_shadow_runner_config(&sample_config());
        assert!(validation.valid);
        let normalized = validation.normalized_config.expect("normalized config");
        assert_eq!(normalized.strategies, vec!["momentum_v1".to_string()]);
        assert_eq!(normalized.symbols, vec!["BTCUSDT".to_string()]);
        assert_eq!(normalized.notes.as_deref(), Some("test"));
    }

    #[test]
    fn runner_config_validation_rejects_invalid_values() {
        let mut config = sample_config();
        config.interval_seconds = 0;
        config.max_runs_per_tick = 0;
        config.symbols = vec!["".to_string()];
        config.timeframe = "bogus".to_string();
        let validation = validate_testnet_shadow_runner_config(&config);
        assert!(!validation.valid);
        assert!(validation.issues.len() >= 4);
    }

    #[test]
    fn run_once_control_action_parses() {
        assert_eq!(
            "RUN_ONCE"
                .parse::<TestnetShadowRunnerControlAction>()
                .expect("action should parse"),
            TestnetShadowRunnerControlAction::RunOnce
        );
        assert!(!RunnerTickMode::ManualRunOnce.scheduled());
        assert!(RunnerTickMode::Scheduled.scheduled());
    }

    #[test]
    fn disabled_config_noops_for_scheduled_ticks() {
        let mut config = persisted_config();
        config.enabled = false;
        assert_eq!(
            scheduled_tick_noop_reason(
                &config,
                &runner_state(TestnetShadowRunnerStatus::Running),
                RunnerTickMode::Scheduled
            ),
            Some("runner config is disabled".to_string())
        );
    }

    #[test]
    fn paused_and_stopped_states_noop_for_scheduled_ticks() {
        let config = persisted_config();
        assert!(scheduled_tick_noop_reason(
            &config,
            &runner_state(TestnetShadowRunnerStatus::Paused),
            RunnerTickMode::Scheduled
        )
        .is_some());
        assert!(scheduled_tick_noop_reason(
            &config,
            &runner_state(TestnetShadowRunnerStatus::Stopped),
            RunnerTickMode::Scheduled
        )
        .is_some());
    }

    #[test]
    fn run_once_bypasses_disabled_scheduled_gate() {
        let mut config = persisted_config();
        config.enabled = false;
        assert_eq!(
            scheduled_tick_noop_reason(
                &config,
                &runner_state(TestnetShadowRunnerStatus::Stopped),
                RunnerTickMode::ManualRunOnce
            ),
            None
        );
    }

    #[test]
    fn max_runs_per_tick_is_respected() {
        let pairs = bounded_strategy_symbol_pairs(&persisted_config());
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("momentum_v1".to_string(), "BTCUSDT".to_string()));
        assert_eq!(
            pairs[2],
            ("volatility_breakout_v1".to_string(), "BTCUSDT".to_string())
        );
    }
}

use aegis_core::{
    BacktestEquityPoint, BacktestPosition, BacktestRequest, BacktestResult, BacktestTrade, Candle,
    CandleInterval, EventEnvelope, ReplayRunStatus, Side, StrategyConfig,
    StrategyConfigUpdateRequest, StrategyEvaluationContext, StrategyExperimentCandidate,
    StrategyExperimentComparison, StrategyExperimentMetric, StrategyExperimentRequest,
    StrategyExperimentResult, StrategyExperimentRun, StrategyExperimentStatus, StrategyId, Symbol,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use db::{
    backtest_result_from_record, get_backtest_run, get_closed_candles_range, get_strategy_status,
    insert_backtest_equity_points, insert_backtest_run, insert_backtest_trade,
    insert_strategy_experiment, insert_strategy_experiment_runs, insert_system_event,
    strategy_config_from_record, update_backtest_run_completed, PgPool,
};
use rust_decimal::Decimal;
use serde_json::json;
use strategy_engine::{
    evaluate as evaluate_strategy, validate_strategy_config, StrategyValidationContext,
};
use telemetry::telemetry;
use uuid::Uuid;

const BPS_DENOMINATOR: i64 = 10_000;

#[derive(Debug, Clone)]
pub struct ReplayEngine {
    pool: PgPool,
    source: String,
}

#[derive(Debug, Clone)]
pub struct BacktestExecution {
    pub result: BacktestResult,
    pub trades: Vec<BacktestTrade>,
    pub equity_curve: Vec<BacktestEquityPoint>,
}

#[derive(Debug, Clone)]
pub struct StrategyExperimentExecution {
    pub result: StrategyExperimentResult,
    pub runs: Vec<StrategyExperimentRun>,
}

#[derive(Debug, Clone)]
struct SimulationState {
    cash: Decimal,
    peak_equity: Decimal,
    position: Option<BacktestPosition>,
    trades: Vec<BacktestTrade>,
    equity_curve: Vec<BacktestEquityPoint>,
    fee_paid: Decimal,
    slippage_cost: Decimal,
}

impl ReplayEngine {
    pub fn new(pool: PgPool, source: impl Into<String>) -> Self {
        Self {
            pool,
            source: source.into(),
        }
    }

    pub async fn run_backtest(&self, request: BacktestRequest) -> Result<BacktestExecution> {
        request.validate()?;
        let started_at = std::time::Instant::now();
        let metrics = telemetry();

        let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
        let run_id = Uuid::new_v4();
        let created_at = Utc::now();
        let config = request.config();

        insert_backtest_run(
            &self.pool,
            run_id,
            &request,
            &config,
            created_at,
            ReplayRunStatus::Running,
            Some(correlation_id),
        )
        .await
        .context("failed to insert backtest run")?;

        self.emit_event(
            correlation_id,
            "replay.backtest.started",
            json!({
                "run_id": run_id,
                "strategy_id": request.strategy_id,
                "symbol": request.symbol,
                "timeframe": request.timeframe,
                "start_time": request.start_time,
                "end_time": request.end_time,
            }),
        )
        .await?;

        let execution = match self
            .execute(run_id, created_at, correlation_id, &request)
            .await
        {
            Ok(execution) => execution,
            Err(err) => {
                metrics.inc_backtest_run(
                    request.strategy_id.as_str(),
                    request.symbol.as_str(),
                    "failed",
                );
                let failed = failure_result(run_id, created_at, Some(correlation_id), &request);
                let _ = update_backtest_run_completed(&self.pool, &failed, &config).await;
                let _ = self
                    .emit_event(
                        correlation_id,
                        "replay.backtest.failed",
                        json!({
                            "run_id": run_id,
                            "strategy_id": request.strategy_id,
                            "symbol": request.symbol,
                            "error": err.to_string(),
                        }),
                    )
                    .await;
                return Err(err);
            }
        };

        update_backtest_run_completed(&self.pool, &execution.result, &config)
            .await
            .context("failed to update completed backtest run")?;

        for trade in &execution.trades {
            insert_backtest_trade(&self.pool, trade)
                .await
                .context("failed to insert backtest trade")?;
        }

        insert_backtest_equity_points(&self.pool, &execution.equity_curve)
            .await
            .context("failed to insert backtest equity curve")?;

        self.emit_event(
            correlation_id,
            "replay.backtest.completed",
            json!({
                "run_id": execution.result.run_id,
                "strategy_id": execution.result.strategy_id,
                "symbol": execution.result.symbol,
                "trade_count": execution.result.trade_count,
                "pnl": execution.result.pnl,
                "pnl_pct": execution.result.pnl_pct,
            }),
        )
        .await?;
        metrics.inc_backtest_run(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            execution.result.status.as_str(),
        );
        metrics.observe_backtest_duration(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            started_at.elapsed(),
        );
        metrics.add_backtest_trades(
            request.strategy_id.as_str(),
            request.symbol.as_str(),
            execution.result.trade_count.max(0) as u64,
        );

        Ok(execution)
    }

    pub async fn run_strategy_experiment(
        &self,
        request: StrategyExperimentRequest,
    ) -> Result<StrategyExperimentExecution> {
        request.validate()?;

        let experiment_id = Uuid::new_v4();
        let correlation_id = request.correlation_id.unwrap_or_else(Uuid::new_v4);
        let created_at = Utc::now();
        let strategy_id: StrategyId = request
            .strategy_id
            .parse()
            .context("invalid strategy_id for strategy experiment")?;
        let symbol = Symbol::new(request.symbol.clone())
            .context("invalid symbol for strategy experiment")?;
        let timeframe: CandleInterval = request
            .timeframe
            .parse()
            .context("invalid timeframe for strategy experiment")?;
        let config_record = get_strategy_status(&self.pool, strategy_id)
            .await?
            .map(|status| status.config)
            .ok_or_else(|| anyhow!("persisted strategy config not found"))?;
        let base_config = strategy_config_from_record(&config_record)
            .context("invalid persisted strategy config")?;
        let candles = get_closed_candles_range(
            &self.pool,
            &symbol,
            timeframe,
            request.start_time,
            request.end_time,
        )
        .await
        .context("failed to load closed candles range")?;

        let candidates = request.candidates();
        if candidates.is_empty() {
            return Err(anyhow!(
                "strategy experiment requires at least one candidate"
            ));
        }

        let mut runs = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let override_request = experiment_strategy_override(&base_config, &request, &candidate);
            let validation = validate_strategy_config(
                &override_request,
                &StrategyValidationContext {
                    supported_symbols: vec![symbol.clone()],
                    max_position_notional: Some(
                        aegis_core::RiskConfig::default().max_position_notional,
                    ),
                },
            );
            let strategy_config = validation
                .normalized_config
                .ok_or_else(|| anyhow!("invalid strategy experiment candidate override"))?;
            let run_request = BacktestRequest {
                strategy_id: request.strategy_id.clone(),
                symbol: request.symbol.clone(),
                timeframe: request.timeframe.clone(),
                start_time: request.start_time,
                end_time: request.end_time,
                initial_capital: request.initial_capital,
                risk_config_id: None,
                risk_config: None,
                fee_bps: request.fee_bps,
                slippage_bps: request.slippage_bps,
                correlation_id: Some(correlation_id),
                holding_candles: candidate
                    .holding_candles
                    .or(strategy_config.holding_candles),
                strategy_config_override: Some(override_request),
            };
            let execution = simulate_backtest(
                Uuid::new_v4(),
                created_at,
                correlation_id,
                &run_request,
                &strategy_config,
                candles.clone(),
            )?;
            runs.push(strategy_experiment_run_from_backtest(
                experiment_id,
                created_at,
                request.initial_capital,
                candidate,
                execution.result,
            ));
        }

        rank_strategy_experiment_runs(&mut runs);
        let comparison = strategy_experiment_comparison(&runs);
        let best_run = comparison
            .best_run_id
            .and_then(|id| runs.iter().find(|run| run.id == id).cloned());
        let worst_run = comparison
            .worst_run_id
            .and_then(|id| runs.iter().find(|run| run.id == id).cloned());
        let status = if runs
            .iter()
            .all(|run| run.status == StrategyExperimentStatus::Completed)
        {
            StrategyExperimentStatus::Completed
        } else {
            StrategyExperimentStatus::Failed
        };
        let result = StrategyExperimentResult {
            experiment_id,
            strategy_id: request.strategy_id.clone(),
            symbol: request.symbol.clone(),
            timeframe: request.timeframe.clone(),
            start_time: request.start_time,
            end_time: request.end_time,
            initial_capital: request.initial_capital,
            fee_bps: request.fee_bps,
            slippage_bps: request.slippage_bps,
            max_signal_age_ms: request.max_signal_age_ms,
            max_runs: request.max_runs,
            status,
            run_count: runs.len() as i32,
            comparison,
            best_run,
            worst_run,
            created_at,
            correlation_id: Some(correlation_id),
        };

        insert_strategy_experiment(&self.pool, &result)
            .await
            .context("failed to insert strategy experiment")?;
        insert_strategy_experiment_runs(&self.pool, &runs)
            .await
            .context("failed to insert strategy experiment runs")?;

        Ok(StrategyExperimentExecution { result, runs })
    }

    async fn execute(
        &self,
        run_id: Uuid,
        created_at: DateTime<Utc>,
        correlation_id: Uuid,
        request: &BacktestRequest,
    ) -> Result<BacktestExecution> {
        let strategy_id: StrategyId = request
            .strategy_id
            .parse()
            .context("invalid strategy_id for backtest")?;
        let symbol = Symbol::new(request.symbol.clone()).context("invalid symbol for backtest")?;
        let timeframe: CandleInterval = request
            .timeframe
            .parse()
            .context("invalid timeframe for backtest")?;
        let config_record = get_strategy_status(&self.pool, strategy_id)
            .await?
            .map(|status| status.config)
            .ok_or_else(|| anyhow!("persisted strategy config not found"))?;
        let strategy_config = if let Some(override_request) = &request.strategy_config_override {
            let validation = validate_strategy_config(
                override_request,
                &StrategyValidationContext {
                    supported_symbols: vec![symbol.clone()],
                    max_position_notional: Some(
                        aegis_core::RiskConfig::default().max_position_notional,
                    ),
                },
            );
            validation
                .normalized_config
                .ok_or_else(|| anyhow!("invalid strategy_config_override for backtest"))?
        } else {
            strategy_config_from_record(&config_record)
                .context("invalid persisted strategy config")?
        };

        let candles = get_closed_candles_range(
            &self.pool,
            &symbol,
            timeframe,
            request.start_time,
            request.end_time,
        )
        .await
        .context("failed to load closed candles range")?;

        if candles.is_empty() {
            return Ok(BacktestExecution {
                result: failure_result(run_id, created_at, Some(correlation_id), request),
                trades: Vec::new(),
                equity_curve: Vec::new(),
            });
        }

        Ok(simulate_backtest(
            run_id,
            created_at,
            correlation_id,
            request,
            &strategy_config,
            candles,
        )?)
    }

    async fn emit_event(
        &self,
        correlation_id: Uuid,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        insert_system_event(
            &self.pool,
            &EventEnvelope::new(event_type, correlation_id, self.source.clone(), payload),
        )
        .await?;
        Ok(())
    }
}

pub async fn fetch_backtest_result(pool: &PgPool, run_id: Uuid) -> Result<Option<BacktestResult>> {
    Ok(get_backtest_run(pool, run_id)
        .await?
        .as_ref()
        .map(backtest_result_from_record)
        .transpose()?)
}

pub fn simulate_backtest(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    correlation_id: Uuid,
    request: &BacktestRequest,
    strategy_config: &StrategyConfig,
    candles: Vec<Candle>,
) -> Result<BacktestExecution> {
    if candles.is_empty() {
        return Ok(BacktestExecution {
            result: failure_result(run_id, created_at, Some(correlation_id), request),
            trades: Vec::new(),
            equity_curve: Vec::new(),
        });
    }

    let strategy_id: StrategyId = request.strategy_id.parse()?;
    let symbol = Symbol::new(request.symbol.clone())?;
    let backtest_config = request.config();
    let mut state = SimulationState {
        cash: request.initial_capital,
        peak_equity: request.initial_capital,
        position: None,
        trades: Vec::new(),
        equity_curve: Vec::new(),
        fee_paid: Decimal::ZERO,
        slippage_cost: Decimal::ZERO,
    };

    for index in 0..candles.len() {
        let candle = &candles[index];

        if let Some(position) = state.position.take() {
            state.position = evaluate_exit(
                run_id,
                created_at,
                &backtest_config,
                position,
                candle,
                &mut state,
                &request.strategy_id,
                &request.symbol,
            )?;
        }

        if state.position.is_none() && index + 1 < candles.len() {
            let evaluation = evaluate_strategy(StrategyEvaluationContext {
                correlation_id,
                strategy_id,
                symbol: symbol.clone(),
                config: strategy_config.clone(),
                candles: candles[..=index].to_vec(),
                evaluated_at: candle.close_time,
            })?;

            if let Some(signal) = evaluation.signal {
                if signal.side == aegis_core::SignalSide::Buy {
                    let next_candle = &candles[index + 1];
                    state.position = maybe_open_position(
                        &backtest_config,
                        strategy_config,
                        next_candle,
                        state.cash,
                    )?;
                }
            }
        }

        record_equity(run_id, candle.close_time, candle.close, &mut state)?;
    }

    if let (Some(position), Some(last_candle)) = (state.position.take(), candles.last()) {
        close_position(
            run_id,
            created_at,
            &backtest_config,
            position,
            last_candle.close_time,
            last_candle.close,
            "replay_end",
            &mut state,
            &request.strategy_id,
            &request.symbol,
        )?;
        record_equity(
            run_id,
            last_candle.close_time,
            last_candle.close,
            &mut state,
        )?;
    }

    let result = build_result(
        run_id,
        created_at,
        Some(correlation_id),
        request,
        &state.trades,
        &state.equity_curve,
        state.fee_paid,
        state.slippage_cost,
        state.cash,
    );

    Ok(BacktestExecution {
        result,
        trades: state.trades,
        equity_curve: state.equity_curve,
    })
}

fn experiment_strategy_override(
    base_config: &StrategyConfig,
    request: &StrategyExperimentRequest,
    candidate: &StrategyExperimentCandidate,
) -> StrategyConfigUpdateRequest {
    StrategyConfigUpdateRequest {
        strategy_id: request.strategy_id.clone(),
        enabled: base_config.enabled,
        mode: base_config.mode,
        symbols: vec![request.symbol.clone()],
        timeframe: request.timeframe.clone(),
        suggested_notional: base_config.suggested_notional,
        max_signal_age_ms: candidate
            .max_signal_age_ms
            .unwrap_or(base_config.max_signal_age_ms),
        cooldown_seconds: base_config.cooldown_seconds,
        lookback_candles: candidate.lookback_candles,
        confidence_floor: base_config.confidence_floor,
        stop_loss_pct: candidate.stop_loss_pct.or(base_config.stop_loss_pct),
        take_profit_pct: candidate.take_profit_pct.or(base_config.take_profit_pct),
        holding_candles: candidate.holding_candles.or(base_config.holding_candles),
        notes: base_config.notes.clone(),
    }
}

fn strategy_experiment_run_from_backtest(
    experiment_id: Uuid,
    created_at: DateTime<Utc>,
    initial_capital: Decimal,
    candidate: StrategyExperimentCandidate,
    result: BacktestResult,
) -> StrategyExperimentRun {
    let fee_slippage_drag_pct =
        calculate_fee_slippage_drag_pct(initial_capital, result.fee_paid, result.slippage_cost);
    let warnings = experiment_warnings(
        result.trade_count,
        result.pnl,
        result.max_drawdown_pct,
        fee_slippage_drag_pct,
    );
    let mut run = StrategyExperimentRun {
        id: Uuid::new_v4(),
        experiment_id,
        rank: 0,
        candidate,
        final_equity: result.final_equity,
        pnl: result.pnl,
        pnl_pct: result.pnl_pct,
        max_drawdown_pct: result.max_drawdown_pct,
        win_rate: result.win_rate,
        trade_count: result.trade_count,
        fee_paid: result.fee_paid,
        slippage_cost: result.slippage_cost,
        fee_slippage_drag_pct,
        score: Decimal::ZERO,
        status: match result.status {
            ReplayRunStatus::Completed => StrategyExperimentStatus::Completed,
            ReplayRunStatus::Failed => StrategyExperimentStatus::Failed,
            ReplayRunStatus::Pending => StrategyExperimentStatus::Pending,
            ReplayRunStatus::Running => StrategyExperimentStatus::Running,
        },
        warnings,
        created_at,
    };
    run.score = calculate_strategy_experiment_score(&run);
    run
}

pub fn calculate_fee_slippage_drag_pct(
    initial_capital: Decimal,
    fee_paid: Decimal,
    slippage_cost: Decimal,
) -> Decimal {
    if initial_capital <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    ((fee_paid + slippage_cost) / initial_capital) * Decimal::new(100, 0)
}

pub fn calculate_strategy_experiment_score(run: &StrategyExperimentRun) -> Decimal {
    let trade_penalty = if run.trade_count > 200 {
        Decimal::from((run.trade_count - 200) / 10)
    } else if run.trade_count > 0 && run.trade_count < 3 {
        Decimal::new(5, 0)
    } else {
        Decimal::ZERO
    };

    run.pnl_pct - (run.max_drawdown_pct / Decimal::new(2, 0)) + (run.win_rate / Decimal::new(10, 0))
        - run.fee_slippage_drag_pct
        - trade_penalty
}

pub fn rank_strategy_experiment_runs(runs: &mut [StrategyExperimentRun]) {
    runs.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| right.pnl_pct.cmp(&left.pnl_pct))
            .then_with(|| left.max_drawdown_pct.cmp(&right.max_drawdown_pct))
            .then_with(|| right.win_rate.cmp(&left.win_rate))
            .then_with(|| left.fee_slippage_drag_pct.cmp(&right.fee_slippage_drag_pct))
            .then_with(|| right.trade_count.cmp(&left.trade_count))
            .then_with(|| left.id.cmp(&right.id))
    });

    for (index, run) in runs.iter_mut().enumerate() {
        run.rank = index as i32 + 1;
    }
}

fn strategy_experiment_comparison(runs: &[StrategyExperimentRun]) -> StrategyExperimentComparison {
    StrategyExperimentComparison {
        ranking_metric: StrategyExperimentMetric::RiskAdjustedScore,
        best_run_id: runs.first().map(|run| run.id),
        worst_run_id: runs.last().map(|run| run.id),
        ranked_run_ids: runs.iter().map(|run| run.id).collect(),
    }
}

fn experiment_warnings(
    trade_count: i32,
    pnl: Decimal,
    max_drawdown_pct: Decimal,
    fee_slippage_drag_pct: Decimal,
) -> Vec<String> {
    let mut warnings = Vec::new();

    if trade_count >= 200 {
        warnings.push("very_high_trade_count".to_string());
    }
    if pnl < Decimal::ZERO && fee_slippage_drag_pct > Decimal::ZERO {
        warnings.push("negative_after_fees".to_string());
    }
    if max_drawdown_pct >= Decimal::new(15, 0) {
        warnings.push("high_drawdown".to_string());
    }
    if trade_count > 0 && trade_count < 3 {
        warnings.push("too_few_trades".to_string());
    }

    warnings
}

fn maybe_open_position(
    backtest_config: &aegis_core::BacktestConfig,
    strategy_config: &StrategyConfig,
    next_candle: &Candle,
    available_cash: Decimal,
) -> Result<Option<BacktestPosition>> {
    let fee_multiplier = Decimal::ONE + (backtest_config.fee_bps / Decimal::from(BPS_DENOMINATOR));
    let max_notional = available_cash / fee_multiplier;
    let target_notional = strategy_config.suggested_notional.min(max_notional);
    if target_notional <= Decimal::ZERO {
        return Ok(None);
    }

    let entry_price = apply_entry_slippage(next_candle.open, backtest_config.slippage_bps);
    let quantity = target_notional / entry_price;
    if quantity <= Decimal::ZERO {
        return Ok(None);
    }

    let executed_notional = quantity * entry_price;
    let entry_slippage_cost = (entry_price - next_candle.open) * quantity;
    let entry_fee = fee_amount(executed_notional, backtest_config.fee_bps);

    Ok(Some(BacktestPosition {
        side: Side::Buy,
        entry_time: next_candle.open_time,
        entry_price,
        quantity,
        notional: executed_notional,
        fee_paid: entry_fee,
        slippage_cost: entry_slippage_cost,
        remaining_holding_candles: backtest_config.holding_candles,
        stop_loss_price: strategy_config
            .stop_loss_pct
            .map(|pct| entry_price * (Decimal::ONE - pct)),
        take_profit_price: strategy_config
            .take_profit_pct
            .map(|pct| entry_price * (Decimal::ONE + pct)),
    }))
}

fn evaluate_exit(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    backtest_config: &aegis_core::BacktestConfig,
    mut position: BacktestPosition,
    candle: &Candle,
    state: &mut SimulationState,
    strategy_id: &str,
    symbol: &str,
) -> Result<Option<BacktestPosition>> {
    if let Some(stop_loss_price) = position.stop_loss_price {
        if candle.low <= stop_loss_price {
            close_position(
                run_id,
                created_at,
                backtest_config,
                position,
                candle.close_time,
                stop_loss_price,
                "stop_loss",
                state,
                strategy_id,
                symbol,
            )?;
            return Ok(None);
        }
    }

    if let Some(take_profit_price) = position.take_profit_price {
        if candle.high >= take_profit_price {
            close_position(
                run_id,
                created_at,
                backtest_config,
                position,
                candle.close_time,
                take_profit_price,
                "take_profit",
                state,
                strategy_id,
                symbol,
            )?;
            return Ok(None);
        }
    }

    if position.remaining_holding_candles > 0 {
        position.remaining_holding_candles -= 1;
    }

    if position.remaining_holding_candles == 0 {
        close_position(
            run_id,
            created_at,
            backtest_config,
            position,
            candle.close_time,
            candle.close,
            "holding_period",
            state,
            strategy_id,
            symbol,
        )?;
        return Ok(None);
    }

    Ok(Some(position))
}

fn close_position(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    backtest_config: &aegis_core::BacktestConfig,
    position: BacktestPosition,
    exit_time: DateTime<Utc>,
    reference_exit_price: Decimal,
    reason: &str,
    state: &mut SimulationState,
    strategy_id: &str,
    symbol: &str,
) -> Result<()> {
    let exit_price = apply_exit_slippage(reference_exit_price, backtest_config.slippage_bps);
    let exit_notional = position.quantity * exit_price;
    let exit_fee = fee_amount(exit_notional, backtest_config.fee_bps);
    let exit_slippage_cost = (reference_exit_price - exit_price) * position.quantity;
    let realized_pnl = exit_notional - exit_fee - position.notional - position.fee_paid;

    state.cash -= position.notional + position.fee_paid;
    state.cash += exit_notional - exit_fee;
    state.fee_paid += position.fee_paid + exit_fee;
    state.slippage_cost += position.slippage_cost + exit_slippage_cost;
    state.trades.push(BacktestTrade {
        id: deterministic_trade_id(run_id, position.entry_time, exit_time),
        run_id,
        strategy_id: strategy_id.to_string(),
        symbol: symbol.to_string(),
        side: position.side,
        entry_time: position.entry_time,
        entry_price: position.entry_price,
        exit_time: Some(exit_time),
        exit_price: Some(exit_price),
        quantity: position.quantity,
        notional: position.notional,
        fee_paid: position.fee_paid + exit_fee,
        slippage_cost: position.slippage_cost + exit_slippage_cost,
        realized_pnl,
        reason: reason.to_string(),
        created_at,
    });
    Ok(())
}

fn record_equity(
    run_id: Uuid,
    timestamp: DateTime<Utc>,
    mark_price: Decimal,
    state: &mut SimulationState,
) -> Result<()> {
    let equity = if let Some(position) = state.position.as_ref() {
        state.cash + (position.quantity * mark_price) - position.fee_paid - position.notional
    } else {
        state.cash
    };
    if equity > state.peak_equity {
        state.peak_equity = equity;
    }
    let drawdown_pct = if state.peak_equity > Decimal::ZERO {
        ((state.peak_equity - equity) / state.peak_equity) * Decimal::new(100, 0)
    } else {
        Decimal::ZERO
    };
    state.equity_curve.push(BacktestEquityPoint {
        id: deterministic_point_id(run_id, timestamp),
        run_id,
        timestamp,
        equity,
        drawdown_pct,
    });
    Ok(())
}

fn build_result(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
    request: &BacktestRequest,
    trades: &[BacktestTrade],
    equity_curve: &[BacktestEquityPoint],
    fee_paid: Decimal,
    slippage_cost: Decimal,
    final_equity: Decimal,
) -> BacktestResult {
    let winning_trades = trades
        .iter()
        .filter(|trade| trade.realized_pnl > Decimal::ZERO)
        .count() as i32;
    let losing_trades = trades
        .iter()
        .filter(|trade| trade.realized_pnl < Decimal::ZERO)
        .count() as i32;
    let trade_count = trades.len() as i32;
    let pnl = final_equity - request.initial_capital;
    let pnl_pct = if request.initial_capital > Decimal::ZERO {
        (pnl / request.initial_capital) * Decimal::new(100, 0)
    } else {
        Decimal::ZERO
    };
    let max_drawdown_pct = equity_curve
        .iter()
        .map(|point| point.drawdown_pct)
        .max()
        .unwrap_or(Decimal::ZERO);
    let win_rate = if trade_count > 0 {
        (Decimal::from(winning_trades) / Decimal::from(trade_count)) * Decimal::new(100, 0)
    } else {
        Decimal::ZERO
    };
    let avg_win = average_trade_pnl(
        trades
            .iter()
            .filter(|trade| trade.realized_pnl > Decimal::ZERO)
            .map(|trade| trade.realized_pnl),
        winning_trades,
    );
    let avg_loss = average_trade_pnl(
        trades
            .iter()
            .filter(|trade| trade.realized_pnl < Decimal::ZERO)
            .map(|trade| trade.realized_pnl),
        losing_trades,
    );

    BacktestResult {
        run_id,
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        initial_capital: request.initial_capital,
        final_equity,
        pnl,
        pnl_pct,
        max_drawdown_pct,
        win_rate,
        trade_count,
        winning_trades,
        losing_trades,
        avg_win,
        avg_loss,
        fee_paid,
        slippage_cost,
        status: ReplayRunStatus::Completed,
        created_at,
        correlation_id,
    }
}

fn failure_result(
    run_id: Uuid,
    created_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
    request: &BacktestRequest,
) -> BacktestResult {
    BacktestResult {
        run_id,
        strategy_id: request.strategy_id.clone(),
        symbol: request.symbol.clone(),
        timeframe: request.timeframe.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        initial_capital: request.initial_capital,
        final_equity: request.initial_capital,
        pnl: Decimal::ZERO,
        pnl_pct: Decimal::ZERO,
        max_drawdown_pct: Decimal::ZERO,
        win_rate: Decimal::ZERO,
        trade_count: 0,
        winning_trades: 0,
        losing_trades: 0,
        avg_win: Decimal::ZERO,
        avg_loss: Decimal::ZERO,
        fee_paid: Decimal::ZERO,
        slippage_cost: Decimal::ZERO,
        status: ReplayRunStatus::Failed,
        created_at,
        correlation_id,
    }
}

fn average_trade_pnl<I>(values: I, count: i32) -> Decimal
where
    I: Iterator<Item = Decimal>,
{
    if count == 0 {
        return Decimal::ZERO;
    }

    values.fold(Decimal::ZERO, |acc, value| acc + value) / Decimal::from(count)
}

fn fee_amount(notional: Decimal, fee_bps: Decimal) -> Decimal {
    notional * fee_bps / Decimal::from(BPS_DENOMINATOR)
}

fn apply_entry_slippage(price: Decimal, slippage_bps: Decimal) -> Decimal {
    price * (Decimal::ONE + slippage_bps / Decimal::from(BPS_DENOMINATOR))
}

fn apply_exit_slippage(price: Decimal, slippage_bps: Decimal) -> Decimal {
    price * (Decimal::ONE - slippage_bps / Decimal::from(BPS_DENOMINATOR))
}

fn deterministic_trade_id(
    run_id: Uuid,
    entry_time: DateTime<Utc>,
    exit_time: DateTime<Utc>,
) -> Uuid {
    let seed =
        (entry_time.timestamp_millis() as u128) << 64 | (exit_time.timestamp_millis() as u128);
    Uuid::from_u128(run_id.as_u128() ^ seed)
}

fn deterministic_point_id(run_id: Uuid, timestamp: DateTime<Utc>) -> Uuid {
    let seed = timestamp.timestamp_millis() as u128;
    Uuid::from_u128(run_id.as_u128() ^ seed)
}

#[cfg(test)]
mod tests {
    use super::{
        calculate_fee_slippage_drag_pct, calculate_strategy_experiment_score,
        experiment_strategy_override, rank_strategy_experiment_runs, simulate_backtest,
    };
    use aegis_core::{
        BacktestRequest, Candle, CandleInterval, MarketDataSource, StrategyConfig,
        StrategyExperimentRequest, StrategyExperimentRun, StrategyExperimentStatus, StrategyId,
        StrategyMode, Symbol,
    };
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    fn sample_request() -> BacktestRequest {
        BacktestRequest {
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start_time: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2026, 5, 1, 1, 0, 0).unwrap(),
            initial_capital: Decimal::new(1_000_000, 0),
            risk_config_id: None,
            risk_config: None,
            fee_bps: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            correlation_id: Some(Uuid::from_u128(0xabc)),
            holding_candles: Some(3),
            strategy_config_override: None,
        }
    }

    fn sample_strategy_config() -> StrategyConfig {
        StrategyConfig {
            strategy_id: StrategyId::MomentumV1,
            enabled: true,
            mode: StrategyMode::Paper,
            symbols: vec![Symbol::new("BTCUSDT").unwrap()],
            timeframe: CandleInterval::OneMinute,
            suggested_notional: Decimal::new(100_000, 0),
            max_signal_age_ms: 180_000,
            cooldown_seconds: 900,
            lookback_candles: 3,
            confidence_floor: None,
            stop_loss_pct: None,
            take_profit_pct: None,
            holding_candles: Some(3),
            notes: None,
        }
    }

    fn candle(index: i64, open: i64, high: i64, low: i64, close: i64) -> Candle {
        let open_time =
            Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap() + Duration::minutes(index);
        Candle {
            id: Uuid::new_v4(),
            exchange: MarketDataSource::Binance,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval: CandleInterval::OneMinute,
            open_time,
            close_time: open_time + Duration::minutes(1),
            open: Decimal::new(open, 0),
            high: Decimal::new(high, 0),
            low: Decimal::new(low, 0),
            close: Decimal::new(close, 0),
            volume: Decimal::new(10, 0),
            quote_volume: Some(Decimal::new(1_000, 0)),
            trade_count: 1,
            is_closed: true,
            created_at: open_time,
            updated_at: open_time,
        }
    }

    fn trending_candles() -> Vec<Candle> {
        vec![
            candle(0, 100, 101, 99, 100),
            candle(1, 100, 102, 99, 101),
            candle(2, 101, 103, 100, 102),
            candle(3, 102, 104, 101, 103),
            candle(4, 103, 106, 102, 105),
            candle(5, 105, 107, 104, 106),
            candle(6, 106, 108, 105, 107),
            candle(7, 107, 109, 106, 108),
        ]
    }

    fn sample_experiment_request() -> StrategyExperimentRequest {
        StrategyExperimentRequest {
            strategy_id: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start_time: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            initial_capital: Decimal::new(1_000_000, 0),
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            lookback_candidates: vec![3, 5, 10],
            holding_candles_candidates: Some(vec![3, 5]),
            stop_loss_pct_candidates: None,
            take_profit_pct_candidates: None,
            max_signal_age_ms: Some(180_000),
            max_runs: Some(3),
            correlation_id: None,
        }
    }

    fn sample_experiment_run(
        id: Uuid,
        pnl_pct: i64,
        max_drawdown_pct: i64,
        trade_count: i32,
        win_rate: i64,
        fee_slippage_drag_pct: i64,
    ) -> StrategyExperimentRun {
        let mut run = StrategyExperimentRun {
            id,
            experiment_id: Uuid::from_u128(0x1000),
            rank: 0,
            candidate: aegis_core::StrategyExperimentCandidate {
                lookback_candles: 3,
                holding_candles: Some(3),
                stop_loss_pct: None,
                take_profit_pct: None,
                max_signal_age_ms: Some(180_000),
            },
            final_equity: Decimal::new(1_000_000 + pnl_pct * 10_000, 0),
            pnl: Decimal::new(pnl_pct * 10_000, 0),
            pnl_pct: Decimal::new(pnl_pct, 0),
            max_drawdown_pct: Decimal::new(max_drawdown_pct, 0),
            win_rate: Decimal::new(win_rate, 0),
            trade_count,
            fee_paid: Decimal::new(500, 0),
            slippage_cost: Decimal::new(250, 0),
            fee_slippage_drag_pct: Decimal::new(fee_slippage_drag_pct, 0),
            score: Decimal::ZERO,
            status: StrategyExperimentStatus::Completed,
            warnings: Vec::new(),
            created_at: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
        };
        run.score = calculate_strategy_experiment_score(&run);
        run
    }

    #[test]
    fn same_candle_input_produces_same_result() {
        let request = sample_request();
        let config = sample_strategy_config();
        let candles = trending_candles();
        let run_id = Uuid::from_u128(1);
        let created_at = Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap();

        let first = simulate_backtest(
            run_id,
            created_at,
            Uuid::from_u128(2),
            &request,
            &config,
            candles.clone(),
        )
        .unwrap();
        let second = simulate_backtest(
            run_id,
            created_at,
            Uuid::from_u128(2),
            &request,
            &config,
            candles,
        )
        .unwrap();

        assert_eq!(first.result, second.result);
        assert_eq!(first.trades, second.trades);
        assert_eq!(first.equity_curve, second.equity_curve);
    }

    #[test]
    fn no_candles_returns_failed_status_cleanly() {
        let request = sample_request();
        let config = sample_strategy_config();
        let result = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &request,
            &config,
            Vec::new(),
        );

        let execution = result.unwrap();
        assert!(execution.trades.is_empty());
        assert_eq!(execution.result.status, aegis_core::ReplayRunStatus::Failed);
    }

    #[test]
    fn buy_signal_opens_simulated_position() {
        let execution = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert!(!execution.trades.is_empty());
        assert_eq!(execution.trades[0].side, aegis_core::Side::Buy);
    }

    #[test]
    fn holding_period_closes_position() {
        let execution = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert_eq!(execution.trades[0].reason, "holding_period");
    }

    #[test]
    fn fee_reduces_pnl() {
        let mut fee_request = sample_request();
        fee_request.fee_bps = Decimal::new(10, 0);
        let no_fee = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();
        let with_fee = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &fee_request,
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert!(with_fee.result.pnl < no_fee.result.pnl);
    }

    #[test]
    fn slippage_reduces_pnl() {
        let mut slip_request = sample_request();
        slip_request.slippage_bps = Decimal::new(10, 0);
        let no_slip = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();
        let with_slip = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &slip_request,
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert!(with_slip.result.pnl < no_slip.result.pnl);
    }

    #[test]
    fn max_drawdown_calculation_is_correct() {
        let execution = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            vec![
                candle(0, 100, 101, 99, 100),
                candle(1, 100, 102, 99, 101),
                candle(2, 101, 103, 100, 102),
                candle(3, 102, 104, 101, 103),
                candle(4, 103, 104, 80, 81),
                candle(5, 81, 82, 80, 81),
                candle(6, 81, 82, 80, 81),
                candle(7, 81, 82, 80, 81),
            ],
        )
        .unwrap();

        assert!(execution.result.max_drawdown_pct > Decimal::ZERO);
    }

    #[test]
    fn win_rate_calculation_is_correct() {
        let execution = simulate_backtest(
            Uuid::from_u128(1),
            Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            Uuid::from_u128(2),
            &sample_request(),
            &sample_strategy_config(),
            trending_candles(),
        )
        .unwrap();

        assert_eq!(execution.result.win_rate, Decimal::new(100, 0));
    }

    #[test]
    fn empty_candidate_list_is_rejected() {
        let mut request = sample_experiment_request();
        request.lookback_candidates.clear();

        assert!(request.validate().is_err());
    }

    #[test]
    fn max_runs_limit_is_enforced() {
        let candidates = sample_experiment_request().candidates();

        assert_eq!(candidates.len(), 3);
    }

    #[test]
    fn fee_slippage_drag_is_included() {
        let drag = calculate_fee_slippage_drag_pct(
            Decimal::new(1_000_000, 0),
            Decimal::new(1_000, 0),
            Decimal::new(500, 0),
        );

        assert_eq!(drag, Decimal::new(15, 2));
    }

    #[test]
    fn score_calculation_penalizes_drawdown_and_costs() {
        let run = sample_experiment_run(Uuid::from_u128(1), 12, 4, 10, 60, 1);

        assert_eq!(run.score, Decimal::new(15, 0));
    }

    #[test]
    fn ranking_orders_highest_score_first() {
        let mut runs = vec![
            sample_experiment_run(Uuid::from_u128(1), 5, 3, 10, 50, 1),
            sample_experiment_run(Uuid::from_u128(2), 12, 4, 10, 60, 1),
            sample_experiment_run(Uuid::from_u128(3), 4, 20, 250, 55, 5),
        ];

        rank_strategy_experiment_runs(&mut runs);

        assert_eq!(runs[0].id, Uuid::from_u128(2));
        assert_eq!(runs[0].rank, 1);
        assert_eq!(runs[2].rank, 3);
    }

    #[test]
    fn experiment_override_does_not_mutate_strategy_config() {
        let base = sample_strategy_config();
        let original = base.clone();
        let candidate = aegis_core::StrategyExperimentCandidate {
            lookback_candles: 10,
            holding_candles: Some(5),
            stop_loss_pct: Some(Decimal::new(2, 0)),
            take_profit_pct: Some(Decimal::new(4, 0)),
            max_signal_age_ms: Some(240_000),
        };

        let override_request =
            experiment_strategy_override(&base, &sample_experiment_request(), &candidate);

        assert_eq!(base, original);
        assert_eq!(override_request.lookback_candles, 10);
        assert_eq!(override_request.holding_candles, Some(5));
    }
}

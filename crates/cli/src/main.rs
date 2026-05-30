use aegis_core::{
    relative_strength_continuation_v1_default_request,
    relative_strength_continuation_v1_default_robustness_matrix_request, CrossAssetExitRule,
    CrossAssetMarketFilter, CrossAssetOverextensionFilter, CrossAssetResearchRequest,
    CrossAssetRobustnessMatrixRequest, CrossAssetVolFilter, OperatorReportFormat,
    OperatorReportRequest, PaperTradingPipelineRequest, ResearchCandidateDecisionRejection,
    ResearchCandidateDecisionRequest, ResearchCandidateEvidenceBundle,
    ResearchCandidateImportBundlePreviewRequest, ResearchCandidateImportBundleRequest,
    ResearchCandidateImportReconciliationRequest, ResearchCandidateReviewRequest,
    ResearchExperimentPlanRunMode, ResearchExperimentPlanRunRequest,
    ResearchHypothesisGenerationRequest, ResearchHypothesisIncludedSource,
    ResearchHypothesisStatus, ResearchStaleRunRecoveryRequest, TestnetShadowRunnerControlAction,
    TestnetShadowRunnerControlRequest,
};
use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use cli::api::{
    build_backtest_request, build_candle_aggregation_request, build_candle_backfill_request,
    build_market_data_quality_query, build_market_data_repair_plan_request,
    build_multi_timeframe_strategy_experiment_request, build_pipeline_request,
    build_research_batch_request, build_research_campaign_request,
    build_research_data_build_request, build_research_data_coverage_query,
    build_research_regime_calibration_request,
    build_research_regime_dataset_from_discovery_request, build_research_regime_dataset_request,
    build_research_regime_discovery_request, build_research_robustness_matrix_request,
    build_risk_config_request, build_strategy_config_request, build_strategy_experiment_request,
    build_strategy_walk_forward_request, ApiClient, ApiClientError,
    CreateResearchCandidateFromExperimentRunRequest, CreateResearchCandidateRequest,
    RecentEventsQuery, ResearchCandidatesQuery, RiskDecisionsQuery,
};
use cli::cli::{
    AnalyticsCommands, AnalyticsStrategyCommands, AnalyticsTestnetCommands, AuthCommands,
    BacktestCommands, Cli, Commands, EventsCommands, ExchangeCommands, ExchangeTestnetCommands,
    ExchangeTestnetPrivateStreamCommands, ExchangeTestnetShadowRunnerCommands, ExperimentCommands,
    MarketCommands, OperatorReportsCommands, OrderCommands, PaperCommands, PipelineCommands,
    ReadinessCommands, ReportsCommands, ResearchBatchCommands, ResearchCampaignCommands,
    ResearchCandidateCommands, ResearchCommands, ResearchCrossAssetCommands,
    ResearchCrossAssetRobustnessMatrixCommands, ResearchCrossAssetRobustnessMatrixRunArgs,
    ResearchCrossAssetRunArgs, ResearchDataCommands, ResearchExperimentPlanCommands,
    ResearchHypothesisCommands, ResearchRegimeCalibrationCommands, ResearchRegimeDatasetCommands,
    ResearchRegimeDiscoveryCommands, ResearchRobustnessMatrixCommands,
    ResearchScheduledJobCommands, ResearchStaleRunCommands, RiskCommands, RiskConfigCommands,
    StrategyCommands, StrategyConfigCommands, StrategyExperimentCommands, RESUME_CONFIRMATION_TEXT,
    TESTNET_ORDER_CONFIRMATION_TEXT,
};
use cli::config::{
    clear_token_file, save_token_file, CliConfig, StoredAuthSession, StoredUserSummary,
};
use cli::output;
use serde::{Deserialize, Serialize};
use std::fs;

fn build_cross_asset_research_request(
    args: &ResearchCrossAssetRunArgs,
) -> anyhow::Result<CrossAssetResearchRequest> {
    if let Some(raw) = args.request_json.as_deref() {
        return serde_json::from_str(raw).context("invalid --request-json");
    }
    let start_time = args.start_time.context("missing --start")?;
    let end_time = args.end_time.context("missing --end")?;
    let market_filter = match args.market_filter.trim().to_ascii_lowercase().as_str() {
        "none" => CrossAssetMarketFilter::None,
        "basket_72h_return_gt" => CrossAssetMarketFilter::Basket72hReturnGt {
            threshold_pct: args
                .market_threshold_pct
                .context("--market-threshold-pct is required")?,
        },
        "basket_24h_return_gt" => CrossAssetMarketFilter::Basket24hReturnGt {
            threshold_pct: args
                .market_threshold_pct
                .context("--market-threshold-pct is required")?,
        },
        "at_least_n_symbols_positive_24h" => CrossAssetMarketFilter::AtLeastNSymbolsPositive24h {
            min_symbols: args
                .market_min_symbols
                .context("--market-min-symbols is required")?,
        },
        other => anyhow::bail!("unsupported --market-filter {other}"),
    };
    let vol_filter = match args.vol_filter.trim().to_ascii_lowercase().as_str() {
        "none" => CrossAssetVolFilter::None,
        "asset_not_extreme_vs_basket" => CrossAssetVolFilter::AssetNotExtremeVsBasket {
            max_ratio: args.vol_max_ratio,
        },
        "basket_vol_below_percentile" => CrossAssetVolFilter::BasketVolBelowPercentile {
            percentile: args.vol_percentile,
        },
        other => anyhow::bail!("unsupported --vol-filter {other}"),
    };
    let overextension_filter = match args
        .overextension_filter
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "none" => CrossAssetOverextensionFilter::None,
        "max_return_24h_pct" => CrossAssetOverextensionFilter::MaxReturn24hPct {
            max_pct: args
                .max_return_24h_pct
                .context("--max-return-24h-pct is required")?,
        },
        "min_distance_from_72h_high_pct" => {
            CrossAssetOverextensionFilter::MinDistanceFrom72hHighPct {
                min_pct: args
                    .min_distance_72h_high_pct
                    .context("--min-distance-72h-high-pct is required")?,
            }
        }
        other => anyhow::bail!("unsupported --overextension-filter {other}"),
    };
    let exit_rule = match args.exit_rule.trim().to_ascii_lowercase().as_str() {
        "fixed_hold" => CrossAssetExitRule::FixedHold,
        "stop_pct" => CrossAssetExitRule::StopPct {
            stop_pct: args.stop_pct.context("--stop-pct is required")?,
        },
        "take_profit_pct" => CrossAssetExitRule::TakeProfitPct {
            take_profit_pct: args
                .take_profit_pct
                .context("--take-profit-pct is required")?,
        },
        other => anyhow::bail!("unsupported --exit-rule {other}"),
    };

    Ok(CrossAssetResearchRequest {
        strategy_kind: aegis_core::CrossAssetStrategyKind::RelativeStrengthContinuationV1Research,
        symbols: args.symbols.clone(),
        timeframe: args.timeframe.clone(),
        start_time,
        end_time,
        ranking_lookback_hours: args
            .ranking_lookback_hours
            .context("missing --ranking-lookback")?,
        rank_metric: args.rank_metric.context("missing --rank-metric")?,
        min_top_return_pct: args.min_top_return_pct,
        min_rank_spread_pct: args
            .min_rank_spread_pct
            .context("missing --min-rank-spread-pct")?,
        holding_hours: args.holding_hours.context("missing --holding-hours")?,
        fee_bps: args.fee_bps,
        slippage_bps: args.slippage_bps,
        one_active_position: args.one_active_position,
        sizing_mode: args.sizing_mode.context("missing --sizing-mode")?,
        min_weight: args.min_weight,
        max_weight: args.max_weight,
        market_filter,
        vol_filter,
        overextension_filter,
        exit_rule,
        correlation_id: args.correlation_id,
    })
}

fn build_cross_asset_robustness_matrix_request(
    args: &ResearchCrossAssetRobustnessMatrixRunArgs,
) -> anyhow::Result<CrossAssetRobustnessMatrixRequest> {
    if let Some(raw) = args.request_json.as_deref() {
        return serde_json::from_str(raw).context("invalid --request-json");
    }
    Ok(CrossAssetRobustnessMatrixRequest {
        strategy_kind: aegis_core::CrossAssetStrategyKind::RelativeStrengthContinuationV1Research,
        symbols: vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "BNBUSDT".to_string(),
        ],
        timeframe: "1h".to_string(),
        windows: Vec::new(),
        ranking_lookback_hours: vec![6, 12, 24],
        rank_metrics: vec![
            aegis_core::CrossAssetRankMetric::RawReturn,
            aegis_core::CrossAssetRankMetric::VolAdjustedReturn,
        ],
        min_top_return_pct: rust_decimal::Decimal::ZERO,
        min_rank_spread_pct: vec![
            rust_decimal::Decimal::new(2, 0),
            rust_decimal::Decimal::new(3, 0),
            rust_decimal::Decimal::new(5, 0),
        ],
        holding_hours: vec![6, 12, 24],
        fee_bps: args
            .fee_bps
            .unwrap_or_else(|| rust_decimal::Decimal::new(10, 0)),
        slippage_bps: args
            .slippage_bps
            .unwrap_or_else(|| rust_decimal::Decimal::new(5, 0)),
        one_active_position: true,
        sizing_modes: vec![
            aegis_core::CrossAssetSizingMode::EqualNotional,
            aegis_core::CrossAssetSizingMode::VolatilityNormalized,
        ],
        min_weight: rust_decimal::Decimal::new(25, 2),
        max_weight: rust_decimal::Decimal::ONE,
        market_filters: vec![
            CrossAssetMarketFilter::None,
            CrossAssetMarketFilter::Basket72hReturnGt {
                threshold_pct: rust_decimal::Decimal::new(-5, 0),
            },
            CrossAssetMarketFilter::AtLeastNSymbolsPositive24h { min_symbols: 2 },
        ],
        vol_filters: vec![
            CrossAssetVolFilter::None,
            CrossAssetVolFilter::AssetNotExtremeVsBasket {
                max_ratio: rust_decimal::Decimal::new(15, 1),
            },
        ],
        overextension_filters: vec![
            CrossAssetOverextensionFilter::None,
            CrossAssetOverextensionFilter::MaxReturn24hPct {
                max_pct: rust_decimal::Decimal::new(8, 0),
            },
            CrossAssetOverextensionFilter::MaxReturn24hPct {
                max_pct: rust_decimal::Decimal::new(10, 0),
            },
        ],
        exit_rules: vec![
            CrossAssetExitRule::FixedHold,
            CrossAssetExitRule::StopPct {
                stop_pct: rust_decimal::Decimal::new(3, 0),
            },
            CrossAssetExitRule::StopPct {
                stop_pct: rust_decimal::Decimal::new(5, 0),
            },
            CrossAssetExitRule::TakeProfitPct {
                take_profit_pct: rust_decimal::Decimal::new(5, 0),
            },
        ],
        max_configs: args.max_configs.unwrap_or(256),
        correlation_id: args.correlation_id,
    })
}

#[derive(Deserialize, Serialize)]
struct ResearchCandidateDecisionErrorResponse {
    message: String,
    rejection: ResearchCandidateDecisionRejection,
}

fn try_print_research_candidate_decision_rejection(
    error: &ApiClientError,
    json_output: bool,
) -> anyhow::Result<bool> {
    let ApiClientError::Http {
        body: Some(body), ..
    } = error
    else {
        return Ok(false);
    };
    let parsed: ResearchCandidateDecisionErrorResponse = match serde_json::from_str(body) {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    if json_output {
        output::print_json(&parsed)?;
    } else {
        output::print_research_candidate_decision_rejection(&parsed.rejection, &parsed.message);
    }
    Ok(true)
}

fn parse_hypothesis_sources(
    values: &[String],
) -> anyhow::Result<Vec<ResearchHypothesisIncludedSource>> {
    values
        .iter()
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "failure_attribution" => Ok(ResearchHypothesisIncludedSource::FailureAttribution),
            "regime_leaderboard" => Ok(ResearchHypothesisIncludedSource::RegimeLeaderboard),
            "opportunity_analysis" => Ok(ResearchHypothesisIncludedSource::OpportunityAnalysis),
            "signal_feature_attribution" => {
                Ok(ResearchHypothesisIncludedSource::SignalFeatureAttribution)
            }
            "exit_attribution" => Ok(ResearchHypothesisIncludedSource::ExitAttribution),
            other => anyhow::bail!("unsupported hypothesis source: {other}"),
        })
        .collect()
}

fn parse_hypothesis_status(value: &str) -> anyhow::Result<ResearchHypothesisStatus> {
    Ok(match value.trim().to_ascii_uppercase().as_str() {
        "PROPOSED" => ResearchHypothesisStatus::Proposed,
        "ACCEPTED_FOR_EXPERIMENT" => ResearchHypothesisStatus::AcceptedForExperiment,
        "REJECTED" => ResearchHypothesisStatus::Rejected,
        "ARCHIVED" => ResearchHypothesisStatus::Archived,
        other => anyhow::bail!("unsupported hypothesis decision: {other}"),
    })
}

fn login_required_message() -> &'static str {
    "login required: run `aegis auth login --email <EMAIL> --password <PASSWORD>` or set AEGIS_ACCESS_TOKEN"
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.validate()?;

    let config = CliConfig::from_env().context("failed to load CLI config")?;
    let client = config
        .auth
        .clone()
        .map(|session| {
            ApiClient::new(config.api_base_url.clone()).with_auth_session(
                session,
                config.token_path.clone(),
                !config.auth_from_env,
            )
        })
        .unwrap_or_else(|| ApiClient::new(config.api_base_url.clone()));

    match cli.command {
        Commands::Auth(command) => match command {
            AuthCommands::Login(args) => {
                let response = ApiClient::new(config.api_base_url.clone())
                    .auth_login(&args.email, &args.password)
                    .await?;
                let refresh_token = response
                    .refresh_token
                    .clone()
                    .context("login response did not include a CLI refresh token")?;
                let session = StoredAuthSession {
                    access_token: response.access_token.clone(),
                    refresh_token: Some(refresh_token),
                    expires_at: Some(response.expires_at),
                    user: Some(StoredUserSummary::from(&response.user)),
                    saved_at: Utc::now(),
                };
                save_token_file(&config.token_path, &session)?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_auth_login(&response.user);
                }
            }
            AuthCommands::Refresh => {
                if config.auth.is_none() {
                    anyhow::bail!("{}", login_required_message());
                }
                let response = client.auth_refresh().await.map_err(|err| {
                    if err.is_login_required() {
                        anyhow::anyhow!("{}", err)
                    } else {
                        anyhow::Error::from(err)
                    }
                })?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_auth_login(&response.user);
                }
            }
            AuthCommands::Me => {
                if config.auth.is_none() {
                    anyhow::bail!("{}", login_required_message());
                }
                let response = client.auth_me().await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_auth_me(&response.user);
                }
            }
            AuthCommands::Logout => {
                if config.auth.is_none() {
                    anyhow::bail!("{}", login_required_message());
                }
                let response = client.auth_logout().await?;
                clear_token_file(&config.token_path)?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_auth_logout();
                }
            }
        },
        Commands::Status => {
            let health = client.system_health().await?;
            let status = client.system_status().await?;
            let risk = client.risk_status().await?;
            let feed = client.market_feed_status().await?;

            if cli.json {
                output::print_json(&serde_json::json!({
                    "health": health,
                    "status": status,
                    "risk": risk,
                    "feed_status": feed,
                }))?;
            } else {
                output::print_status(&health, &status, &risk, &feed);
            }
        }
        Commands::Metrics(args) => {
            let response = client.metrics().await?;
            let filtered = if let Some(pattern) = args.grep.as_deref() {
                response
                    .lines()
                    .filter(|line| line.contains(pattern))
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                response
            };
            println!("{filtered}");
        }
        Commands::Kill { reason } => {
            let response = client.activate_kill_switch(reason).await?;
            if cli.json {
                output::print_json(&response)?;
            } else {
                output::print_risk_action(&response);
            }
        }
        Commands::Resume(args) => {
            let response = client
                .resume_trading(RESUME_CONFIRMATION_TEXT, args.reason)
                .await?;
            if cli.json {
                output::print_json(&response)?;
            } else {
                output::print_risk_action(&response);
            }
        }
        Commands::Pipeline(command) => match command {
            PipelineCommands::Run(args) => {
                let request: PaperTradingPipelineRequest = build_pipeline_request(&args);
                let response = client.run_pipeline(&request).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_pipeline_result(&response);
                }
            }
        },
        Commands::Strategy(command) => match command {
            StrategyCommands::List => {
                let response = client.list_strategies().await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_strategy_list(&response);
                }
            }
            StrategyCommands::Config(command) => match command {
                StrategyConfigCommands::Get { strategy_id } => {
                    let response = client.strategy_config(&strategy_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_status(&response);
                    }
                }
                StrategyConfigCommands::Validate(args) => {
                    let request = build_strategy_config_request(&args)?;
                    let response = client
                        .validate_strategy_config(&args.strategy_id, &request)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_config_validation(&response);
                    }
                }
                StrategyConfigCommands::Update(args) => {
                    let request = build_strategy_config_request(&args)?;
                    let response = client
                        .update_strategy_config(&args.strategy_id, &request)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_status(&response);
                    }
                }
                StrategyConfigCommands::Versions { strategy_id } => {
                    let response = client.strategy_config_versions(&strategy_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_config_versions(&response);
                    }
                }
                StrategyConfigCommands::Audit { strategy_id } => {
                    let response = client.strategy_config_audit(&strategy_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_config_audit(&response);
                    }
                }
            },
            StrategyCommands::DryRun(args) => {
                let response = client
                    .strategy_dry_run(&args.strategy_id, args.symbol, args.timeframe)
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_strategy_dry_run(&response);
                }
            }
            StrategyCommands::Diagnostics(args) => {
                let response = client
                    .strategy_diagnostics(
                        &args.strategy_id,
                        args.symbol,
                        args.timeframe,
                        args.limit,
                    )
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_strategy_diagnostics(&response);
                }
            }
            StrategyCommands::OpportunityAnalysis(args) => {
                let response = client
                    .strategy_opportunity_analysis(
                        &args.strategy_id,
                        args.symbol,
                        args.timeframe,
                        args.start_time,
                        args.end_time,
                        args.limit_samples,
                        args.include_examples,
                    )
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_strategy_opportunity_analysis(&response);
                }
            }
            StrategyCommands::ExitAttribution(args) => {
                let response = client
                    .strategy_exit_attribution(
                        &args.strategy_id,
                        args.symbol,
                        args.timeframe,
                        args.start_time,
                        args.end_time,
                        args.experiment_run_id,
                        args.holding_windows,
                        args.fee_bps,
                        args.slippage_bps,
                    )
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_strategy_exit_attribution(&response);
                }
            }
            StrategyCommands::SignalFeatureAttribution(args) => {
                let response = client
                    .strategy_signal_feature_attribution(
                        &args.strategy_id,
                        args.symbol,
                        args.timeframe,
                        args.start_time,
                        args.end_time,
                        args.experiment_run_id,
                        args.holding_window,
                        args.fee_bps,
                        args.slippage_bps,
                        args.min_samples_per_bucket,
                    )
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_strategy_signal_feature_attribution(&response);
                }
            }
            StrategyCommands::CompressionRefinement(args) => {
                let response = client
                    .compression_breakout_refinement(
                        args.symbol,
                        args.timeframe,
                        args.start_time,
                        args.end_time,
                        args.fee_bps,
                        args.slippage_bps,
                        args.max_configs,
                        args.holding_windows,
                    )
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_compression_breakout_refinement(&response);
                }
            }
            StrategyCommands::Enable { strategy_id } => {
                let response = client.enable_strategy(&strategy_id).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_strategy_status(&response);
                }
            }
            StrategyCommands::Disable { strategy_id } => {
                let response = client.disable_strategy(&strategy_id).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_strategy_status(&response);
                }
            }
        },
        Commands::Orders(command) => match command {
            OrderCommands::List(args) => {
                let response = client.list_orders().await?;
                let orders = response
                    .orders
                    .into_iter()
                    .take(args.limit)
                    .collect::<Vec<_>>();
                if cli.json {
                    output::print_json(&serde_json::json!({
                        "orders": orders,
                        "request_id": response.request_id,
                        "correlation_id": response.correlation_id,
                        "timestamp": response.timestamp,
                    }))?;
                } else {
                    output::print_orders(&orders);
                }
            }
            OrderCommands::Get { order_id } => {
                let response = client.get_order(order_id).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_order_detail(&response.order);
                }
            }
        },
        Commands::Events(command) => match command {
            EventsCommands::List(args) => {
                let response = client
                    .recent_events(&RecentEventsQuery {
                        limit: args.limit,
                        event_type: args.event_type,
                        source: args.source,
                        correlation_id: args.correlation_id,
                    })
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_events(&response);
                }
            }
        },
        Commands::Risk(command) => match command {
            RiskCommands::Config(command) => match command {
                RiskConfigCommands::Get => {
                    let response = client.risk_config().await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_risk_config(&response);
                    }
                }
                RiskConfigCommands::Validate(args) => {
                    let request = build_risk_config_request(&args)?;
                    let response = client.validate_risk_config(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_risk_config_validation(&response);
                    }
                }
                RiskConfigCommands::Update(args) => {
                    let request = build_risk_config_request(&args)?;
                    let response = client.update_risk_config(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_risk_config(&response);
                    }
                }
                RiskConfigCommands::Versions => {
                    let response = client.risk_config_versions().await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_risk_config_versions(&response);
                    }
                }
                RiskConfigCommands::Audit => {
                    let response = client.risk_config_audit().await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_risk_config_audit(&response);
                    }
                }
            },
            RiskCommands::Decisions(args) => {
                let response = client
                    .risk_decisions(&RiskDecisionsQuery {
                        limit: args.limit,
                        symbol: args.symbol,
                    })
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_risk_decisions(&response);
                }
            }
        },
        Commands::Market(command) => match command {
            MarketCommands::Backfill(args) => {
                let request = build_candle_backfill_request(&args)?;
                let response = client.backfill_candles(&request).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_backfill_result(&response);
                }
            }
            MarketCommands::Backfills(args) => {
                let response = client.list_backfill_runs(args.limit).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_backfill_runs(&response);
                }
            }
            MarketCommands::BackfillGet { run_id } => {
                let response = client.get_backfill_run(run_id).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_backfill_run(&response);
                }
            }
            MarketCommands::RepairPlan(args) => {
                let request = build_market_data_repair_plan_request(
                    &args,
                    aegis_core::MarketDataRepairMode::PlanOnly,
                )?;
                let response = client.repair_plan(&request).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_market_data_repair_plan(&response.plan);
                }
            }
            MarketCommands::RepairRun(args) => {
                let request = build_market_data_repair_plan_request(
                    &args,
                    aegis_core::MarketDataRepairMode::Repair,
                )?;
                let response = client
                    .repair_run(&aegis_core::MarketDataRepairRunRequest { plan: request })
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_market_data_repair_run(&response.run);
                }
            }
            MarketCommands::RepairRuns(args) => {
                let response = client.list_repair_runs(args.limit).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_market_data_repair_runs(&response);
                }
            }
            MarketCommands::RepairGet { run_id } => {
                let response = client.get_repair_run(run_id).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_market_data_repair_run(&response.run);
                }
            }
            MarketCommands::ProviderHealth(args) => {
                let response = client.market_provider_health(&args.provider).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_provider_health(&response);
                }
            }
            MarketCommands::AggregateCandles(args) => {
                let request = build_candle_aggregation_request(&args)?;
                let response = client.aggregate_candles(&request).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_candle_aggregation_result(&response);
                }
            }
            MarketCommands::AggregationStatus => {
                let response = client.candle_aggregation_status().await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_candle_aggregation_status(&response.rows);
                }
            }
            MarketCommands::CandleCoverage(args) => {
                let response = client.candle_coverage(&args.symbol).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_candle_coverage(&response.coverage);
                }
            }
            MarketCommands::CandleQuality(args) => {
                let query = build_market_data_quality_query(&args);
                let response = client.candle_quality(&query).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_market_data_quality_report(&response.report);
                }
            }
        },
        Commands::Research(command) => match command {
            ResearchCommands::StateSnapshot => {
                let response = client.get_research_state_snapshot().await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_research_state_snapshot(&response);
                }
            }
            ResearchCommands::Data(command) => match command {
                ResearchDataCommands::Coverage(args) => {
                    let query = build_research_data_coverage_query(&args);
                    let response = client.get_research_data_coverage(&query).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_data_coverage(&response.coverage);
                    }
                }
                ResearchDataCommands::Build(args) => {
                    let request = build_research_data_build_request(&args);
                    let response = client.build_research_dataset(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_dataset_build(&response.build);
                    }
                }
                ResearchDataCommands::Builds(args) => {
                    let response = client.list_research_dataset_builds(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_dataset_builds(&response.builds);
                    }
                }
                ResearchDataCommands::BuildGet { build_id } => {
                    let response = client.get_research_dataset_build(build_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_dataset_build(&response.build);
                    }
                }
            },
            ResearchCommands::RegimeDatasets(command) => match command {
                ResearchRegimeDatasetCommands::Build(args) => {
                    let request = build_research_regime_dataset_request(&args)?;
                    let response = client.build_research_regime_dataset(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_dataset(&response.dataset);
                    }
                }
                ResearchRegimeDatasetCommands::FromDiscovery(args) => {
                    let request = build_research_regime_dataset_from_discovery_request(&args)?;
                    let response = client
                        .build_research_regime_dataset_from_discovery(&request)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_dataset(&response.dataset);
                    }
                }
                ResearchRegimeDatasetCommands::List(args) => {
                    let response = client.list_research_regime_datasets(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_datasets(&response.datasets);
                    }
                }
                ResearchRegimeDatasetCommands::Get { dataset_id } => {
                    let response = client.get_research_regime_dataset(dataset_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_dataset(&response.dataset);
                    }
                }
                ResearchRegimeDatasetCommands::Windows { dataset_id } => {
                    let response = client
                        .get_research_regime_dataset_windows(dataset_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_windows(&response.windows);
                    }
                }
            },
            ResearchCommands::RegimeDiscovery(command) => match command {
                ResearchRegimeDiscoveryCommands::Run(args) => {
                    let request = build_research_regime_discovery_request(&args)?;
                    let response = client.run_research_regime_discovery(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_discovery(&response.discovery);
                    }
                }
                ResearchRegimeDiscoveryCommands::List(args) => {
                    let response = client.list_research_regime_discoveries(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_discoveries(&response.discoveries);
                    }
                }
                ResearchRegimeDiscoveryCommands::Get { discovery_id } => {
                    let response = client.get_research_regime_discovery(discovery_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_discovery(&response.discovery);
                    }
                }
                ResearchRegimeDiscoveryCommands::Windows { discovery_id } => {
                    let response = client
                        .get_research_regime_discovery_windows(discovery_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_discovery_windows(&response.windows);
                    }
                }
            },
            ResearchCommands::RegimeCalibration(command) => match command {
                ResearchRegimeCalibrationCommands::Run(args) => {
                    let request = build_research_regime_calibration_request(&args)?;
                    let response = client.run_research_regime_calibration(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_calibration(&response.calibration);
                    }
                }
                ResearchRegimeCalibrationCommands::List(args) => {
                    let response = client.list_research_regime_calibrations(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        for calibration in &response.calibrations {
                            output::print_research_regime_calibration(calibration);
                        }
                    }
                }
                ResearchRegimeCalibrationCommands::Get { calibration_id } => {
                    let response = client
                        .get_research_regime_calibration(calibration_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_calibration(&response.calibration);
                    }
                }
                ResearchRegimeCalibrationCommands::Candidates { calibration_id } => {
                    let response = client
                        .get_research_regime_calibration_candidates(calibration_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_calibration_candidates(&response.candidates);
                    }
                }
            },
            ResearchCommands::Campaigns(command) => match command {
                ResearchCampaignCommands::Run(args) => {
                    let request = build_research_campaign_request(&args)?;
                    let response = client.run_research_campaign(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_campaign(&response.campaign);
                    }
                }
                ResearchCampaignCommands::List(args) => {
                    let response = client.list_research_campaigns(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_campaigns(&response.campaigns);
                    }
                }
                ResearchCampaignCommands::Get { campaign_id } => {
                    let response = client.get_research_campaign(campaign_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_campaign(&response.campaign);
                    }
                }
                ResearchCampaignCommands::Batches { campaign_id } => {
                    let response = client.get_research_campaign_batches(campaign_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_campaign_batches(&response.batches);
                    }
                }
                ResearchCampaignCommands::Summary { campaign_id } => {
                    let response = client.get_research_campaign_summary(campaign_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_campaign_summary(&response.summary);
                    }
                }
                ResearchCampaignCommands::FailureAttribution { campaign_id } => {
                    let response = client
                        .get_research_campaign_failure_attribution(campaign_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_campaign_failure_attribution(&response.attribution);
                    }
                }
                ResearchCampaignCommands::RegimeLeaderboard { campaign_id } => {
                    let response = client
                        .get_research_campaign_regime_leaderboard(campaign_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_regime_strategy_leaderboard(&response.leaderboard);
                    }
                }
            },
            ResearchCommands::Batches(command) => match command {
                ResearchBatchCommands::Run(args) => {
                    let request = build_research_batch_request(&args)?;
                    let response = client.run_research_batch(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_batch(&response.batch);
                    }
                }
                ResearchBatchCommands::List(args) => {
                    let response = client.list_research_batches(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_batches(&response.batches);
                    }
                }
                ResearchBatchCommands::Get { batch_id } => {
                    let response = client.get_research_batch(batch_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_batch(&response.batch);
                    }
                }
                ResearchBatchCommands::Steps { batch_id } => {
                    let response = client.get_research_batch_steps(batch_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_batch_steps(&response.steps);
                    }
                }
                ResearchBatchCommands::Triage { batch_id } => {
                    let response = client.get_research_batch_triage(batch_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_batch_triage(&response.triage);
                    }
                }
            },
            ResearchCommands::ScheduledJobs(command) => match command {
                ResearchScheduledJobCommands::List(args) => {
                    let response = client.list_scheduled_research_jobs(args.limit).await?;
                    output::print_json(&response)?;
                }
                ResearchScheduledJobCommands::Get { id } => {
                    let response = client.get_scheduled_research_job(id).await?;
                    output::print_json(&response)?;
                }
                ResearchScheduledJobCommands::Create(args) => {
                    let request = (&args).into();
                    let response = client.create_scheduled_research_job(&request).await?;
                    output::print_json(&response)?;
                }
                ResearchScheduledJobCommands::Pause { id } => {
                    let response = client.pause_scheduled_research_job(id).await?;
                    output::print_json(&response)?;
                }
                ResearchScheduledJobCommands::Resume { id } => {
                    let response = client.resume_scheduled_research_job(id).await?;
                    output::print_json(&response)?;
                }
                ResearchScheduledJobCommands::Runs { id, limit } => {
                    let response = client.list_scheduled_research_job_runs(id, limit).await?;
                    output::print_json(&response)?;
                }
                ResearchScheduledJobCommands::RunOnce { id } => {
                    let response = client.run_once_scheduled_research_job(id).await?;
                    output::print_json(&response)?;
                }
                ResearchScheduledJobCommands::ResetFailures { id } => {
                    let response = client.reset_scheduled_research_job_failures(id).await?;
                    output::print_json(&response)?;
                }
                ResearchScheduledJobCommands::BootstrapSafe(args) => {
                    let request = (&args).into();
                    let response = client
                        .bootstrap_safe_scheduled_research_jobs(&request)
                        .await?;
                    output::print_json(&response)?;
                }
            },
            ResearchCommands::StaleRuns(command) => match command {
                ResearchStaleRunCommands::RecoverPreview(args) => {
                    let request = ResearchStaleRunRecoveryRequest {
                        older_than_minutes: args.older_than_minutes,
                        dry_run: true,
                        target_types: if args.target_types.is_empty() {
                            None
                        } else {
                            Some(args.target_types.clone())
                        },
                        limit: args.limit,
                        correlation_id: args.correlation_id,
                        confirmation: None,
                    };
                    let response = client.recover_stale_research_runs_preview(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_stale_run_recovery(&response.result);
                    }
                }
                ResearchStaleRunCommands::Recover(args) => {
                    let request = ResearchStaleRunRecoveryRequest {
                        older_than_minutes: args.older_than_minutes,
                        dry_run: false,
                        target_types: if args.target_types.is_empty() {
                            None
                        } else {
                            Some(args.target_types.clone())
                        },
                        limit: args.limit,
                        correlation_id: args.correlation_id,
                        confirmation: args.confirm.clone(),
                    };
                    let response = client.recover_stale_research_runs(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_stale_run_recovery(&response.result);
                    }
                }
            },
            ResearchCommands::Candidates(command) => match command {
                ResearchCandidateCommands::List(args) => {
                    let response = client
                        .list_research_candidates(&ResearchCandidatesQuery {
                            strategy_id: args.strategy_id,
                            symbol: args.symbol,
                            timeframe: args.timeframe,
                            status: args.status,
                            limit: args.limit,
                        })
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidates(&response.candidates);
                    }
                }
                ResearchCandidateCommands::Watchlist(args) => {
                    let response = client.get_research_candidate_watchlist(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_watchlist(&response.watchlist);
                    }
                }
                ResearchCandidateCommands::Get { candidate_id } => {
                    let response = client.get_research_candidate(candidate_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate(&response.candidate);
                        output::print_research_candidate_evidence_provenance(
                            response.evidence_provenance.as_ref(),
                        );
                    }
                }
                ResearchCandidateCommands::ExportBundle(args) => {
                    let response = client
                        .export_research_candidate_bundle(args.candidate_id)
                        .await?;
                    let bytes = serde_json::to_vec_pretty(&response.bundle)?;
                    fs::write(&args.output, bytes).with_context(|| {
                        format!("failed to write bundle to {}", args.output.display())
                    })?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        println!(
                            "exported research candidate bundle: {}",
                            args.output.display()
                        );
                        println!(
                            "bundle_fingerprint: {}",
                            response.bundle.integrity.bundle_fingerprint
                        );
                    }
                }
                ResearchCandidateCommands::ImportBundlePreview(args) => {
                    let bytes = fs::read(&args.file).with_context(|| {
                        format!("failed to read bundle from {}", args.file.display())
                    })?;
                    let bundle: ResearchCandidateEvidenceBundle = serde_json::from_slice(&bytes)
                        .with_context(|| {
                            format!("failed to parse bundle JSON from {}", args.file.display())
                        })?;
                    let response = client
                        .preview_research_candidate_import_bundle(
                            &ResearchCandidateImportBundlePreviewRequest { bundle },
                        )
                        .await?;
                    output::print_json(&response)?;
                }
                ResearchCandidateCommands::ImportBundle(args) => {
                    let bytes = fs::read(&args.file).with_context(|| {
                        format!("failed to read bundle from {}", args.file.display())
                    })?;
                    let bundle: ResearchCandidateEvidenceBundle = serde_json::from_slice(&bytes)
                        .with_context(|| {
                            format!("failed to parse bundle JSON from {}", args.file.display())
                        })?;
                    let response = client
                        .import_research_candidate_bundle(&ResearchCandidateImportBundleRequest {
                            bundle,
                            confirm: args.confirm,
                            correlation_id: None,
                        })
                        .await?;
                    output::print_json(&response)?;
                }
                ResearchCandidateCommands::RecordReconciliation(args) => {
                    let response = client
                        .record_research_candidate_import_reconciliation(
                            args.import_id,
                            &ResearchCandidateImportReconciliationRequest {
                                reconciliation_status: args.status,
                                local_validation_window_start: args.local_validation_window_start,
                                local_validation_window_end: args.local_validation_window_end,
                                local_walk_forward_status: args.local_walk_forward_status,
                                local_worst_window_pnl: args.local_worst_window_pnl,
                                local_recommendation: args.local_recommendation,
                                reconciliation_summary_json: args.summary_json,
                                recommended_next_action: args.recommended_next_action,
                                confirm: args.confirm,
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if response.result.candidate_id != args.candidate_id {
                        anyhow::bail!(
                            "import {} belongs to candidate {}, not {}",
                            args.import_id,
                            response.result.candidate_id,
                            args.candidate_id
                        );
                    }
                    output::print_json(&response)?;
                }
                ResearchCandidateCommands::Events { candidate_id } => {
                    let response = client.list_research_candidate_events(candidate_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_events(&response.events);
                    }
                }
                ResearchCandidateCommands::Reviews { candidate_id } => {
                    let response = client.list_research_candidate_reviews(candidate_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_reviews(&response.reviews);
                    }
                }
                ResearchCandidateCommands::Observations { candidate_id } => {
                    let response = client
                        .list_research_candidate_observations(candidate_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_observations(&response.history);
                    }
                }
                ResearchCandidateCommands::ObservationSummary { candidate_id } => {
                    let response = client
                        .get_research_candidate_observation_summary(candidate_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_observation_summary(&response.summary);
                    }
                }
                ResearchCandidateCommands::Qualification(args) => {
                    let response = client
                        .get_research_candidate_qualification(args.candidate_id, &args.thresholds())
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_qualification(&response.qualification);
                    }
                }
                ResearchCandidateCommands::QualificationEvaluate(args) => {
                    let response = client
                        .evaluate_research_candidate_qualification(
                            args.candidate_id,
                            &args.thresholds(),
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_qualification_evaluation(
                            &response.evaluation,
                            response.change.as_ref(),
                            response.trend,
                        );
                    }
                }
                ResearchCandidateCommands::QualificationHistory(args) => {
                    let response = client
                        .get_research_candidate_qualification_history(args.candidate_id, args.limit)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_qualification_history(&response.history);
                    }
                }
                ResearchCandidateCommands::TestnetReviewDossier { candidate_id } => {
                    let response = client
                        .get_research_candidate_testnet_review_dossier(candidate_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_testnet_review_dossier(&response.dossier);
                    }
                }
                ResearchCandidateCommands::AcceptShadowPreview { candidate_id } => {
                    let response = client
                        .get_research_candidate_accept_shadow_preview(candidate_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_accept_shadow_preview(&response.preview);
                    }
                }
                ResearchCandidateCommands::AcceptShadowApply(args) => {
                    let request = (&args).into();
                    let response = client
                        .apply_research_candidate_accept_shadow(args.candidate_id, &request)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_accept_shadow_apply(&response.result);
                    }
                }
                ResearchCandidateCommands::WalkForward { candidate_id } => {
                    let response = client
                        .get_research_candidate_walk_forward(candidate_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_walk_forward_evidence(
                            response.latest.as_ref(),
                            &response.evidence,
                        );
                    }
                }
                ResearchCandidateCommands::LinkWalkForward(args) => {
                    let response = client
                        .link_research_candidate_walk_forward(
                            args.candidate_id,
                            args.walk_forward_run_id,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_walk_forward_evidence(
                            response.latest.as_ref(),
                            &response.evidence,
                        );
                    }
                }
                ResearchCandidateCommands::ShadowPerformance(args) => {
                    let response = client
                        .get_research_candidate_shadow_performance(
                            args.candidate_id,
                            args.start_time,
                            args.end_time,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_shadow_performance(&response.performance);
                    }
                }
                ResearchCandidateCommands::ShadowPnl(args) => {
                    let response = client
                        .get_research_candidate_shadow_pnl_attribution(
                            args.candidate_id,
                            &args.holding_windows,
                            args.fee_bps,
                            args.slippage_bps,
                            args.extreme_pnl_threshold_pct,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_shadow_pnl_attribution(
                            &response.attribution,
                        );
                    }
                }
                ResearchCandidateCommands::ShadowRuns(args) => {
                    let response = client
                        .list_research_candidate_shadow_runs(
                            args.candidate_id,
                            args.start_time,
                            args.end_time,
                            args.limit,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_shadow_runs(&response.runs);
                    }
                }
                ResearchCandidateCommands::Create(args) => {
                    let config = serde_json::from_str(&args.config_json)
                        .context("failed to parse --config-json as JSON")?;
                    let response = client
                        .create_research_candidate(&CreateResearchCandidateRequest {
                            strategy_id: args.strategy_id,
                            symbol: args.symbol,
                            timeframe: args.timeframe,
                            config,
                            notes: args.notes,
                            correlation_id: None,
                        })
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate(&response.candidate);
                        output::print_research_candidate_evidence_provenance(
                            response.evidence_provenance.as_ref(),
                        );
                    }
                }
                ResearchCandidateCommands::FromExperimentRun(args) => {
                    let response = client
                        .create_research_candidate_from_experiment_run(
                            &CreateResearchCandidateFromExperimentRunRequest {
                                experiment_run_id: args.run_id,
                                walk_forward_run_id: args.walk_forward_run_id,
                                notes: args.notes,
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate(&response.candidate);
                        output::print_research_candidate_evidence_provenance(
                            response.evidence_provenance.as_ref(),
                        );
                    }
                }
                ResearchCandidateCommands::Observe { candidate_id } => {
                    let response = client.observe_research_candidate(candidate_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_observation(&response.observation);
                    }
                }
                ResearchCandidateCommands::ShadowObserveOnce(args) => {
                    let response = client
                        .shadow_observe_research_candidate_once(
                            args.candidate_id,
                            &aegis_core::ResearchCandidateShadowObserveOnceRequest {
                                allow_duplicate_operational_check: args
                                    .allow_duplicate_operational_check,
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_shadow_observe_once(&response.result);
                    }
                }
                ResearchCandidateCommands::Review(args) => {
                    let response = client
                        .create_research_candidate_review(
                            args.candidate_id,
                            &ResearchCandidateReviewRequest {
                                action: args.action,
                                reason: args.reason,
                                notes: args.notes,
                                qualification_evaluation_id: args.qualification_evaluation_id,
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_review_result(&response.result);
                    }
                }
                ResearchCandidateCommands::Decide(args) => {
                    let response = match client
                        .decide_research_candidate(
                            args.candidate_id,
                            &ResearchCandidateDecisionRequest {
                                decision: args.decision,
                                reason: args.reason,
                                notes: args.notes,
                                acknowledge_runner_mismatch: args.acknowledge_runner_mismatch,
                                acknowledge_overfit_risk: args.acknowledge_overfit_risk,
                                correlation_id: None,
                            },
                        )
                        .await
                    {
                        Ok(value) => value,
                        Err(err)
                            if try_print_research_candidate_decision_rejection(&err, cli.json)? =>
                        {
                            return Ok(());
                        }
                        Err(err) => return Err(err.into()),
                    };
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate(&response.candidate);
                        output::print_research_candidate_evidence_provenance(
                            response.evidence_provenance.as_ref(),
                        );
                    }
                }
                ResearchCandidateCommands::PromoteShadowPreview(args) => {
                    let request = (&args).into();
                    let response = client
                        .preview_research_candidate_shadow_promotion(args.candidate_id, &request)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_shadow_promotion_preview(
                            &response.preview,
                        );
                    }
                }
                ResearchCandidateCommands::PromoteShadowApply(args) => {
                    let request = (&args).into();
                    let response = client
                        .apply_research_candidate_shadow_promotion(args.candidate_id, &request)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_candidate_shadow_promotion_result(&response.result);
                    }
                }
            },
            ResearchCommands::Hypotheses(command) => match command {
                ResearchHypothesisCommands::Generate(args) => {
                    let request = ResearchHypothesisGenerationRequest {
                        campaign_id: args.campaign_id,
                        batch_id: args.batch_id,
                        candidate_id: args.candidate_id,
                        include_sources: parse_hypothesis_sources(&args.include_sources)?,
                        persist: !args.no_persist,
                    };
                    let response = client.generate_research_hypotheses(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_hypothesis_generation(&response.result);
                    }
                }
                ResearchHypothesisCommands::List(args) => {
                    let response = client.list_research_hypotheses(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_hypotheses(&response.hypotheses);
                    }
                }
                ResearchHypothesisCommands::Get { hypothesis_id } => {
                    let response = client.get_research_hypothesis(hypothesis_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_hypothesis(&response.hypothesis);
                    }
                }
                ResearchHypothesisCommands::Decide(args) => {
                    let decision = parse_hypothesis_status(&args.decision)?;
                    let response = client
                        .decide_research_hypothesis(
                            args.hypothesis_id,
                            decision,
                            args.reason.clone(),
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_hypothesis(&response.hypothesis);
                    }
                }
                ResearchHypothesisCommands::Plan { hypothesis_id } => {
                    let response = client
                        .create_research_experiment_plan(hypothesis_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_experiment_plan(&response.plan);
                    }
                }
            },
            ResearchCommands::ExperimentPlans(command) => match command {
                ResearchExperimentPlanCommands::List(args) => {
                    let response = client.list_research_experiment_plans(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_experiment_plans(&response.plans);
                    }
                }
                ResearchExperimentPlanCommands::Get { plan_id } => {
                    let response = client.get_research_experiment_plan(plan_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_experiment_plan(&response.plan);
                    }
                }
                ResearchExperimentPlanCommands::Validate { plan_id } => {
                    let response = client.validate_research_experiment_plan(plan_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_experiment_plan(&response.plan);
                    }
                }
                ResearchExperimentPlanCommands::RunPreview { plan_id } => {
                    let response = client.preview_research_experiment_plan_run(plan_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_experiment_plan_run(&response.result);
                    }
                }
                ResearchExperimentPlanCommands::Run(args) => {
                    let response = client
                        .run_research_experiment_plan(
                            args.plan_id,
                            &ResearchExperimentPlanRunRequest {
                                mode: ResearchExperimentPlanRunMode::Run,
                                confirmation: Some(args.confirm),
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_experiment_plan_run(&response.result);
                    }
                }
                ResearchExperimentPlanCommands::Archive(args) => {
                    let response = client
                        .archive_research_experiment_plan(args.plan_id, args.reason.clone())
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_research_experiment_plan(&response.plan);
                    }
                }
            },
            ResearchCommands::RobustnessMatrix(command) => match command {
                ResearchRobustnessMatrixCommands::Run(args) => {
                    let request = build_research_robustness_matrix_request(&args)?;
                    let response = client.run_strategy_robustness_matrix(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_robustness_matrix(&response.matrix);
                        output::print_strategy_robustness_matrix_cells(&response.cells);
                    }
                }
                ResearchRobustnessMatrixCommands::List(args) => {
                    let response = client.strategy_robustness_matrix_runs(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_robustness_matrices(&response.matrices);
                    }
                }
                ResearchRobustnessMatrixCommands::Get { run_id } => {
                    let response = client.strategy_robustness_matrix_run(run_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_robustness_matrix(&response.matrix);
                    }
                }
                ResearchRobustnessMatrixCommands::Cells { run_id } => {
                    let response = client.strategy_robustness_matrix_cells(run_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_robustness_matrix_cells(&response.cells);
                    }
                }
            },
            ResearchCommands::CrossAsset(command) => match command {
                ResearchCrossAssetCommands::Run(args) => {
                    let request = build_cross_asset_research_request(&args)?;
                    let response = client.run_cross_asset_research(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_cross_asset_research_run(&response.run);
                    }
                }
                ResearchCrossAssetCommands::RunRelativeStrengthV1(args) => {
                    let request = relative_strength_continuation_v1_default_request(
                        args.start_time,
                        args.end_time,
                        args.correlation_id,
                    );
                    let response = client.run_cross_asset_research(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_cross_asset_research_run(&response.run);
                    }
                }
                ResearchCrossAssetCommands::List(args) => {
                    let response = client.list_cross_asset_research_runs(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        for run in &response.runs {
                            output::print_cross_asset_research_run(run);
                        }
                    }
                }
                ResearchCrossAssetCommands::Get { run_id } => {
                    let response = client.get_cross_asset_research_run(run_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_cross_asset_research_run(&response.run);
                    }
                }
                ResearchCrossAssetCommands::Trades { run_id } => {
                    let response = client.list_cross_asset_research_trades(run_id).await?;
                    output::print_json(&response)?;
                }
                ResearchCrossAssetCommands::Windows { run_id } => {
                    let response = client.list_cross_asset_research_windows(run_id).await?;
                    output::print_json(&response)?;
                }
                ResearchCrossAssetCommands::RelativeStrengthV1(command) => match command {
                    cli::cli::ResearchCrossAssetRelativeStrengthV1Commands::Dossier => {
                        let response = client
                            .get_cross_asset_relative_strength_v1_dossier()
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_cross_asset_relative_strength_v1_dossier(
                                &response.dossier,
                            );
                        }
                    }
                    cli::cli::ResearchCrossAssetRelativeStrengthV1Commands::CandidateGatePreview => {
                        let response = client
                            .get_cross_asset_relative_strength_v1_candidate_gate_preview()
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_cross_asset_candidate_gate_preview(&response.preview);
                        }
                    }
                },
                ResearchCrossAssetCommands::RobustnessMatrix(command) => match command {
                    ResearchCrossAssetRobustnessMatrixCommands::Run(args) => {
                        let request = build_cross_asset_robustness_matrix_request(&args)?;
                        let response = client.run_cross_asset_robustness_matrix(&request).await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_cross_asset_robustness_matrix(&response.matrix);
                        }
                    }
                    ResearchCrossAssetRobustnessMatrixCommands::RunRelativeStrengthV1(args) => {
                        let request = if let Some(raw) = args.request_json.as_deref() {
                            serde_json::from_str(raw).context("invalid --request-json")?
                        } else {
                            relative_strength_continuation_v1_default_robustness_matrix_request(
                                Vec::new(),
                                args.correlation_id,
                            )
                        };
                        let response = client.run_cross_asset_robustness_matrix(&request).await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_cross_asset_robustness_matrix(&response.matrix);
                        }
                    }
                    ResearchCrossAssetRobustnessMatrixCommands::List(args) => {
                        let response = client
                            .list_cross_asset_robustness_matrix_runs(args.limit)
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            for matrix in &response.matrices {
                                output::print_cross_asset_robustness_matrix(matrix);
                            }
                        }
                    }
                    ResearchCrossAssetRobustnessMatrixCommands::Get { run_id } => {
                        let response = client.get_cross_asset_robustness_matrix_run(run_id).await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_cross_asset_robustness_matrix(&response.matrix);
                        }
                    }
                    ResearchCrossAssetRobustnessMatrixCommands::Cells { run_id } => {
                        let response = client
                            .list_cross_asset_robustness_matrix_cells(run_id)
                            .await?;
                        output::print_json(&response)?;
                    }
                },
            },
        },
        Commands::Backtest(command) => match command {
            BacktestCommands::Run(args) => {
                let request = build_backtest_request(&args)?;
                let response = client.run_backtest(&request).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_backtest_accepted(&response);
                }
            }
            BacktestCommands::List(args) => {
                let response = client.backtest_runs(args.limit).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_backtest_runs(&response.runs);
                }
            }
            BacktestCommands::Get { run_id } => {
                let response = client.backtest_run(run_id).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_backtest_run(&response.run);
                }
            }
        },
        Commands::Experiments(command) => match command {
            ExperimentCommands::Strategy(command) => match command {
                StrategyExperimentCommands::Run(args) => {
                    let request = build_strategy_experiment_request(&args)?;
                    let response = client.run_strategy_experiment(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_experiment(&response.experiment);
                        output::print_strategy_experiment_runs(&response.runs);
                    }
                }
                StrategyExperimentCommands::MultiTimeframe(args) => {
                    let request = build_multi_timeframe_strategy_experiment_request(&args)?;
                    let response = client
                        .run_multi_timeframe_strategy_experiment(&request)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_multi_timeframe_strategy_experiment(&response.comparison);
                    }
                }
                StrategyExperimentCommands::WalkForward(args) => {
                    let request = build_strategy_walk_forward_request(&args)?;
                    let response = client.run_strategy_walk_forward(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_walk_forward(&response.walk_forward);
                        output::print_strategy_walk_forward_windows(&response.windows);
                    }
                }
                StrategyExperimentCommands::WalkForwardList(args) => {
                    let response = client.strategy_walk_forward_runs(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_walk_forward_runs(&response.walk_forwards);
                    }
                }
                StrategyExperimentCommands::WalkForwardGet { walk_forward_id } => {
                    let response = client.strategy_walk_forward_run(walk_forward_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_walk_forward(&response.walk_forward);
                    }
                }
                StrategyExperimentCommands::WalkForwardWindows { walk_forward_id } => {
                    let response = client
                        .strategy_walk_forward_windows(walk_forward_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_walk_forward_windows(&response.windows);
                    }
                }
                StrategyExperimentCommands::List(args) => {
                    let response = client.strategy_experiments(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_experiments(&response.experiments);
                    }
                }
                StrategyExperimentCommands::Get { experiment_id } => {
                    let response = client.strategy_experiment(experiment_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_experiment(&response.experiment);
                    }
                }
                StrategyExperimentCommands::Runs { experiment_id } => {
                    let response = client.strategy_experiment_runs(experiment_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_experiment_runs(&response.runs);
                    }
                }
            },
        },
        Commands::Exchange(command) => match command {
            ExchangeCommands::Testnet(command) => match command {
                ExchangeTestnetCommands::Status => {
                    let response = client.exchange_testnet_status().await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_status(&response);
                    }
                }
                ExchangeTestnetCommands::PrivateStream(command) => match command {
                    ExchangeTestnetPrivateStreamCommands::Status => {
                        let response = client.exchange_testnet_private_stream_status().await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_exchange_private_stream_status(&response);
                        }
                    }
                    ExchangeTestnetPrivateStreamCommands::Events(args) => {
                        let response = client
                            .exchange_testnet_private_stream_events(
                                args.limit,
                                args.client_order_id,
                                args.event_type,
                            )
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_exchange_private_stream_events(&response.events);
                        }
                    }
                    ExchangeTestnetPrivateStreamCommands::ListenKey => {
                        let response = client.exchange_testnet_private_stream_listen_key().await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_exchange_private_stream_listen_key(&response);
                        }
                    }
                    ExchangeTestnetPrivateStreamCommands::Keepalive(args) => {
                        let response = client
                            .exchange_testnet_private_stream_keepalive(&args.listen_key)
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_exchange_private_stream_listen_key(&response);
                        }
                    }
                    ExchangeTestnetPrivateStreamCommands::Close(args) => {
                        let response = client
                            .exchange_testnet_private_stream_close(&args.listen_key)
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_exchange_private_stream_listen_key(&response);
                        }
                    }
                },
                ExchangeTestnetCommands::Symbols => {
                    let response = client.exchange_testnet_symbols().await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_symbols(&response);
                    }
                }
                ExchangeTestnetCommands::PipelinePreview(args) => {
                    let response = client
                        .exchange_testnet_pipeline_preview(
                            &aegis_core::ExchangeTestnetPipelinePreviewRequest {
                                risk_decision_id: args.risk_decision_id,
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_pipeline_preview(&response.preview);
                    }
                }
                ExchangeTestnetCommands::PipelineSubmit(args) => {
                    let response = client
                        .exchange_testnet_pipeline_submit(
                            &aegis_core::ExchangeTestnetPipelineSubmitRequest {
                                risk_decision_id: args.risk_decision_id,
                                confirmation_text: args.confirm.expect("validated confirm"),
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_pipeline_submit(&response);
                    }
                }
                ExchangeTestnetCommands::ShadowRun(args) => {
                    let response = client.exchange_testnet_shadow_run(&args.into()).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_shadow_run(&response.run);
                    }
                }
                ExchangeTestnetCommands::ShadowRuns(args) => {
                    let response = client.exchange_testnet_shadow_runs(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_shadow_runs(&response);
                    }
                }
                ExchangeTestnetCommands::ShadowGet { run_id } => {
                    let response = client.exchange_testnet_shadow_get(run_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_shadow_run(&response.run);
                    }
                }
                ExchangeTestnetCommands::ShadowPromotionPreview(args) => {
                    let response = client
                        .exchange_testnet_shadow_promotion_preview(&args.into())
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_shadow_promotion(&response.promotion);
                    }
                }
                ExchangeTestnetCommands::ShadowPromotions(args) => {
                    let response = client
                        .exchange_testnet_shadow_promotions(args.limit)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_shadow_promotions(&response);
                    }
                }
                ExchangeTestnetCommands::ShadowPromotionGet { promotion_id } => {
                    let response = client
                        .exchange_testnet_shadow_promotion_get(promotion_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_shadow_promotion(&response.promotion);
                    }
                }
                ExchangeTestnetCommands::ShadowPromotionSubmit(args) => {
                    let response = client
                        .exchange_testnet_shadow_promotion_submit(
                            args.promotion_id,
                            &aegis_core::TestnetShadowPromotionSubmitRequest {
                                confirmation_text: args.confirm.expect("validated confirm"),
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_shadow_promotion_submit(&response.result);
                    }
                }
                ExchangeTestnetCommands::ShadowRunner(command) => match command {
                    ExchangeTestnetShadowRunnerCommands::Status => {
                        let response = client.exchange_testnet_shadow_runner_status().await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_testnet_shadow_runner_status(&response);
                        }
                    }
                    ExchangeTestnetShadowRunnerCommands::Config => {
                        let response = client.exchange_testnet_shadow_runner_config().await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_testnet_shadow_runner_config(&response.config);
                        }
                    }
                    ExchangeTestnetShadowRunnerCommands::ConfigUpdate(args) => {
                        let payload = args.try_into()?;
                        let response = client
                            .exchange_testnet_shadow_runner_config_update(&payload)
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_testnet_shadow_runner_config(&response.config);
                        }
                    }
                    ExchangeTestnetShadowRunnerCommands::RunOnce => {
                        let response = client
                            .exchange_testnet_shadow_runner_control(
                                &TestnetShadowRunnerControlRequest {
                                    action: TestnetShadowRunnerControlAction::RunOnce,
                                    correlation_id: None,
                                },
                            )
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_testnet_shadow_runner_control(&response);
                        }
                    }
                    ExchangeTestnetShadowRunnerCommands::Pause => {
                        let response = client
                            .exchange_testnet_shadow_runner_control(
                                &TestnetShadowRunnerControlRequest {
                                    action: TestnetShadowRunnerControlAction::Pause,
                                    correlation_id: None,
                                },
                            )
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_testnet_shadow_runner_control(&response);
                        }
                    }
                    ExchangeTestnetShadowRunnerCommands::Resume => {
                        let response = client
                            .exchange_testnet_shadow_runner_control(
                                &TestnetShadowRunnerControlRequest {
                                    action: TestnetShadowRunnerControlAction::Resume,
                                    correlation_id: None,
                                },
                            )
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_testnet_shadow_runner_control(&response);
                        }
                    }
                    ExchangeTestnetShadowRunnerCommands::Start => {
                        let response = client
                            .exchange_testnet_shadow_runner_control(
                                &TestnetShadowRunnerControlRequest {
                                    action: TestnetShadowRunnerControlAction::Start,
                                    correlation_id: None,
                                },
                            )
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_testnet_shadow_runner_control(&response);
                        }
                    }
                    ExchangeTestnetShadowRunnerCommands::Stop => {
                        let response = client
                            .exchange_testnet_shadow_runner_control(
                                &TestnetShadowRunnerControlRequest {
                                    action: TestnetShadowRunnerControlAction::Stop,
                                    correlation_id: None,
                                },
                            )
                            .await?;
                        if cli.json {
                            output::print_json(&response)?;
                        } else {
                            output::print_testnet_shadow_runner_control(&response);
                        }
                    }
                },
                ExchangeTestnetCommands::Balances => {
                    let response = client.exchange_testnet_balances().await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_balances(&response);
                    }
                }
                ExchangeTestnetCommands::OrderSubmit(args) => {
                    let response = client
                        .exchange_testnet_order_submit(
                            &cli::api::ExchangeTestnetOrderSubmitRequest {
                                symbol: args.symbol,
                                side: args.side,
                                order_type: args.order_type,
                                time_in_force: args.time_in_force,
                                quantity: args.quantity.map(|value| value.to_string()),
                                quote_notional: args.quote_notional.map(|value| value.to_string()),
                                limit_price: args.limit_price.map(|value| value.to_string()),
                                risk_decision_id: args.risk_decision_id,
                                confirmation_text: TESTNET_ORDER_CONFIRMATION_TEXT.to_string(),
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_order(&response);
                    }
                }
                ExchangeTestnetCommands::OrderGet { client_order_id } => {
                    let response = client.exchange_testnet_order_get(&client_order_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_order(&response);
                    }
                }
                ExchangeTestnetCommands::OrderLifecycle { client_order_id } => {
                    let response = client
                        .exchange_testnet_order_lifecycle(&client_order_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_order_lifecycle(&response);
                    }
                }
                ExchangeTestnetCommands::OrderCancel(args) => {
                    let response = client
                        .exchange_testnet_order_cancel(
                            &args.client_order_id,
                            TESTNET_ORDER_CONFIRMATION_TEXT,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_order(&response);
                    }
                }
                ExchangeTestnetCommands::OrderRepair(args) => {
                    let response = client
                        .exchange_testnet_order_repair(
                            &args.client_order_id,
                            &cli::api::ExchangeTestnetOrderRepairRequest {
                                action: args.action,
                                confirmation_text: args.confirm.unwrap_or_default(),
                                reason: args.reason,
                                force: args.force,
                                correlation_id: None,
                            },
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_repair(&response);
                    }
                }
                ExchangeTestnetCommands::OrderRepairs { client_order_id } => {
                    let response = client
                        .exchange_testnet_order_repairs(&client_order_id)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_testnet_repairs(&response.repairs);
                    }
                }
                ExchangeTestnetCommands::Reconcile(args) => {
                    let response = client
                        .exchange_testnet_reconcile(&cli::api::ExchangeTestnetReconcileRequest {
                            limit: Some(args.limit),
                            status_filter: if args.status_filter.is_empty() {
                                None
                            } else {
                                Some(args.status_filter)
                            },
                            correlation_id: None,
                        })
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_reconciliation_result(&response.result);
                    }
                }
                ExchangeTestnetCommands::ReconciliationRuns(args) => {
                    let response = client.exchange_reconciliation_runs(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_reconciliation_runs(&response.runs);
                    }
                }
                ExchangeTestnetCommands::ReconciliationGet { run_id } => {
                    let response = client.exchange_reconciliation_run(run_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_reconciliation_run(&response.run);
                    }
                }
                ExchangeTestnetCommands::ReconciliationMismatches { run_id } => {
                    let response = client.exchange_reconciliation_mismatches(run_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_exchange_reconciliation_mismatches(&response.mismatches);
                    }
                }
            },
        },
        Commands::Paper(command) => match command {
            PaperCommands::Account => {
                let response = client.paper_account().await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_paper_account(&response);
                }
            }
            PaperCommands::Positions(args) => {
                let response = client.paper_positions(args.limit, &args.status).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_paper_positions(&response);
                }
            }
            PaperCommands::Close(args) => {
                let response = client
                    .paper_close(
                        args.position_id,
                        args.confirm.as_deref().expect("validated confirm"),
                        args.reason,
                    )
                    .await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_paper_close(&response);
                }
            }
            PaperCommands::Pnl => {
                let response = client.paper_pnl().await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_paper_pnl(&response);
                }
            }
            PaperCommands::Equity(args) => {
                let response = client.paper_equity(args.limit).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_paper_equity(&response);
                }
            }
            PaperCommands::Journal(args) => {
                let response = client.paper_journal(args.limit).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_paper_journal(&response);
                }
            }
            PaperCommands::Mark => {
                let response = client.paper_mark().await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_paper_pnl(&response);
                }
            }
        },
        Commands::Analytics(command) => match command {
            AnalyticsCommands::Strategy(command) => match command {
                AnalyticsStrategyCommands::Performance(args) => {
                    let response = client
                        .strategy_performance(
                            args.strategy_id,
                            args.symbol,
                            args.timeframe,
                            args.mode,
                            args.start_time,
                            args.end_time,
                            args.limit,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_performance_summary(&response);
                    }
                }
                AnalyticsStrategyCommands::Rankings(args) => {
                    let response = client
                        .strategy_rankings(args.mode, args.symbol, args.timeframe, args.limit)
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_performance_rankings(&response);
                    }
                }
                AnalyticsStrategyCommands::DecisionBreakdown(args) => {
                    let response = client
                        .strategy_decision_breakdown(
                            &args.strategy_id,
                            args.symbol,
                            args.timeframe,
                            args.start_time,
                            args.end_time,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_strategy_decision_breakdown(&response);
                    }
                }
            },
            AnalyticsCommands::Testnet(command) => match command {
                AnalyticsTestnetCommands::PromotionFunnel(args) => {
                    let response = client
                        .testnet_promotion_funnel(
                            args.strategy_id,
                            args.symbol,
                            args.timeframe,
                            args.start_time,
                            args.end_time,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_promotion_funnel_summary(&response);
                    }
                }
                AnalyticsTestnetCommands::PromotionOutcomes(args) => {
                    let response = client
                        .testnet_promotion_outcomes(
                            args.strategy_id,
                            args.symbol,
                            args.timeframe,
                            args.start_time,
                            args.end_time,
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_promotion_outcomes(&response);
                    }
                }
                AnalyticsTestnetCommands::PromotionRows(args) => {
                    let response = client
                        .testnet_promotion_rows(
                            args.strategy_id,
                            args.symbol,
                            args.timeframe,
                            args.start_time,
                            args.end_time,
                            Some(args.limit),
                        )
                        .await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_testnet_promotion_rows(&response);
                    }
                }
            },
        },
        Commands::Reports(command) => match command {
            ReportsCommands::Operator(command) => match command {
                OperatorReportsCommands::Daily(args) => {
                    let request = OperatorReportRequest::try_from(&args)?;
                    let response = client.generate_operator_report(&request).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else if request.format == OperatorReportFormat::Markdown {
                        if let Some(markdown) = response.report.markdown.as_deref() {
                            println!("{markdown}");
                        } else {
                            output::print_operator_report(&response);
                        }
                    } else {
                        output::print_operator_report(&response);
                    }
                }
                OperatorReportsCommands::List(args) => {
                    let response = client.list_operator_reports(args.limit).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_operator_report_list(&response);
                    }
                }
                OperatorReportsCommands::Get { report_id } => {
                    let response = client.get_operator_report(report_id).await?;
                    if cli.json {
                        output::print_json(&response)?;
                    } else {
                        output::print_operator_report(&response);
                    }
                }
            },
        },
        Commands::Readiness(command) => match command {
            ReadinessCommands::Check(args) => {
                let request = (&args).try_into()?;
                let response = client.execution_readiness_check(&request).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_execution_readiness(&response);
                }
            }
            ReadinessCommands::Snapshots(args) => {
                let response = client.execution_readiness_snapshots(args.limit).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_execution_readiness_snapshots(&response);
                }
            }
            ReadinessCommands::Get { readiness_id } => {
                let response = client.execution_readiness_get(readiness_id).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_execution_readiness(&response);
                }
            }
        },
    }

    Ok(())
}

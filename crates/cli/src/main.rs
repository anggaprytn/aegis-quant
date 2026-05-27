use aegis_core::{
    OperatorReportFormat, OperatorReportRequest, PaperTradingPipelineRequest,
    ResearchCandidateDecisionRejection, ResearchCandidateDecisionRequest,
    ResearchCandidateReviewRequest, TestnetShadowRunnerControlAction,
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
    ResearchCandidateCommands, ResearchCommands, ResearchDataCommands, RiskCommands,
    RiskConfigCommands, StrategyCommands, StrategyConfigCommands, StrategyExperimentCommands,
    RESUME_CONFIRMATION_TEXT, TESTNET_ORDER_CONFIRMATION_TEXT,
};
use cli::config::{
    clear_token_file, save_token_file, CliConfig, StoredAuthSession, StoredUserSummary,
};
use cli::output;
use serde::{Deserialize, Serialize};

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
                    }
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

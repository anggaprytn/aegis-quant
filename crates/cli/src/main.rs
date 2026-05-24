use aegis_core::{
    PaperTradingPipelineRequest, TestnetShadowRunnerControlAction,
    TestnetShadowRunnerControlRequest,
};
use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use cli::api::{
    build_backtest_request, build_candle_backfill_request, build_pipeline_request,
    build_risk_config_request, build_strategy_config_request, ApiClient, RecentEventsQuery,
    RiskDecisionsQuery,
};
use cli::cli::{
    AnalyticsCommands, AnalyticsStrategyCommands, AnalyticsTestnetCommands, AuthCommands,
    BacktestCommands, Cli, Commands, EventsCommands, ExchangeCommands, ExchangeTestnetCommands,
    ExchangeTestnetPrivateStreamCommands, ExchangeTestnetShadowRunnerCommands, MarketCommands,
    OrderCommands, PaperCommands, PipelineCommands, RiskCommands, RiskConfigCommands,
    StrategyCommands, StrategyConfigCommands, RESUME_CONFIRMATION_TEXT,
    TESTNET_ORDER_CONFIRMATION_TEXT,
};
use cli::config::{
    clear_token_file, save_token_file, CliConfig, StoredAuthSession, StoredUserSummary,
};
use cli::output;

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
    }

    Ok(())
}

use aegis_core::PaperTradingPipelineRequest;
use anyhow::Context;
use clap::Parser;
use cli::api::{
    build_backtest_request, build_candle_backfill_request, build_pipeline_request, ApiClient,
    RecentEventsQuery, RiskDecisionsQuery,
};
use cli::cli::{
    BacktestCommands, Cli, Commands, EventsCommands, MarketCommands, OrderCommands, PaperCommands,
    PipelineCommands, RiskCommands, StrategyCommands, RESUME_CONFIRMATION_TEXT,
};
use cli::config::CliConfig;
use cli::output;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    cli.validate()?;

    let config = CliConfig::from_env().context("failed to load CLI config")?;
    let client = ApiClient::new(config.api_base_url);

    match cli.command {
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
                let response = client.paper_positions(args.limit).await?;
                if cli.json {
                    output::print_json(&response)?;
                } else {
                    output::print_paper_positions(&response);
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
    }

    Ok(())
}

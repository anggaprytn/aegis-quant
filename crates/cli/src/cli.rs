use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

use aegis_core::{
    expected_testnet_pipeline_confirmation, ExecutionReadinessRequest, ExecutionReadinessTarget,
    OperatorReportFormat, OperatorReportRequest, TestnetShadowPromotionRequest,
    TestnetShadowRunRequest, TestnetShadowRunnerConfigInput, TestnetShadowRunnerStaleFeedPolicy,
};

pub const RESUME_CONFIRMATION_TEXT: &str = "RESUME TRADING";
pub const TESTNET_ORDER_CONFIRMATION_TEXT: &str = "TESTNET ORDER";

#[derive(Debug, Parser)]
#[command(name = "aegis", about = "Aegis Quant operational CLI")]
pub struct Cli {
    #[arg(long, global = true, help = "Print raw JSON responses")]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Commands::Resume(args) = &self.command {
            if args.confirm.as_deref() != Some(RESUME_CONFIRMATION_TEXT) {
                anyhow::bail!(
                    "resume requires --confirm {:?} exactly",
                    RESUME_CONFIRMATION_TEXT
                );
            }
        }
        if let Commands::Paper(PaperCommands::Close(args)) = &self.command {
            if args.confirm.is_none() {
                anyhow::bail!("paper close requires --confirm \"CLOSE <SYMBOL>\"");
            }
        }
        if let Commands::Exchange(ExchangeCommands::Testnet(command)) = &self.command {
            match command {
                ExchangeTestnetCommands::OrderSubmit(args) => {
                    if args.confirm.as_deref() != Some(TESTNET_ORDER_CONFIRMATION_TEXT) {
                        anyhow::bail!(
                            "exchange testnet order-submit requires --confirm {:?} exactly",
                            TESTNET_ORDER_CONFIRMATION_TEXT
                        );
                    }
                }
                ExchangeTestnetCommands::OrderCancel(args) => {
                    if args.confirm.as_deref() != Some(TESTNET_ORDER_CONFIRMATION_TEXT) {
                        anyhow::bail!(
                            "exchange testnet order-cancel requires --confirm {:?} exactly",
                            TESTNET_ORDER_CONFIRMATION_TEXT
                        );
                    }
                }
                ExchangeTestnetCommands::OrderRepair(args) => {
                    let expected = if args.action.eq_ignore_ascii_case("SAFE_CANCEL_REQUEST") {
                        format!("CANCEL TESTNET {}", args.client_order_id)
                    } else {
                        format!("REPAIR TESTNET {}", args.client_order_id)
                    };
                    if args.confirm.as_deref() != Some(expected.as_str()) {
                        anyhow::bail!(
                            "exchange testnet order-repair requires --confirm {:?} exactly",
                            expected
                        );
                    }
                }
                ExchangeTestnetCommands::PipelineSubmit(args) => {
                    let expected = expected_testnet_pipeline_confirmation(&args.symbol);
                    if args.confirm.as_deref() != Some(expected.as_str()) {
                        anyhow::bail!(
                            "exchange testnet pipeline-submit requires --confirm {:?} exactly",
                            expected
                        );
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(subcommand)]
    Auth(AuthCommands),
    Status,
    Metrics(MetricsArgs),
    Kill {
        #[arg(long)]
        reason: Option<String>,
    },
    Resume(ResumeArgs),
    #[command(subcommand)]
    Pipeline(PipelineCommands),
    #[command(subcommand)]
    Strategy(StrategyCommands),
    #[command(subcommand)]
    Orders(OrderCommands),
    #[command(subcommand)]
    Events(EventsCommands),
    #[command(subcommand)]
    Risk(RiskCommands),
    #[command(subcommand)]
    Market(MarketCommands),
    #[command(subcommand)]
    Backtest(BacktestCommands),
    #[command(subcommand)]
    Paper(PaperCommands),
    #[command(subcommand)]
    Analytics(AnalyticsCommands),
    #[command(subcommand)]
    Reports(ReportsCommands),
    #[command(subcommand)]
    Readiness(ReadinessCommands),
    #[command(subcommand)]
    Exchange(ExchangeCommands),
}

#[derive(Debug, Subcommand)]
pub enum ReadinessCommands {
    Check(ReadinessCheckArgs),
    Snapshots(ReadinessSnapshotsArgs),
    Get { readiness_id: Uuid },
}

#[derive(Debug, Args)]
pub struct ReadinessCheckArgs {
    #[arg(long)]
    pub target: String,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long = "strategy")]
    pub strategy_id: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long = "promotion-id")]
    pub promotion_id: Option<Uuid>,
    #[arg(long = "risk-decision-id")]
    pub risk_decision_id: Option<Uuid>,
    #[arg(long = "start")]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long = "end")]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long, default_value_t = false)]
    pub persist: bool,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

impl TryFrom<&ReadinessCheckArgs> for ExecutionReadinessRequest {
    type Error = anyhow::Error;

    fn try_from(value: &ReadinessCheckArgs) -> Result<Self, Self::Error> {
        Ok(Self {
            target: value.target.parse::<ExecutionReadinessTarget>()?,
            symbol: value.symbol.clone(),
            strategy_id: value.strategy_id.clone(),
            timeframe: value.timeframe.clone(),
            promotion_id: value.promotion_id,
            risk_decision_id: value.risk_decision_id,
            start_time: value.start_time,
            end_time: value.end_time,
            persist: value.persist,
            correlation_id: value.correlation_id,
        })
    }
}

#[derive(Debug, Args)]
pub struct ReadinessSnapshotsArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Debug, Subcommand)]
pub enum ReportsCommands {
    #[command(subcommand)]
    Operator(OperatorReportsCommands),
}

#[derive(Debug, Subcommand)]
pub enum OperatorReportsCommands {
    Daily(OperatorReportDailyArgs),
    List(OperatorReportListArgs),
    Get { report_id: Uuid },
}

#[derive(Debug, Args)]
pub struct OperatorReportDailyArgs {
    #[arg(long = "start")]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long = "end")]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long = "strategy")]
    pub strategy_id: Option<String>,
    #[arg(long, default_value = "json")]
    pub format: String,
    #[arg(long, default_value_t = false)]
    pub persist: bool,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

impl TryFrom<&OperatorReportDailyArgs> for OperatorReportRequest {
    type Error = anyhow::Error;

    fn try_from(value: &OperatorReportDailyArgs) -> Result<Self, Self::Error> {
        Ok(OperatorReportRequest {
            start_time: value.start_time,
            end_time: value.end_time,
            symbol: value.symbol.clone(),
            strategy_id: value.strategy_id.clone(),
            format: value.format.parse::<OperatorReportFormat>()?,
            persist: value.persist,
            correlation_id: value.correlation_id,
        })
    }
}

#[derive(Debug, Args)]
pub struct OperatorReportListArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Debug, Subcommand)]
pub enum ExchangeCommands {
    #[command(subcommand)]
    Testnet(ExchangeTestnetCommands),
}

#[derive(Debug, Subcommand)]
pub enum ExchangeTestnetCommands {
    Status,
    PipelinePreview(ExchangeTestnetPipelinePreviewArgs),
    PipelineSubmit(ExchangeTestnetPipelineSubmitArgs),
    ShadowRun(ExchangeTestnetShadowRunArgs),
    ShadowRuns(ExchangeTestnetShadowRunsArgs),
    ShadowGet {
        run_id: Uuid,
    },
    ShadowPromotionPreview(ExchangeTestnetShadowPromotionPreviewArgs),
    ShadowPromotions(ExchangeTestnetShadowPromotionsArgs),
    ShadowPromotionGet {
        promotion_id: Uuid,
    },
    ShadowPromotionSubmit(ExchangeTestnetShadowPromotionSubmitArgs),
    #[command(subcommand)]
    ShadowRunner(ExchangeTestnetShadowRunnerCommands),
    #[command(subcommand)]
    PrivateStream(ExchangeTestnetPrivateStreamCommands),
    Symbols,
    Balances,
    OrderSubmit(ExchangeTestnetOrderSubmitArgs),
    OrderGet {
        client_order_id: String,
    },
    OrderLifecycle {
        client_order_id: String,
    },
    OrderCancel(ExchangeTestnetOrderCancelArgs),
    OrderRepair(ExchangeTestnetOrderRepairArgs),
    OrderRepairs {
        client_order_id: String,
    },
    Reconcile(ExchangeTestnetReconcileArgs),
    ReconciliationRuns(ExchangeReconciliationRunsArgs),
    ReconciliationGet {
        run_id: Uuid,
    },
    ReconciliationMismatches {
        run_id: Uuid,
    },
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetPipelinePreviewArgs {
    #[arg(long = "risk-decision-id")]
    pub risk_decision_id: Uuid,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetPipelineSubmitArgs {
    #[arg(long = "risk-decision-id")]
    pub risk_decision_id: Uuid,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub confirm: Option<String>,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetShadowRunArgs {
    #[arg(long = "strategy")]
    pub strategy_id: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
}

impl From<ExchangeTestnetShadowRunArgs> for TestnetShadowRunRequest {
    fn from(value: ExchangeTestnetShadowRunArgs) -> Self {
        Self {
            strategy_id: value.strategy_id,
            symbol: value.symbol,
            timeframe: value.timeframe,
            correlation_id: None,
        }
    }
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetShadowPromotionPreviewArgs {
    pub shadow_run_id: Uuid,
}

impl From<ExchangeTestnetShadowPromotionPreviewArgs> for TestnetShadowPromotionRequest {
    fn from(value: ExchangeTestnetShadowPromotionPreviewArgs) -> Self {
        Self {
            shadow_run_id: value.shadow_run_id,
            correlation_id: None,
        }
    }
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetShadowRunsArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetShadowPromotionsArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetShadowPromotionSubmitArgs {
    pub promotion_id: Uuid,
    #[arg(long)]
    pub confirm: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum ExchangeTestnetShadowRunnerCommands {
    Status,
    Config,
    ConfigUpdate(ExchangeTestnetShadowRunnerConfigUpdateArgs),
    RunOnce,
    Pause,
    Resume,
    Start,
    Stop,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetShadowRunnerConfigUpdateArgs {
    #[arg(long)]
    pub enabled: bool,
    #[arg(long = "interval-seconds")]
    pub interval_seconds: i32,
    #[arg(long, value_delimiter = ',')]
    pub strategies: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    pub symbols: Vec<String>,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "max-runs-per-tick")]
    pub max_runs_per_tick: i32,
    #[arg(long = "stale-feed-policy", default_value = "SKIP")]
    pub stale_feed_policy: String,
    #[arg(long)]
    pub notes: Option<String>,
}

impl TryFrom<ExchangeTestnetShadowRunnerConfigUpdateArgs> for TestnetShadowRunnerConfigInput {
    type Error = anyhow::Error;

    fn try_from(value: ExchangeTestnetShadowRunnerConfigUpdateArgs) -> anyhow::Result<Self> {
        Ok(Self {
            enabled: value.enabled,
            interval_seconds: value.interval_seconds,
            strategies: value.strategies,
            symbols: value.symbols,
            timeframe: value.timeframe,
            max_runs_per_tick: value.max_runs_per_tick,
            stale_feed_policy: value
                .stale_feed_policy
                .parse::<TestnetShadowRunnerStaleFeedPolicy>()?,
            notes: value.notes,
        })
    }
}

#[derive(Debug, Subcommand)]
pub enum ExchangeTestnetPrivateStreamCommands {
    Status,
    Events(ExchangeTestnetPrivateStreamEventsArgs),
    ListenKey,
    Keepalive(ExchangeTestnetPrivateStreamListenKeyArgs),
    Close(ExchangeTestnetPrivateStreamListenKeyArgs),
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetPrivateStreamEventsArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
    #[arg(long = "client-order-id")]
    pub client_order_id: Option<String>,
    #[arg(long = "event-type")]
    pub event_type: Option<String>,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetPrivateStreamListenKeyArgs {
    #[arg(long = "listen-key")]
    pub listen_key: String,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetOrderSubmitArgs {
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub side: String,
    #[arg(long = "type")]
    pub order_type: String,
    #[arg(long = "time-in-force")]
    pub time_in_force: Option<String>,
    #[arg(long)]
    pub quantity: Option<rust_decimal::Decimal>,
    #[arg(long = "quote-notional")]
    pub quote_notional: Option<rust_decimal::Decimal>,
    #[arg(long = "limit-price")]
    pub limit_price: Option<rust_decimal::Decimal>,
    #[arg(long = "risk-decision-id")]
    pub risk_decision_id: Option<Uuid>,
    #[arg(long)]
    pub confirm: Option<String>,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetOrderCancelArgs {
    pub client_order_id: String,
    #[arg(long)]
    pub confirm: Option<String>,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetOrderRepairArgs {
    pub client_order_id: String,
    #[arg(long)]
    pub action: String,
    #[arg(long)]
    pub confirm: Option<String>,
    #[arg(long)]
    pub reason: Option<String>,
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct ExchangeTestnetReconcileArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
    #[arg(long = "status-filter")]
    pub status_filter: Vec<String>,
}

#[derive(Debug, Args)]
pub struct ExchangeReconciliationRunsArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Debug, Subcommand)]
pub enum AuthCommands {
    Login(AuthLoginArgs),
    Refresh,
    Me,
    Logout,
}

#[derive(Debug, Args)]
pub struct AuthLoginArgs {
    #[arg(long)]
    pub email: String,
    #[arg(long)]
    pub password: String,
}

#[derive(Debug, Args)]
pub struct ResumeArgs {
    #[arg(long, help = "Must match RESUME TRADING exactly")]
    pub confirm: Option<String>,
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct MetricsArgs {
    #[arg(long, help = "Filter exposed metric lines by substring")]
    pub grep: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum PipelineCommands {
    Run(PipelineRunArgs),
}

#[derive(Debug, Args)]
pub struct PipelineRunArgs {
    #[arg(long)]
    pub strategy: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long)]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Subcommand)]
pub enum StrategyCommands {
    List,
    #[command(subcommand)]
    Config(StrategyConfigCommands),
    DryRun(StrategyDryRunArgs),
    Diagnostics(StrategyDiagnosticsArgs),
    Enable {
        strategy_id: String,
    },
    Disable {
        strategy_id: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum StrategyConfigCommands {
    Get { strategy_id: String },
    Validate(StrategyConfigArgs),
    Update(StrategyConfigArgs),
    Versions { strategy_id: String },
    Audit { strategy_id: String },
}

#[derive(Debug, Args, Clone)]
pub struct StrategyConfigArgs {
    pub strategy_id: String,
    #[arg(long, default_value_t = true)]
    pub enabled: bool,
    #[arg(long, default_value = "paper")]
    pub mode: String,
    #[arg(long = "symbol", required = true)]
    pub symbols: Vec<String>,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "suggested-notional")]
    pub suggested_notional: rust_decimal::Decimal,
    #[arg(long = "max-signal-age-ms")]
    pub max_signal_age_ms: i64,
    #[arg(long = "cooldown-seconds")]
    pub cooldown_seconds: u32,
    #[arg(long = "lookback-candles")]
    pub lookback_candles: u32,
    #[arg(long = "confidence-floor")]
    pub confidence_floor: Option<rust_decimal::Decimal>,
    #[arg(long = "stop-loss-pct")]
    pub stop_loss_pct: Option<rust_decimal::Decimal>,
    #[arg(long = "take-profit-pct")]
    pub take_profit_pct: Option<rust_decimal::Decimal>,
    #[arg(long = "holding-candles")]
    pub holding_candles: Option<u32>,
    #[arg(long)]
    pub notes: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct StrategyDryRunArgs {
    pub strategy_id: String,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct StrategyDiagnosticsArgs {
    pub strategy_id: String,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Debug, Subcommand)]
pub enum OrderCommands {
    List(OrderListArgs),
    Get { order_id: Uuid },
}

#[derive(Debug, Args)]
pub struct OrderListArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
}

#[derive(Debug, Subcommand)]
pub enum EventsCommands {
    List(EventsListArgs),
}

#[derive(Debug, Args)]
pub struct EventsListArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
    #[arg(long = "event-type")]
    pub event_type: Option<String>,
    #[arg(long)]
    pub source: Option<String>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Subcommand)]
pub enum RiskCommands {
    #[command(subcommand)]
    Config(RiskConfigCommands),
    Decisions(RiskDecisionsArgs),
}

#[derive(Debug, Subcommand)]
pub enum RiskConfigCommands {
    Get,
    Validate(RiskConfigArgs),
    Update(RiskConfigArgs),
    Versions,
    Audit,
}

#[derive(Debug, Args, Clone)]
pub struct RiskConfigArgs {
    #[arg(long = "max-open-positions")]
    pub max_open_positions: u32,
    #[arg(long = "max-daily-loss-pct")]
    pub max_daily_loss_pct: rust_decimal::Decimal,
    #[arg(long = "max-weekly-loss-pct")]
    pub max_weekly_loss_pct: rust_decimal::Decimal,
    #[arg(long = "max-position-notional")]
    pub max_position_notional: rust_decimal::Decimal,
    #[arg(long = "max-slippage-pct")]
    pub max_slippage_pct: rust_decimal::Decimal,
    #[arg(long = "max-consecutive-losses")]
    pub max_consecutive_losses: u32,
    #[arg(long = "cooldown-seconds")]
    pub cooldown_seconds: u32,
    #[arg(long = "max-signal-age-ms")]
    pub max_signal_age_ms: i64,
    #[arg(long = "stale-feed-threshold-seconds")]
    pub stale_feed_threshold_seconds: u32,
}

#[derive(Debug, Subcommand)]
pub enum MarketCommands {
    Backfill(MarketBackfillArgs),
    Backfills(MarketBackfillsArgs),
    BackfillGet { run_id: Uuid },
}

#[derive(Debug, Args)]
pub struct MarketBackfillArgs {
    #[arg(long, default_value = "binance")]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long = "timeframe")]
    pub timeframe: String,
    #[arg(long)]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long)]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "limit-per-request")]
    pub limit_per_request: Option<u16>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct MarketBackfillsArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Debug, Args)]
pub struct RiskDecisionsArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
    #[arg(long)]
    pub symbol: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum BacktestCommands {
    Run(BacktestRunArgs),
    List(BacktestListArgs),
    Get { run_id: Uuid },
}

#[derive(Debug, Subcommand)]
pub enum PaperCommands {
    Account,
    Positions(PaperListArgs),
    Close(PaperCloseArgs),
    Pnl,
    Equity(PaperListArgs),
    Journal(PaperListArgs),
    Mark,
}

#[derive(Debug, Subcommand)]
pub enum AnalyticsCommands {
    #[command(subcommand)]
    Strategy(AnalyticsStrategyCommands),
    #[command(subcommand)]
    Testnet(AnalyticsTestnetCommands),
}

#[derive(Debug, Subcommand)]
pub enum AnalyticsStrategyCommands {
    Performance(AnalyticsPerformanceArgs),
    Rankings(AnalyticsRankingsArgs),
    DecisionBreakdown(AnalyticsDecisionBreakdownArgs),
}

#[derive(Debug, Subcommand)]
pub enum AnalyticsTestnetCommands {
    PromotionFunnel(AnalyticsTestnetPromotionFunnelArgs),
    PromotionOutcomes(AnalyticsTestnetPromotionOutcomesArgs),
    PromotionRows(AnalyticsTestnetPromotionRowsArgs),
}

#[derive(Debug, Args)]
pub struct AnalyticsPerformanceArgs {
    #[arg(long = "strategy")]
    pub strategy_id: Option<String>,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long)]
    pub mode: String,
    #[arg(long)]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long)]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long)]
    pub limit: Option<i64>,
}

#[derive(Debug, Args)]
pub struct AnalyticsRankingsArgs {
    #[arg(long)]
    pub mode: String,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Debug, Args)]
pub struct AnalyticsDecisionBreakdownArgs {
    pub strategy_id: String,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long)]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long)]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Args)]
pub struct AnalyticsTestnetPromotionFunnelArgs {
    #[arg(long = "strategy")]
    pub strategy_id: Option<String>,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long)]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long)]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Args)]
pub struct AnalyticsTestnetPromotionOutcomesArgs {
    #[arg(long = "strategy")]
    pub strategy_id: Option<String>,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long)]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long)]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Args)]
pub struct AnalyticsTestnetPromotionRowsArgs {
    #[arg(long = "strategy")]
    pub strategy_id: Option<String>,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long)]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long)]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
}

#[derive(Debug, Args)]
pub struct PaperListArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
    #[arg(long, default_value = "ALL")]
    pub status: String,
}

#[derive(Debug, Args)]
pub struct PaperCloseArgs {
    pub position_id: Uuid,
    #[arg(long, help = "Must match CLOSE <SYMBOL> exactly")]
    pub confirm: Option<String>,
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct BacktestRunArgs {
    #[arg(long)]
    pub strategy: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long)]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long)]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "initial-capital")]
    pub initial_capital: rust_decimal::Decimal,
    #[arg(long = "fee-bps")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "holding-candles")]
    pub holding_candles: Option<u32>,
    #[arg(long = "risk-config-id")]
    pub risk_config_id: Option<Uuid>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct BacktestListArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[cfg(test)]
mod tests {
    use super::ExchangeTestnetShadowPromotionPreviewArgs;
    use super::{Cli, Commands, RESUME_CONFIRMATION_TEXT, TESTNET_ORDER_CONFIRMATION_TEXT};
    use aegis_core::{expected_testnet_pipeline_confirmation, TestnetShadowPromotionRequest};
    use clap::Parser;
    use uuid::Uuid;

    #[test]
    fn resume_confirmation_accepts_exact_text() {
        let cli = Cli::try_parse_from(["aegis", "resume", "--confirm", RESUME_CONFIRMATION_TEXT])
            .expect("cli parses");

        assert!(cli.validate().is_ok());
    }

    #[test]
    fn resume_confirmation_rejects_missing_text() {
        let cli = Cli::try_parse_from(["aegis", "resume"]).expect("cli parses");

        assert!(cli.validate().is_err());
    }

    #[test]
    fn resume_confirmation_rejects_non_exact_text() {
        let cli = Cli::try_parse_from(["aegis", "resume", "--confirm", "resume trading"])
            .expect("cli parses");

        assert!(cli.validate().is_err());
    }

    #[test]
    fn metrics_command_parses_optional_grep() {
        let cli = Cli::try_parse_from(["aegis", "metrics", "--grep", "paper"]).expect("cli parses");

        assert!(matches!(cli.command, Commands::Metrics(_)));
    }

    #[test]
    fn exchange_testnet_submit_requires_exact_confirmation() {
        let cli = Cli::try_parse_from([
            "aegis",
            "exchange",
            "testnet",
            "order-submit",
            "--symbol",
            "BTCUSDT",
            "--side",
            "BUY",
            "--type",
            "MARKET",
            "--quote-notional",
            "10",
            "--confirm",
            TESTNET_ORDER_CONFIRMATION_TEXT,
        ])
        .expect("cli parses");

        assert!(cli.validate().is_ok());
    }

    #[test]
    fn exchange_testnet_cancel_rejects_wrong_confirmation() {
        let cli = Cli::try_parse_from([
            "aegis",
            "exchange",
            "testnet",
            "order-cancel",
            "client-1",
            "--confirm",
            "wrong",
        ])
        .expect("cli parses");

        assert!(cli.validate().is_err());
    }

    #[test]
    fn exchange_testnet_repair_requires_exact_confirmation() {
        let cli = Cli::try_parse_from([
            "aegis",
            "exchange",
            "testnet",
            "order-repair",
            "client-1",
            "--action",
            "MANUAL_RECHECK",
            "--confirm",
            "REPAIR TESTNET client-1",
        ])
        .expect("cli parses");

        assert!(cli.validate().is_ok());
    }

    #[test]
    fn exchange_testnet_safe_cancel_requires_cancel_confirmation() {
        let cli = Cli::try_parse_from([
            "aegis",
            "exchange",
            "testnet",
            "order-repair",
            "client-1",
            "--action",
            "SAFE_CANCEL_REQUEST",
            "--confirm",
            "REPAIR TESTNET client-1",
        ])
        .expect("cli parses");

        assert!(cli.validate().is_err());
    }

    #[test]
    fn exchange_testnet_pipeline_submit_requires_symbol_confirmation() {
        let expected = expected_testnet_pipeline_confirmation("BTCUSDT");
        let cli = Cli::try_parse_from([
            "aegis",
            "exchange",
            "testnet",
            "pipeline-submit",
            "--risk-decision-id",
            "00000000-0000-0000-0000-000000000123",
            "--symbol",
            "BTCUSDT",
            "--confirm",
            &expected,
        ])
        .expect("cli parses");

        assert!(cli.validate().is_ok());
    }
    #[test]
    fn shadow_promotion_preview_request_serializes_expected_wire_shape() {
        let shadow_run_id =
            Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("valid uuid");
        let request =
            TestnetShadowPromotionRequest::from(ExchangeTestnetShadowPromotionPreviewArgs {
                shadow_run_id,
            });
        let value = serde_json::to_value(request).expect("serializes");

        assert_eq!(value["shadow_run_id"], shadow_run_id.to_string());
        assert!(value["correlation_id"].is_null());
    }
}

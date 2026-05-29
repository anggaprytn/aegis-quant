use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

use aegis_core::{
    expected_research_candidate_shadow_promotion_confirmation,
    expected_testnet_pipeline_confirmation, ExecutionReadinessRequest, ExecutionReadinessTarget,
    OperatorReportFormat, OperatorReportRequest, ResearchCandidateDecision,
    ResearchCandidateQualificationThresholds, ResearchCandidateReviewAction,
    ResearchCandidateShadowPromotionMode, ResearchCandidateShadowPromotionRequest,
    ResearchStaleRunRecoveryTargetType, ScheduledResearchBootstrapSafeRequest,
    ScheduledResearchJobKind, ScheduledResearchJobRequest, TestnetShadowPromotionRequest,
    TestnetShadowRunRequest, TestnetShadowRunnerConfigInput, TestnetShadowRunnerStaleFeedPolicy,
    RESEARCH_STALE_RUN_RECOVERY_CONFIRMATION,
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
        if let Commands::Research(ResearchCommands::Candidates(command)) = &self.command {
            if let ResearchCandidateCommands::PromoteShadowApply(args) = command {
                let expected =
                    expected_research_candidate_shadow_promotion_confirmation(args.candidate_id);
                if args.confirm.as_deref() != Some(expected.as_str()) {
                    anyhow::bail!(
                        "research candidates promote-shadow-apply requires --confirm {:?} exactly",
                        expected
                    );
                }
            }
            if let ResearchCandidateCommands::Review(args) = command {
                if matches!(
                    args.action,
                    ResearchCandidateReviewAction::RejectFromWatchlist
                        | ResearchCandidateReviewAction::ArchiveFromWatchlist
                ) && args
                    .reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    anyhow::bail!(
                        "research candidates review requires --reason for action {}",
                        args.action.as_str()
                    );
                }
            }
        }
        if let Commands::Research(ResearchCommands::StaleRuns(command)) = &self.command {
            if let ResearchStaleRunCommands::Recover(args) = command {
                if args.confirm.as_deref() != Some(RESEARCH_STALE_RUN_RECOVERY_CONFIRMATION) {
                    anyhow::bail!(
                        "research stale-runs recover requires --confirm {:?} exactly",
                        RESEARCH_STALE_RUN_RECOVERY_CONFIRMATION
                    );
                }
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
    Research(ResearchCommands),
    #[command(subcommand)]
    Backtest(BacktestCommands),
    #[command(subcommand)]
    Experiments(ExperimentCommands),
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
    #[arg(long)]
    pub interval: Option<String>,
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
            interval: value.interval.clone(),
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
    OpportunityAnalysis(StrategyOpportunityAnalysisArgs),
    ExitAttribution(StrategyExitAttributionArgs),
    SignalFeatureAttribution(StrategySignalFeatureAttributionArgs),
    CompressionRefinement(CompressionBreakoutRefinementArgs),
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
    #[arg(long = "trend-lookback-candles")]
    pub trend_lookback_candles: Option<u32>,
    #[arg(long = "momentum-lookback-candles")]
    pub momentum_lookback_candles: Option<u32>,
    #[arg(long = "breakout-lookback-candles")]
    pub breakout_lookback_candles: Option<u32>,
    #[arg(long = "min-close-above-sma-pct")]
    pub min_close_above_sma_pct: Option<rust_decimal::Decimal>,
    #[arg(long = "max-close-above-sma-pct")]
    pub max_close_above_sma_pct: Option<rust_decimal::Decimal>,
    #[arg(long = "min-momentum-return-pct")]
    pub min_momentum_return_pct: Option<rust_decimal::Decimal>,
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

#[derive(Debug, Args, Clone)]
pub struct StrategyOpportunityAnalysisArgs {
    pub strategy_id: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "start")]
    pub start_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "limit-samples")]
    pub limit_samples: Option<usize>,
    #[arg(long = "include-examples", default_value_t = true)]
    pub include_examples: bool,
}

#[derive(Debug, Args, Clone)]
pub struct StrategyExitAttributionArgs {
    pub strategy_id: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "start")]
    pub start_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "experiment-run-id")]
    pub experiment_run_id: Option<Uuid>,
    #[arg(
        long = "holding-windows",
        value_delimiter = ',',
        default_value = "1,3,5,10,20"
    )]
    pub holding_windows: Vec<u32>,
    #[arg(long = "fee-bps")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps")]
    pub slippage_bps: rust_decimal::Decimal,
}

#[derive(Debug, Args, Clone)]
pub struct StrategySignalFeatureAttributionArgs {
    pub strategy_id: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "start")]
    pub start_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "experiment-run-id")]
    pub experiment_run_id: Option<Uuid>,
    #[arg(long = "holding-window", default_value_t = 5)]
    pub holding_window: u32,
    #[arg(long = "fee-bps", default_value = "10")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps", default_value = "5")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "min-samples-per-bucket", default_value_t = 5)]
    pub min_samples_per_bucket: u32,
}

#[derive(Debug, Args, Clone)]
pub struct CompressionBreakoutRefinementArgs {
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "start")]
    pub start_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "fee-bps")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "max-configs")]
    pub max_configs: Option<usize>,
    #[arg(
        long = "holding-windows",
        value_delimiter = ',',
        default_value = "5,10,20"
    )]
    pub holding_windows: Vec<u32>,
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
    RepairPlan(MarketRepairArgs),
    RepairRun(MarketRepairArgs),
    RepairRuns(MarketBackfillsArgs),
    RepairGet { run_id: Uuid },
    ProviderHealth(MarketProviderHealthArgs),
    AggregateCandles(MarketAggregateCandlesArgs),
    AggregationStatus,
    CandleCoverage(MarketCandleCoverageArgs),
    CandleQuality(MarketCandleQualityArgs),
}

#[derive(Debug, Subcommand)]
pub enum ResearchCommands {
    #[command(subcommand)]
    Data(ResearchDataCommands),
    #[command(name = "regime-datasets", subcommand)]
    RegimeDatasets(ResearchRegimeDatasetCommands),
    #[command(name = "regime-discovery", subcommand)]
    RegimeDiscovery(ResearchRegimeDiscoveryCommands),
    #[command(name = "regime-calibration", subcommand)]
    RegimeCalibration(ResearchRegimeCalibrationCommands),
    #[command(subcommand)]
    Campaigns(ResearchCampaignCommands),
    #[command(subcommand)]
    Batches(ResearchBatchCommands),
    #[command(subcommand)]
    Candidates(ResearchCandidateCommands),
    #[command(subcommand)]
    Hypotheses(ResearchHypothesisCommands),
    #[command(name = "experiment-plans", subcommand)]
    ExperimentPlans(ResearchExperimentPlanCommands),
    #[command(name = "robustness-matrix", subcommand)]
    RobustnessMatrix(ResearchRobustnessMatrixCommands),
    #[command(name = "scheduled-jobs", subcommand)]
    ScheduledJobs(ResearchScheduledJobCommands),
    #[command(name = "stale-runs", subcommand)]
    StaleRuns(ResearchStaleRunCommands),
}

#[derive(Debug, Subcommand)]
pub enum ResearchStaleRunCommands {
    #[command(name = "recover-preview")]
    RecoverPreview(ResearchStaleRunArgs),
    Recover(ResearchStaleRunRecoverArgs),
}

#[derive(Debug, Args)]
pub struct ResearchStaleRunArgs {
    #[arg(long = "older-than-minutes", default_value_t = 60)]
    pub older_than_minutes: i64,
    #[arg(long = "target-types", value_delimiter = ',')]
    pub target_types: Vec<ResearchStaleRunRecoveryTargetType>,
    #[arg(long)]
    pub limit: Option<i64>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct ResearchStaleRunRecoverArgs {
    #[arg(long = "older-than-minutes", default_value_t = 60)]
    pub older_than_minutes: i64,
    #[arg(long = "target-types", value_delimiter = ',')]
    pub target_types: Vec<ResearchStaleRunRecoveryTargetType>,
    #[arg(long)]
    pub limit: Option<i64>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
    #[arg(long)]
    pub confirm: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum ResearchScheduledJobCommands {
    List(ResearchBatchListArgs),
    Get {
        id: Uuid,
    },
    Create(ResearchScheduledJobCreateArgs),
    Pause {
        id: Uuid,
    },
    Resume {
        id: Uuid,
    },
    Runs {
        id: Uuid,
        #[arg(long, default_value_t = 20)]
        limit: i64,
    },
    #[command(name = "run-once")]
    RunOnce {
        id: Uuid,
    },
    #[command(name = "reset-failures")]
    ResetFailures {
        id: Uuid,
    },
    #[command(name = "bootstrap-safe")]
    BootstrapSafe(ResearchScheduledJobBootstrapSafeArgs),
}

#[derive(Debug, Args)]
pub struct ResearchScheduledJobCreateArgs {
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub kind: ScheduledResearchJobKind,
    #[arg(long, default_value_t = false)]
    pub enabled: bool,
    #[arg(long = "interval-seconds")]
    pub interval_seconds: i64,
    #[arg(long, default_value = "{}")]
    pub request: serde_json::Value,
    #[arg(long = "max-runs-per-tick", default_value_t = 1)]
    pub max_runs_per_tick: i32,
    #[arg(long = "next-run-at")]
    pub next_run_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<&ResearchScheduledJobCreateArgs> for ScheduledResearchJobRequest {
    fn from(value: &ResearchScheduledJobCreateArgs) -> Self {
        Self {
            name: value.name.clone(),
            kind: value.kind,
            enabled: value.enabled,
            interval_seconds: value.interval_seconds,
            request: value.request.clone(),
            max_runs_per_tick: value.max_runs_per_tick,
            next_run_at: value.next_run_at,
        }
    }
}

#[derive(Debug, Args)]
pub struct ResearchScheduledJobBootstrapSafeArgs {
    #[arg(long, default_value_t = false)]
    pub enable: bool,
    #[arg(long)]
    pub symbols: Option<String>,
    #[arg(long)]
    pub intervals: Option<String>,
    #[arg(long = "dry-run", default_value_t = false)]
    pub dry_run: bool,
    #[arg(long = "replace-existing", default_value_t = false)]
    pub replace_existing: bool,
}

impl From<&ResearchScheduledJobBootstrapSafeArgs> for ScheduledResearchBootstrapSafeRequest {
    fn from(value: &ResearchScheduledJobBootstrapSafeArgs) -> Self {
        Self {
            enable: value.enable,
            symbols: comma_list(value.symbols.as_deref()),
            intervals: comma_list(value.intervals.as_deref()),
            dry_run: value.dry_run,
            replace_existing: value.replace_existing,
        }
    }
}

fn comma_list(value: Option<&str>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[derive(Debug, Subcommand)]
pub enum ResearchHypothesisCommands {
    Generate(ResearchHypothesisGenerateArgs),
    List(ResearchBatchListArgs),
    Get { hypothesis_id: Uuid },
    Decide(ResearchHypothesisDecisionArgs),
    Plan { hypothesis_id: Uuid },
}

#[derive(Debug, Subcommand)]
pub enum ResearchExperimentPlanCommands {
    List(ResearchBatchListArgs),
    Get {
        plan_id: Uuid,
    },
    Validate {
        plan_id: Uuid,
    },
    #[command(name = "run-preview")]
    RunPreview {
        plan_id: Uuid,
    },
    Run(ResearchExperimentPlanRunArgs),
    Archive(ResearchExperimentPlanArchiveArgs),
}

#[derive(Debug, Args)]
pub struct ResearchExperimentPlanRunArgs {
    pub plan_id: Uuid,
    #[arg(long)]
    pub confirm: String,
}

#[derive(Debug, Args)]
pub struct ResearchExperimentPlanArchiveArgs {
    pub plan_id: Uuid,
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct ResearchHypothesisGenerateArgs {
    #[arg(long = "campaign-id")]
    pub campaign_id: Option<Uuid>,
    #[arg(long = "batch-id")]
    pub batch_id: Option<Uuid>,
    #[arg(long = "candidate-id")]
    pub candidate_id: Option<Uuid>,
    #[arg(long = "include-sources", value_delimiter = ',')]
    pub include_sources: Vec<String>,
    #[arg(long = "no-persist", default_value_t = false)]
    pub no_persist: bool,
}

#[derive(Debug, Args)]
pub struct ResearchHypothesisDecisionArgs {
    pub hypothesis_id: Uuid,
    #[arg(long)]
    pub decision: String,
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum ResearchRegimeDatasetCommands {
    Build(ResearchRegimeDatasetBuildArgs),
    FromDiscovery(ResearchRegimeDatasetFromDiscoveryArgs),
    List(ResearchBatchListArgs),
    Get { dataset_id: Uuid },
    Windows { dataset_id: Uuid },
}

#[derive(Debug, Args)]
pub struct ResearchRegimeDatasetBuildArgs {
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "start")]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "window-hours")]
    pub window_hours: i64,
    #[arg(long = "step-hours")]
    pub step_hours: i64,
    #[arg(long = "min-candles-per-window", default_value_t = 5)]
    pub min_candles_per_window: i32,
    #[arg(long = "target-regimes", value_delimiter = ',')]
    pub target_regimes: Option<Vec<String>>,
    #[arg(long = "max-windows-per-regime")]
    pub max_windows_per_regime: Option<u32>,
    #[arg(long = "allow-degraded-data", default_value_t = false)]
    pub allow_degraded_data: bool,
}

#[derive(Debug, Args)]
pub struct ResearchRegimeDatasetFromDiscoveryArgs {
    pub discovery_id: Uuid,
    #[arg(long = "target-regimes", value_delimiter = ',')]
    pub target_regimes: Option<Vec<String>>,
    #[arg(long = "max-windows-per-regime")]
    pub max_windows_per_regime: Option<u32>,
}

#[derive(Debug, Subcommand)]
pub enum ResearchRegimeDiscoveryCommands {
    Run(ResearchRegimeDiscoveryRunArgs),
    List(ResearchBatchListArgs),
    Get { discovery_id: Uuid },
    Windows { discovery_id: Uuid },
}

#[derive(Debug, Subcommand)]
pub enum ResearchRegimeCalibrationCommands {
    Run(ResearchRegimeCalibrationRunArgs),
    List(ResearchBatchListArgs),
    Get { calibration_id: Uuid },
    Candidates { calibration_id: Uuid },
}

#[derive(Debug, Args)]
pub struct ResearchRegimeCalibrationRunArgs {
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "scan-start")]
    pub scan_start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "scan-end")]
    pub scan_end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "window-hours")]
    pub window_hours: i64,
    #[arg(long = "step-hours")]
    pub step_hours: i64,
    #[arg(long = "target-min-windows-per-regime", default_value_t = 5)]
    pub target_min_windows_per_regime: u32,
}

#[derive(Debug, Args)]
pub struct ResearchRegimeDiscoveryRunArgs {
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "scan-start")]
    pub scan_start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "scan-end")]
    pub scan_end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "window-hours")]
    pub window_hours: i64,
    #[arg(long = "step-hours")]
    pub step_hours: i64,
    #[arg(long = "target-regimes", value_delimiter = ',')]
    pub target_regimes: Option<Vec<String>>,
    #[arg(long = "max-windows-per-regime", default_value_t = 20)]
    pub max_windows_per_regime: u32,
    #[arg(long = "min-confidence")]
    pub min_confidence: Option<rust_decimal::Decimal>,
    #[arg(long = "allow-missing-candles", default_value_t = false)]
    pub allow_missing_candles: bool,
    #[arg(long = "auto-backfill-missing", default_value_t = false)]
    pub auto_backfill_missing: bool,
    #[arg(long = "calibration-id")]
    pub calibration_id: Option<Uuid>,
}

#[derive(Debug, Subcommand)]
pub enum ResearchRobustnessMatrixCommands {
    Run(ResearchRobustnessMatrixRunArgs),
    List(ResearchBatchListArgs),
    Get { run_id: Uuid },
    Cells { run_id: Uuid },
}

#[derive(Debug, Args)]
pub struct ResearchRobustnessMatrixRunArgs {
    #[arg(long = "strategies", value_delimiter = ',')]
    pub strategies: Vec<String>,
    #[arg(long = "symbols", value_delimiter = ',')]
    pub symbols: Vec<String>,
    #[arg(long = "timeframes", value_delimiter = ',')]
    pub timeframes: Vec<String>,
    #[arg(long = "start")]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "window-hours")]
    pub window_hours: i64,
    #[arg(long = "step-hours")]
    pub step_hours: i64,
    #[arg(long = "initial-capital", default_value = "10000")]
    pub initial_capital: rust_decimal::Decimal,
    #[arg(long = "fee-bps", default_value = "10")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps", default_value = "5")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "holding-candles")]
    pub holding_candles: Option<u32>,
    #[arg(long = "min-trades-per-cell", default_value_t = 5)]
    pub min_trades_per_cell: i32,
    #[arg(long = "min-profitable-window-ratio", default_value = "0.5")]
    pub min_profitable_window_ratio: rust_decimal::Decimal,
}

#[derive(Debug, Subcommand)]
pub enum ResearchDataCommands {
    Coverage(ResearchDataCoverageArgs),
    Build(ResearchDataBuildArgs),
    Builds(ResearchDataBuildsArgs),
    BuildGet { build_id: Uuid },
}

#[derive(Debug, Subcommand)]
pub enum ResearchBatchCommands {
    Run(ResearchBatchRunArgs),
    List(ResearchBatchListArgs),
    Get { batch_id: Uuid },
    Steps { batch_id: Uuid },
    Triage { batch_id: Uuid },
}

#[derive(Debug, Args)]
pub struct ResearchBatchRunArgs {
    #[arg(long = "strategy")]
    pub strategy: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long = "base-interval", default_value = "1m")]
    pub base_interval: String,
    #[arg(long = "target-intervals", value_delimiter = ',')]
    pub target_intervals: Vec<String>,
    #[arg(long = "start")]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "initial-capital", default_value = "10000")]
    pub initial_capital: rust_decimal::Decimal,
    #[arg(long = "fee-bps", default_value = "10")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps", default_value = "5")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "experiment-timeframes", value_delimiter = ',')]
    pub experiment_timeframes: Vec<String>,
    #[arg(long = "lookbacks", value_delimiter = ',')]
    pub lookbacks: Vec<u32>,
    #[arg(long = "trend-lookbacks", value_delimiter = ',')]
    pub trend_lookbacks: Option<Vec<u32>>,
    #[arg(long = "momentum-lookbacks", value_delimiter = ',')]
    pub momentum_lookbacks: Option<Vec<u32>>,
    #[arg(long = "breakout-lookbacks", value_delimiter = ',')]
    pub breakout_lookbacks: Option<Vec<u32>>,
    #[arg(long = "min-close-above-sma-pct", value_delimiter = ',')]
    pub min_close_above_sma_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "max-close-above-sma-pct", value_delimiter = ',')]
    pub max_close_above_sma_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "min-momentum-return-pct", value_delimiter = ',')]
    pub min_momentum_return_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "holding-candles", value_delimiter = ',')]
    pub holding_candles: Option<Vec<u32>>,
    #[arg(long = "walk-forward-top-n", default_value_t = 3)]
    pub walk_forward_top_n: u32,
    #[arg(long = "max-candidates", default_value_t = 3)]
    pub max_candidates: u32,
    #[arg(long = "no-repair-degraded-data", default_value_t = false)]
    pub no_repair_degraded_data: bool,
    #[arg(long = "no-create-candidates", default_value_t = false)]
    pub no_create_candidates: bool,
    #[arg(long = "candidate-creation-mode")]
    pub candidate_creation_mode: Option<String>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct ResearchBatchListArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Debug, Subcommand)]
pub enum ResearchCampaignCommands {
    Run(ResearchCampaignRunArgs),
    List(ResearchBatchListArgs),
    Get {
        campaign_id: Uuid,
    },
    Batches {
        campaign_id: Uuid,
    },
    Summary {
        campaign_id: Uuid,
    },
    FailureAttribution {
        campaign_id: Uuid,
    },
    #[command(name = "regime-leaderboard")]
    RegimeLeaderboard {
        campaign_id: Uuid,
    },
}

#[derive(Debug, Args)]
pub struct ResearchCampaignRunArgs {
    #[arg(long = "strategies", value_delimiter = ',')]
    pub strategies: Vec<String>,
    #[arg(long = "symbols", value_delimiter = ',')]
    pub symbols: Vec<String>,
    #[arg(long = "timeframes", value_delimiter = ',')]
    pub timeframes: Vec<String>,
    #[arg(long = "start")]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "window-hours")]
    pub window_hours: i64,
    #[arg(long = "step-hours")]
    pub step_hours: i64,
    #[arg(long = "initial-capital", default_value = "10000")]
    pub initial_capital: rust_decimal::Decimal,
    #[arg(long = "fee-bps", default_value = "10")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps", default_value = "5")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "max-batches")]
    pub max_batches: Option<u32>,
    #[arg(long = "regime-dataset-id")]
    pub regime_dataset_id: Option<Uuid>,
    #[arg(long = "target-regimes", value_delimiter = ',')]
    pub target_regimes: Option<Vec<String>>,
    #[arg(long = "max-windows-per-regime")]
    pub max_windows_per_regime: Option<u32>,
    #[arg(long = "max-candidates-per-batch", default_value_t = 3)]
    pub max_candidates_per_batch: u32,
    #[arg(long = "walk-forward-top-n", default_value_t = 3)]
    pub walk_forward_top_n: u32,
    #[arg(long = "base-interval", default_value = "1m")]
    pub base_interval: String,
    #[arg(long = "lookbacks", value_delimiter = ',', default_value = "10,20,50")]
    pub lookbacks: Vec<u32>,
    #[arg(long = "trend-lookbacks", value_delimiter = ',')]
    pub trend_lookbacks: Option<Vec<u32>>,
    #[arg(long = "momentum-lookbacks", value_delimiter = ',')]
    pub momentum_lookbacks: Option<Vec<u32>>,
    #[arg(long = "breakout-lookbacks", value_delimiter = ',')]
    pub breakout_lookbacks: Option<Vec<u32>>,
    #[arg(long = "min-close-above-sma-pct", value_delimiter = ',')]
    pub min_close_above_sma_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "max-close-above-sma-pct", value_delimiter = ',')]
    pub max_close_above_sma_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "min-momentum-return-pct", value_delimiter = ',')]
    pub min_momentum_return_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "holding-candles", value_delimiter = ',')]
    pub holding_candles: Option<Vec<u32>>,
    #[arg(long = "no-repair-degraded-data", default_value_t = false)]
    pub no_repair_degraded_data: bool,
    #[arg(long = "candidate-creation-mode")]
    pub candidate_creation_mode: Option<String>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Subcommand)]
pub enum ResearchCandidateCommands {
    List(ResearchCandidateListArgs),
    Watchlist(ResearchCandidateWatchlistArgs),
    Get { candidate_id: Uuid },
    Events { candidate_id: Uuid },
    Reviews { candidate_id: Uuid },
    Observations { candidate_id: Uuid },
    ObservationSummary { candidate_id: Uuid },
    Qualification(ResearchCandidateQualificationArgs),
    QualificationEvaluate(ResearchCandidateQualificationArgs),
    QualificationHistory(ResearchCandidateQualificationHistoryArgs),
    TestnetReviewDossier { candidate_id: Uuid },
    AcceptShadowPreview { candidate_id: Uuid },
    WalkForward { candidate_id: Uuid },
    LinkWalkForward(ResearchCandidateLinkWalkForwardArgs),
    ShadowPerformance(ResearchCandidateShadowWindowArgs),
    ShadowPnl(ResearchCandidateShadowPnlArgs),
    ShadowRuns(ResearchCandidateShadowRunsArgs),
    Create(ResearchCandidateCreateArgs),
    FromExperimentRun(ResearchCandidateFromExperimentRunArgs),
    Observe { candidate_id: Uuid },
    Review(ResearchCandidateReviewArgs),
    Decide(ResearchCandidateDecideArgs),
    PromoteShadowPreview(ResearchCandidatePromoteShadowPreviewArgs),
    PromoteShadowApply(ResearchCandidatePromoteShadowApplyArgs),
}

#[derive(Debug, Args)]
pub struct ResearchCandidateListArgs {
    #[arg(long = "strategy")]
    pub strategy_id: Option<String>,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<String>,
    #[arg(long)]
    pub status: Option<String>,
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateCreateArgs {
    #[arg(long = "strategy")]
    pub strategy_id: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "config-json")]
    pub config_json: String,
    #[arg(long)]
    pub notes: Option<String>,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateFromExperimentRunArgs {
    pub run_id: Uuid,
    #[arg(long = "walk-forward-run-id")]
    pub walk_forward_run_id: Option<Uuid>,
    #[arg(long)]
    pub notes: Option<String>,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateLinkWalkForwardArgs {
    pub candidate_id: Uuid,
    #[arg(long = "run-id")]
    pub walk_forward_run_id: Uuid,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateDecideArgs {
    pub candidate_id: Uuid,
    #[arg(long)]
    pub decision: ResearchCandidateDecision,
    #[arg(long)]
    pub reason: Option<String>,
    #[arg(long)]
    pub notes: Option<String>,
    #[arg(long, default_value_t = false)]
    pub acknowledge_runner_mismatch: bool,
    #[arg(long, default_value_t = false)]
    pub acknowledge_overfit_risk: bool,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateReviewArgs {
    pub candidate_id: Uuid,
    #[arg(long)]
    pub action: ResearchCandidateReviewAction,
    #[arg(long)]
    pub reason: Option<String>,
    #[arg(long)]
    pub notes: Option<String>,
    #[arg(long)]
    pub qualification_evaluation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct ResearchCandidatePromoteShadowPreviewArgs {
    pub candidate_id: Uuid,
    #[arg(long, default_value_t = false)]
    pub allow_missing_runner_alignment: bool,
}

#[derive(Debug, Args)]
pub struct ResearchCandidatePromoteShadowApplyArgs {
    pub candidate_id: Uuid,
    #[arg(long)]
    pub confirm: Option<String>,
    #[arg(long, default_value_t = false)]
    pub allow_missing_runner_alignment: bool,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateQualificationArgs {
    pub candidate_id: Uuid,
    #[arg(long)]
    pub min_shadow_runs: Option<i64>,
    #[arg(long)]
    pub min_would_submit_count: Option<i64>,
    #[arg(long)]
    pub max_risk_rejection_rate_pct: Option<rust_decimal::Decimal>,
    #[arg(long)]
    pub max_error_or_skipped_rate_pct: Option<rust_decimal::Decimal>,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateQualificationHistoryArgs {
    pub candidate_id: Uuid,
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateWatchlistArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
}

impl ResearchCandidateQualificationArgs {
    pub fn thresholds(&self) -> ResearchCandidateQualificationThresholds {
        let mut thresholds = ResearchCandidateQualificationThresholds::default();
        if let Some(value) = self.min_shadow_runs {
            thresholds.min_shadow_runs = value.max(0);
        }
        if let Some(value) = self.min_would_submit_count {
            thresholds.min_would_submit_count = value.max(0);
        }
        if let Some(value) = self.max_risk_rejection_rate_pct {
            thresholds.max_risk_rejection_rate_pct = value.max(rust_decimal::Decimal::ZERO);
        }
        if let Some(value) = self.max_error_or_skipped_rate_pct {
            thresholds.max_error_or_skipped_rate_pct = value.max(rust_decimal::Decimal::ZERO);
        }
        thresholds
    }
}

#[derive(Debug, Args)]
pub struct ResearchCandidateShadowWindowArgs {
    pub candidate_id: Uuid,
    #[arg(long = "start")]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long = "end")]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateShadowPnlArgs {
    pub candidate_id: Uuid,
    #[arg(long = "holding-windows", default_value = "1,3,5,10")]
    pub holding_windows: String,
    #[arg(long = "fee-bps", default_value = "10")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps", default_value = "5")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "extreme-pnl-threshold-pct", default_value = "5")]
    pub extreme_pnl_threshold_pct: rust_decimal::Decimal,
}

#[derive(Debug, Args)]
pub struct ResearchCandidateShadowRunsArgs {
    pub candidate_id: Uuid,
    #[arg(long = "start")]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long = "end")]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    #[arg(long, default_value_t = 100)]
    pub limit: i64,
}

impl From<&ResearchCandidatePromoteShadowPreviewArgs> for ResearchCandidateShadowPromotionRequest {
    fn from(value: &ResearchCandidatePromoteShadowPreviewArgs) -> Self {
        Self {
            mode: ResearchCandidateShadowPromotionMode::PreviewOnly,
            allow_missing_runner_alignment: value.allow_missing_runner_alignment,
            confirmation_text: None,
            correlation_id: None,
        }
    }
}

impl From<&ResearchCandidatePromoteShadowApplyArgs> for ResearchCandidateShadowPromotionRequest {
    fn from(value: &ResearchCandidatePromoteShadowApplyArgs) -> Self {
        Self {
            mode: ResearchCandidateShadowPromotionMode::Apply,
            allow_missing_runner_alignment: value.allow_missing_runner_alignment,
            confirmation_text: value.confirm.clone(),
            correlation_id: None,
        }
    }
}

#[derive(Debug, Args)]
pub struct ResearchDataCoverageArgs {
    #[arg(long, default_value = "binance")]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub intervals: String,
    #[arg(long)]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long)]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "required-coverage-pct")]
    pub required_coverage_pct: Option<rust_decimal::Decimal>,
}

#[derive(Debug, Args)]
pub struct ResearchDataBuildArgs {
    #[arg(long, default_value = "binance")]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub intervals: String,
    #[arg(long)]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long)]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "required-coverage-pct")]
    pub required_coverage_pct: Option<rust_decimal::Decimal>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct ResearchDataBuildsArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
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
pub struct MarketRepairArgs {
    #[arg(long, default_value = "binance")]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long = "start")]
    pub start_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "max-ranges", default_value_t = 100)]
    pub max_ranges: i32,
    #[arg(long = "no-reaggregate-derived-intervals", default_value_t = false)]
    pub no_reaggregate_derived_intervals: bool,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct MarketProviderHealthArgs {
    #[arg(long, default_value = "binance")]
    pub provider: String,
}

#[derive(Debug, Args)]
pub struct MarketAggregateCandlesArgs {
    #[arg(long, default_value = "binance")]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long = "source")]
    pub source_interval: String,
    #[arg(long = "target")]
    pub target_interval: String,
    #[arg(long = "start")]
    pub start_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct MarketCandleCoverageArgs {
    #[arg(long)]
    pub symbol: String,
}

#[derive(Debug, Args)]
pub struct MarketCandleQualityArgs {
    #[arg(long, default_value = "binance")]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub interval: String,
    #[arg(long = "start")]
    pub start_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end_time: chrono::DateTime<chrono::Utc>,
    #[arg(long = "expected-interval-seconds")]
    pub expected_interval_seconds: Option<i64>,
    #[arg(long = "max-allowed-gap-count")]
    pub max_allowed_gap_count: Option<i64>,
    #[arg(long = "max-allowed-gap-pct")]
    pub max_allowed_gap_pct: Option<rust_decimal::Decimal>,
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
pub enum ExperimentCommands {
    #[command(subcommand)]
    Strategy(StrategyExperimentCommands),
}

#[derive(Debug, Subcommand)]
pub enum StrategyExperimentCommands {
    Run(StrategyExperimentRunArgs),
    MultiTimeframe(StrategyMultiTimeframeExperimentRunArgs),
    WalkForward(StrategyWalkForwardRunArgs),
    WalkForwardList(BacktestListArgs),
    WalkForwardGet { walk_forward_id: Uuid },
    WalkForwardWindows { walk_forward_id: Uuid },
    List(BacktestListArgs),
    Get { experiment_id: Uuid },
    Runs { experiment_id: Uuid },
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

#[derive(Debug, Args)]
pub struct StrategyExperimentRunArgs {
    #[arg(long = "strategy")]
    pub strategy: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "start")]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "initial-capital")]
    pub initial_capital: rust_decimal::Decimal,
    #[arg(long = "fee-bps")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "lookbacks", value_delimiter = ',')]
    pub lookbacks: Vec<u32>,
    #[arg(long = "trend-lookbacks", value_delimiter = ',')]
    pub trend_lookbacks: Option<Vec<u32>>,
    #[arg(long = "momentum-lookbacks", value_delimiter = ',')]
    pub momentum_lookbacks: Option<Vec<u32>>,
    #[arg(long = "breakout-lookbacks", value_delimiter = ',')]
    pub breakout_lookbacks: Option<Vec<u32>>,
    #[arg(long = "min-close-above-sma-pct", value_delimiter = ',')]
    pub min_close_above_sma_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "max-close-above-sma-pct", value_delimiter = ',')]
    pub max_close_above_sma_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "min-momentum-return-pct", value_delimiter = ',')]
    pub min_momentum_return_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "holding-candles", value_delimiter = ',')]
    pub holding_candles: Option<Vec<u32>>,
    #[arg(long = "stop-loss-pct", value_delimiter = ',')]
    pub stop_loss_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "take-profit-pct", value_delimiter = ',')]
    pub take_profit_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "max-signal-age-ms")]
    pub max_signal_age_ms: Option<i64>,
    #[arg(long = "max-runs")]
    pub max_runs: Option<u32>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct StrategyMultiTimeframeExperimentRunArgs {
    #[arg(long = "strategy")]
    pub strategy: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long = "timeframes", value_delimiter = ',')]
    pub timeframes: Vec<String>,
    #[arg(long = "start")]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "initial-capital")]
    pub initial_capital: rust_decimal::Decimal,
    #[arg(long = "fee-bps")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "lookbacks", value_delimiter = ',')]
    pub lookbacks: Vec<u32>,
    #[arg(long = "trend-lookbacks", value_delimiter = ',')]
    pub trend_lookbacks: Option<Vec<u32>>,
    #[arg(long = "momentum-lookbacks", value_delimiter = ',')]
    pub momentum_lookbacks: Option<Vec<u32>>,
    #[arg(long = "breakout-lookbacks", value_delimiter = ',')]
    pub breakout_lookbacks: Option<Vec<u32>>,
    #[arg(long = "min-close-above-sma-pct", value_delimiter = ',')]
    pub min_close_above_sma_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "max-close-above-sma-pct", value_delimiter = ',')]
    pub max_close_above_sma_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "min-momentum-return-pct", value_delimiter = ',')]
    pub min_momentum_return_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "holding-candles", value_delimiter = ',')]
    pub holding_candles: Option<Vec<u32>>,
    #[arg(long = "stop-loss-pct", value_delimiter = ',')]
    pub stop_loss_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "take-profit-pct", value_delimiter = ',')]
    pub take_profit_pct: Option<Vec<rust_decimal::Decimal>>,
    #[arg(long = "max-signal-age-ms")]
    pub max_signal_age_ms: Option<i64>,
    #[arg(long = "max-runs")]
    pub max_runs: Option<u32>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct StrategyWalkForwardRunArgs {
    #[arg(long = "strategy")]
    pub strategy: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: String,
    #[arg(long = "start")]
    pub start: chrono::DateTime<chrono::Utc>,
    #[arg(long = "end")]
    pub end: chrono::DateTime<chrono::Utc>,
    #[arg(long = "experiment-run-id")]
    pub experiment_run_id: Option<Uuid>,
    #[arg(long = "config-json")]
    pub config_json: Option<serde_json::Value>,
    #[arg(
        long = "train-hours",
        alias = "train-window-hours",
        default_value_t = 0
    )]
    pub train_hours: i64,
    #[arg(long = "test-hours", alias = "test-window-hours")]
    pub test_hours: i64,
    #[arg(long = "step-hours", alias = "step-window-hours")]
    pub step_hours: i64,
    #[arg(long = "initial-capital")]
    pub initial_capital: rust_decimal::Decimal,
    #[arg(long = "fee-bps")]
    pub fee_bps: rust_decimal::Decimal,
    #[arg(long = "slippage-bps")]
    pub slippage_bps: rust_decimal::Decimal,
    #[arg(long = "lookback-candles", default_value_t = 0)]
    pub lookback_candles: u32,
    #[arg(long = "trend-lookback")]
    pub trend_lookback: Option<u32>,
    #[arg(long = "momentum-lookback")]
    pub momentum_lookback: Option<u32>,
    #[arg(long = "breakout-lookback")]
    pub breakout_lookback: Option<u32>,
    #[arg(long = "holding-candles")]
    pub holding_candles: Option<u32>,
    #[arg(long = "stop-loss-pct")]
    pub stop_loss_pct: Option<rust_decimal::Decimal>,
    #[arg(long = "take-profit-pct")]
    pub take_profit_pct: Option<rust_decimal::Decimal>,
    #[arg(long = "max-signal-age-ms")]
    pub max_signal_age_ms: Option<i64>,
    #[arg(long = "min-required-test-windows", alias = "min-windows")]
    pub min_required_test_windows: Option<u32>,
    #[arg(long = "correlation-id")]
    pub correlation_id: Option<Uuid>,
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

    #[test]
    fn research_candidate_observe_parses_candidate_id() {
        let candidate_id =
            Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("valid uuid");
        let cli = Cli::try_parse_from([
            "aegis",
            "research",
            "candidates",
            "observe",
            &candidate_id.to_string(),
        ])
        .expect("cli parses");

        let Commands::Research(super::ResearchCommands::Candidates(
            super::ResearchCandidateCommands::Observe {
                candidate_id: parsed_candidate_id,
            },
        )) = cli.command
        else {
            panic!("expected research observe command");
        };

        assert_eq!(parsed_candidate_id, candidate_id);
    }

    #[test]
    fn research_candidate_observations_and_summary_parse_candidate_id() {
        let candidate_id =
            Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("valid uuid");

        let observations = Cli::try_parse_from([
            "aegis",
            "research",
            "candidates",
            "observations",
            &candidate_id.to_string(),
        ])
        .expect("observations cli parses");
        assert!(matches!(
            observations.command,
            Commands::Research(super::ResearchCommands::Candidates(
                super::ResearchCandidateCommands::Observations {
                    candidate_id: parsed_candidate_id,
                },
            )) if parsed_candidate_id == candidate_id
        ));

        let summary = Cli::try_parse_from([
            "aegis",
            "research",
            "candidates",
            "observation-summary",
            &candidate_id.to_string(),
        ])
        .expect("summary cli parses");
        assert!(matches!(
            summary.command,
            Commands::Research(super::ResearchCommands::Candidates(
                super::ResearchCandidateCommands::ObservationSummary {
                    candidate_id: parsed_candidate_id,
                },
            )) if parsed_candidate_id == candidate_id
        ));
    }
}

use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

pub const RESUME_CONFIRMATION_TEXT: &str = "RESUME TRADING";

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

        Ok(())
    }
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Status,
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
}

#[derive(Debug, Args)]
pub struct ResumeArgs {
    #[arg(long, help = "Must match RESUME TRADING exactly")]
    pub confirm: Option<String>,
    #[arg(long)]
    pub reason: Option<String>,
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
    Enable { strategy_id: String },
    Disable { strategy_id: String },
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
    Decisions(RiskDecisionsArgs),
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
    Pnl,
    Equity(PaperListArgs),
    Journal(PaperListArgs),
    Mark,
}

#[derive(Debug, Args)]
pub struct PaperListArgs {
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
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
    use super::{Cli, RESUME_CONFIRMATION_TEXT};
    use clap::Parser;

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
}

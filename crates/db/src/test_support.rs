use std::{env, path::PathBuf, str::FromStr};

use anyhow::{bail, Context, Result};
use sqlx::{
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::{ensure_system_state, PgPool};

const TEST_TABLES: &[&str] = &[
    "microstructure_liquidity_metrics",
    "microstructure_imbalance_metrics",
    "microstructure_spread_metrics",
    "microstructure_collector_runs",
    "exchange_private_stream_events",
    "exchange_private_stream_state",
    "exchange_reconciliation_mismatches",
    "exchange_reconciliation_runs",
    "research_stale_run_recoveries",
    "candidate_review_events",
    "cross_asset_candidate_shadow_observation_rankings",
    "cross_asset_candidate_shadow_observations",
    "cross_asset_robustness_matrix_cells",
    "cross_asset_robustness_matrix_runs",
    "cross_asset_research_windows",
    "cross_asset_research_trades",
    "cross_asset_research_runs",
    "research_experiment_plan_runs",
    "research_experiment_plan_events",
    "research_experiment_plans",
    "research_hypotheses",
    "research_campaign_batches",
    "research_campaigns",
    "research_batch_steps",
    "research_candidate_proposals",
    "research_batches",
    "research_regime_calibration_candidates",
    "research_regime_calibrations",
    "research_regime_discovery_windows",
    "research_regime_discoveries",
    "research_regime_windows",
    "research_regime_datasets",
    "research_dataset_build_steps",
    "research_dataset_builds",
    "testnet_shadow_promotions",
    "scheduled_research_job_runs",
    "scheduled_research_jobs",
    "testnet_shadow_runner_state",
    "testnet_shadow_runner_config",
    "exchange_testnet_repair_actions",
    "exchange_testnet_orders",
    "paper_trade_journal",
    "paper_equity_snapshots",
    "paper_fills",
    "paper_positions",
    "paper_accounts",
    "backtest_equity_curve",
    "backtest_trades",
    "backtest_runs",
    "strategy_walk_forward_windows",
    "strategy_walk_forward_runs",
    "strategy_candidate_observation_checks",
    "strategy_candidate_observations",
    "audit_logs",
    "system_events",
    "orders",
    "risk_decisions",
    "strategy_state",
    "signals",
    "strategy_configs",
    "market_feed_status",
    "candles",
    "market_ticks",
    "system_state",
    "symbols",
    "sessions",
    "users",
];

#[derive(Debug)]
pub struct TestDatabase {
    pub pool: PgPool,
    pub database_url: String,
}

impl TestDatabase {
    pub async fn setup() -> Result<Self> {
        let database_url = resolve_test_database_url()?;
        assert_safe_test_database(&database_url)?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .with_context(|| format!("failed to connect to test database: {database_url}"))?;

        run_migrations(&pool).await?;
        reset_database(&pool).await?;
        ensure_system_state(&pool).await?;

        Ok(Self { pool, database_url })
    }
}

pub fn resolve_test_database_url() -> Result<String> {
    env::var("TEST_DATABASE_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .context("set TEST_DATABASE_URL or DATABASE_URL before running DB integration tests")
}

pub fn assert_safe_test_database(database_url: &str) -> Result<()> {
    if env::var("ALLOW_NON_TEST_DB").as_deref() == Ok("1") {
        return Ok(());
    }

    let options = PgConnectOptions::from_str(database_url)
        .with_context(|| format!("invalid Postgres connection string: {database_url}"))?;
    let database_name = options
        .get_database()
        .ok_or_else(|| anyhow::anyhow!("database name is missing from connection string"))?;

    if !database_name.to_ascii_lowercase().contains("test") {
        bail!(
            "refusing to run integration tests against database '{database_name}'. \
             Use a database name containing 'test' or set ALLOW_NON_TEST_DB=1 explicitly."
        );
    }

    Ok(())
}

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    let migrations_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let migrator = Migrator::new(migrations_dir.as_path()).await?;
    migrator.run(pool).await?;
    Ok(())
}

pub async fn reset_database(pool: &PgPool) -> Result<()> {
    let truncate_sql = format!(
        "TRUNCATE TABLE {} RESTART IDENTITY CASCADE",
        TEST_TABLES.join(", ")
    );
    sqlx::query(&truncate_sql).execute(pool).await?;
    Ok(())
}

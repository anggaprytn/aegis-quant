use std::{fs, path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use db::{
    baseline_migrations, ensure_schema_migrations_table, load_migration_files, migration_status,
    run_pending_migrations, test_support, MigrationBaselineConfig, MigrationRunConfig,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use uuid::Uuid;

struct IsolatedMigrationDb {
    pool: PgPool,
    admin_pool: PgPool,
    schema: String,
}

impl IsolatedMigrationDb {
    async fn setup() -> Result<Self> {
        let database_url = test_support::resolve_test_database_url()?;
        test_support::assert_safe_test_database(&database_url)?;
        let schema = format!("migration_{}", Uuid::new_v4().simple());
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin_pool)
            .await?;
        let options =
            PgConnectOptions::from_str(&database_url)?.options([("search_path", &schema)]);
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Ok(Self {
            pool,
            admin_pool,
            schema,
        })
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin_pool)
            .await?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn temp_migrations_dir(files: &[(&str, &str)]) -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("aegis-migrations-{}", Uuid::new_v4()));
    fs::create_dir_all(&dir)?;
    for (filename, contents) in files {
        fs::write(dir.join(filename), contents)?;
    }
    Ok(dir)
}

async fn table_exists(pool: &PgPool, table: &str) -> Result<bool> {
    let exists = sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::TEXT")
        .bind(table)
        .fetch_one(pool)
        .await?;
    Ok(exists.is_some())
}

async fn ledger_count(pool: &PgPool) -> Result<i64> {
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM schema_migrations")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

async fn create_baseline_safety_tables(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE TABLE scheduled_research_jobs (id INT PRIMARY KEY);
        CREATE TABLE research_candidates (id INT PRIMARY KEY);
        CREATE TABLE derivatives_funding_rates (id INT PRIMARY KEY);
        CREATE TABLE candidate_review_events (id INT PRIMARY KEY);
        CREATE TABLE symbols (id INT PRIMARY KEY);
        CREATE TABLE market_ticks (id INT PRIMARY KEY);
        CREATE TABLE candles (id INT PRIMARY KEY);
        CREATE TABLE research_batches (id INT PRIMARY KEY);
        CREATE TABLE strategy_experiments (id INT PRIMARY KEY);
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

fn run_config(dir: PathBuf) -> MigrationRunConfig {
    MigrationRunConfig {
        migrations_dir: dir,
        applied_by: Some("migration-ledger-test".to_string()),
    }
}

fn baseline_config(dir: PathBuf, dry_run: bool) -> MigrationBaselineConfig {
    MigrationBaselineConfig {
        migrations_dir: dir,
        up_to: "0073".to_string(),
        confirm_production_baseline: true,
        dry_run,
        applied_by: Some("migration-ledger-test".to_string()),
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn migration_ledger_table_creation() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    ensure_schema_migrations_table(&db.pool).await?;
    assert!(table_exists(&db.pool, "schema_migrations").await?);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn migration_skipped_when_already_applied() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    let dir = temp_migrations_dir(&[(
        "0001_create_skipped_probe.sql",
        "CREATE TABLE skipped_probe (id INT);",
    )])?;
    ensure_schema_migrations_table(&db.pool).await?;
    let file = load_migration_files(&dir)?
        .pop()
        .context("migration file should load")?;
    sqlx::query(
        r#"
        INSERT INTO schema_migrations (version, filename, checksum_sha256, applied_by)
        VALUES ($1, $2, $3, 'test')
        "#,
    )
    .bind(&file.version)
    .bind(&file.filename)
    .bind(&file.checksum_sha256)
    .execute(&db.pool)
    .await?;

    let report = run_pending_migrations(&db.pool, &run_config(dir)).await?;
    assert_eq!(report.applied.len(), 0);
    assert_eq!(report.skipped.len(), 1);
    assert!(!table_exists(&db.pool, "skipped_probe").await?);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn pending_migration_applied_once() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    let dir = temp_migrations_dir(&[(
        "0001_create_pending_once.sql",
        "CREATE TABLE pending_once (id INT);",
    )])?;

    let first = run_pending_migrations(&db.pool, &run_config(dir.clone())).await?;
    let second = run_pending_migrations(&db.pool, &run_config(dir)).await?;

    assert_eq!(first.applied.len(), 1);
    assert_eq!(second.applied.len(), 0);
    assert_eq!(second.skipped.len(), 1);
    assert!(table_exists(&db.pool, "pending_once").await?);
    assert_eq!(ledger_count(&db.pool).await?, 1);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn checksum_mismatch_detected() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    let dir = temp_migrations_dir(&[(
        "0001_create_checksum_probe.sql",
        "CREATE TABLE checksum_probe (id INT);",
    )])?;
    ensure_schema_migrations_table(&db.pool).await?;
    sqlx::query(
        r#"
        INSERT INTO schema_migrations (version, filename, checksum_sha256, applied_by)
        VALUES ('0001', '0001_create_checksum_probe.sql', 'not-the-real-checksum', 'test')
        "#,
    )
    .execute(&db.pool)
    .await?;

    let err = run_pending_migrations(&db.pool, &run_config(dir))
        .await
        .expect_err("checksum mismatch should stop migration");
    assert!(err.to_string().contains("checksum mismatch"));
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn baseline_dry_run_records_nothing() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    create_baseline_safety_tables(&db.pool).await?;
    let dir = temp_migrations_dir(&[
        (
            "0001_create_dry_run_probe.sql",
            "CREATE TABLE dry_run_probe (id INT);",
        ),
        (
            "0073_create_dry_run_marker.sql",
            "CREATE TABLE dry_run_marker (id INT);",
        ),
    ])?;

    let report = baseline_migrations(&db.pool, &baseline_config(dir, true)).await?;

    assert!(report.dry_run);
    assert_eq!(report.would_record.len(), 2);
    assert_eq!(ledger_count(&db.pool).await?, 0);
    assert!(!table_exists(&db.pool, "dry_run_marker").await?);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn baseline_records_without_executing_sql() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    create_baseline_safety_tables(&db.pool).await?;
    let dir = temp_migrations_dir(&[
        (
            "0001_create_baseline_probe.sql",
            "CREATE TABLE baseline_probe (id INT);",
        ),
        (
            "0073_create_baseline_marker.sql",
            "CREATE TABLE baseline_marker (id INT);",
        ),
    ])?;

    let report = baseline_migrations(&db.pool, &baseline_config(dir, false)).await?;

    assert_eq!(report.recorded.len(), 2);
    assert_eq!(ledger_count(&db.pool).await?, 2);
    assert!(!table_exists(&db.pool, "baseline_marker").await?);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn baseline_refuses_empty_db() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    let dir = temp_migrations_dir(&[(
        "0073_create_empty_db_probe.sql",
        "CREATE TABLE empty_db_probe (id INT);",
    )])?;

    let err = baseline_migrations(&db.pool, &baseline_config(dir, false))
        .await
        .expect_err("empty DB should be refused");
    assert!(err.to_string().contains("database schema is empty"));
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn failed_migration_stops_sequence() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    let dir = temp_migrations_dir(&[
        (
            "0001_fail_after_create.sql",
            "CREATE TABLE failed_first (id INT); SELECT * FROM missing_table;",
        ),
        (
            "0002_create_after_failure.sql",
            "CREATE TABLE after_failure (id INT);",
        ),
    ])?;

    let err = run_pending_migrations(&db.pool, &run_config(dir))
        .await
        .expect_err("failed migration should stop sequence");
    assert!(err
        .to_string()
        .contains("migration 0001_fail_after_create.sql failed"));
    assert_eq!(ledger_count(&db.pool).await?, 0);
    assert!(!table_exists(&db.pool, "after_failure").await?);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL or DATABASE_URL pointing to a test database"]
async fn status_output_includes_pending_migrations() -> Result<()> {
    let db = IsolatedMigrationDb::setup().await?;
    let dir = temp_migrations_dir(&[
        (
            "0001_create_status_applied.sql",
            "CREATE TABLE status_applied (id INT);",
        ),
        (
            "0002_create_status_pending.sql",
            "CREATE TABLE status_pending (id INT);",
        ),
    ])?;
    ensure_schema_migrations_table(&db.pool).await?;
    let files = load_migration_files(&dir)?;
    sqlx::query(
        r#"
        INSERT INTO schema_migrations (version, filename, checksum_sha256, applied_by)
        VALUES ($1, $2, $3, 'test')
        "#,
    )
    .bind(&files[0].version)
    .bind(&files[0].filename)
    .bind(&files[0].checksum_sha256)
    .execute(&db.pool)
    .await?;

    let report = migration_status(&db.pool, &dir).await?;

    assert_eq!(report.applied_count, 1);
    assert_eq!(report.pending_count, 1);
    assert_eq!(report.pending[0].version, "0002");
    assert_eq!(report.pending[0].filename, "0002_create_status_pending.sql");
    db.cleanup().await
}

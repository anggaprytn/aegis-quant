use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::PgPool;

const LEDGER_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by TEXT,
    execution_ms BIGINT,
    success BOOLEAN NOT NULL DEFAULT TRUE
)
"#;

const AEGIS_BASELINE_REQUIRED_OBJECTS: &[&str] = &[
    "scheduled_research_jobs",
    "derivatives_funding_rates",
    "candidate_review_events",
    "symbols",
    "market_ticks",
    "candles",
    "research_batches",
    "strategy_experiments",
];

const AEGIS_BASELINE_CANDIDATE_OBJECTS: &[&str] =
    &["research_candidates", "strategy_research_candidates"];

const VERSIONED_OBJECTS: &[(&str, &str)] = &[
    ("symbols", "0001"),
    ("market_ticks", "0004"),
    ("candles", "0004"),
    ("strategy_experiments", "0022"),
    ("strategy_research_candidates", "0026"),
    ("research_candidates", "0029"),
    ("research_batches", "0038"),
    ("scheduled_research_jobs", "0052"),
    ("derivatives_funding_rates", "0071"),
    ("candidate_review_events", "0073"),
    ("microstructure_collector_runs", "0074"),
    ("microstructure_spread_metrics", "0074"),
    ("microstructure_imbalance_metrics", "0074"),
    ("microstructure_liquidity_metrics", "0074"),
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationFile {
    pub version: String,
    pub filename: String,
    pub checksum_sha256: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaMigrationRecord {
    pub version: String,
    pub filename: String,
    pub checksum_sha256: String,
    pub applied_at: DateTime<Utc>,
    pub applied_by: Option<String>,
    pub execution_ms: Option<i64>,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppliedMigration {
    pub version: String,
    pub filename: String,
    pub execution_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkippedMigration {
    pub version: String,
    pub filename: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChecksumMismatch {
    pub version: String,
    pub filename: String,
    pub expected_sha256: String,
    pub actual_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationRunReport {
    pub total_migrations: usize,
    pub applied: Vec<AppliedMigration>,
    pub skipped: Vec<SkippedMigration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationBaselineReport {
    pub target_version: String,
    pub dry_run: bool,
    pub total_considered: usize,
    pub already_applied: Vec<SkippedMigration>,
    pub recorded: Vec<SkippedMigration>,
    pub would_record: Vec<SkippedMigration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationStatusReport {
    pub total_migrations: usize,
    pub applied_count: usize,
    pub pending_count: usize,
    pub latest_applied: Option<SchemaMigrationRecord>,
    pub checksum_mismatches: Vec<ChecksumMismatch>,
    pub pending: Vec<MigrationFile>,
}

#[derive(Debug, Clone)]
pub struct MigrationRunConfig {
    pub migrations_dir: PathBuf,
    pub applied_by: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MigrationBaselineConfig {
    pub migrations_dir: PathBuf,
    pub up_to: String,
    pub confirm_production_baseline: bool,
    pub dry_run: bool,
    pub applied_by: Option<String>,
}

pub async fn ensure_schema_migrations_table(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(LEDGER_TABLE_SQL).execute(pool).await?;
    Ok(())
}

pub async fn run_pending_migrations(
    pool: &PgPool,
    config: &MigrationRunConfig,
) -> Result<MigrationRunReport> {
    ensure_schema_migrations_table(pool).await?;
    let files = load_migration_files(&config.migrations_dir)?;
    let ledger = load_schema_migration_records(pool).await?;
    let mut applied = Vec::new();
    let mut skipped = Vec::new();

    for file in &files {
        if let Some(record) = ledger.get(&file.version) {
            verify_checksum(file, record)?;
            skipped.push(SkippedMigration {
                version: file.version.clone(),
                filename: file.filename.clone(),
            });
            continue;
        }

        let sql = fs::read_to_string(&file.path)
            .with_context(|| format!("failed to read migration {}", file.path.display()))?;
        let started = Instant::now();
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(&sql)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("migration {} failed", file.filename))?;
        let execution_ms = started.elapsed().as_millis().try_into().unwrap_or(i64::MAX);
        sqlx::query(
            r#"
            INSERT INTO schema_migrations
                (version, filename, checksum_sha256, applied_by, execution_ms, success)
            VALUES ($1, $2, $3, $4, $5, TRUE)
            "#,
        )
        .bind(&file.version)
        .bind(&file.filename)
        .bind(&file.checksum_sha256)
        .bind(config.applied_by.as_deref())
        .bind(execution_ms)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        applied.push(AppliedMigration {
            version: file.version.clone(),
            filename: file.filename.clone(),
            execution_ms,
        });
    }

    Ok(MigrationRunReport {
        total_migrations: files.len(),
        applied,
        skipped,
    })
}

pub async fn baseline_migrations(
    pool: &PgPool,
    config: &MigrationBaselineConfig,
) -> Result<MigrationBaselineReport> {
    if !config.confirm_production_baseline {
        bail!("baseline refused: pass --confirm-production-baseline explicitly");
    }

    ensure_schema_migrations_table(pool).await?;
    let files = load_migration_files(&config.migrations_dir)?;
    if !files.iter().any(|file| file.version == config.up_to) {
        bail!(
            "baseline refused: target migration version {} does not exist",
            config.up_to
        );
    }
    let selected: Vec<MigrationFile> = files
        .into_iter()
        .filter(|file| file.version <= config.up_to)
        .collect();

    verify_existing_aegis_database_for_baseline(pool, &config.up_to).await?;

    let ledger = load_schema_migration_records(pool).await?;
    let mut already_applied = Vec::new();
    let mut to_record = Vec::new();
    for file in &selected {
        if let Some(record) = ledger.get(&file.version) {
            verify_checksum(file, record)?;
            already_applied.push(SkippedMigration {
                version: file.version.clone(),
                filename: file.filename.clone(),
            });
        } else {
            to_record.push(SkippedMigration {
                version: file.version.clone(),
                filename: file.filename.clone(),
            });
        }
    }

    if config.dry_run {
        return Ok(MigrationBaselineReport {
            target_version: config.up_to.clone(),
            dry_run: true,
            total_considered: selected.len(),
            already_applied,
            recorded: Vec::new(),
            would_record: to_record,
        });
    }

    let mut tx = pool.begin().await?;
    for file in selected
        .iter()
        .filter(|file| !ledger.contains_key(&file.version))
    {
        sqlx::query(
            r#"
            INSERT INTO schema_migrations
                (version, filename, checksum_sha256, applied_by, execution_ms, success)
            VALUES ($1, $2, $3, $4, NULL, TRUE)
            "#,
        )
        .bind(&file.version)
        .bind(&file.filename)
        .bind(&file.checksum_sha256)
        .bind(config.applied_by.as_deref())
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(MigrationBaselineReport {
        target_version: config.up_to.clone(),
        dry_run: false,
        total_considered: selected.len(),
        already_applied,
        recorded: to_record,
        would_record: Vec::new(),
    })
}

pub async fn migration_status(
    pool: &PgPool,
    migrations_dir: impl AsRef<Path>,
) -> Result<MigrationStatusReport> {
    ensure_schema_migrations_table(pool).await?;
    let files = load_migration_files(migrations_dir.as_ref())?;
    let ledger = load_schema_migration_records(pool).await?;
    let file_versions: BTreeSet<&str> = files.iter().map(|file| file.version.as_str()).collect();
    let mut pending = Vec::new();
    let mut checksum_mismatches = Vec::new();

    for file in &files {
        if let Some(record) = ledger.get(&file.version) {
            if record.checksum_sha256 != file.checksum_sha256 {
                checksum_mismatches.push(ChecksumMismatch {
                    version: file.version.clone(),
                    filename: file.filename.clone(),
                    expected_sha256: record.checksum_sha256.clone(),
                    actual_sha256: file.checksum_sha256.clone(),
                });
            }
        } else {
            pending.push(file.clone());
        }
    }

    let applied_count = ledger
        .values()
        .filter(|record| file_versions.contains(record.version.as_str()))
        .count();
    let latest_applied = ledger
        .values()
        .filter(|record| file_versions.contains(record.version.as_str()))
        .max_by(|left, right| left.version.cmp(&right.version))
        .cloned();

    Ok(MigrationStatusReport {
        total_migrations: files.len(),
        applied_count,
        pending_count: pending.len(),
        latest_applied,
        checksum_mismatches,
        pending,
    })
}

pub fn load_migration_files(migrations_dir: impl AsRef<Path>) -> Result<Vec<MigrationFile>> {
    let migrations_dir = migrations_dir.as_ref();
    let mut files = Vec::new();
    for entry in fs::read_dir(migrations_dir)
        .with_context(|| format!("failed to read migrations dir {}", migrations_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("sql") {
            continue;
        }
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .context("migration filename is not valid UTF-8")?
            .to_string();
        let version = parse_migration_version(&filename)?;
        let contents = fs::read(&path)
            .with_context(|| format!("failed to read migration {}", path.display()))?;
        let checksum_sha256 = sha256_hex(&contents);
        files.push(MigrationFile {
            version,
            filename,
            checksum_sha256,
            path,
        });
    }
    files.sort_by(|left, right| left.filename.cmp(&right.filename));

    let mut seen = BTreeSet::new();
    for file in &files {
        if !seen.insert(file.version.clone()) {
            bail!("duplicate migration version {}", file.version);
        }
    }

    Ok(files)
}

fn parse_migration_version(filename: &str) -> Result<String> {
    let prefix = filename
        .split_once('_')
        .map(|(prefix, _)| prefix)
        .context("migration filename must start with a version prefix like 0001_")?;
    if prefix.len() != 4 || !prefix.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("migration filename {filename} must start with a four-digit version");
    }
    Ok(prefix.to_string())
}

fn sha256_hex(contents: &[u8]) -> String {
    let digest = Sha256::digest(contents);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

async fn load_schema_migration_records(
    pool: &PgPool,
) -> Result<BTreeMap<String, SchemaMigrationRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT version, filename, checksum_sha256, applied_at, applied_by, execution_ms, success
        FROM schema_migrations
        ORDER BY version
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut records = BTreeMap::new();
    for row in rows {
        let record = SchemaMigrationRecord {
            version: row.try_get("version")?,
            filename: row.try_get("filename")?,
            checksum_sha256: row.try_get("checksum_sha256")?,
            applied_at: row.try_get("applied_at")?,
            applied_by: row.try_get("applied_by")?,
            execution_ms: row.try_get("execution_ms")?,
            success: row.try_get("success")?,
        };
        records.insert(record.version.clone(), record);
    }
    Ok(records)
}

fn verify_checksum(file: &MigrationFile, record: &SchemaMigrationRecord) -> Result<()> {
    if record.checksum_sha256 != file.checksum_sha256 {
        bail!(
            "checksum mismatch for migration {} ({}): ledger={} file={}",
            file.version,
            file.filename,
            record.checksum_sha256,
            file.checksum_sha256
        );
    }
    Ok(())
}

async fn verify_existing_aegis_database_for_baseline(
    pool: &PgPool,
    target_version: &str,
) -> Result<()> {
    let schema = current_schema(pool).await?;
    let rows = sqlx::query(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_type = 'BASE TABLE'
        "#,
    )
    .bind(&schema)
    .fetch_all(pool)
    .await?;
    let tables: BTreeSet<String> = rows
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("table_name").ok())
        .filter(|name| name != "schema_migrations" && name != "_sqlx_migrations")
        .collect();

    if tables.is_empty() {
        bail!("baseline refused: database schema is empty");
    }

    let missing_required: Vec<&str> = AEGIS_BASELINE_REQUIRED_OBJECTS
        .iter()
        .copied()
        .filter(|table| !tables.contains(*table))
        .collect();
    let has_candidate_table = AEGIS_BASELINE_CANDIDATE_OBJECTS
        .iter()
        .any(|table| tables.contains(*table));
    if !missing_required.is_empty() || !has_candidate_table {
        bail!(
            "baseline refused: database does not look like the expected Aegis production schema; missing_required={:?} has_candidate_table={}",
            missing_required,
            has_candidate_table
        );
    }

    let incompatible: Vec<String> = VERSIONED_OBJECTS
        .iter()
        .filter(|(table, version)| tables.contains(*table) && *version > target_version)
        .map(|(table, version)| format!("{table} requires baseline target >= {version}"))
        .collect();
    if !incompatible.is_empty() {
        bail!(
            "baseline refused: target version {} is lower than detected schema objects: {}",
            target_version,
            incompatible.join(", ")
        );
    }

    Ok(())
}

async fn current_schema(pool: &PgPool) -> Result<String> {
    let schema = sqlx::query_scalar::<_, String>("SELECT current_schema()")
        .fetch_one(pool)
        .await?;
    Ok(schema)
}

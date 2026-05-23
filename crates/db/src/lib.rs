use aegis_core::EventEnvelope;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, Row};
use uuid::Uuid;

pub const MIGRATIONS_DIR: &str = "crates/db/migrations";
const GLOBAL_SYSTEM_STATE_KEY: &str = "global";
pub use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct DbConfig {
    pub database_url: String,
    pub max_connections: u32,
}

impl DbConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            max_connections: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemEventRecord {
    pub event_id: Uuid,
    pub event_type: String,
    pub source: String,
    pub correlation_id: Uuid,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStateRecord {
    pub state_key: String,
    pub kill_switch_enabled: bool,
    pub kill_switch_reason: Option<String>,
    pub updated_by_actor: String,
    pub updated_by_actor_id: Option<Uuid>,
    pub last_correlation_id: Uuid,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StateActor {
    pub actor: String,
    pub actor_id: Option<Uuid>,
}

impl StateActor {
    pub fn system(actor: impl Into<String>) -> Self {
        Self {
            actor: actor.into(),
            actor_id: None,
        }
    }
}

pub async fn connect_pool(config: &DbConfig) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await?;

    Ok(pool)
}

pub async fn check_health(pool: &PgPool) -> Result<()> {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await?;

    Ok(())
}

pub async fn ensure_system_state(pool: &PgPool) -> Result<SystemStateRecord> {
    let bootstrap_correlation_id = Uuid::new_v4();
    let row = sqlx::query(
        r#"
        INSERT INTO system_state (
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        )
        VALUES ($1, FALSE, NULL, $2, NULL, $3, NOW())
        ON CONFLICT (state_key) DO UPDATE
        SET updated_at = system_state.updated_at
        RETURNING
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        "#,
    )
    .bind(GLOBAL_SYSTEM_STATE_KEY)
    .bind("system.bootstrap")
    .bind(bootstrap_correlation_id)
    .fetch_one(pool)
    .await?;

    Ok(map_system_state(&row))
}

pub async fn get_system_state(pool: &PgPool) -> Result<SystemStateRecord> {
    let row = sqlx::query(
        r#"
        SELECT
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        FROM system_state
        WHERE state_key = $1
        "#,
    )
    .bind(GLOBAL_SYSTEM_STATE_KEY)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => Ok(map_system_state(&row)),
        None => ensure_system_state(pool).await,
    }
}

pub async fn set_kill_switch_state(
    pool: &PgPool,
    actor: &StateActor,
    correlation_id: Uuid,
    source: &str,
    enabled: bool,
    reason: Option<String>,
) -> Result<SystemStateRecord> {
    let mut tx = pool.begin().await?;
    let action = if enabled {
        "risk.kill_switch.activate"
    } else {
        "risk.kill_switch.resume"
    };
    let event_type = if enabled {
        "system.kill_switch.enabled"
    } else {
        "system.kill_switch.disabled"
    };

    let state_row = sqlx::query(
        r#"
        INSERT INTO system_state (
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (state_key) DO UPDATE
        SET
            kill_switch_enabled = EXCLUDED.kill_switch_enabled,
            kill_switch_reason = EXCLUDED.kill_switch_reason,
            updated_by_actor = EXCLUDED.updated_by_actor,
            updated_by_actor_id = EXCLUDED.updated_by_actor_id,
            last_correlation_id = EXCLUDED.last_correlation_id,
            updated_at = NOW()
        RETURNING
            state_key,
            kill_switch_enabled,
            kill_switch_reason,
            updated_by_actor,
            updated_by_actor_id,
            last_correlation_id,
            updated_at
        "#,
    )
    .bind(GLOBAL_SYSTEM_STATE_KEY)
    .bind(enabled)
    .bind(reason.as_deref())
    .bind(&actor.actor)
    .bind(actor.actor_id)
    .bind(correlation_id)
    .fetch_one(&mut *tx)
    .await?;

    let updated_state = map_system_state(&state_row);
    let metadata = json!({
        "actor_id": actor.actor_id,
        "kill_switch_enabled": updated_state.kill_switch_enabled,
        "kill_switch_reason": updated_state.kill_switch_reason,
        "state_key": updated_state.state_key,
    });

    sqlx::query(
        r#"
        INSERT INTO audit_logs (id, correlation_id, actor, action, target, metadata)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(correlation_id)
    .bind(&actor.actor)
    .bind(action)
    .bind("system_state.kill_switch")
    .bind(&metadata)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO system_events (id, correlation_id, event_type, source, payload, occurred_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(correlation_id)
    .bind(event_type)
    .bind(source)
    .bind(&metadata)
    .bind(Utc::now())
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(updated_state)
}

pub async fn insert_system_event(
    pool: &PgPool,
    event: &EventEnvelope,
) -> Result<SystemEventRecord> {
    let row = sqlx::query(
        r#"
        INSERT INTO system_events (id, correlation_id, event_type, source, payload, occurred_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING
            id,
            correlation_id,
            event_type,
            source,
            payload,
            occurred_at,
            created_at
        "#,
    )
    .bind(event.event_id)
    .bind(event.correlation_id)
    .bind(&event.event_type)
    .bind(&event.source)
    .bind(&event.payload)
    .bind(event.occurred_at)
    .fetch_one(pool)
    .await?;

    Ok(map_system_event(&row))
}

pub async fn list_recent_system_events(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<SystemEventRecord>> {
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            correlation_id,
            event_type,
            source,
            payload,
            occurred_at,
            created_at
        FROM system_events
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(map_system_event).collect())
}

pub async fn get_system_event(pool: &PgPool, event_id: Uuid) -> Result<Option<SystemEventRecord>> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            correlation_id,
            event_type,
            source,
            payload,
            occurred_at,
            created_at
        FROM system_events
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(map_system_event))
}

fn map_system_event(row: &sqlx::postgres::PgRow) -> SystemEventRecord {
    SystemEventRecord {
        event_id: row.get("id"),
        correlation_id: row.get("correlation_id"),
        event_type: row.get("event_type"),
        source: row.get("source"),
        payload: row.get("payload"),
        occurred_at: row.get("occurred_at"),
        created_at: row.get("created_at"),
    }
}

fn map_system_state(row: &sqlx::postgres::PgRow) -> SystemStateRecord {
    SystemStateRecord {
        state_key: row.get("state_key"),
        kill_switch_enabled: row.get("kill_switch_enabled"),
        kill_switch_reason: row.get("kill_switch_reason"),
        updated_by_actor: row.get("updated_by_actor"),
        updated_by_actor_id: row.get("updated_by_actor_id"),
        last_correlation_id: row.get("last_correlation_id"),
        updated_at: row.get("updated_at"),
    }
}

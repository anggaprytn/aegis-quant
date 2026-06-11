use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use aegis_core::{User, UserRole, UserStatus};
use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const DEFAULT_API_BASE_URL: &str = "http://127.0.0.1:3100";
pub const DEFAULT_MICROSTRUCTURE_RETENTION_DAYS: i64 = 30;
pub const DEFAULT_MICROSTRUCTURE_RUN_RETENTION_DAYS: i64 = 90;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliConfig {
    pub api_base_url: Url,
    pub auth: Option<StoredAuthSession>,
    pub token_path: PathBuf,
    pub auth_from_env: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicrostructureRetentionConfig {
    pub metrics_retention_days: i64,
    pub run_retention_days: i64,
}

impl MicrostructureRetentionConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            metrics_retention_days: read_positive_i64_env(
                "MICROSTRUCTURE_RETENTION_DAYS",
                DEFAULT_MICROSTRUCTURE_RETENTION_DAYS,
            )?,
            run_retention_days: read_positive_i64_env(
                "MICROSTRUCTURE_RUN_RETENTION_DAYS",
                DEFAULT_MICROSTRUCTURE_RUN_RETENTION_DAYS,
            )?,
        })
    }
}

fn read_positive_i64_env(name: &str, default_value: i64) -> anyhow::Result<i64> {
    let Some(raw) = std::env::var(name).ok() else {
        return Ok(default_value);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(default_value);
    }
    let parsed = trimmed
        .parse::<i64>()
        .map_err(|err| anyhow::anyhow!("{name} must be a positive integer: {err}"))?;
    if parsed <= 0 {
        anyhow::bail!("{name} must be greater than zero");
    }
    Ok(parsed)
}

impl CliConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var("AEGIS_API_BASE_URL").ok();
        let resolved = resolve_api_base_url(raw.as_deref());
        let token_path = default_token_path()?;
        let env_access_token = std::env::var("AEGIS_ACCESS_TOKEN")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let auth_from_env = env_access_token.is_some();
        let auth = if let Some(access_token) = env_access_token {
            Some(StoredAuthSession {
                access_token,
                refresh_token: None,
                expires_at: None,
                user: None,
                saved_at: Utc::now(),
            })
        } else {
            load_token_file(&token_path).ok()
        };

        Ok(Self {
            api_base_url: normalize_base_url(&resolved)?,
            auth,
            token_path,
            auth_from_env,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredUserSummary {
    pub id: Uuid,
    pub email: String,
    pub role: UserRole,
    pub status: UserStatus,
    pub last_login_at: Option<DateTime<Utc>>,
}

impl From<&User> for StoredUserSummary {
    fn from(user: &User) -> Self {
        Self {
            id: user.id,
            email: user.email.clone(),
            role: user.role,
            status: user.status,
            last_login_at: user.last_login_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredAuthSession {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub user: Option<StoredUserSummary>,
    pub saved_at: DateTime<Utc>,
}

pub fn resolve_api_base_url(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_API_BASE_URL)
        .to_string()
}

pub fn normalize_base_url(value: &str) -> anyhow::Result<Url> {
    let mut normalized = value.trim_end_matches('/').to_string();
    if normalized.is_empty() {
        normalized = DEFAULT_API_BASE_URL.to_string();
    }
    Ok(Url::parse(&normalized)?)
}

pub fn default_token_path() -> anyhow::Result<PathBuf> {
    if let Ok(path) = std::env::var("XDG_CONFIG_HOME") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed).join("aegis").join("token.json"));
        }
    }

    let home = std::env::var("HOME")?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("aegis")
        .join("token.json"))
}

pub fn load_token_file(path: &Path) -> anyhow::Result<StoredAuthSession> {
    let raw = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

pub fn save_token_file(path: &Path, session: &StoredAuthSession) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let bytes = serde_json::to_vec_pretty(session)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(&bytes)?;
        file.flush()?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }

    #[cfg(not(unix))]
    {
        fs::write(path, &bytes)?;
    }

    Ok(())
}

pub fn clear_token_file(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        clear_token_file, load_token_file, normalize_base_url, resolve_api_base_url,
        save_token_file, CliConfig, MicrostructureRetentionConfig, StoredAuthSession,
        StoredUserSummary, DEFAULT_MICROSTRUCTURE_RETENTION_DAYS,
        DEFAULT_MICROSTRUCTURE_RUN_RETENTION_DAYS,
    };
    use aegis_core::{UserRole, UserStatus};
    use chrono::{TimeZone, Utc};
    use std::{
        env,
        path::PathBuf,
        sync::{Mutex, OnceLock},
    };
    use uuid::Uuid;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn temp_token_path(name: &str) -> PathBuf {
        let unique = format!(
            "aegis-cli-config-{name}-{}-{}.json",
            std::process::id(),
            Uuid::new_v4()
        );
        env::temp_dir().join(unique)
    }

    fn sample_session() -> StoredAuthSession {
        StoredAuthSession {
            access_token: "access-token".to_string(),
            refresh_token: Some("refresh-token".to_string()),
            expires_at: Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 15, 0).unwrap()),
            user: Some(StoredUserSummary {
                id: Uuid::from_u128(0x1234),
                email: "owner@example.com".to_string(),
                role: UserRole::Owner,
                status: UserStatus::Active,
                last_login_at: Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()),
            }),
            saved_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap(),
        }
    }

    #[test]
    fn base_url_falls_back_when_env_missing() {
        assert_eq!(
            resolve_api_base_url(None),
            "http://127.0.0.1:3100".to_string()
        );
    }

    #[test]
    fn base_url_falls_back_when_env_blank() {
        assert_eq!(
            resolve_api_base_url(Some("   ")),
            "http://127.0.0.1:3100".to_string()
        );
    }

    #[test]
    fn base_url_env_overrides_default() {
        assert_eq!(
            resolve_api_base_url(Some("http://127.0.0.1:3100")),
            "http://127.0.0.1:3100".to_string()
        );
    }

    #[test]
    fn normalize_base_url_trims_trailing_slash() {
        let url = normalize_base_url("http://127.0.0.1:3000/").expect("valid url");
        assert_eq!(url.as_str(), "http://127.0.0.1:3000/");
    }

    #[test]
    fn token_file_round_trips_full_session() {
        let path = temp_token_path("roundtrip");
        let session = sample_session();

        save_token_file(&path, &session).expect("session should save");
        let loaded = load_token_file(&path).expect("session should load");

        assert_eq!(loaded, session);
        clear_token_file(&path).expect("token file should clear");
    }

    #[test]
    fn env_access_token_overrides_file_session() {
        let _guard = env_lock().lock().expect("env lock");
        let config_root = env::temp_dir().join(format!(
            "aegis-cli-config-root-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        let token_path = config_root.join("aegis").join("token.json");
        let session = sample_session();
        save_token_file(&token_path, &session).expect("session should save");

        env::set_var("AEGIS_API_BASE_URL", "http://127.0.0.1:3100");
        env::set_var("AEGIS_ACCESS_TOKEN", "env-access-token");
        env::set_var("XDG_CONFIG_HOME", config_root.to_string_lossy().to_string());

        let config = CliConfig::from_env().expect("config should load");

        assert!(config.auth_from_env);
        assert_eq!(
            config.auth.expect("auth").access_token,
            "env-access-token".to_string()
        );

        clear_token_file(&token_path).expect("token file should clear");
        env::remove_var("AEGIS_API_BASE_URL");
        env::remove_var("AEGIS_ACCESS_TOKEN");
        env::remove_var("XDG_CONFIG_HOME");
    }

    #[test]
    fn microstructure_retention_defaults_when_env_missing() {
        let _guard = env_lock().lock().expect("env lock");
        env::remove_var("MICROSTRUCTURE_RETENTION_DAYS");
        env::remove_var("MICROSTRUCTURE_RUN_RETENTION_DAYS");

        let config = MicrostructureRetentionConfig::from_env().expect("config should load");

        assert_eq!(
            config.metrics_retention_days,
            DEFAULT_MICROSTRUCTURE_RETENTION_DAYS
        );
        assert_eq!(
            config.run_retention_days,
            DEFAULT_MICROSTRUCTURE_RUN_RETENTION_DAYS
        );
    }

    #[test]
    fn microstructure_retention_env_overrides_defaults() {
        let _guard = env_lock().lock().expect("env lock");
        env::set_var("MICROSTRUCTURE_RETENTION_DAYS", "14");
        env::set_var("MICROSTRUCTURE_RUN_RETENTION_DAYS", "45");

        let config = MicrostructureRetentionConfig::from_env().expect("config should load");

        assert_eq!(config.metrics_retention_days, 14);
        assert_eq!(config.run_retention_days, 45);

        env::remove_var("MICROSTRUCTURE_RETENTION_DAYS");
        env::remove_var("MICROSTRUCTURE_RUN_RETENTION_DAYS");
    }

    #[test]
    fn microstructure_retention_rejects_non_positive_values() {
        let _guard = env_lock().lock().expect("env lock");
        env::set_var("MICROSTRUCTURE_RETENTION_DAYS", "0");
        env::remove_var("MICROSTRUCTURE_RUN_RETENTION_DAYS");

        let err = MicrostructureRetentionConfig::from_env()
            .expect_err("zero retention should be rejected");

        assert!(err
            .to_string()
            .contains("MICROSTRUCTURE_RETENTION_DAYS must be greater than zero"));

        env::remove_var("MICROSTRUCTURE_RETENTION_DAYS");
    }
}

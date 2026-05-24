use std::{fs, path::PathBuf};

use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::{Deserialize, Serialize};

const DEFAULT_API_BASE_URL: &str = "http://127.0.0.1:3000";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliConfig {
    pub api_base_url: Url,
    pub access_token: Option<String>,
    pub token_path: PathBuf,
}

impl CliConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var("AEGIS_API_BASE_URL").ok();
        let resolved = resolve_api_base_url(raw.as_deref());
        let token_path = default_token_path()?;
        let access_token = std::env::var("AEGIS_ACCESS_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                load_token_file(&token_path)
                    .ok()
                    .map(|token| token.access_token)
            });
        Ok(Self {
            api_base_url: normalize_base_url(&resolved)?,
            access_token,
            token_path,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAccessToken {
    pub access_token: String,
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

pub fn load_token_file(path: &PathBuf) -> anyhow::Result<StoredAccessToken> {
    let raw = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

pub fn save_token_file(path: &PathBuf, access_token: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let payload = StoredAccessToken {
        access_token: access_token.to_string(),
        saved_at: Utc::now(),
    };
    fs::write(path, serde_json::to_vec_pretty(&payload)?)?;
    Ok(())
}

pub fn clear_token_file(path: &PathBuf) -> anyhow::Result<()> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{normalize_base_url, resolve_api_base_url};

    #[test]
    fn base_url_falls_back_when_env_missing() {
        assert_eq!(
            resolve_api_base_url(None),
            "http://127.0.0.1:3000".to_string()
        );
    }

    #[test]
    fn base_url_falls_back_when_env_blank() {
        assert_eq!(
            resolve_api_base_url(Some("   ")),
            "http://127.0.0.1:3000".to_string()
        );
    }

    #[test]
    fn normalize_base_url_trims_trailing_slash() {
        let url = normalize_base_url("http://127.0.0.1:3000/").expect("valid url");
        assert_eq!(url.as_str(), "http://127.0.0.1:3000/");
    }
}

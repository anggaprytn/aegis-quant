use reqwest::Url;

const DEFAULT_API_BASE_URL: &str = "http://127.0.0.1:3000";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliConfig {
    pub api_base_url: Url,
}

impl CliConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var("AEGIS_API_BASE_URL").ok();
        let resolved = resolve_api_base_url(raw.as_deref());
        Ok(Self {
            api_base_url: normalize_base_url(&resolved)?,
        })
    }
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

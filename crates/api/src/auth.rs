use std::{env, time::Duration};

use aegis_core::{validate_password_length, AuthenticatedActor, User, UserRole, UserStatus};
use anyhow::{anyhow, Context, Result};
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use chrono::{DateTime, Utc};
use cookie::{time::Duration as CookieDuration, Cookie, SameSite};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const REFRESH_COOKIE_NAME: &str = "aegis_refresh_token";
const DEV_ACTOR_EMAIL: &str = "auth-disabled@local.aegis";

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub disabled: bool,
    pub jwt_secret: Option<String>,
    pub access_token_ttl: Duration,
    pub refresh_token_ttl: Duration,
    pub cookie_secure: bool,
    pub protect_metrics: bool,
    pub bootstrap_owner_email: Option<String>,
    pub bootstrap_owner_password: Option<String>,
}

impl AuthConfig {
    pub fn from_env() -> Result<Self, String> {
        let disabled = env_flag("AEGIS_AUTH_DISABLED", false)?;
        let jwt_secret = env::var("AEGIS_JWT_SECRET")
            .ok()
            .filter(|value| !value.trim().is_empty());
        if !disabled && jwt_secret.is_none() {
            return Err("AEGIS_JWT_SECRET must be set when auth is enabled".to_string());
        }

        let access_token_ttl = env_seconds("AEGIS_ACCESS_TOKEN_TTL_SECONDS", 900)?;
        let refresh_token_ttl = env_seconds("AEGIS_REFRESH_TOKEN_TTL_SECONDS", 86_400)?;
        let cookie_secure = env_flag("AEGIS_COOKIE_SECURE", false)?;
        let protect_metrics = env_flag("AEGIS_PROTECT_METRICS", false)?;

        Ok(Self {
            disabled,
            jwt_secret,
            access_token_ttl,
            refresh_token_ttl,
            cookie_secure,
            protect_metrics,
            bootstrap_owner_email: env::var("AEGIS_BOOTSTRAP_OWNER_EMAIL")
                .ok()
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| !value.is_empty()),
            bootstrap_owner_password: env::var("AEGIS_BOOTSTRAP_OWNER_PASSWORD")
                .ok()
                .filter(|value| !value.trim().is_empty()),
        })
    }

    pub fn encoding_key(&self) -> Result<EncodingKey> {
        self.jwt_secret
            .as_deref()
            .map(|value| EncodingKey::from_secret(value.as_bytes()))
            .ok_or_else(|| anyhow!("JWT secret is not configured"))
    }

    pub fn decoding_key(&self) -> Result<DecodingKey> {
        self.jwt_secret
            .as_deref()
            .map(|value| DecodingKey::from_secret(value.as_bytes()))
            .ok_or_else(|| anyhow!("JWT secret is not configured"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessTokenClaims {
    pub sub: String,
    pub email: String,
    pub role: UserRole,
    pub session_id: String,
    pub exp: usize,
    pub iat: usize,
}

#[derive(Debug, Clone)]
pub struct IssuedAccessToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RefreshToken {
    pub raw: String,
    pub hash: String,
}

pub fn hash_password(password: &str) -> Result<String> {
    validate_password_length(password)?;
    let salt = SaltString::generate(&mut OsRng);
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|err| anyhow!("failed to hash password: {err}"))?
        .to_string())
}

pub fn verify_password(password: &str, password_hash: &str) -> Result<bool> {
    let parsed_hash = PasswordHash::new(password_hash)
        .map_err(|err| anyhow!("failed to parse stored password hash: {err}"))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

pub fn issue_access_token(
    config: &AuthConfig,
    user: &User,
    session_id: Uuid,
    now: DateTime<Utc>,
) -> Result<IssuedAccessToken> {
    let expires_at = now
        + chrono::Duration::from_std(config.access_token_ttl)
            .context("invalid access token ttl")?;
    let claims = AccessTokenClaims {
        sub: user.id.to_string(),
        email: user.email.clone(),
        role: user.role,
        session_id: session_id.to_string(),
        exp: expires_at.timestamp() as usize,
        iat: now.timestamp() as usize,
    };
    let token = jsonwebtoken::encode(&Header::default(), &claims, &config.encoding_key()?)?;

    Ok(IssuedAccessToken { token, expires_at })
}

pub fn decode_access_token(config: &AuthConfig, token: &str) -> Result<AccessTokenClaims> {
    let mut validation = Validation::default();
    validation.validate_exp = true;
    let data =
        jsonwebtoken::decode::<AccessTokenClaims>(token, &config.decoding_key()?, &validation)?;
    Ok(data.claims)
}

pub fn actor_from_claims(claims: AccessTokenClaims) -> Result<AuthenticatedActor> {
    Ok(AuthenticatedActor {
        user_id: Uuid::parse_str(&claims.sub)?,
        email: claims.email,
        role: claims.role,
        session_id: Some(Uuid::parse_str(&claims.session_id)?),
    })
}

pub fn issue_refresh_token(session_id: Uuid) -> RefreshToken {
    let mut secret = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut secret);
    let secret_hex = secret
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let raw = format!("{session_id}.{secret_hex}");

    RefreshToken {
        hash: hash_refresh_token(&raw),
        raw,
    }
}

pub fn parse_refresh_token(raw: &str) -> Result<Uuid> {
    let (session_id, _) = raw
        .split_once('.')
        .ok_or_else(|| anyhow!("invalid refresh token format"))?;
    Ok(Uuid::parse_str(session_id)?)
}

pub fn hash_refresh_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

pub fn build_refresh_cookie(config: &AuthConfig, refresh_token: &str) -> Cookie<'static> {
    Cookie::build((REFRESH_COOKIE_NAME, refresh_token.to_string()))
        .http_only(true)
        .same_site(SameSite::Lax)
        .path("/")
        .secure(config.cookie_secure)
        .max_age(CookieDuration::seconds(
            config.refresh_token_ttl.as_secs() as i64
        ))
        .build()
}

pub fn clear_refresh_cookie(config: &AuthConfig) -> Cookie<'static> {
    Cookie::build((REFRESH_COOKIE_NAME, ""))
        .http_only(true)
        .same_site(SameSite::Lax)
        .path("/")
        .secure(config.cookie_secure)
        .max_age(CookieDuration::seconds(0))
        .build()
}

pub fn bootstrap_credentials(config: &AuthConfig) -> Result<(String, String)> {
    let email = config
        .bootstrap_owner_email
        .clone()
        .ok_or_else(|| anyhow!("AEGIS_BOOTSTRAP_OWNER_EMAIL must be set"))?;
    let password = config
        .bootstrap_owner_password
        .clone()
        .ok_or_else(|| anyhow!("AEGIS_BOOTSTRAP_OWNER_PASSWORD must be set"))?;
    validate_password_length(&password)?;
    Ok((email, password))
}

pub fn dev_actor() -> AuthenticatedActor {
    AuthenticatedActor {
        user_id: Uuid::nil(),
        email: DEV_ACTOR_EMAIL.to_string(),
        role: UserRole::Owner,
        session_id: None,
    }
}

pub fn dev_user(now: DateTime<Utc>) -> User {
    User {
        id: Uuid::nil(),
        email: DEV_ACTOR_EMAIL.to_string(),
        role: UserRole::Owner,
        status: UserStatus::Active,
        created_at: now,
        updated_at: now,
        last_login_at: None,
    }
}

fn env_flag(name: &str, default: bool) -> Result<bool, String> {
    env::var(name)
        .ok()
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            other => Err(format!("invalid {name}: {other}")),
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn env_seconds(name: &str, default: u64) -> Result<Duration, String> {
    let seconds = env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|err| format!("invalid {name}: {err}"))
        })
        .transpose()?
        .unwrap_or(default);
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::{
        actor_from_claims, decode_access_token, hash_password, issue_access_token, verify_password,
        AccessTokenClaims, AuthConfig,
    };
    use aegis_core::{validate_password_length, User, UserRole, UserStatus};
    use chrono::{Duration, TimeZone, Utc};
    use std::time::Duration as StdDuration;
    use uuid::Uuid;

    fn config() -> AuthConfig {
        AuthConfig {
            disabled: false,
            jwt_secret: Some("test-secret".to_string()),
            access_token_ttl: StdDuration::from_secs(900),
            refresh_token_ttl: StdDuration::from_secs(86_400),
            cookie_secure: false,
            protect_metrics: false,
            bootstrap_owner_email: None,
            bootstrap_owner_password: None,
        }
    }

    fn user() -> User {
        let now = Utc::now();
        User {
            id: Uuid::new_v4(),
            email: "owner@example.com".to_string(),
            role: UserRole::Owner,
            status: UserStatus::Active,
            created_at: now,
            updated_at: now,
            last_login_at: None,
        }
    }

    #[test]
    fn password_hash_round_trip() {
        let hash = hash_password("correct horse battery").expect("hash should succeed");
        assert!(verify_password("correct horse battery", &hash).expect("verify should succeed"));
        assert!(!verify_password("wrong password", &hash).expect("verify should succeed"));
    }

    #[test]
    fn password_length_is_rejected() {
        let err = validate_password_length("short").expect_err("short password must fail");
        assert!(err.to_string().contains("at least"));
    }

    #[test]
    fn access_token_round_trip() {
        let now = Utc::now();
        let session_id = Uuid::new_v4();
        let issued = issue_access_token(&config(), &user(), session_id, now).expect("token");
        let claims = decode_access_token(&config(), &issued.token).expect("decode");
        let actor = actor_from_claims(claims).expect("actor");
        assert_eq!(actor.session_id, Some(session_id));
        assert_eq!(actor.role, UserRole::Owner);
    }

    #[test]
    fn expired_token_is_rejected() {
        let now = Utc::now();
        let claims = AccessTokenClaims {
            sub: Uuid::new_v4().to_string(),
            email: "owner@example.com".to_string(),
            role: UserRole::Owner,
            session_id: Uuid::new_v4().to_string(),
            exp: (now - Duration::seconds(120)).timestamp() as usize,
            iat: (now - Duration::seconds(180)).timestamp() as usize,
        };
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &config().encoding_key().expect("key"),
        )
        .expect("token");
        let err = decode_access_token(&config(), &token).expect_err("expired token should fail");
        assert!(err.to_string().to_ascii_lowercase().contains("expired"));
    }
}

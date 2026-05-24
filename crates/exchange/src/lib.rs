use std::{collections::BTreeMap, env, sync::Arc};

use aegis_core::{
    ExchangeBalance, ExchangeCancelAck, ExchangeCancelRequest, ExchangeEnvironment, ExchangeError,
    ExchangeExecutionReport, ExchangeExecutionReportType, ExchangeExecutionStatus,
    ExchangeListenKeyStatus, ExchangeName, ExchangeOrderAck, ExchangeOrderRequest,
    ExchangeOrderSide, ExchangeOrderState, ExchangeOrderStatus, ExchangeOrderTimeInForce,
    ExchangeOrderType, ExchangePrivateStreamEvent, ExchangePrivateStreamSource,
    ExchangePrivateStreamState, ExchangePrivateStreamStatus, ExchangeRateLimitState,
    ExchangeRequestMode, ExchangeSymbolInfo, TestnetExecutionState, TestnetExecutionStateError,
    TestnetExecutionTransition, TestnetExecutionTransitionResult, TestnetExecutionTransitionSource,
    TestnetOrderLifecycleSnapshot,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Method,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
pub type Result<T> = std::result::Result<T, ExchangeError>;

#[cfg(any(test, feature = "test-utils"))]
pub mod testing;

const DEFAULT_TESTNET_BASE_URL: &str = "https://testnet.binance.vision";
const DEFAULT_TESTNET_WS_BASE_URL: &str = "wss://stream.testnet.binance.vision/ws";
const HEADER_API_KEY: &str = "X-MBX-APIKEY";

#[async_trait]
pub trait ExchangeAdapter: Send + Sync {
    async fn get_exchange_info(&self) -> Result<Vec<ExchangeSymbolInfo>>;
    async fn get_balances(&self) -> Result<Vec<ExchangeBalance>>;
    async fn submit_order(&self, order: ExchangeOrderRequest) -> Result<ExchangeOrderAck>;
    async fn cancel_order(&self, request: ExchangeCancelRequest) -> Result<ExchangeCancelAck>;
    async fn get_order_status(&self, client_order_id: &str) -> Result<ExchangeOrderStatus>;
}

#[derive(Clone)]
pub struct BinanceSpotTestnetAdapter {
    config: Arc<BinanceSpotTestnetConfig>,
    http: reqwest::Client,
}

#[derive(Clone)]
pub struct BinanceSpotTestnetConfig {
    pub environment: ExchangeEnvironment,
    pub rest_base_url: String,
    pub ws_base_url: String,
    pub api_key: Option<SecretString>,
    pub api_secret: Option<SecretString>,
    pub recv_window_ms: Option<u64>,
}

impl BinanceSpotTestnetConfig {
    pub fn from_env() -> Self {
        Self {
            environment: ExchangeEnvironment::Testnet,
            rest_base_url: env::var("BINANCE_TESTNET_REST_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_TESTNET_BASE_URL.to_string()),
            ws_base_url: env::var("BINANCE_TESTNET_WS_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_TESTNET_WS_BASE_URL.to_string()),
            api_key: env::var("BINANCE_TESTNET_API_KEY")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(SecretString::new),
            api_secret: env::var("BINANCE_TESTNET_API_SECRET")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(SecretString::new),
            recv_window_ms: env::var("BINANCE_TESTNET_RECV_WINDOW_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok()),
        }
    }

    pub fn validate_environment(&self, environment: ExchangeEnvironment) -> Result<()> {
        if environment == ExchangeEnvironment::Live {
            return Err(ExchangeError::LiveEnvironmentDisabled);
        }
        if environment != self.environment {
            return Err(ExchangeError::Configuration(format!(
                "adapter is configured for {} only",
                self.environment.as_str()
            )));
        }
        Ok(())
    }

    fn credentials(&self) -> Result<(&str, &str)> {
        let api_key = self.api_key.as_ref().ok_or_else(|| {
            ExchangeError::Configuration("BINANCE_TESTNET_API_KEY is not set".to_string())
        })?;
        let api_secret = self.api_secret.as_ref().ok_or_else(|| {
            ExchangeError::Configuration("BINANCE_TESTNET_API_SECRET is not set".to_string())
        })?;
        Ok((api_key.expose_secret(), api_secret.expose_secret()))
    }
}

impl BinanceSpotTestnetAdapter {
    pub fn new(config: BinanceSpotTestnetConfig) -> Self {
        Self {
            config: Arc::new(config),
            http: reqwest::Client::new(),
        }
    }

    pub fn config(&self) -> &BinanceSpotTestnetConfig {
        &self.config
    }

    pub fn status(&self) -> BinanceTestnetStatus {
        BinanceTestnetStatus {
            exchange: ExchangeName::Binance,
            environment: self.config.environment,
            rest_base_url: self.config.rest_base_url.clone(),
            ws_base_url: self.config.ws_base_url.clone(),
            configured: self.config.api_key.is_some() && self.config.api_secret.is_some(),
            request_mode: ExchangeRequestMode::Signed,
            rate_limits: ExchangeRateLimitState {
                request_weight: None,
                orders_1m: None,
                raw_requests_5m: None,
                retry_after_ms: None,
            },
        }
    }

    fn signed_headers(&self) -> Result<HeaderMap> {
        let (api_key, _) = self.config.credentials()?;
        let mut headers = HeaderMap::new();
        headers.insert(
            HEADER_API_KEY,
            HeaderValue::from_str(api_key).map_err(|err| {
                ExchangeError::Configuration(format!("invalid API key header value: {err}"))
            })?,
        );
        Ok(headers)
    }

    fn request_url(&self, path: &str, query: &str) -> String {
        if query.is_empty() {
            format!(
                "{}{}",
                self.config.rest_base_url.trim_end_matches('/'),
                path
            )
        } else {
            format!(
                "{}{}?{}",
                self.config.rest_base_url.trim_end_matches('/'),
                path,
                query
            )
        }
    }

    pub async fn create_listen_key(&self) -> Result<BinanceListenKey> {
        self.config
            .validate_environment(ExchangeEnvironment::Testnet)?;
        let headers = self.signed_headers()?;
        let response: BinanceListenKeyResponse = self
            .request(Method::POST, "/api/v3/userDataStream", "", Some(headers))
            .await?;
        Ok(BinanceListenKey {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            status: ExchangeListenKeyStatus::Active,
            listen_key: response.listen_key,
            created_at: Utc::now(),
        })
    }

    pub async fn keepalive_listen_key(&self, listen_key: &str) -> Result<BinanceListenKey> {
        self.config
            .validate_environment(ExchangeEnvironment::Testnet)?;
        if listen_key.trim().is_empty() {
            return Err(ExchangeError::Validation(
                "listen key cannot be empty".to_string(),
            ));
        }
        let headers = self.signed_headers()?;
        let query = format!("listenKey={listen_key}");
        let response: BinanceListenKeyResponse = self
            .request(Method::PUT, "/api/v3/userDataStream", &query, Some(headers))
            .await?;
        Ok(BinanceListenKey {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            status: ExchangeListenKeyStatus::Active,
            listen_key: response.listen_key,
            created_at: Utc::now(),
        })
    }

    pub async fn close_listen_key(&self, listen_key: &str) -> Result<BinanceListenKeyClosed> {
        self.config
            .validate_environment(ExchangeEnvironment::Testnet)?;
        if listen_key.trim().is_empty() {
            return Err(ExchangeError::Validation(
                "listen key cannot be empty".to_string(),
            ));
        }
        let headers = self.signed_headers()?;
        let query = format!("listenKey={listen_key}");
        let _: Value = self
            .request(
                Method::DELETE,
                "/api/v3/userDataStream",
                &query,
                Some(headers),
            )
            .await?;
        Ok(BinanceListenKeyClosed {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            status: ExchangeListenKeyStatus::Closed,
            closed_at: Utc::now(),
        })
    }

    pub fn build_user_stream_url(&self, listen_key: &str) -> Result<String> {
        self.config
            .validate_environment(ExchangeEnvironment::Testnet)?;
        if listen_key.trim().is_empty() {
            return Err(ExchangeError::Validation(
                "listen key cannot be empty".to_string(),
            ));
        }

        Ok(format!(
            "{}/{}",
            self.config.ws_base_url.trim_end_matches('/'),
            listen_key.trim()
        ))
    }

    async fn public_get<T>(&self, path: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.request(Method::GET, path, "", None).await
    }

    async fn signed_request<T>(
        &self,
        method: Method,
        path: &str,
        params: BTreeMap<String, String>,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.config
            .validate_environment(ExchangeEnvironment::Testnet)?;
        let (_, api_secret) = self.config.credentials()?;
        let mut params = params;
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        if let Some(recv_window_ms) = self.config.recv_window_ms {
            params
                .entry("recvWindow".to_string())
                .or_insert_with(|| recv_window_ms.to_string());
        }
        let query = build_query_string(&params);
        let signature = sign_query(api_secret, &query)?;
        let signed_query = format!("{query}&signature={signature}");
        let headers = self.signed_headers()?;
        self.request(method, path, &signed_query, Some(headers))
            .await
    }

    async fn request<T>(
        &self,
        method: Method,
        path: &str,
        query: &str,
        headers: Option<HeaderMap>,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let url = self.request_url(path, query);
        let mut request = self.http.request(method, &url);
        if let Some(headers) = headers {
            request = request.headers(headers);
        }
        let response = request
            .send()
            .await
            .map_err(|err| ExchangeError::Transport(err.to_string()))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|err| ExchangeError::Transport(err.to_string()))?;
        if !status.is_success() {
            let message = String::from_utf8_lossy(&body).to_string();
            if status.as_u16() == 401 {
                return Err(ExchangeError::Authentication);
            }
            if status.as_u16() == 429 {
                return Err(ExchangeError::RateLimited);
            }
            return Err(ExchangeError::Api(message));
        }
        serde_json::from_slice(&body).map_err(|err| ExchangeError::Serialization(err.to_string()))
    }
}

#[async_trait]
impl ExchangeAdapter for BinanceSpotTestnetAdapter {
    async fn get_exchange_info(&self) -> Result<Vec<ExchangeSymbolInfo>> {
        self.config
            .validate_environment(ExchangeEnvironment::Testnet)?;
        let response: BinanceExchangeInfoResponse = self.public_get("/api/v3/exchangeInfo").await?;
        Ok(response
            .symbols
            .into_iter()
            .map(|symbol| ExchangeSymbolInfo {
                exchange: ExchangeName::Binance,
                environment: ExchangeEnvironment::Testnet,
                symbol: symbol.symbol,
                base_asset: symbol.base_asset,
                quote_asset: symbol.quote_asset,
                status: symbol.status,
                min_price: find_filter_decimal(&symbol.filters, "PRICE_FILTER", "minPrice"),
                tick_size: find_filter_decimal(&symbol.filters, "PRICE_FILTER", "tickSize"),
                min_qty: find_filter_decimal(&symbol.filters, "LOT_SIZE", "minQty"),
                step_size: find_filter_decimal(&symbol.filters, "LOT_SIZE", "stepSize"),
                min_notional: find_filter_decimal(&symbol.filters, "NOTIONAL", "minNotional")
                    .or_else(|| {
                        find_filter_decimal(&symbol.filters, "MIN_NOTIONAL", "minNotional")
                    }),
            })
            .collect())
    }

    async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
        let response: BinanceAccountResponse = self
            .signed_request(Method::GET, "/api/v3/account", BTreeMap::new())
            .await?;
        Ok(response
            .balances
            .into_iter()
            .map(|balance| ExchangeBalance {
                exchange: ExchangeName::Binance,
                environment: ExchangeEnvironment::Testnet,
                asset: balance.asset,
                free: parse_decimal(&balance.free).unwrap_or_default(),
                locked: parse_decimal(&balance.locked).unwrap_or_default(),
            })
            .collect())
    }

    async fn submit_order(&self, order: ExchangeOrderRequest) -> Result<ExchangeOrderAck> {
        order
            .validate()
            .map_err(|err| ExchangeError::Validation(err.to_string()))?;
        self.config.validate_environment(order.environment)?;

        let mut params = BTreeMap::new();
        params.insert("symbol".to_string(), order.symbol.as_str().to_string());
        params.insert("side".to_string(), order.side.as_str().to_string());
        params.insert("type".to_string(), order.order_type.as_str().to_string());
        params.insert(
            "newClientOrderId".to_string(),
            order.client_order_id.clone(),
        );
        if let Some(quantity) = order.quantity {
            params.insert("quantity".to_string(), quantity.normalize().to_string());
        }
        if let Some(notional) = order.quote_notional {
            params.insert(
                "quoteOrderQty".to_string(),
                notional.normalize().to_string(),
            );
        }
        if let Some(limit_price) = order.limit_price {
            params.insert("price".to_string(), limit_price.normalize().to_string());
        }
        if let Some(time_in_force) = order.time_in_force {
            params.insert(
                "timeInForce".to_string(),
                time_in_force.as_str().to_string(),
            );
        }
        if let Some(recv_window_ms) = order.recv_window_ms {
            params.insert("recvWindow".to_string(), recv_window_ms.to_string());
        }

        let response: BinanceOrderResponse = self
            .signed_request(Method::POST, "/api/v3/order", params)
            .await?;
        Ok(response.into_exchange_ack())
    }

    async fn cancel_order(&self, request: ExchangeCancelRequest) -> Result<ExchangeCancelAck> {
        request
            .validate()
            .map_err(|err| ExchangeError::Validation(err.to_string()))?;
        self.config.validate_environment(request.environment)?;

        let mut params = BTreeMap::new();
        params.insert("symbol".to_string(), request.symbol.as_str().to_string());
        params.insert(
            "origClientOrderId".to_string(),
            request.client_order_id.clone(),
        );
        if let Some(recv_window_ms) = request.recv_window_ms {
            params.insert("recvWindow".to_string(), recv_window_ms.to_string());
        }

        let response: BinanceOrderResponse = self
            .signed_request(Method::DELETE, "/api/v3/order", params)
            .await?;
        Ok(response.into_exchange_cancel_ack())
    }

    async fn get_order_status(&self, client_order_id: &str) -> Result<ExchangeOrderStatus> {
        if client_order_id.trim().is_empty() {
            return Err(ExchangeError::Validation(
                "client_order_id cannot be empty".to_string(),
            ));
        }

        let mut params = BTreeMap::new();
        params.insert("origClientOrderId".to_string(), client_order_id.to_string());
        let response: BinanceOrderStatusResponse = self
            .signed_request(Method::GET, "/api/v3/order", params)
            .await?;
        response.into_exchange_status()
    }
}

#[derive(Debug, Clone)]
pub struct BinanceTestnetStatus {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub rest_base_url: String,
    pub ws_base_url: String,
    pub configured: bool,
    pub request_mode: ExchangeRequestMode,
    pub rate_limits: ExchangeRateLimitState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceListenKey {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub status: ExchangeListenKeyStatus,
    pub listen_key: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceListenKeyClosed {
    pub exchange: ExchangeName,
    pub environment: ExchangeEnvironment,
    pub status: ExchangeListenKeyStatus,
    pub closed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateStreamEventProcessResult {
    pub event: ExchangePrivateStreamEvent,
    pub execution_report: Option<ExchangeExecutionReport>,
}

pub fn build_query_string(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn sign_query(secret: &str, query: &str) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|err| ExchangeError::Configuration(err.to_string()))?;
    mac.update(query.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn parse_decimal(value: &str) -> Option<rust_decimal::Decimal> {
    value.parse().ok()
}

fn find_filter_decimal(
    filters: &[Value],
    filter_type: &str,
    field: &str,
) -> Option<rust_decimal::Decimal> {
    filters.iter().find_map(|filter| {
        let current_type = filter.get("filterType")?.as_str()?;
        if current_type != filter_type {
            return None;
        }
        filter.get(field)?.as_str()?.parse().ok()
    })
}

fn parse_order_state(value: &str) -> Result<ExchangeOrderState> {
    match value {
        "NEW" => Ok(ExchangeOrderState::New),
        "PARTIALLY_FILLED" => Ok(ExchangeOrderState::PartiallyFilled),
        "FILLED" => Ok(ExchangeOrderState::Filled),
        "CANCELED" => Ok(ExchangeOrderState::Canceled),
        "PENDING_CANCEL" => Ok(ExchangeOrderState::PendingCancel),
        "REJECTED" => Ok(ExchangeOrderState::Rejected),
        "EXPIRED" => Ok(ExchangeOrderState::Expired),
        other => Err(ExchangeError::Serialization(format!(
            "unsupported exchange order status: {other}"
        ))),
    }
}

fn parse_order_side(value: &str) -> Result<ExchangeOrderSide> {
    match value {
        "BUY" => Ok(ExchangeOrderSide::Buy),
        "SELL" => Ok(ExchangeOrderSide::Sell),
        other => Err(ExchangeError::Serialization(format!(
            "unsupported exchange order side: {other}"
        ))),
    }
}

fn parse_order_type(value: &str) -> Result<ExchangeOrderType> {
    match value {
        "MARKET" => Ok(ExchangeOrderType::Market),
        "LIMIT" => Ok(ExchangeOrderType::Limit),
        other => Err(ExchangeError::Serialization(format!(
            "unsupported exchange order type: {other}"
        ))),
    }
}

fn parse_time_in_force(value: Option<String>) -> Result<Option<ExchangeOrderTimeInForce>> {
    match value.as_deref() {
        None | Some("") => Ok(None),
        Some("GTC") => Ok(Some(ExchangeOrderTimeInForce::Gtc)),
        Some("IOC") => Ok(Some(ExchangeOrderTimeInForce::Ioc)),
        Some("FOK") => Ok(Some(ExchangeOrderTimeInForce::Fok)),
        Some(other) => Err(ExchangeError::Serialization(format!(
            "unsupported exchange time_in_force: {other}"
        ))),
    }
}

fn parse_execution_report_type(value: &str) -> ExchangeExecutionReportType {
    match value {
        "NEW" => ExchangeExecutionReportType::New,
        "CANCELED" => ExchangeExecutionReportType::Canceled,
        "REPLACED" => ExchangeExecutionReportType::Replaced,
        "REJECTED" => ExchangeExecutionReportType::Rejected,
        "TRADE" => ExchangeExecutionReportType::Trade,
        "EXPIRED" => ExchangeExecutionReportType::Expired,
        "TRADE_PREVENTION" => ExchangeExecutionReportType::TradePrevention,
        _ => ExchangeExecutionReportType::Unknown,
    }
}

fn parse_execution_status(value: &str) -> ExchangeExecutionStatus {
    match value {
        "NEW" => ExchangeExecutionStatus::New,
        "PARTIALLY_FILLED" => ExchangeExecutionStatus::PartiallyFilled,
        "FILLED" => ExchangeExecutionStatus::Filled,
        "CANCELED" => ExchangeExecutionStatus::Canceled,
        "PENDING_CANCEL" => ExchangeExecutionStatus::PendingCancel,
        "REJECTED" => ExchangeExecutionStatus::Rejected,
        "EXPIRED" => ExchangeExecutionStatus::Expired,
        "EXPIRED_IN_MATCH" => ExchangeExecutionStatus::ExpiredInMatch,
        _ => ExchangeExecutionStatus::Unknown,
    }
}

pub fn local_testnet_order_status_from_private_execution_report(
    report: &ExchangeExecutionReport,
) -> &'static str {
    report.order_status.as_str()
}

pub fn validate_testnet_transition(
    previous: Option<TestnetExecutionState>,
    next: TestnetExecutionState,
    source: TestnetExecutionTransitionSource,
) -> std::result::Result<TestnetExecutionTransitionResult, TestnetExecutionStateError> {
    if matches!(next, TestnetExecutionState::UnknownExchangeState) {
        return Ok(TestnetExecutionTransitionResult {
            previous_state: previous,
            next_state: next,
            source,
            accepted: true,
            terminal: false,
            requires_reconciliation: true,
        });
    }

    let accepted = match previous {
        None => matches!(
            next,
            TestnetExecutionState::IntentCreated
                | TestnetExecutionState::RiskApproved
                | TestnetExecutionState::OrderPrepared
                | TestnetExecutionState::OrderSubmitRequested
                | TestnetExecutionState::ReconciliationRequired
                | TestnetExecutionState::UnknownExchangeState
                | TestnetExecutionState::Failed
        ),
        Some(prev) if prev.is_terminal() => false,
        Some(TestnetExecutionState::IntentCreated) => matches!(
            next,
            TestnetExecutionState::RiskApproved
                | TestnetExecutionState::OrderPrepared
                | TestnetExecutionState::OrderSubmitRequested
                | TestnetExecutionState::Rejected
                | TestnetExecutionState::Failed
                | TestnetExecutionState::ReconciliationRequired
        ),
        Some(TestnetExecutionState::RiskApproved) => matches!(
            next,
            TestnetExecutionState::OrderPrepared
                | TestnetExecutionState::OrderSubmitRequested
                | TestnetExecutionState::Rejected
                | TestnetExecutionState::Failed
                | TestnetExecutionState::ReconciliationRequired
        ),
        Some(TestnetExecutionState::OrderPrepared) => matches!(
            next,
            TestnetExecutionState::OrderSubmitRequested
                | TestnetExecutionState::Rejected
                | TestnetExecutionState::Failed
                | TestnetExecutionState::ReconciliationRequired
        ),
        Some(TestnetExecutionState::OrderSubmitRequested) => matches!(
            next,
            TestnetExecutionState::ExchangeAcked
                | TestnetExecutionState::New
                | TestnetExecutionState::PartiallyFilled
                | TestnetExecutionState::Filled
                | TestnetExecutionState::Rejected
                | TestnetExecutionState::Expired
                | TestnetExecutionState::CancelRequested
                | TestnetExecutionState::ReconciliationRequired
                | TestnetExecutionState::Failed
        ),
        Some(TestnetExecutionState::ExchangeAcked) => matches!(
            next,
            TestnetExecutionState::New
                | TestnetExecutionState::PartiallyFilled
                | TestnetExecutionState::Filled
                | TestnetExecutionState::CancelRequested
                | TestnetExecutionState::Cancelled
                | TestnetExecutionState::Rejected
                | TestnetExecutionState::Expired
                | TestnetExecutionState::ReconciliationRequired
                | TestnetExecutionState::Failed
        ),
        Some(TestnetExecutionState::New) => matches!(
            next,
            TestnetExecutionState::PartiallyFilled
                | TestnetExecutionState::Filled
                | TestnetExecutionState::CancelRequested
                | TestnetExecutionState::Cancelled
                | TestnetExecutionState::Rejected
                | TestnetExecutionState::Expired
                | TestnetExecutionState::ReconciliationRequired
                | TestnetExecutionState::Failed
        ),
        Some(TestnetExecutionState::PartiallyFilled) => matches!(
            next,
            TestnetExecutionState::PartiallyFilled
                | TestnetExecutionState::Filled
                | TestnetExecutionState::CancelRequested
                | TestnetExecutionState::Cancelled
                | TestnetExecutionState::Expired
                | TestnetExecutionState::ReconciliationRequired
                | TestnetExecutionState::Failed
        ),
        Some(TestnetExecutionState::CancelRequested) => matches!(
            next,
            TestnetExecutionState::Cancelled
                | TestnetExecutionState::Filled
                | TestnetExecutionState::PartiallyFilled
                | TestnetExecutionState::ReconciliationRequired
                | TestnetExecutionState::Failed
        ),
        Some(TestnetExecutionState::ReconciliationRequired) => matches!(
            next,
            TestnetExecutionState::ExchangeAcked
                | TestnetExecutionState::New
                | TestnetExecutionState::PartiallyFilled
                | TestnetExecutionState::Filled
                | TestnetExecutionState::CancelRequested
                | TestnetExecutionState::Cancelled
                | TestnetExecutionState::Rejected
                | TestnetExecutionState::Expired
                | TestnetExecutionState::UnknownExchangeState
                | TestnetExecutionState::Failed
                | TestnetExecutionState::ReconciliationRequired
        ),
        Some(TestnetExecutionState::UnknownExchangeState) => matches!(
            next,
            TestnetExecutionState::ReconciliationRequired
                | TestnetExecutionState::UnknownExchangeState
                | TestnetExecutionState::Failed
        ),
        Some(TestnetExecutionState::Filled)
        | Some(TestnetExecutionState::Cancelled)
        | Some(TestnetExecutionState::Rejected)
        | Some(TestnetExecutionState::Expired)
        | Some(TestnetExecutionState::Failed) => false,
    };

    if !accepted {
        return Err(TestnetExecutionStateError::InvalidTransition {
            previous_state: previous,
            next_state: next,
            transition_source: source,
        });
    }

    Ok(TestnetExecutionTransitionResult {
        previous_state: previous,
        next_state: next,
        source,
        accepted: true,
        terminal: next.is_terminal(),
        requires_reconciliation: matches!(
            next,
            TestnetExecutionState::ReconciliationRequired
                | TestnetExecutionState::UnknownExchangeState
        ),
    })
}

pub fn apply_testnet_transition(
    snapshot: &TestnetOrderLifecycleSnapshot,
    next_state: TestnetExecutionState,
    source: TestnetExecutionTransitionSource,
    reason: Option<String>,
    payload: Option<Value>,
) -> std::result::Result<TestnetExecutionTransition, TestnetExecutionStateError> {
    validate_testnet_transition(Some(snapshot.current_state), next_state, source)?;
    Ok(TestnetExecutionTransition {
        previous_state: Some(snapshot.current_state),
        next_state,
        source,
        reason,
        payload,
    })
}

pub fn map_exchange_ack_to_transition(
    ack: &ExchangeOrderAck,
) -> (TestnetExecutionState, Option<&'static str>) {
    let next = match ack.status {
        ExchangeOrderState::New => TestnetExecutionState::ExchangeAcked,
        ExchangeOrderState::PartiallyFilled => TestnetExecutionState::PartiallyFilled,
        ExchangeOrderState::Filled => TestnetExecutionState::Filled,
        ExchangeOrderState::Canceled | ExchangeOrderState::PendingCancel => {
            TestnetExecutionState::ReconciliationRequired
        }
        ExchangeOrderState::Rejected => TestnetExecutionState::Rejected,
        ExchangeOrderState::Expired => TestnetExecutionState::Expired,
    };
    (next, Some("exchange_ack"))
}

pub fn map_private_execution_report_to_transition(
    report: &ExchangeExecutionReport,
) -> (TestnetExecutionState, Option<&'static str>) {
    let next = match report.order_status {
        ExchangeExecutionStatus::New => TestnetExecutionState::New,
        ExchangeExecutionStatus::PartiallyFilled => TestnetExecutionState::PartiallyFilled,
        ExchangeExecutionStatus::Filled => TestnetExecutionState::Filled,
        ExchangeExecutionStatus::Canceled => TestnetExecutionState::Cancelled,
        ExchangeExecutionStatus::PendingCancel => TestnetExecutionState::CancelRequested,
        ExchangeExecutionStatus::Rejected => TestnetExecutionState::Rejected,
        ExchangeExecutionStatus::Expired | ExchangeExecutionStatus::ExpiredInMatch => {
            TestnetExecutionState::Expired
        }
        ExchangeExecutionStatus::Unknown => TestnetExecutionState::UnknownExchangeState,
    };
    let reason = match report.execution_type {
        ExchangeExecutionReportType::New => Some("execution_report_new"),
        ExchangeExecutionReportType::Canceled => Some("execution_report_canceled"),
        ExchangeExecutionReportType::Rejected => Some("execution_report_rejected"),
        ExchangeExecutionReportType::Trade => Some("execution_report_trade"),
        ExchangeExecutionReportType::Expired => Some("execution_report_expired"),
        ExchangeExecutionReportType::TradePrevention => Some("execution_report_trade_prevention"),
        ExchangeExecutionReportType::Replaced => Some("execution_report_replaced"),
        ExchangeExecutionReportType::Unknown => Some("execution_report_unknown"),
    };
    (next, reason)
}

pub fn map_rest_reconciliation_status_to_transition(
    status: &ExchangeOrderStatus,
) -> (TestnetExecutionState, Option<&'static str>) {
    let next = match status.status {
        ExchangeOrderState::New => TestnetExecutionState::New,
        ExchangeOrderState::PartiallyFilled => TestnetExecutionState::PartiallyFilled,
        ExchangeOrderState::Filled => TestnetExecutionState::Filled,
        ExchangeOrderState::Canceled => TestnetExecutionState::Cancelled,
        ExchangeOrderState::PendingCancel => TestnetExecutionState::CancelRequested,
        ExchangeOrderState::Rejected => TestnetExecutionState::Rejected,
        ExchangeOrderState::Expired => TestnetExecutionState::Expired,
    };
    (next, Some("rest_reconciliation_status"))
}

pub fn map_cancel_ack_to_transition(
    ack: &ExchangeCancelAck,
) -> (TestnetExecutionState, Option<&'static str>) {
    let next = match ack.status {
        ExchangeOrderState::Canceled => TestnetExecutionState::Cancelled,
        ExchangeOrderState::PendingCancel => TestnetExecutionState::CancelRequested,
        ExchangeOrderState::PartiallyFilled => TestnetExecutionState::PartiallyFilled,
        ExchangeOrderState::Filled => TestnetExecutionState::Filled,
        ExchangeOrderState::New => TestnetExecutionState::ReconciliationRequired,
        ExchangeOrderState::Rejected => TestnetExecutionState::ReconciliationRequired,
        ExchangeOrderState::Expired => TestnetExecutionState::Expired,
    };
    (next, Some("exchange_cancel_ack"))
}

pub fn hash_listen_key(listen_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(listen_key.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn mask_listen_key(listen_key: &str) -> String {
    let trimmed = listen_key.trim();
    if trimmed.len() <= 8 {
        return "********".to_string();
    }

    format!("{}***{}", &trimmed[..4], &trimmed[trimmed.len() - 4..])
}

pub fn build_private_stream_state(
    status: ExchangePrivateStreamStatus,
    listen_key_hash: Option<String>,
    connected_at: Option<DateTime<Utc>>,
    last_event_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    reconnect_count: i32,
) -> ExchangePrivateStreamState {
    ExchangePrivateStreamState {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        status,
        listen_key_hash,
        connected_at,
        last_event_at,
        last_error,
        reconnect_count,
        updated_at: Utc::now(),
    }
}

pub fn private_stream_is_stale(
    state: &ExchangePrivateStreamState,
    now: DateTime<Utc>,
    threshold: std::time::Duration,
) -> bool {
    state
        .last_event_at
        .and_then(|last_event_at| now.signed_duration_since(last_event_at).to_std().ok())
        .map(|age| age > threshold)
        .unwrap_or(false)
}

pub fn parse_binance_private_stream_event(
    payload: &Value,
    received_at: DateTime<Utc>,
) -> Result<PrivateStreamEventProcessResult> {
    let event_type = payload
        .get("e")
        .and_then(Value::as_str)
        .ok_or_else(|| ExchangeError::Serialization("missing event type".to_string()))?;
    let event_time = payload
        .get("E")
        .and_then(Value::as_i64)
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or(received_at);

    match event_type {
        "executionReport" => {
            let report = parse_binance_execution_report(payload)?;
            Ok(PrivateStreamEventProcessResult {
                event: ExchangePrivateStreamEvent {
                    exchange: ExchangeName::Binance,
                    environment: ExchangeEnvironment::Testnet,
                    source: ExchangePrivateStreamSource::Websocket,
                    event_type: event_type.to_string(),
                    symbol: Some(report.symbol.clone()),
                    client_order_id: Some(report.client_order_id.clone()),
                    exchange_order_id: report.exchange_order_id.clone(),
                    execution_type: Some(report.execution_type),
                    order_status: Some(report.order_status),
                    event_time,
                    received_at,
                    raw_payload: payload.clone(),
                },
                execution_report: Some(report),
            })
        }
        _ => Ok(PrivateStreamEventProcessResult {
            event: ExchangePrivateStreamEvent {
                exchange: ExchangeName::Binance,
                environment: ExchangeEnvironment::Testnet,
                source: ExchangePrivateStreamSource::Websocket,
                event_type: event_type.to_string(),
                symbol: payload
                    .get("s")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                client_order_id: payload
                    .get("c")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                exchange_order_id: payload
                    .get("i")
                    .map(|value| stringify_json_scalar(value.clone())),
                execution_type: None,
                order_status: None,
                event_time,
                received_at,
                raw_payload: payload.clone(),
            },
            execution_report: None,
        }),
    }
}

pub fn parse_binance_execution_report(payload: &Value) -> Result<ExchangeExecutionReport> {
    let symbol = payload.get("s").and_then(Value::as_str).ok_or_else(|| {
        ExchangeError::Serialization("missing executionReport symbol".to_string())
    })?;
    let client_order_id = payload.get("c").and_then(Value::as_str).ok_or_else(|| {
        ExchangeError::Serialization("missing executionReport client order id".to_string())
    })?;
    let side = parse_order_side(payload.get("S").and_then(Value::as_str).ok_or_else(|| {
        ExchangeError::Serialization("missing executionReport side".to_string())
    })?)?;
    let order_type =
        parse_order_type(payload.get("o").and_then(Value::as_str).ok_or_else(|| {
            ExchangeError::Serialization("missing executionReport order type".to_string())
        })?)?;
    let report = ExchangeExecutionReport {
        exchange: ExchangeName::Binance,
        environment: ExchangeEnvironment::Testnet,
        symbol: symbol.to_string(),
        client_order_id: client_order_id.to_string(),
        exchange_order_id: payload
            .get("i")
            .map(|value| stringify_json_scalar(value.clone()))
            .filter(|value| !value.is_empty() && value != "null"),
        side,
        order_type,
        time_in_force: parse_time_in_force(
            payload
                .get("f")
                .and_then(Value::as_str)
                .map(ToString::to_string),
        )?,
        order_status: parse_execution_status(
            payload
                .get("X")
                .and_then(Value::as_str)
                .unwrap_or("UNKNOWN"),
        ),
        execution_type: parse_execution_report_type(
            payload
                .get("x")
                .and_then(Value::as_str)
                .unwrap_or("UNKNOWN"),
        ),
        last_executed_qty: payload
            .get("l")
            .and_then(Value::as_str)
            .and_then(parse_decimal)
            .unwrap_or_default(),
        cumulative_filled_qty: payload
            .get("z")
            .and_then(Value::as_str)
            .and_then(parse_decimal)
            .unwrap_or_default(),
        last_executed_price: payload
            .get("L")
            .and_then(Value::as_str)
            .and_then(parse_decimal)
            .unwrap_or_default(),
        commission_amount: payload.get("n").and_then(Value::as_str).and_then(|value| {
            if value.is_empty() {
                None
            } else {
                parse_decimal(value)
            }
        }),
        commission_asset: payload.get("N").and_then(Value::as_str).and_then(|value| {
            if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            }
        }),
        event_time: payload
            .get("E")
            .and_then(Value::as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis)
            .unwrap_or_else(Utc::now),
        transaction_time: payload
            .get("T")
            .and_then(Value::as_i64)
            .and_then(DateTime::<Utc>::from_timestamp_millis),
        raw_payload: payload.clone(),
    };
    report
        .validate()
        .map_err(|err| ExchangeError::Validation(err.to_string()))?;
    Ok(report)
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoResponse {
    symbols: Vec<BinanceSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct BinanceSymbolInfo {
    symbol: String,
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    filters: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct BinanceAccountResponse {
    balances: Vec<BinanceBalance>,
}

#[derive(Debug, Deserialize)]
struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

#[derive(Debug, Deserialize)]
struct BinanceBalance {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BinanceOrderResponse {
    symbol: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    #[serde(rename = "orderId")]
    order_id: Option<Value>,
    status: String,
    #[serde(rename = "transactTime")]
    transact_time: Option<i64>,
    #[serde(rename = "executedQty")]
    executed_qty: Option<String>,
    #[serde(rename = "cummulativeQuoteQty")]
    cumulative_quote_qty: Option<String>,
    #[serde(rename = "workingTime")]
    working_time: Option<i64>,
}

impl BinanceOrderResponse {
    fn into_exchange_ack(self) -> ExchangeOrderAck {
        let raw_payload = serde_json::to_value(&self).unwrap_or(Value::Null);
        ExchangeOrderAck {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: self.symbol,
            client_order_id: self.client_order_id,
            exchange_order_id: self.order_id.map(stringify_json_scalar),
            status: parse_order_state(&self.status).unwrap_or(ExchangeOrderState::New),
            transact_time: millis_to_datetime(self.transact_time).unwrap_or_else(Utc::now),
            executed_qty: self
                .executed_qty
                .as_deref()
                .and_then(parse_decimal)
                .unwrap_or_default(),
            cumulative_quote_qty: self
                .cumulative_quote_qty
                .as_deref()
                .and_then(parse_decimal)
                .unwrap_or_default(),
            is_working: self.working_time.map(|_| true),
            raw_payload,
        }
    }

    fn into_exchange_cancel_ack(self) -> ExchangeCancelAck {
        let raw_payload = serde_json::to_value(&self).unwrap_or(Value::Null);
        ExchangeCancelAck {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: self.symbol,
            client_order_id: self.client_order_id,
            exchange_order_id: self.order_id.map(stringify_json_scalar),
            status: parse_order_state(&self.status).unwrap_or(ExchangeOrderState::Canceled),
            cancelled_at: millis_to_datetime(self.transact_time).unwrap_or_else(Utc::now),
            raw_payload,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BinanceOrderStatusResponse {
    symbol: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    #[serde(rename = "orderId")]
    order_id: Value,
    status: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(rename = "timeInForce")]
    time_in_force: Option<String>,
    #[serde(rename = "origQty")]
    original_qty: Option<String>,
    #[serde(rename = "executedQty")]
    executed_qty: String,
    #[serde(rename = "cummulativeQuoteQty")]
    cumulative_quote_qty: String,
    price: String,
    #[serde(rename = "updateTime")]
    update_time: Option<i64>,
}

impl BinanceOrderStatusResponse {
    fn into_exchange_status(self) -> Result<ExchangeOrderStatus> {
        let raw_payload = serde_json::to_value(&self).unwrap_or(Value::Null);
        Ok(ExchangeOrderStatus {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: self.symbol,
            client_order_id: self.client_order_id,
            exchange_order_id: Some(stringify_json_scalar(self.order_id)),
            status: parse_order_state(&self.status)?,
            side: parse_order_side(&self.side)?,
            order_type: parse_order_type(&self.order_type)?,
            time_in_force: parse_time_in_force(self.time_in_force)?,
            original_qty: self.original_qty.as_deref().and_then(parse_decimal),
            executed_qty: parse_decimal(&self.executed_qty).unwrap_or_default(),
            cumulative_quote_qty: parse_decimal(&self.cumulative_quote_qty).unwrap_or_default(),
            limit_price: parse_decimal(&self.price),
            updated_at: millis_to_datetime(self.update_time).unwrap_or_else(Utc::now),
            raw_payload,
        })
    }
}

fn millis_to_datetime(value: Option<i64>) -> Option<DateTime<Utc>> {
    value.and_then(DateTime::<Utc>::from_timestamp_millis)
}

fn stringify_json_scalar(value: Value) -> String {
    match value {
        Value::String(inner) => inner,
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_query_string, hash_listen_key, map_cancel_ack_to_transition,
        map_exchange_ack_to_transition, map_private_execution_report_to_transition,
        map_rest_reconciliation_status_to_transition, mask_listen_key,
        parse_binance_execution_report, sign_query, validate_testnet_transition,
        BinanceSpotTestnetAdapter, BinanceSpotTestnetConfig,
    };
    use aegis_core::{
        ExchangeCancelAck, ExchangeEnvironment, ExchangeExecutionReportType,
        ExchangeExecutionStatus, ExchangeName, ExchangeOrderAck, ExchangeOrderRequest,
        ExchangeOrderSide, ExchangeOrderState, ExchangeOrderStatus, ExchangeOrderType, Symbol,
        TestnetExecutionState, TestnetExecutionTransitionSource,
    };
    use chrono::Utc;
    use rust_decimal::Decimal;
    use serde_json::json;

    fn adapter() -> BinanceSpotTestnetAdapter {
        BinanceSpotTestnetAdapter::new(BinanceSpotTestnetConfig {
            environment: ExchangeEnvironment::Testnet,
            rest_base_url: "https://testnet.binance.vision".to_string(),
            ws_base_url: "wss://stream.testnet.binance.vision/ws".to_string(),
            api_key: None,
            api_secret: None,
            recv_window_ms: None,
        })
    }

    #[test]
    fn signing_is_deterministic() {
        let signature = sign_query("fake-secret", "symbol=BTCUSDT&timestamp=1").expect("signature");
        assert_eq!(
            signature,
            "05413c17c46ac8222eae047f28eba5e095e557c626f8dc6560a9d5882f2eefcf"
        );
    }

    #[test]
    fn query_string_is_sorted_deterministically() {
        let mut params = std::collections::BTreeMap::new();
        params.insert("symbol".to_string(), "BTCUSDT".to_string());
        params.insert("timestamp".to_string(), "1".to_string());
        assert_eq!(build_query_string(&params), "symbol=BTCUSDT&timestamp=1");
    }

    #[test]
    fn live_environment_is_rejected() {
        let adapter = adapter();
        let err = adapter
            .config()
            .validate_environment(ExchangeEnvironment::Live)
            .expect_err("live should be rejected");
        assert!(matches!(
            err,
            aegis_core::ExchangeError::LiveEnvironmentDisabled
        ));
    }

    #[test]
    fn missing_credentials_are_rejected() {
        let adapter = adapter();
        let err = adapter.config().credentials().expect_err("missing creds");
        assert!(matches!(err, aegis_core::ExchangeError::Configuration(_)));
    }

    #[test]
    fn request_validation_passes_for_testnet_market_order() {
        let request = ExchangeOrderRequest {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Market,
            time_in_force: None,
            quantity: None,
            quote_notional: Some(Decimal::new(10, 0)),
            limit_price: None,
            client_order_id: "client-1".to_string(),
            recv_window_ms: None,
            risk_decision_id: None,
        };

        request.validate().expect("valid request");
    }

    #[test]
    fn listen_key_url_construction_uses_testnet_ws_base() {
        let url = adapter()
            .build_user_stream_url("listen-key-1")
            .expect("url should build");
        assert_eq!(url, "wss://stream.testnet.binance.vision/ws/listen-key-1");
    }

    #[test]
    fn listen_key_masking_and_hashing_are_stable() {
        assert_eq!(mask_listen_key("abcdefghijklmnopqrstuvwxyz"), "abcd***wxyz");
        assert_eq!(
            hash_listen_key("listen-key"),
            "d2132747304b33d5c0da8efb961d0de7f2edddd2a32cc7e80cef635cb8ad0e47"
        );
    }

    #[test]
    fn parses_new_execution_report() {
        let report = parse_binance_execution_report(&json!({
            "e":"executionReport",
            "E":1710000000000i64,
            "s":"BTCUSDT",
            "c":"client-new",
            "i":12345,
            "S":"BUY",
            "o":"LIMIT",
            "f":"GTC",
            "x":"NEW",
            "X":"NEW",
            "l":"0",
            "z":"0",
            "L":"0",
            "n":"0",
            "N":null,
            "T":1710000000000i64
        }))
        .expect("report should parse");

        assert_eq!(report.execution_type, ExchangeExecutionReportType::New);
        assert_eq!(report.order_status, ExchangeExecutionStatus::New);
        assert_eq!(report.last_executed_qty, Decimal::ZERO);
    }

    #[test]
    fn parses_trade_filled_execution_report() {
        let report = parse_binance_execution_report(&json!({
            "e":"executionReport",
            "E":1710000000000i64,
            "s":"BTCUSDT",
            "c":"client-filled",
            "i":12345,
            "S":"BUY",
            "o":"MARKET",
            "f":"GTC",
            "x":"TRADE",
            "X":"FILLED",
            "l":"0.01000000",
            "z":"0.01000000",
            "L":"65000.12",
            "n":"0.00001000",
            "N":"BNB",
            "T":1710000000100i64
        }))
        .expect("report should parse");

        assert_eq!(report.execution_type, ExchangeExecutionReportType::Trade);
        assert_eq!(report.order_status, ExchangeExecutionStatus::Filled);
        assert_eq!(report.commission_asset.as_deref(), Some("BNB"));
        assert!(report.fill_event().is_some());
    }

    #[test]
    fn parses_canceled_execution_report() {
        let report = parse_binance_execution_report(&json!({
            "e":"executionReport",
            "E":1710000000000i64,
            "s":"BTCUSDT",
            "c":"client-cancel",
            "i":12345,
            "S":"SELL",
            "o":"LIMIT",
            "f":"GTC",
            "x":"CANCELED",
            "X":"CANCELED",
            "l":"0",
            "z":"0",
            "L":"0",
            "n":"0",
            "N":null,
            "T":1710000000200i64
        }))
        .expect("report should parse");

        assert_eq!(report.execution_type, ExchangeExecutionReportType::Canceled);
        assert_eq!(report.order_status, ExchangeExecutionStatus::Canceled);
    }

    #[test]
    fn parses_partially_filled_execution_report() {
        let report = parse_binance_execution_report(&json!({
            "e":"executionReport",
            "E":1710000000000i64,
            "s":"BTCUSDT",
            "c":"client-partial",
            "i":12345,
            "S":"BUY",
            "o":"LIMIT",
            "f":"GTC",
            "x":"TRADE",
            "X":"PARTIALLY_FILLED",
            "l":"0.00500000",
            "z":"0.00500000",
            "L":"64999.99",
            "n":"0.00000500",
            "N":"BNB",
            "T":1710000000300i64
        }))
        .expect("report should parse");

        assert_eq!(
            report.order_status,
            ExchangeExecutionStatus::PartiallyFilled
        );
    }

    #[test]
    fn unknown_execution_status_maps_to_unknown() {
        let report = parse_binance_execution_report(&json!({
            "e":"executionReport",
            "E":1710000000000i64,
            "s":"BTCUSDT",
            "c":"client-unknown",
            "i":12345,
            "S":"BUY",
            "o":"MARKET",
            "f":"GTC",
            "x":"SOMETHING_NEW",
            "X":"MYSTERY_STATUS",
            "l":"0",
            "z":"0",
            "L":"0",
            "n":"0",
            "N":null,
            "T":1710000000400i64
        }))
        .expect("report should parse");

        assert_eq!(report.execution_type, ExchangeExecutionReportType::Unknown);
        assert_eq!(report.order_status, ExchangeExecutionStatus::Unknown);
    }

    #[test]
    fn private_execution_report_maps_local_testnet_order_status() {
        let report = parse_binance_execution_report(&json!({
            "e":"executionReport",
            "E":1710000000000i64,
            "s":"BTCUSDT",
            "c":"client-map",
            "i":12345,
            "S":"BUY",
            "o":"MARKET",
            "f":"GTC",
            "x":"TRADE",
            "X":"PARTIALLY_FILLED",
            "l":"0.01",
            "z":"0.01",
            "L":"65000",
            "n":"0",
            "N":null,
            "T":1710000000000i64
        }))
        .expect("report should parse");

        assert_eq!(
            super::local_testnet_order_status_from_private_execution_report(&report),
            "PARTIALLY_FILLED"
        );
    }

    #[test]
    fn ack_does_not_imply_fill() {
        let ack = ExchangeOrderAck {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "BTCUSDT".to_string(),
            client_order_id: "client-1".to_string(),
            exchange_order_id: Some("123".to_string()),
            status: ExchangeOrderState::New,
            transact_time: Utc::now(),
            executed_qty: Decimal::ZERO,
            cumulative_quote_qty: Decimal::ZERO,
            is_working: Some(true),
            raw_payload: json!({"status":"NEW"}),
        };

        let (next, _) = map_exchange_ack_to_transition(&ack);
        assert_eq!(next, TestnetExecutionState::ExchangeAcked);
    }

    #[test]
    fn valid_transition_acked_to_new() {
        validate_testnet_transition(
            Some(TestnetExecutionState::ExchangeAcked),
            TestnetExecutionState::New,
            TestnetExecutionTransitionSource::PrivateStream,
        )
        .expect("transition should be valid");
    }

    #[test]
    fn valid_new_to_partial_to_filled() {
        validate_testnet_transition(
            Some(TestnetExecutionState::New),
            TestnetExecutionState::PartiallyFilled,
            TestnetExecutionTransitionSource::PrivateStream,
        )
        .expect("new to partial");
        validate_testnet_transition(
            Some(TestnetExecutionState::PartiallyFilled),
            TestnetExecutionState::Filled,
            TestnetExecutionTransitionSource::PrivateStream,
        )
        .expect("partial to filled");
    }

    #[test]
    fn valid_new_to_cancel_requested_to_cancelled() {
        validate_testnet_transition(
            Some(TestnetExecutionState::New),
            TestnetExecutionState::CancelRequested,
            TestnetExecutionTransitionSource::ApiCancel,
        )
        .expect("new to cancel_requested");
        validate_testnet_transition(
            Some(TestnetExecutionState::CancelRequested),
            TestnetExecutionState::Cancelled,
            TestnetExecutionTransitionSource::ExchangeCancelAck,
        )
        .expect("cancel_requested to cancelled");
    }

    #[test]
    fn invalid_filled_to_new_rejected() {
        assert!(validate_testnet_transition(
            Some(TestnetExecutionState::Filled),
            TestnetExecutionState::New,
            TestnetExecutionTransitionSource::PrivateStream,
        )
        .is_err());
    }

    #[test]
    fn invalid_cancelled_to_partial_rejected() {
        assert!(validate_testnet_transition(
            Some(TestnetExecutionState::Cancelled),
            TestnetExecutionState::PartiallyFilled,
            TestnetExecutionTransitionSource::PrivateStream,
        )
        .is_err());
    }

    #[test]
    fn unknown_exchange_state_maps_safely() {
        let report = parse_binance_execution_report(&json!({
            "e":"executionReport","E":1710000000000i64,"s":"BTCUSDT","c":"client-unknown-lifecycle",
            "i":12345,"S":"BUY","o":"MARKET","f":"GTC","x":"UNKNOWN","X":"MYSTERY_STATUS",
            "l":"0","z":"0","L":"0","n":"0","N":null,"T":1710000000400i64
        }))
        .expect("report should parse");
        let (next, _) = map_private_execution_report_to_transition(&report);
        assert!(matches!(
            next,
            TestnetExecutionState::UnknownExchangeState
                | TestnetExecutionState::ReconciliationRequired
        ));
    }

    #[test]
    fn private_stream_report_maps_through_validator() {
        let report = parse_binance_execution_report(&json!({
            "e":"executionReport","E":1710000000000i64,"s":"BTCUSDT","c":"client-stream",
            "i":12345,"S":"BUY","o":"MARKET","f":"GTC","x":"NEW","X":"NEW",
            "l":"0","z":"0","L":"0","n":"0","N":null,"T":1710000000000i64
        }))
        .expect("report should parse");
        let (next, _) = map_private_execution_report_to_transition(&report);
        let result = validate_testnet_transition(
            Some(TestnetExecutionState::ExchangeAcked),
            next,
            TestnetExecutionTransitionSource::PrivateStream,
        )
        .expect("validator should accept");
        assert_eq!(result.next_state, TestnetExecutionState::New);
    }

    #[test]
    fn rest_reconciliation_maps_through_same_validator() {
        let status = ExchangeOrderStatus {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "BTCUSDT".to_string(),
            client_order_id: "client-rest".to_string(),
            exchange_order_id: Some("123".to_string()),
            status: ExchangeOrderState::PartiallyFilled,
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Market,
            time_in_force: None,
            original_qty: Some(Decimal::new(1, 0)),
            executed_qty: Decimal::new(5, 1),
            cumulative_quote_qty: Decimal::new(500, 0),
            limit_price: None,
            updated_at: Utc::now(),
            raw_payload: json!({"status":"PARTIALLY_FILLED"}),
        };
        let (next, _) = map_rest_reconciliation_status_to_transition(&status);
        let result = validate_testnet_transition(
            Some(TestnetExecutionState::New),
            next,
            TestnetExecutionTransitionSource::RestReconciliation,
        )
        .expect("same validator");
        assert_eq!(result.next_state, TestnetExecutionState::PartiallyFilled);
    }

    #[test]
    fn cancel_ack_does_not_blindly_imply_success() {
        let ack = ExchangeCancelAck {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "BTCUSDT".to_string(),
            client_order_id: "client-cancel-ack".to_string(),
            exchange_order_id: Some("123".to_string()),
            status: ExchangeOrderState::New,
            cancelled_at: Utc::now(),
            raw_payload: json!({"status":"NEW"}),
        };
        let (next, _) = map_cancel_ack_to_transition(&ack);
        assert_eq!(next, TestnetExecutionState::ReconciliationRequired);
    }
}

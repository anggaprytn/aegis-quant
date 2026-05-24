use std::{collections::BTreeMap, env, sync::Arc};

use aegis_core::{
    ExchangeBalance, ExchangeCancelAck, ExchangeCancelRequest, ExchangeEnvironment, ExchangeError,
    ExchangeName, ExchangeOrderAck, ExchangeOrderRequest, ExchangeOrderSide, ExchangeOrderState,
    ExchangeOrderStatus, ExchangeOrderTimeInForce, ExchangeOrderType, ExchangeRateLimitState,
    ExchangeRequestMode, ExchangeSymbolInfo,
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
use sha2::Sha256;

pub type Result<T> = std::result::Result<T, ExchangeError>;

const DEFAULT_TESTNET_BASE_URL: &str = "https://testnet.binance.vision";
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
    pub configured: bool,
    pub request_mode: ExchangeRequestMode,
    pub rate_limits: ExchangeRateLimitState,
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
        build_query_string, sign_query, BinanceSpotTestnetAdapter, BinanceSpotTestnetConfig,
    };
    use aegis_core::{
        ExchangeEnvironment, ExchangeName, ExchangeOrderRequest, ExchangeOrderSide,
        ExchangeOrderType, Symbol,
    };
    use rust_decimal::Decimal;

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
        let adapter = BinanceSpotTestnetAdapter::new(BinanceSpotTestnetConfig {
            environment: ExchangeEnvironment::Testnet,
            rest_base_url: "https://testnet.binance.vision".to_string(),
            api_key: None,
            api_secret: None,
            recv_window_ms: None,
        });
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
        let adapter = BinanceSpotTestnetAdapter::new(BinanceSpotTestnetConfig {
            environment: ExchangeEnvironment::Testnet,
            rest_base_url: "https://testnet.binance.vision".to_string(),
            api_key: None,
            api_secret: None,
            recv_window_ms: None,
        });
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
}

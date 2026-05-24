use aegis_core::{BacktestRequest, PaperTradingPipelineRequest, PaperTradingPipelineResult};
use anyhow::Context;
use chrono::{DateTime, Utc};
use reqwest::{Method, StatusCode, Url};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ApiClientError {
    #[error("request to {endpoint} failed: {message}")]
    Transport { endpoint: String, message: String },
    #[error("request to {endpoint} returned HTTP {status}: {message}")]
    Http {
        endpoint: String,
        status: StatusCode,
        message: String,
        body: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct ApiClient {
    base_url: Url,
    http: reqwest::Client,
    auth_header: Option<String>,
}

impl ApiClient {
    pub fn new(base_url: Url) -> Self {
        Self {
            base_url,
            http: reqwest::Client::new(),
            auth_header: None,
        }
    }

    pub async fn get<T>(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<T, ApiClientError>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.request(Method::GET, endpoint, query, Option::<&Value>::None)
            .await
    }

    pub async fn post<T, B>(&self, endpoint: &str, body: &B) -> Result<T, ApiClientError>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize + ?Sized,
    {
        self.request(Method::POST, endpoint, &[], Some(body)).await
    }

    pub async fn get_value(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<Value, ApiClientError> {
        self.get(endpoint, query).await
    }

    fn endpoint_url(&self, endpoint: &str) -> Result<Url, ApiClientError> {
        let path = endpoint.trim_start_matches('/');
        self.base_url
            .join(path)
            .map_err(|err| ApiClientError::Transport {
                endpoint: endpoint.to_string(),
                message: format!("invalid endpoint URL: {err}"),
            })
    }

    async fn request<T, B>(
        &self,
        method: Method,
        endpoint: &str,
        query: &[(&str, String)],
        body: Option<&B>,
    ) -> Result<T, ApiClientError>
    where
        T: for<'de> Deserialize<'de>,
        B: Serialize + ?Sized,
    {
        let mut url = self.endpoint_url(endpoint)?;
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in query {
                if !value.is_empty() {
                    pairs.append_pair(key, value);
                }
            }
        }

        let mut request = self.http.request(method, url.clone());
        request = request.header("content-type", "application/json");
        if let Some(auth_header) = &self.auth_header {
            request = request.header("authorization", auth_header);
        }
        if let Some(payload) = body {
            request = request.json(payload);
        }

        let response = request
            .send()
            .await
            .map_err(|err| ApiClientError::Transport {
                endpoint: endpoint.to_string(),
                message: err.to_string(),
            })?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|err| ApiClientError::Transport {
                endpoint: endpoint.to_string(),
                message: err.to_string(),
            })?;

        if !status.is_success() {
            let safe_body = String::from_utf8_lossy(&bytes).trim().to_string();
            let message = if safe_body.is_empty() {
                format!("request failed with status {status}")
            } else {
                safe_body.clone()
            };
            return Err(ApiClientError::Http {
                endpoint: endpoint.to_string(),
                status,
                message,
                body: if safe_body.is_empty() {
                    None
                } else {
                    Some(safe_body)
                },
            });
        }

        serde_json::from_slice(&bytes).map_err(|err| ApiClientError::Transport {
            endpoint: endpoint.to_string(),
            message: format!("failed to parse JSON response: {err}"),
        })
    }

    pub async fn system_health(&self) -> Result<HealthResponse, ApiClientError> {
        self.get("/system/health", &[]).await
    }

    pub async fn system_status(&self) -> Result<StatusResponse, ApiClientError> {
        self.get("/system/status", &[]).await
    }

    pub async fn risk_status(&self) -> Result<RiskStatusResponse, ApiClientError> {
        self.get("/risk/status", &[]).await
    }

    pub async fn market_feed_status(&self) -> Result<FeedStatusResponse, ApiClientError> {
        self.get("/market/feed-status", &[]).await
    }

    pub async fn activate_kill_switch(
        &self,
        reason: Option<String>,
    ) -> Result<RiskActionResponse, ApiClientError> {
        self.post("/risk/kill-switch", &KillSwitchRequest { reason })
            .await
    }

    pub async fn resume_trading(
        &self,
        confirmation_text: &str,
        reason: Option<String>,
    ) -> Result<RiskActionResponse, ApiClientError> {
        self.post(
            "/risk/resume",
            &ResumeRequest {
                confirmation_text: confirmation_text.to_string(),
                reason,
            },
        )
        .await
    }

    pub async fn run_pipeline(
        &self,
        request: &PaperTradingPipelineRequest,
    ) -> Result<PaperTradingPipelineResult, ApiClientError> {
        self.post("/paper/pipeline/run", request).await
    }

    pub async fn list_strategies(&self) -> Result<StrategyListResponse, ApiClientError> {
        self.get("/strategy/list", &[]).await
    }

    pub async fn enable_strategy(
        &self,
        strategy_id: &str,
    ) -> Result<StrategyStatusResponse, ApiClientError> {
        self.post(&format!("/strategy/{strategy_id}/enable"), &EmptyRequest)
            .await
    }

    pub async fn disable_strategy(
        &self,
        strategy_id: &str,
    ) -> Result<StrategyStatusResponse, ApiClientError> {
        self.post(&format!("/strategy/{strategy_id}/disable"), &EmptyRequest)
            .await
    }

    pub async fn list_orders(&self) -> Result<OrdersResponse, ApiClientError> {
        self.get("/orders", &[]).await
    }

    pub async fn get_order(&self, order_id: Uuid) -> Result<OrderResponse, ApiClientError> {
        self.get(&format!("/orders/{order_id}"), &[]).await
    }

    pub async fn recent_events(
        &self,
        query: &RecentEventsQuery,
    ) -> Result<RecentEventsResponse, ApiClientError> {
        let params = query.to_query_params();
        self.get("/events/recent", &params).await
    }

    pub async fn risk_decisions(
        &self,
        query: &RiskDecisionsQuery,
    ) -> Result<RiskDecisionsResponse, ApiClientError> {
        self.get("/risk/decisions", &query.to_query_params()).await
    }

    pub async fn run_backtest(
        &self,
        request: &BacktestRequest,
    ) -> Result<BacktestRunAcceptedResponse, ApiClientError> {
        self.post("/backtest/run", request).await
    }

    pub async fn backtest_runs(&self, limit: i64) -> Result<BacktestRunsResponse, ApiClientError> {
        self.get("/backtest/runs", &[("limit", limit.to_string())])
            .await
    }

    pub async fn backtest_run(&self, run_id: Uuid) -> Result<BacktestRunResponse, ApiClientError> {
        self.get(&format!("/backtest/runs/{run_id}"), &[]).await
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecentEventsQuery {
    pub limit: i64,
    pub event_type: Option<String>,
    pub source: Option<String>,
    pub correlation_id: Option<Uuid>,
}

impl RecentEventsQuery {
    pub fn to_query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![("limit", self.limit.to_string())];
        if let Some(event_type) = self.event_type.as_deref().filter(|value| !value.is_empty()) {
            params.push(("event_type", event_type.to_string()));
        }
        if let Some(source) = self.source.as_deref().filter(|value| !value.is_empty()) {
            params.push(("source", source.to_string()));
        }
        if let Some(correlation_id) = self.correlation_id {
            params.push(("correlation_id", correlation_id.to_string()));
        }
        params
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RiskDecisionsQuery {
    pub limit: i64,
    pub symbol: Option<String>,
}

impl RiskDecisionsQuery {
    pub fn to_query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![("limit", self.limit.to_string())];
        if let Some(symbol) = self.symbol.as_deref().filter(|value| !value.is_empty()) {
            params.push(("symbol", symbol.to_string()));
        }
        params
    }
}

#[derive(Debug, Serialize)]
struct KillSwitchRequest {
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct ResumeRequest {
    confirmation_text: String,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct EmptyRequest;

#[derive(Debug, Deserialize, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub environment: String,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StatusResponse {
    pub service: String,
    pub environment: String,
    pub market_mode: String,
    pub started_at: DateTime<Utc>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
    pub dependencies: Dependencies,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Dependencies {
    pub database: DependencyStatus,
    pub event_bus: DependencyStatus,
    pub exchange_execution: DependencyStatus,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DependencyStatus {
    pub status: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemStateSnapshot {
    pub enabled: bool,
    pub reason: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: ActorResponse,
    pub last_correlation_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ActorResponse {
    pub actor: String,
    pub actor_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskStatusResponse {
    pub status: String,
    pub market_mode: String,
    pub paper_trading_allowed: bool,
    pub live_trading_allowed: bool,
    pub resume_confirmation_required: String,
    pub kill_switch: SystemStateSnapshot,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskActionResponse {
    pub status: String,
    pub message: String,
    pub market_mode: String,
    pub paper_trading_allowed: bool,
    pub live_trading_allowed: bool,
    pub kill_switch: SystemStateSnapshot,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MarketFeedStatusRecord {
    pub exchange: String,
    pub symbol: String,
    pub status: String,
    pub freshness_status: String,
    pub last_event_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub reconnect_count: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FeedStatusResponse {
    pub feeds: Vec<MarketFeedStatusRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyStatusView {
    pub strategy_id: String,
    pub status: String,
    pub mode: String,
    pub symbols: Vec<String>,
    pub timeframe: String,
    pub suggested_notional: String,
    pub momentum_lookback_candles: i32,
    pub breakout_lookback_candles: i32,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub last_evaluation_reason: Option<String>,
    pub last_signal_id: Option<Uuid>,
    pub last_signal_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyListResponse {
    pub strategies: Vec<StrategyStatusView>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StrategyStatusResponse {
    pub strategy: StrategyStatusView,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderRecord {
    pub order_id: Uuid,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub signal_id: Option<Uuid>,
    pub correlation_id: Uuid,
    pub risk_decision_id: Uuid,
    pub strategy_id: Option<String>,
    pub idempotency_key: String,
    pub requested_notional: Option<String>,
    pub symbol: String,
    pub side: String,
    pub quantity: String,
    pub filled_qty: String,
    pub limit_price: Option<String>,
    pub mode: String,
    pub market_mode: String,
    pub status: String,
    pub execution_state: String,
    pub status_reason: Option<String>,
    pub filled_price: Option<String>,
    pub avg_fill_price: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub rejected_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrdersResponse {
    pub orders: Vec<OrderRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderResponse {
    pub order: OrderRecord,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SystemEventRecord {
    pub event_id: Uuid,
    pub event_type: String,
    pub source: String,
    pub correlation_id: Uuid,
    pub payload: Option<Value>,
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RecentEventsResponse {
    pub events: Vec<SystemEventRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskDecisionRecord {
    pub id: Uuid,
    pub signal_id: Option<Uuid>,
    pub decision: String,
    pub approved_notional: Option<String>,
    pub risk_score: Option<String>,
    pub reasons: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Uuid,
    pub strategy_id: Option<String>,
    pub symbol: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RiskDecisionsResponse {
    pub decisions: Vec<RiskDecisionRecord>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BacktestRunAcceptedResponse {
    pub run_id: Uuid,
    pub status: String,
    pub strategy_id: String,
    pub symbol: String,
    pub trade_count: i32,
    pub pnl: String,
    pub pnl_pct: String,
    pub max_drawdown_pct: String,
    pub win_rate: String,
    pub fee_paid: String,
    pub slippage_cost: String,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BacktestResult {
    pub run_id: Uuid,
    pub strategy_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub initial_capital: Decimal,
    pub final_equity: Decimal,
    pub pnl: Decimal,
    pub pnl_pct: Decimal,
    pub max_drawdown_pct: Decimal,
    pub win_rate: Decimal,
    pub trade_count: i32,
    pub winning_trades: i32,
    pub losing_trades: i32,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub fee_paid: Decimal,
    pub slippage_cost: Decimal,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BacktestRunsResponse {
    pub runs: Vec<BacktestResult>,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BacktestRunResponse {
    pub run: BacktestResult,
    pub request_id: String,
    pub correlation_id: String,
    pub timestamp: DateTime<Utc>,
}

pub fn build_backtest_request(
    args: &crate::cli::BacktestRunArgs,
) -> anyhow::Result<BacktestRequest> {
    let request = BacktestRequest {
        strategy_id: args.strategy.clone(),
        symbol: args.symbol.clone(),
        timeframe: args.timeframe.clone(),
        start_time: args.start,
        end_time: args.end,
        initial_capital: args.initial_capital,
        risk_config_id: args.risk_config_id,
        risk_config: None,
        fee_bps: args.fee_bps,
        slippage_bps: args.slippage_bps,
        correlation_id: args.correlation_id,
        holding_candles: args.holding_candles,
    };
    request.validate().context("invalid backtest request")?;
    Ok(request)
}

pub fn build_pipeline_request(args: &crate::cli::PipelineRunArgs) -> PaperTradingPipelineRequest {
    PaperTradingPipelineRequest {
        strategy_id: args.strategy.clone(),
        symbol: args.symbol.clone(),
        timeframe: args.timeframe.clone(),
        correlation_id: args.correlation_id,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_backtest_request, build_pipeline_request, RecentEventsQuery, RiskDecisionsQuery,
    };
    use crate::cli::{BacktestRunArgs, PipelineRunArgs};
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use uuid::Uuid;

    #[test]
    fn recent_events_query_builds_expected_params() {
        let correlation_id =
            Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("valid uuid");
        let query = RecentEventsQuery {
            limit: 50,
            event_type: Some("risk.rejected".to_string()),
            source: Some("aegis-quant-api".to_string()),
            correlation_id: Some(correlation_id),
        };

        assert_eq!(
            query.to_query_params(),
            vec![
                ("limit", "50".to_string()),
                ("event_type", "risk.rejected".to_string()),
                ("source", "aegis-quant-api".to_string()),
                ("correlation_id", correlation_id.to_string()),
            ]
        );
    }

    #[test]
    fn risk_decisions_query_omits_blank_symbol() {
        let query = RiskDecisionsQuery {
            limit: 10,
            symbol: Some(String::new()),
        };

        assert_eq!(query.to_query_params(), vec![("limit", "10".to_string())]);
    }

    #[test]
    fn backtest_request_serializes_expected_wire_shape() {
        let args = BacktestRunArgs {
            strategy: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            start: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
            initial_capital: Decimal::new(1000000, 0),
            fee_bps: Decimal::new(10, 0),
            slippage_bps: Decimal::new(5, 0),
            holding_candles: Some(3),
            risk_config_id: None,
            correlation_id: None,
        };

        let request = build_backtest_request(&args).expect("valid request");
        let value = serde_json::to_value(request).expect("serializes");

        assert_eq!(value["strategy_id"], "momentum_v1");
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["timeframe"], "1m");
        assert_eq!(value["initial_capital"], "1000000");
        assert_eq!(value["fee_bps"], "10");
        assert_eq!(value["slippage_bps"], "5");
        assert_eq!(value["holding_candles"], 3);
    }

    #[test]
    fn pipeline_request_serializes_expected_wire_shape() {
        let correlation_id =
            Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("valid uuid");
        let args = PipelineRunArgs {
            strategy: "momentum_v1".to_string(),
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
            correlation_id: Some(correlation_id),
        };

        let request = build_pipeline_request(&args);
        let value = serde_json::to_value(request).expect("serializes");

        assert_eq!(value["strategy_id"], "momentum_v1");
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["timeframe"], "1m");
        assert_eq!(value["correlation_id"], correlation_id.to_string());
    }
}

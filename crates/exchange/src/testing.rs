use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use aegis_core::{
    ExchangeBalance, ExchangeCancelAck, ExchangeCancelRequest, ExchangeEnvironment, ExchangeError,
    ExchangeName, ExchangeOrderAck, ExchangeOrderRequest, ExchangeOrderSide, ExchangeOrderState,
    ExchangeOrderStatus, ExchangeOrderTimeInForce, ExchangeOrderType, ExchangeRequestMode,
    ExchangeSymbolInfo,
};
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::{json, Value};

use crate::{BinanceTestnetStatus, ExchangeAdapter, Result};

#[derive(Debug, Clone)]
pub struct FakeExchangeAdapter {
    state: Arc<Mutex<FakeExchangeState>>,
}

#[derive(Debug, Clone, Default)]
pub struct FakeExchangeCallLog {
    pub exchange_info_requests: usize,
    pub balance_requests: usize,
    pub submitted_orders: Vec<ExchangeOrderRequest>,
    pub cancelled_orders: Vec<ExchangeCancelRequest>,
    pub status_requests: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FakeSubmitAck {
    pub exchange_order_id: Option<String>,
    pub status: ExchangeOrderState,
    pub executed_qty: Decimal,
    pub cumulative_quote_qty: Decimal,
    pub is_working: Option<bool>,
    pub raw_payload: Value,
}

#[derive(Debug, Clone)]
pub struct FakeCancelAck {
    pub exchange_order_id: Option<String>,
    pub status: ExchangeOrderState,
    pub raw_payload: Value,
}

#[derive(Debug, Clone)]
pub struct FakeOrderStatus {
    pub exchange_order_id: Option<String>,
    pub status: ExchangeOrderState,
    pub side: ExchangeOrderSide,
    pub order_type: ExchangeOrderType,
    pub time_in_force: Option<ExchangeOrderTimeInForce>,
    pub original_qty: Option<Decimal>,
    pub executed_qty: Decimal,
    pub cumulative_quote_qty: Decimal,
    pub limit_price: Option<Decimal>,
    pub raw_payload: Value,
}

#[derive(Debug, Clone)]
enum FakeSubmitBehavior {
    Ack(FakeSubmitAck),
    Error(ExchangeError),
}

#[derive(Debug, Clone)]
enum FakeCancelBehavior {
    Ack(FakeCancelAck),
    Error(ExchangeError),
}

#[derive(Debug, Clone)]
enum FakeStatusBehavior {
    Status(FakeOrderStatus),
    Error(ExchangeError),
}

#[derive(Debug, Clone)]
struct FakeExchangeState {
    exchange_info: Vec<ExchangeSymbolInfo>,
    balances: Vec<ExchangeBalance>,
    submit_behaviors: VecDeque<FakeSubmitBehavior>,
    cancel_behaviors: VecDeque<FakeCancelBehavior>,
    status_behaviors: HashMap<String, FakeStatusBehavior>,
    call_log: FakeExchangeCallLog,
}

impl Default for FakeExchangeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for FakeSubmitAck {
    fn default() -> Self {
        Self {
            exchange_order_id: Some("fake-exchange-order-1".to_string()),
            status: ExchangeOrderState::New,
            executed_qty: Decimal::ZERO,
            cumulative_quote_qty: Decimal::ZERO,
            is_working: Some(true),
            raw_payload: json!({ "status": "NEW", "source": "fake" }),
        }
    }
}

impl Default for FakeCancelAck {
    fn default() -> Self {
        Self {
            exchange_order_id: Some("fake-exchange-order-1".to_string()),
            status: ExchangeOrderState::Canceled,
            raw_payload: json!({ "status": "CANCELED", "source": "fake" }),
        }
    }
}

impl FakeOrderStatus {
    pub fn new(status: ExchangeOrderState) -> Self {
        Self {
            exchange_order_id: Some("fake-exchange-order-1".to_string()),
            status,
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Limit,
            time_in_force: Some(ExchangeOrderTimeInForce::Gtc),
            original_qty: Some(Decimal::ONE),
            executed_qty: Decimal::ZERO,
            cumulative_quote_qty: Decimal::ZERO,
            limit_price: Some(Decimal::new(100_000, 0)),
            raw_payload: json!({ "status": status.as_str(), "source": "fake" }),
        }
    }
}

impl FakeExchangeAdapter {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeExchangeState {
                exchange_info: default_exchange_info(),
                balances: default_balances(),
                submit_behaviors: VecDeque::new(),
                cancel_behaviors: VecDeque::new(),
                status_behaviors: HashMap::new(),
                call_log: FakeExchangeCallLog::default(),
            })),
        }
    }

    pub fn status() -> BinanceTestnetStatus {
        BinanceTestnetStatus {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            rest_base_url: "https://fake.testnet.invalid".to_string(),
            ws_base_url: "wss://fake.testnet.invalid/ws".to_string(),
            configured: true,
            request_mode: ExchangeRequestMode::Signed,
            rate_limits: aegis_core::ExchangeRateLimitState {
                request_weight: None,
                orders_1m: None,
                raw_requests_5m: None,
                retry_after_ms: None,
            },
        }
    }

    pub fn set_exchange_info(&self, exchange_info: Vec<ExchangeSymbolInfo>) {
        self.state
            .lock()
            .expect("fake exchange mutex")
            .exchange_info = exchange_info;
    }

    pub fn set_balances(&self, balances: Vec<ExchangeBalance>) {
        self.state.lock().expect("fake exchange mutex").balances = balances;
    }

    pub fn push_submit_ack(&self, ack: FakeSubmitAck) {
        self.state
            .lock()
            .expect("fake exchange mutex")
            .submit_behaviors
            .push_back(FakeSubmitBehavior::Ack(ack));
    }

    pub fn push_submit_error(&self, err: ExchangeError) {
        self.state
            .lock()
            .expect("fake exchange mutex")
            .submit_behaviors
            .push_back(FakeSubmitBehavior::Error(err));
    }

    pub fn push_cancel_ack(&self, ack: FakeCancelAck) {
        self.state
            .lock()
            .expect("fake exchange mutex")
            .cancel_behaviors
            .push_back(FakeCancelBehavior::Ack(ack));
    }

    pub fn push_cancel_error(&self, err: ExchangeError) {
        self.state
            .lock()
            .expect("fake exchange mutex")
            .cancel_behaviors
            .push_back(FakeCancelBehavior::Error(err));
    }

    pub fn set_order_status(&self, client_order_id: impl Into<String>, status: FakeOrderStatus) {
        self.state
            .lock()
            .expect("fake exchange mutex")
            .status_behaviors
            .insert(client_order_id.into(), FakeStatusBehavior::Status(status));
    }

    pub fn set_order_status_error(&self, client_order_id: impl Into<String>, err: ExchangeError) {
        self.state
            .lock()
            .expect("fake exchange mutex")
            .status_behaviors
            .insert(client_order_id.into(), FakeStatusBehavior::Error(err));
    }

    pub fn calls(&self) -> FakeExchangeCallLog {
        self.state
            .lock()
            .expect("fake exchange mutex")
            .call_log
            .clone()
    }

    pub fn missing_order_error() -> ExchangeError {
        ExchangeError::Api("Order does not exist. -2013".to_string())
    }
}

#[async_trait]
impl ExchangeAdapter for FakeExchangeAdapter {
    async fn get_exchange_info(&self) -> Result<Vec<ExchangeSymbolInfo>> {
        let mut state = self.state.lock().expect("fake exchange mutex");
        state.call_log.exchange_info_requests += 1;
        Ok(state.exchange_info.clone())
    }

    async fn get_balances(&self) -> Result<Vec<ExchangeBalance>> {
        let mut state = self.state.lock().expect("fake exchange mutex");
        state.call_log.balance_requests += 1;
        Ok(state.balances.clone())
    }

    async fn submit_order(&self, order: ExchangeOrderRequest) -> Result<ExchangeOrderAck> {
        order
            .validate()
            .map_err(|err| ExchangeError::Validation(err.to_string()))?;
        let mut state = self.state.lock().expect("fake exchange mutex");
        state.call_log.submitted_orders.push(order.clone());
        match state.submit_behaviors.pop_front() {
            Some(FakeSubmitBehavior::Ack(ack)) => Ok(ExchangeOrderAck {
                exchange: ExchangeName::Binance,
                environment: ExchangeEnvironment::Testnet,
                symbol: order.symbol.to_string(),
                client_order_id: order.client_order_id,
                exchange_order_id: ack.exchange_order_id,
                status: ack.status,
                transact_time: Utc::now(),
                executed_qty: ack.executed_qty,
                cumulative_quote_qty: ack.cumulative_quote_qty,
                is_working: ack.is_working,
                raw_payload: ack.raw_payload,
            }),
            Some(FakeSubmitBehavior::Error(err)) => Err(err),
            None => Ok(ExchangeOrderAck {
                exchange: ExchangeName::Binance,
                environment: ExchangeEnvironment::Testnet,
                symbol: order.symbol.to_string(),
                client_order_id: order.client_order_id,
                exchange_order_id: Some("fake-exchange-order-default".to_string()),
                status: ExchangeOrderState::New,
                transact_time: Utc::now(),
                executed_qty: Decimal::ZERO,
                cumulative_quote_qty: Decimal::ZERO,
                is_working: Some(true),
                raw_payload: json!({ "status": "NEW", "source": "fake_default" }),
            }),
        }
    }

    async fn cancel_order(&self, request: ExchangeCancelRequest) -> Result<ExchangeCancelAck> {
        request
            .validate()
            .map_err(|err| ExchangeError::Validation(err.to_string()))?;
        let mut state = self.state.lock().expect("fake exchange mutex");
        state.call_log.cancelled_orders.push(request.clone());
        match state.cancel_behaviors.pop_front() {
            Some(FakeCancelBehavior::Ack(ack)) => Ok(ExchangeCancelAck {
                exchange: ExchangeName::Binance,
                environment: ExchangeEnvironment::Testnet,
                symbol: request.symbol.to_string(),
                client_order_id: request.client_order_id,
                exchange_order_id: ack.exchange_order_id,
                status: ack.status,
                cancelled_at: Utc::now(),
                raw_payload: ack.raw_payload,
            }),
            Some(FakeCancelBehavior::Error(err)) => Err(err),
            None => Ok(ExchangeCancelAck {
                exchange: ExchangeName::Binance,
                environment: ExchangeEnvironment::Testnet,
                symbol: request.symbol.to_string(),
                client_order_id: request.client_order_id,
                exchange_order_id: Some("fake-exchange-order-default".to_string()),
                status: ExchangeOrderState::Canceled,
                cancelled_at: Utc::now(),
                raw_payload: json!({ "status": "CANCELED", "source": "fake_default" }),
            }),
        }
    }

    async fn get_order_status(&self, client_order_id: &str) -> Result<ExchangeOrderStatus> {
        if client_order_id.trim().is_empty() {
            return Err(ExchangeError::Validation(
                "client_order_id cannot be empty".to_string(),
            ));
        }

        let mut state = self.state.lock().expect("fake exchange mutex");
        state
            .call_log
            .status_requests
            .push(client_order_id.to_string());
        match state.status_behaviors.get(client_order_id).cloned() {
            Some(FakeStatusBehavior::Status(status)) => Ok(ExchangeOrderStatus {
                exchange: ExchangeName::Binance,
                environment: ExchangeEnvironment::Testnet,
                symbol: "BTCUSDT".to_string(),
                client_order_id: client_order_id.to_string(),
                exchange_order_id: status.exchange_order_id,
                status: status.status,
                side: status.side,
                order_type: status.order_type,
                time_in_force: status.time_in_force,
                original_qty: status.original_qty,
                executed_qty: status.executed_qty,
                cumulative_quote_qty: status.cumulative_quote_qty,
                limit_price: status.limit_price,
                updated_at: Utc::now(),
                raw_payload: status.raw_payload,
            }),
            Some(FakeStatusBehavior::Error(err)) => Err(err),
            None => Err(Self::missing_order_error()),
        }
    }
}

fn default_exchange_info() -> Vec<ExchangeSymbolInfo> {
    vec![
        ExchangeSymbolInfo {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "BTCUSDT".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USDT".to_string(),
            status: "TRADING".to_string(),
            min_price: Some(Decimal::new(1, 2)),
            tick_size: Some(Decimal::new(1, 2)),
            min_qty: Some(Decimal::new(1, 5)),
            step_size: Some(Decimal::new(1, 5)),
            min_notional: Some(Decimal::new(10, 0)),
        },
        ExchangeSymbolInfo {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: "ETHUSDT".to_string(),
            base_asset: "ETH".to_string(),
            quote_asset: "USDT".to_string(),
            status: "TRADING".to_string(),
            min_price: Some(Decimal::new(1, 2)),
            tick_size: Some(Decimal::new(1, 2)),
            min_qty: Some(Decimal::new(1, 4)),
            step_size: Some(Decimal::new(1, 4)),
            min_notional: Some(Decimal::new(10, 0)),
        },
    ]
}

fn default_balances() -> Vec<ExchangeBalance> {
    vec![
        ExchangeBalance {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            asset: "USDT".to_string(),
            free: Decimal::new(50_000, 0),
            locked: Decimal::ZERO,
        },
        ExchangeBalance {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            asset: "BTC".to_string(),
            free: Decimal::new(5, 1),
            locked: Decimal::ZERO,
        },
        ExchangeBalance {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            asset: "ETH".to_string(),
            free: Decimal::new(25, 1),
            locked: Decimal::ZERO,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::{FakeExchangeAdapter, FakeOrderStatus, FakeSubmitAck};
    use crate::ExchangeAdapter;
    use aegis_core::{
        ExchangeCancelRequest, ExchangeEnvironment, ExchangeError, ExchangeName,
        ExchangeOrderRequest, ExchangeOrderSide, ExchangeOrderState, ExchangeOrderType, Symbol,
    };
    use rust_decimal::Decimal;

    fn sample_order(client_order_id: &str) -> ExchangeOrderRequest {
        ExchangeOrderRequest {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            side: ExchangeOrderSide::Buy,
            order_type: ExchangeOrderType::Limit,
            time_in_force: Some(aegis_core::ExchangeOrderTimeInForce::Gtc),
            quantity: Some(Decimal::ONE),
            quote_notional: None,
            limit_price: Some(Decimal::new(100_000, 0)),
            client_order_id: client_order_id.to_string(),
            recv_window_ms: None,
            risk_decision_id: None,
        }
    }

    fn sample_cancel(client_order_id: &str) -> ExchangeCancelRequest {
        ExchangeCancelRequest {
            exchange: ExchangeName::Binance,
            environment: ExchangeEnvironment::Testnet,
            symbol: Symbol::new("BTCUSDT").expect("symbol"),
            client_order_id: client_order_id.to_string(),
            recv_window_ms: None,
        }
    }

    #[tokio::test]
    async fn configurable_submit_ack_uses_request_client_order_id() {
        let adapter = FakeExchangeAdapter::new();
        adapter.push_submit_ack(FakeSubmitAck {
            exchange_order_id: Some("ack-1".to_string()),
            ..FakeSubmitAck::default()
        });

        let ack = adapter
            .submit_order(sample_order("client-1"))
            .await
            .expect("submit ack");

        assert_eq!(ack.client_order_id, "client-1");
        assert_eq!(ack.exchange_order_id.as_deref(), Some("ack-1"));
        assert_eq!(adapter.calls().submitted_orders.len(), 1);
    }

    #[tokio::test]
    async fn configurable_submit_error_is_returned() {
        let adapter = FakeExchangeAdapter::new();
        adapter.push_submit_error(ExchangeError::Transport("timeout".to_string()));

        let err = adapter
            .submit_order(sample_order("client-2"))
            .await
            .expect_err("submit should fail");

        assert_eq!(err, ExchangeError::Transport("timeout".to_string()));
    }

    #[tokio::test]
    async fn configurable_order_status_is_returned() {
        let adapter = FakeExchangeAdapter::new();
        adapter.set_order_status("client-3", FakeOrderStatus::new(ExchangeOrderState::Filled));

        let status = adapter.get_order_status("client-3").await.expect("status");

        assert_eq!(status.status, ExchangeOrderState::Filled);
        assert_eq!(
            adapter.calls().status_requests,
            vec!["client-3".to_string()]
        );
    }

    #[tokio::test]
    async fn configurable_cancel_behavior_is_returned() {
        let adapter = FakeExchangeAdapter::new();
        adapter.push_cancel_error(ExchangeError::Api("cancel rejected".to_string()));

        let err = adapter
            .cancel_order(sample_cancel("client-4"))
            .await
            .expect_err("cancel should fail");

        assert_eq!(err, ExchangeError::Api("cancel rejected".to_string()));
    }

    #[tokio::test]
    async fn no_network_is_needed_for_exchange_info_or_balances() {
        let adapter = FakeExchangeAdapter::new();

        let symbols = adapter.get_exchange_info().await.expect("symbols");
        let balances = adapter.get_balances().await.expect("balances");

        assert_eq!(symbols.len(), 2);
        assert!(symbols.iter().any(|symbol| symbol.symbol == "BTCUSDT"));
        assert!(balances.iter().any(|balance| balance.asset == "USDT"));
        let calls = adapter.calls();
        assert_eq!(calls.exchange_info_requests, 1);
        assert_eq!(calls.balance_requests, 1);
    }
}

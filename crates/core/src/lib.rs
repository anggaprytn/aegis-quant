use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

pub type Quantity = Decimal;
pub type Price = Decimal;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into().trim().to_ascii_uppercase();
        if value.is_empty() {
            return Err(CoreError::EmptySymbol);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for Symbol {
    type Error = CoreError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketMode {
    Paper,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Money {
    pub amount: Decimal,
    pub currency: String,
}

impl Money {
    pub fn new(amount: Decimal, currency: impl Into<String>) -> Self {
        Self {
            amount,
            currency: currency.into().to_ascii_uppercase(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub signal_id: Uuid,
    pub correlation_id: Uuid,
    pub symbol: Symbol,
    pub side: Side,
    pub strength: Decimal,
    pub strategy_name: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskDecision {
    Approved {
        decision_id: Uuid,
        rationale: String,
    },
    Rejected {
        decision_id: Uuid,
        rationale: String,
    },
    ManualReview {
        decision_id: Uuid,
        rationale: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Created,
    Accepted,
    Rejected,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    PendingRisk,
    RiskApproved,
    RiskRejected,
    ReadyForPaperExecution,
    SubmittedToPaperBroker,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub correlation_id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub source: String,
    pub payload: Value,
}

impl EventEnvelope {
    pub fn new(
        event_type: impl Into<String>,
        correlation_id: Uuid,
        source: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            correlation_id,
            event_type: event_type.into(),
            occurred_at: Utc::now(),
            source: source.into(),
            payload,
        }
    }
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("symbol cannot be empty")]
    EmptySymbol,
}

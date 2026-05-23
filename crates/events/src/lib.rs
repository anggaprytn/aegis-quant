use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use aegis_core::EventEnvelope;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    MarketTickReceived,
    SignalGenerated,
    RiskDecisionMade,
    OrderIntentCreated,
    ExecutionStateChanged,
    SystemHealthReported,
    AuditLogCaptured,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MarketTickReceived => "market_tick_received",
            Self::SignalGenerated => "signal_generated",
            Self::RiskDecisionMade => "risk_decision_made",
            Self::OrderIntentCreated => "order_intent_created",
            Self::ExecutionStateChanged => "execution_state_changed",
            Self::SystemHealthReported => "system_health_reported",
            Self::AuditLogCaptured => "audit_log_captured",
        }
    }
}

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: EventEnvelope) -> Result<()>;
}

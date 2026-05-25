use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
pub use db::SystemEventRecord;
use db::{insert_system_event, PgPool};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub use aegis_core::EventEnvelope;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemEventType {
    SystemStarted,
    MarketFeedConnected,
    MarketFeedDisconnected,
    MarketFeedStale,
    MarketTradeReceived,
    MarketCandleClosed,
    MarketBackfillStarted,
    MarketBackfillPageFetched,
    MarketBackfillCompleted,
    MarketBackfillFailed,
    ResearchDatasetBuildStarted,
    ResearchDatasetBackfillCompleted,
    ResearchDatasetAggregateCompleted,
    ResearchDatasetBuildCompleted,
    ResearchDatasetBuildFailed,
    SignalGenerated,
    RiskApproved,
    RiskRejected,
    OrderSubmitted,
    OrderAcked,
    OrderFilled,
    SystemKillSwitchEnabled,
    SystemKillSwitchDisabled,
}

impl SystemEventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SystemStarted => "system.started",
            Self::MarketFeedConnected => "market.feed.connected",
            Self::MarketFeedDisconnected => "market.feed.disconnected",
            Self::MarketFeedStale => "market.feed.stale",
            Self::MarketTradeReceived => "market.trade.received",
            Self::MarketCandleClosed => "market.candle.closed",
            Self::MarketBackfillStarted => "market.backfill.started",
            Self::MarketBackfillPageFetched => "market.backfill.page_fetched",
            Self::MarketBackfillCompleted => "market.backfill.completed",
            Self::MarketBackfillFailed => "market.backfill.failed",
            Self::ResearchDatasetBuildStarted => "research.dataset.build.started",
            Self::ResearchDatasetBackfillCompleted => "research.dataset.backfill.completed",
            Self::ResearchDatasetAggregateCompleted => "research.dataset.aggregate.completed",
            Self::ResearchDatasetBuildCompleted => "research.dataset.build.completed",
            Self::ResearchDatasetBuildFailed => "research.dataset.build.failed",
            Self::SignalGenerated => "signal.generated",
            Self::RiskApproved => "risk.approved",
            Self::RiskRejected => "risk.rejected",
            Self::OrderSubmitted => "order.submitted",
            Self::OrderAcked => "order.acked",
            Self::OrderFilled => "order.filled",
            Self::SystemKillSwitchEnabled => "system.kill_switch.enabled",
            Self::SystemKillSwitchDisabled => "system.kill_switch.disabled",
        }
    }

    pub fn into_event(
        self,
        correlation_id: Uuid,
        source: impl Into<String>,
        payload: Value,
    ) -> EventEnvelope {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            correlation_id,
            event_type: self.as_str().to_string(),
            occurred_at: Utc::now(),
            source: source.into(),
            payload,
        }
    }
}

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: EventEnvelope) -> Result<()>;
}

#[derive(Clone)]
pub struct PostgresEventPublisher {
    pool: PgPool,
}

impl PostgresEventPublisher {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventPublisher for PostgresEventPublisher {
    async fn publish(&self, event: EventEnvelope) -> Result<()> {
        insert_system_event(&self.pool, &event).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SystemEventType;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn system_event_types_use_audit_friendly_names() {
        assert_eq!(SystemEventType::SystemStarted.as_str(), "system.started");
        assert_eq!(
            SystemEventType::MarketFeedConnected.as_str(),
            "market.feed.connected"
        );
        assert_eq!(
            SystemEventType::MarketTradeReceived.as_str(),
            "market.trade.received"
        );
        assert_eq!(
            SystemEventType::MarketBackfillCompleted.as_str(),
            "market.backfill.completed"
        );
        assert_eq!(
            SystemEventType::ResearchDatasetBuildCompleted.as_str(),
            "research.dataset.build.completed"
        );
        assert_eq!(SystemEventType::RiskRejected.as_str(), "risk.rejected");
        assert_eq!(
            SystemEventType::SystemKillSwitchDisabled.as_str(),
            "system.kill_switch.disabled"
        );
    }

    #[test]
    fn builder_creates_envelope_with_required_fields() {
        let correlation_id = Uuid::new_v4();
        let event = SystemEventType::SignalGenerated.into_event(
            correlation_id,
            "strategy-engine",
            json!({ "strategy": "mean_reversion" }),
        );

        assert_eq!(event.correlation_id, correlation_id);
        assert_eq!(event.event_type, "signal.generated");
        assert_eq!(event.source, "strategy-engine");
        assert_eq!(event.payload, json!({ "strategy": "mean_reversion" }));
    }
}

use aegis_core::{RiskDecision, Signal};

pub trait RiskEngine: Send + Sync {
    fn evaluate(&self, signal: &Signal) -> RiskDecision;
}

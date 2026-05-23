use aegis_core::{ExecutionState, RiskDecision, Signal};

pub trait ExecutionEngine: Send + Sync {
    fn prepare(&self, signal: &Signal, decision: &RiskDecision) -> ExecutionState;
}

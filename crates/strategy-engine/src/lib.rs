use aegis_core::Signal;

pub trait StrategyEngine: Send + Sync {
    fn evaluate(&self) -> Option<Signal>;
}

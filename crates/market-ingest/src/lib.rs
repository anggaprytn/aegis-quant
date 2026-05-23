use aegis_core::Symbol;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketIngestConfig {
    pub symbols: Vec<Symbol>,
}

impl MarketIngestConfig {
    pub fn new(symbols: Vec<Symbol>) -> Self {
        Self { symbols }
    }
}

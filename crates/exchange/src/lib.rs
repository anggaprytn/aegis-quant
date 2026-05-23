use aegis_core::MarketMode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeAdapter {
    pub name: String,
    pub market_mode: MarketMode,
}

impl ExchangeAdapter {
    pub fn disabled(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            market_mode: MarketMode::Disabled,
        }
    }
}

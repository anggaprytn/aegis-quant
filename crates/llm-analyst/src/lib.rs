use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalystNote {
    pub summary: String,
    pub advisory_only: bool,
}

impl AnalystNote {
    pub fn advisory(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            advisory_only: true,
        }
    }
}

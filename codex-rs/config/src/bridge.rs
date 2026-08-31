//! User-facing configuration shared by remote bridge adapters.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fs;
use std::path::Path;
use thiserror::Error;

/// Top-level `[bridge]` settings shared by every remote provider adapter.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct BridgeConfigToml {
    /// Include reasoning summaries and raw reasoning content in remote output.
    #[serde(default)]
    pub include_reasoning: bool,
    /// Include tool calls and tool activity in remote output.
    #[serde(default)]
    pub include_tool_calls: bool,
    /// Include command and file-change approval notices in remote output.
    /// Permission requests are not controlled by this setting.
    #[serde(default)]
    pub include_approval_notices: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BridgeConfigError {
    #[error("failed to read bridge configuration")]
    Read,
    #[error("failed to parse bridge configuration")]
    Parse,
}

/// Loads exactly the `[bridge]` table from the base user config file.
pub fn load_user_bridge_config(path: &Path) -> Result<Option<BridgeConfigToml>, BridgeConfigError> {
    let contents = fs::read_to_string(path).map_err(|_| BridgeConfigError::Read)?;
    let mut document: toml::Value =
        toml::from_str(&contents).map_err(|_| BridgeConfigError::Parse)?;
    let table = document
        .as_table_mut()
        .and_then(|table| table.remove("bridge"));
    let Some(table) = table else {
        return Ok(None);
    };
    let contents = toml::to_string(&table).map_err(|_| BridgeConfigError::Parse)?;
    toml::from_str(&contents)
        .map(Some)
        .map_err(|_| BridgeConfigError::Parse)
}

#[cfg(test)]
#[path = "bridge_tests.rs"]
mod tests;

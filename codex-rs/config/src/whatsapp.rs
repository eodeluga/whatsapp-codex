//! User-facing WhatsApp configuration.
//!
//! WhatsApp is an optional Codex input transport.  The user config records
//! only that choice and the private account that may send it input. Gateway
//! credentials and container details are runtime state owned by Codex.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

/// Top-level `[whatsapp]` settings stored in the base user configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct WhatsAppConfigToml {
    #[serde(default)]
    pub onboarding_complete: bool,
    #[serde(default)]
    pub enabled: bool,
    pub account_phone_number: Option<String>,
}

/// Private local gateway state. This is deliberately stored outside
/// `config.toml`; users neither enter nor edit these values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WhatsAppRuntimeConfig {
    #[serde(default = "default_bridge_name")]
    pub bridge_name: String,
    pub transport_base_url: String,
    pub transport_api_token: String,
    pub webhook_signing_secret: String,
    pub webhook_url: String,
    pub bridge_listen: String,
    pub state_path: PathBuf,
    pub max_queued_prompts: usize,
    /// Legacy compatibility field. Delivery uses provider capabilities.
    #[serde(default, skip_serializing)]
    pub output_chunk_chars: usize,
    /// Legacy compatibility field. Edit scheduling belongs to the delivery worker.
    #[serde(default, skip_serializing)]
    pub edit_interval_ms: u64,
    pub dedupe_capacity: usize,
    pub dedupe_ttl_hours: u64,
}

fn default_bridge_name() -> String {
    "WhatsApp".to_string()
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WhatsAppConfigError {
    #[error("WhatsApp configuration is incomplete")]
    Incomplete,
    #[error("WhatsApp account phone number must be canonical E.164")]
    InvalidPhoneNumber,
    #[error("failed to read WhatsApp configuration")]
    Read,
    #[error("failed to parse WhatsApp configuration")]
    Parse,
    #[error("failed to persist WhatsApp runtime state")]
    Write,
}

impl WhatsAppConfigToml {
    pub fn is_complete(&self) -> bool {
        self.onboarding_complete && self.validate().is_ok()
    }

    /// Validates the small user-owned portion of the integration.
    pub fn validate(&self) -> Result<(), WhatsAppConfigError> {
        if !self.enabled {
            return Ok(());
        }
        let phone_number = self
            .account_phone_number
            .as_deref()
            .ok_or(WhatsAppConfigError::Incomplete)?;
        if canonical_e164(phone_number).is_some() {
            Ok(())
        } else {
            Err(WhatsAppConfigError::InvalidPhoneNumber)
        }
    }

    pub fn self_chat_jid(&self) -> Result<String, WhatsAppConfigError> {
        let phone_number = self
            .account_phone_number
            .as_deref()
            .ok_or(WhatsAppConfigError::Incomplete)?;
        let canonical =
            canonical_e164(phone_number).ok_or(WhatsAppConfigError::InvalidPhoneNumber)?;
        Ok(format!("{}@s.whatsapp.net", &canonical[1..]))
    }

    /// Returns the user-owned configuration safe for config RPC responses.
    /// Gateway credentials live only in private runtime state, so this value
    /// contains no secret material to replace.
    pub fn redacted(&self) -> Self {
        self.clone()
    }
}

/// Parses a canonical E.164 phone number and returns its normalized form.
pub fn canonical_e164(value: &str) -> Option<&str> {
    let digits = value.strip_prefix('+')?;
    if (8..=15).contains(&digits.len())
        && !digits.starts_with('0')
        && digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        Some(value)
    } else {
        None
    }
}

/// Loads exactly the `[whatsapp]` table from the base user config file.
pub fn load_user_whatsapp_config(
    path: &Path,
) -> Result<Option<WhatsAppConfigToml>, WhatsAppConfigError> {
    let contents = fs::read_to_string(path).map_err(|_| WhatsAppConfigError::Read)?;
    let mut document: toml::Value =
        toml::from_str(&contents).map_err(|_| WhatsAppConfigError::Parse)?;
    let table = document
        .as_table_mut()
        .and_then(|table| table.remove("whatsapp"));
    let Some(table) = table else {
        return Ok(None);
    };
    let contents = toml::to_string(&table).map_err(|_| WhatsAppConfigError::Parse)?;
    toml::from_str(&contents)
        .map(Some)
        .map_err(|_| WhatsAppConfigError::Parse)
}

pub fn whatsapp_runtime_path(codex_home: &Path) -> PathBuf {
    codex_home.join("whatsapp").join("runtime.json")
}

pub fn load_whatsapp_runtime_config(
    codex_home: &Path,
) -> Result<WhatsAppRuntimeConfig, WhatsAppConfigError> {
    let bytes =
        fs::read(whatsapp_runtime_path(codex_home)).map_err(|_| WhatsAppConfigError::Read)?;
    serde_json::from_slice(&bytes).map_err(|_| WhatsAppConfigError::Parse)
}

pub fn save_whatsapp_runtime_config(
    codex_home: &Path,
    runtime: &WhatsAppRuntimeConfig,
) -> Result<(), WhatsAppConfigError> {
    let path = whatsapp_runtime_path(codex_home);
    let parent = path.parent().ok_or(WhatsAppConfigError::Write)?;
    fs::create_dir_all(parent).map_err(|_| WhatsAppConfigError::Write)?;
    let contents = serde_json::to_vec_pretty(runtime).map_err(|_| WhatsAppConfigError::Parse)?;
    fs::write(&path, contents).map_err(|_| WhatsAppConfigError::Write)?;
    set_private_permissions(&path).map_err(|_| WhatsAppConfigError::Write)
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// The user-facing table contains no credentials. Runtime credentials are not
/// represented in TOML values and therefore cannot be exposed by config reads.
pub fn redact_whatsapp_toml_value(_value: &mut toml::Value) {}

#[cfg(test)]
#[path = "whatsapp_tests.rs"]
mod tests;

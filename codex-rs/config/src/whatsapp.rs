//! Configuration shared by the WhatsApp onboarding flow and bridge.
//!
//! The integration is deliberately user-config-only. Its credentials are not
//! suitable for repository, profile, or session configuration layers.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use thiserror::Error;

pub const REDACTED_SECRET: &str = "[redacted]";
pub const DEFAULT_TRIGGER_PREFIX: &str = "!codex ";
pub const DEFAULT_OPENWA_API_BASE_URL: &str = "http://openwa:2785/api";
pub const DEFAULT_OPENWA_WEBHOOK_URL: &str = "http://codex-whatsapp-bridge:8787/webhooks/openwa";
pub const DEFAULT_APP_SERVER_ENDPOINT: &str =
    "unix:///codex-home/app-server-control/app-server-control.sock";
pub const DEFAULT_BRIDGE_LISTEN: &str = "0.0.0.0:8787";
pub const DEFAULT_STATE_PATH: &str = "/codex-home/whatsapp/state.json";
pub const DEFAULT_MAX_QUEUED_PROMPTS: usize = 20;
pub const DEFAULT_OUTPUT_CHUNK_CHARS: usize = 3500;
pub const DEFAULT_EDIT_INTERVAL_MS: u64 = 1500;
pub const DEFAULT_DEDUPE_CAPACITY: usize = 10_000;
pub const DEFAULT_DEDUPE_TTL_HOURS: u64 = 168;

/// Top-level `[whatsapp]` settings stored exclusively in the base user config.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct WhatsAppConfigToml {
    #[serde(default)]
    pub onboarding_complete: bool,
    #[serde(default)]
    pub enabled: bool,
    pub account_phone_number: Option<String>,
    pub workspace: Option<PathBuf>,
    pub trigger_prefix: Option<String>,
    pub openwa: Option<OpenWaConfigToml>,
    pub bridge: Option<WhatsAppBridgeConfigToml>,
}

/// OpenWA connection and webhook settings.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct OpenWaConfigToml {
    pub api_base_url: Option<String>,
    pub session_id: Option<String>,
    pub api_key: Option<String>,
    pub webhook_signing_secret: Option<String>,
    pub webhook_url: Option<String>,
}

/// Local bridge runtime settings.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct WhatsAppBridgeConfigToml {
    pub app_server_endpoint: Option<String>,
    /// Opt in to a TCP app-server endpoint for advanced deployments.
    #[serde(default)]
    pub allow_tcp_app_server: bool,
    pub listen: Option<String>,
    pub state_path: Option<PathBuf>,
    pub max_queued_prompts: Option<usize>,
    pub output_chunk_chars: Option<usize>,
    pub edit_interval_ms: Option<u64>,
    pub dedupe_capacity: Option<usize>,
    pub dedupe_ttl_hours: Option<u64>,
}

impl fmt::Debug for WhatsAppConfigToml {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WhatsAppConfigToml")
            .field("onboarding_complete", &self.onboarding_complete)
            .field("enabled", &self.enabled)
            .field("account_phone_number", &self.account_phone_number)
            .field("workspace", &self.workspace)
            .field("trigger_prefix", &self.trigger_prefix)
            .field("openwa", &self.openwa)
            .field("bridge", &self.bridge)
            .finish()
    }
}

impl fmt::Debug for OpenWaConfigToml {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenWaConfigToml")
            .field("api_base_url", &self.api_base_url)
            .field("session_id", &self.session_id)
            .field("api_key", &self.api_key.as_ref().map(|_| REDACTED_SECRET))
            .field(
                "webhook_signing_secret",
                &self
                    .webhook_signing_secret
                    .as_ref()
                    .map(|_| REDACTED_SECRET),
            )
            .field("webhook_url", &self.webhook_url)
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WhatsAppConfigError {
    #[error("WhatsApp configuration is incomplete")]
    Incomplete,
    #[error("WhatsApp workspace must be an absolute existing directory")]
    InvalidWorkspace,
    #[error("WhatsApp account phone number must be canonical E.164")]
    InvalidPhoneNumber,
    #[error("OpenWA API base URL must include /api")]
    InvalidOpenWaApiBaseUrl,
    #[error("OpenWA session ID and API key must be non-empty")]
    InvalidOpenWaCredentials,
    #[error("WhatsApp webhook signing secret must contain at least 32 random bytes")]
    WeakWebhookSigningSecret,
    #[error("WhatsApp app-server endpoint must use unix:// unless TCP is explicitly enabled")]
    InvalidAppServerEndpoint,
    #[error("WhatsApp trigger prefix must be non-empty")]
    EmptyTriggerPrefix,
    #[error("WhatsApp queue and output limits must be positive")]
    InvalidLimits,
    #[error("failed to read WhatsApp configuration")]
    Read,
    #[error("failed to parse WhatsApp configuration")]
    Parse,
}

impl WhatsAppConfigToml {
    pub fn is_complete(&self) -> bool {
        self.enabled && self.validate().is_ok()
    }

    /// Validates only local configuration. It intentionally does not contact
    /// OpenWA or app-server so onboarding can complete while they are offline.
    pub fn validate(&self) -> Result<(), WhatsAppConfigError> {
        if !self.enabled {
            return Ok(());
        }
        let phone_number = self
            .account_phone_number
            .as_deref()
            .ok_or(WhatsAppConfigError::Incomplete)?;
        if canonical_e164(phone_number).is_none() {
            return Err(WhatsAppConfigError::InvalidPhoneNumber);
        }
        let workspace = self
            .workspace
            .as_deref()
            .ok_or(WhatsAppConfigError::Incomplete)?;
        if !workspace.is_absolute() || !workspace.is_dir() {
            return Err(WhatsAppConfigError::InvalidWorkspace);
        }
        if self.trigger_prefix().is_empty() {
            return Err(WhatsAppConfigError::EmptyTriggerPrefix);
        }
        let openwa = self
            .openwa
            .as_ref()
            .ok_or(WhatsAppConfigError::Incomplete)?;
        if !openwa.api_base_url().ends_with("/api") {
            return Err(WhatsAppConfigError::InvalidOpenWaApiBaseUrl);
        }
        if openwa.session_id.as_deref().is_none_or(str::is_empty)
            || openwa.api_key.as_deref().is_none_or(str::is_empty)
        {
            return Err(WhatsAppConfigError::InvalidOpenWaCredentials);
        }
        if !openwa.has_secure_webhook_signing_secret() {
            return Err(WhatsAppConfigError::WeakWebhookSigningSecret);
        }
        let bridge = self.bridge.as_ref().cloned().unwrap_or_default();
        let endpoint = bridge.app_server_endpoint();
        if !endpoint.starts_with("unix://") && !bridge.allow_tcp_app_server {
            return Err(WhatsAppConfigError::InvalidAppServerEndpoint);
        }
        if bridge.max_queued_prompts() == 0
            || bridge.output_chunk_chars() == 0
            || bridge.edit_interval_ms() == 0
            || bridge.dedupe_capacity() == 0
            || bridge.dedupe_ttl_hours() == 0
        {
            return Err(WhatsAppConfigError::InvalidLimits);
        }
        Ok(())
    }

    pub fn self_chat_jid(&self) -> Result<String, WhatsAppConfigError> {
        let phone_number = self
            .account_phone_number
            .as_deref()
            .ok_or(WhatsAppConfigError::Incomplete)?;
        let canonical =
            canonical_e164(phone_number).ok_or(WhatsAppConfigError::InvalidPhoneNumber)?;
        Ok(format!("{}@c.us", &canonical[1..]))
    }

    pub fn trigger_prefix(&self) -> &str {
        self.trigger_prefix
            .as_deref()
            .unwrap_or(DEFAULT_TRIGGER_PREFIX)
    }

    pub fn redacted(&self) -> Self {
        let mut redacted = self.clone();
        if let Some(openwa) = &mut redacted.openwa {
            if openwa.api_key.is_some() {
                openwa.api_key = Some(REDACTED_SECRET.to_string());
            }
            if openwa.webhook_signing_secret.is_some() {
                openwa.webhook_signing_secret = Some(REDACTED_SECRET.to_string());
            }
        }
        redacted
    }
}

impl OpenWaConfigToml {
    pub fn api_base_url(&self) -> &str {
        self.api_base_url
            .as_deref()
            .unwrap_or(DEFAULT_OPENWA_API_BASE_URL)
    }

    pub fn webhook_url(&self) -> &str {
        self.webhook_url
            .as_deref()
            .unwrap_or(DEFAULT_OPENWA_WEBHOOK_URL)
    }

    fn has_secure_webhook_signing_secret(&self) -> bool {
        self.webhook_signing_secret
            .as_deref()
            .and_then(|secret| URL_SAFE_NO_PAD.decode(secret).ok())
            .is_some_and(|secret| secret.len() >= 32)
    }
}

impl WhatsAppBridgeConfigToml {
    pub fn app_server_endpoint(&self) -> &str {
        self.app_server_endpoint
            .as_deref()
            .unwrap_or(DEFAULT_APP_SERVER_ENDPOINT)
    }

    pub fn listen(&self) -> &str {
        self.listen.as_deref().unwrap_or(DEFAULT_BRIDGE_LISTEN)
    }

    pub fn state_path(&self) -> &Path {
        self.state_path
            .as_deref()
            .unwrap_or_else(|| Path::new(DEFAULT_STATE_PATH))
    }

    pub fn max_queued_prompts(&self) -> usize {
        self.max_queued_prompts
            .unwrap_or(DEFAULT_MAX_QUEUED_PROMPTS)
    }

    pub fn output_chunk_chars(&self) -> usize {
        self.output_chunk_chars
            .unwrap_or(DEFAULT_OUTPUT_CHUNK_CHARS)
    }

    pub fn edit_interval_ms(&self) -> u64 {
        self.edit_interval_ms.unwrap_or(DEFAULT_EDIT_INTERVAL_MS)
    }

    pub fn dedupe_capacity(&self) -> usize {
        self.dedupe_capacity.unwrap_or(DEFAULT_DEDUPE_CAPACITY)
    }

    pub fn dedupe_ttl_hours(&self) -> u64 {
        self.dedupe_ttl_hours.unwrap_or(DEFAULT_DEDUPE_TTL_HOURS)
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
/// Other Codex configuration is deliberately not interpreted by the bridge.
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

/// Redacts WhatsApp credentials in a raw TOML value before it can leave the
/// process through diagnostics, config reads, or lockfile exports.
pub fn redact_whatsapp_toml_value(value: &mut toml::Value) {
    let Some(openwa) = value
        .as_table_mut()
        .and_then(|root| root.get_mut("whatsapp"))
        .and_then(toml::Value::as_table_mut)
        .and_then(|whatsapp| whatsapp.get_mut("openwa"))
        .and_then(toml::Value::as_table_mut)
    else {
        return;
    };
    for key in ["api_key", "webhook_signing_secret"] {
        if openwa.contains_key(key) {
            openwa.insert(
                key.to_string(),
                toml::Value::String(REDACTED_SECRET.to_string()),
            );
        }
    }
}

#[cfg(test)]
#[path = "whatsapp_tests.rs"]
mod tests;

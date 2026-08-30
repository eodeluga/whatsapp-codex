use codex_transcript::EntryOrigin;
use codex_transcript::TranscriptKey;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use thiserror::Error;

/// An opaque provider conversation identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderConversationId(String);

impl ProviderConversationId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ProviderConversationId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for ProviderConversationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// An opaque provider message identifier returned after a send.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderMessageId(String);

impl ProviderMessageId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ProviderMessageId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// A provider-owned reference to an inbound attachment.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderAttachment(String);

impl ProviderAttachment {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Inbound content after a provider has verified its identity and authenticity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundEnvelope {
    pub idempotency_key: String,
    pub provider_message_id: ProviderMessageId,
    pub conversation_id: ProviderConversationId,
    pub sender_id: String,
    pub body: String,
    pub attachments: Vec<ProviderAttachment>,
}

/// Capabilities that affect delivery scheduling, not provider authentication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCapabilities {
    pub message_limit: usize,
    pub edit_support: bool,
    pub attachment_support: bool,
    pub rich_interaction_support: bool,
}

/// A provider health snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderStatus {
    pub ready: bool,
    pub account: Option<String>,
}

/// Errors that can be surfaced by an adapter without exposing SDK details.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProviderError {
    #[error("provider transport failed")]
    Transport,
    #[error("provider rejected the request")]
    Unauthorized,
    #[error("provider is unavailable")]
    Unavailable,
    #[error("provider returned an invalid response")]
    InvalidResponse,
}

/// A provider-neutral adapter consumed by the delivery worker.
pub trait ProviderAdapter: Send + Sync {
    fn capabilities(&self) -> ProviderCapabilities;

    fn status(
        &self,
    ) -> impl std::future::Future<Output = Result<ProviderStatus, ProviderError>> + Send;

    fn send_text(
        &self,
        conversation_id: ProviderConversationId,
        text: String,
    ) -> impl std::future::Future<Output = Result<ProviderMessageId, ProviderError>> + Send;

    fn edit_text(
        &self,
        conversation_id: ProviderConversationId,
        message_id: ProviderMessageId,
        text: String,
    ) -> impl std::future::Future<Output = Result<(), ProviderError>> + Send;
}

/// A visible transcript revision waiting for provider delivery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryIntent {
    pub conversation_id: ProviderConversationId,
    pub key: TranscriptKey,
    pub origin: EntryOrigin,
    pub text: String,
    pub revision: u64,
    pub committed: bool,
}

/// Split text by Unicode scalar values without changing its contents.
pub fn segment_text(text: &str, limit: usize) -> Vec<String> {
    assert!(limit > 0, "provider message limits must be positive");
    if text.is_empty() {
        return vec![String::new()];
    }
    let chars: Vec<char> = text.chars().collect();
    chars
        .chunks(limit)
        .map(|chunk| chunk.iter().collect())
        .collect()
}

//! OpenWA webhook verification, normalization, and allowlist filtering.

use hmac::Hmac;
use hmac::Mac;
use serde::Deserialize;
use serde::Serialize;
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenWaWebhook {
    pub event: String,
    pub session_id: String,
    pub idempotency_key: Option<String>,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenWaMessage {
    pub id: String,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub body: String,
    #[serde(rename = "type")]
    pub message_type: String,
    #[serde(default)]
    pub is_group: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMessage {
    pub idempotency_key: String,
    pub message_id: String,
    pub chat_id: String,
    pub body: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WebhookError {
    #[error("invalid webhook signature")]
    InvalidSignature,
    #[error("invalid webhook body")]
    InvalidBody,
}

pub fn verify_signature(secret: &[u8], body: &[u8], signature: &str) -> bool {
    let normalized = signature.strip_prefix("sha256=").unwrap_or(signature);
    let Ok(actual) = hex_decode(normalized) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&actual).is_ok()
}

pub fn parse_verified_webhook(
    secret: &[u8],
    signature: &str,
    body: &[u8],
) -> Result<OpenWaWebhook, WebhookError> {
    if !verify_signature(secret, body, signature) {
        return Err(WebhookError::InvalidSignature);
    }
    serde_json::from_slice(body).map_err(|_| WebhookError::InvalidBody)
}

pub fn filter_inbound(
    webhook: OpenWaWebhook,
    session_id: &str,
    self_chat_id: &str,
    outbound_message_ids: impl Fn(&str) -> bool,
) -> Option<InboundMessage> {
    if !matches!(webhook.event.as_str(), "message.received" | "message.sent")
        || webhook.session_id != session_id
    {
        return None;
    }
    let message = serde_json::from_value::<OpenWaMessage>(webhook.data).ok()?;
    if message.message_type != "chat" || message.is_group || outbound_message_ids(&message.id) {
        return None;
    }
    let chat_id = normalized_chat_id(&message.from, &message.to)?;
    if chat_id != self_chat_id {
        return None;
    }
    Some(InboundMessage {
        idempotency_key: webhook
            .idempotency_key
            .unwrap_or_else(|| message.id.clone()),
        message_id: message.id,
        chat_id,
        body: message.body,
    })
}

fn normalized_chat_id(from: &str, to: &str) -> Option<String> {
    (from == to && from.ends_with("@c.us")).then(|| from.to_string())
}

fn hex_decode(value: &str) -> Result<Vec<u8>, ()> {
    if !value.is_ascii() || !value.len().is_multiple_of(2) {
        return Err(());
    }
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| ()))
        .collect()
}

#[cfg(test)]
#[path = "webhook_tests.rs"]
mod tests;

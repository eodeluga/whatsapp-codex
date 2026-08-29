//! Signed, transport-neutral inbound event handling.

use crate::attachment::InboundAttachment;
use hmac::Hmac;
use hmac::Mac;
use serde::Deserialize;
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportEvent {
    pub event: String,
    pub idempotency_key: String,
    pub data: TransportMessage,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportMessage {
    pub body: String,
    pub chat_id: String,
    pub from_me: bool,
    pub id: String,
    pub is_group: bool,
    #[serde(default)]
    pub is_self_chat: bool,
    #[serde(default)]
    pub attachment: Option<InboundAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMessage {
    pub idempotency_key: String,
    pub message_id: String,
    pub chat_id: String,
    pub body: String,
    pub attachment: Option<InboundAttachment>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WebhookError {
    #[error("invalid transport event signature")]
    InvalidSignature,
    #[error("invalid transport event body")]
    InvalidBody,
}

pub fn parse_verified_event(
    secret: &[u8],
    signature: &str,
    body: &[u8],
) -> Result<TransportEvent, WebhookError> {
    if !verify_signature(secret, body, signature) {
        return Err(WebhookError::InvalidSignature);
    }
    serde_json::from_slice(body).map_err(|_| WebhookError::InvalidBody)
}

pub fn filter_inbound(
    event: TransportEvent,
    self_chat_id: &str,
    outbound_message_ids: impl Fn(&str) -> bool,
) -> Option<InboundMessage> {
    let message = event.data;
    if event.event != "message"
        || message.is_group
        || !message.from_me
        || (!message.is_self_chat && message.chat_id != self_chat_id)
        || (message.body.is_empty() && message.attachment.is_none())
        || outbound_message_ids(&message.id)
    {
        return None;
    }
    Some(InboundMessage {
        idempotency_key: event.idempotency_key,
        message_id: message.id,
        chat_id: message.chat_id,
        body: message.body,
        attachment: message.attachment,
    })
}

fn verify_signature(secret: &[u8], body: &[u8], signature: &str) -> bool {
    let Some(encoded) = signature.strip_prefix("sha256=") else {
        return false;
    };
    let Ok(actual) = hex_decode(encoded) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&actual).is_ok()
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
#[path = "transport_webhook_tests.rs"]
mod tests;

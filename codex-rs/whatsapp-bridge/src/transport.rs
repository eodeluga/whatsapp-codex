//! Transport-neutral client for a private remote-input plugin.

use codex_messaging::ProviderAdapter;
use codex_messaging::ProviderCapabilities;
use codex_messaging::ProviderConversationId;
use codex_messaging::ProviderError;
use codex_messaging::ProviderMessageId;
use codex_messaging::ProviderStatus;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;
use thiserror::Error;

pub const MAX_TEXT_CHARS: usize = 4096;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransportError {
    #[error("remote transport request failed")]
    Transport,
    #[error("remote transport rejected the request")]
    Unauthorized,
    #[error("remote transport is unavailable")]
    Unavailable,
    #[error("remote transport returned an invalid response")]
    InvalidResponse,
    #[error("remote transport text is too long")]
    TextTooLong,
}

/// The versioned internal contract implemented by remote-input transport plugins.
pub trait TransportClient: Send + Sync {
    fn status(
        &self,
    ) -> impl std::future::Future<Output = Result<TransportStatus, TransportError>> + Send;
    fn send_text(
        &self,
        chat_id: String,
        text: String,
    ) -> impl std::future::Future<Output = Result<String, TransportError>> + Send;
    fn edit_text(
        &self,
        chat_id: String,
        message_id: String,
        text: String,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;
    fn pairing_qr(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<String>, TransportError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportStatus {
    pub status: String,
    pub account: Option<String>,
}

#[derive(Clone)]
pub struct HttpTransportClient {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

impl HttpTransportClient {
    pub fn new(base_url: String, token: String) -> Result<Self, TransportError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| TransportError::Transport)?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
        })
    }

    fn authorised(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.bearer_auth(&self.token)
    }
}

impl TransportClient for HttpTransportClient {
    async fn status(&self) -> Result<TransportStatus, TransportError> {
        let response = self
            .authorised(self.client.get(format!("{}/v1/status", self.base_url)))
            .send()
            .await
            .map_err(|_| TransportError::Transport)?;
        ensure_success(response.status())?;
        response
            .json()
            .await
            .map_err(|_| TransportError::InvalidResponse)
    }

    async fn send_text(&self, chat_id: String, text: String) -> Result<String, TransportError> {
        if text.chars().count() > MAX_TEXT_CHARS {
            return Err(TransportError::TextTooLong);
        }
        let response = self
            .authorised(
                self.client
                    .post(format!("{}/v1/messages", self.base_url))
                    .json(&SendTextRequest { chat_id, text }),
            )
            .send()
            .await
            .map_err(|_| TransportError::Transport)?;
        ensure_success(response.status())?;
        response
            .json::<SendTextResponse>()
            .await
            .map(|response| response.id)
            .map_err(|_| TransportError::InvalidResponse)
    }

    async fn edit_text(
        &self,
        chat_id: String,
        message_id: String,
        text: String,
    ) -> Result<(), TransportError> {
        if text.chars().count() > MAX_TEXT_CHARS {
            return Err(TransportError::TextTooLong);
        }
        let response = self
            .authorised(
                self.client
                    .post(format!("{}/v1/messages/edit", self.base_url))
                    .json(&EditTextRequest {
                        chat_id,
                        message_id,
                        text,
                    }),
            )
            .send()
            .await
            .map_err(|_| TransportError::Transport)?;
        ensure_success(response.status())
    }

    async fn pairing_qr(&self) -> Result<Option<String>, TransportError> {
        let response = self
            .authorised(self.client.get(format!("{}/v1/pairing", self.base_url)))
            .send()
            .await
            .map_err(|_| TransportError::Transport)?;
        ensure_success(response.status())?;
        response
            .json::<PairingResponse>()
            .await
            .map(|response| response.qr_code)
            .map_err(|_| TransportError::InvalidResponse)
    }
}

impl ProviderAdapter for HttpTransportClient {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            message_limit: MAX_TEXT_CHARS,
            edit_support: true,
            attachment_support: true,
            rich_interaction_support: false,
        }
    }

    async fn status(&self) -> Result<ProviderStatus, ProviderError> {
        TransportClient::status(self)
            .await
            .map(|status| ProviderStatus {
                ready: status.status.eq_ignore_ascii_case("ready"),
                account: status.account,
            })
            .map_err(provider_error)
    }

    async fn send_text(
        &self,
        conversation_id: ProviderConversationId,
        text: String,
    ) -> Result<ProviderMessageId, ProviderError> {
        TransportClient::send_text(self, conversation_id.as_str().to_string(), text)
            .await
            .map(ProviderMessageId::new)
            .map_err(provider_error)
    }

    async fn edit_text(
        &self,
        conversation_id: ProviderConversationId,
        message_id: ProviderMessageId,
        text: String,
    ) -> Result<(), ProviderError> {
        TransportClient::edit_text(
            self,
            conversation_id.as_str().to_string(),
            message_id.as_str().to_string(),
            text,
        )
        .await
        .map_err(provider_error)
    }
}

fn provider_error(error: TransportError) -> ProviderError {
    match error {
        TransportError::Transport | TransportError::TextTooLong => ProviderError::Transport,
        TransportError::Unauthorized => ProviderError::Unauthorized,
        TransportError::Unavailable => ProviderError::Unavailable,
        TransportError::InvalidResponse => ProviderError::InvalidResponse,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SendTextRequest {
    chat_id: String,
    text: String,
}
#[derive(Deserialize)]
struct SendTextResponse {
    id: String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EditTextRequest {
    chat_id: String,
    message_id: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PairingResponse {
    qr_code: Option<String>,
}

fn ensure_success(status: StatusCode) -> Result<(), TransportError> {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(TransportError::Unauthorized),
        StatusCode::CONFLICT | StatusCode::SERVICE_UNAVAILABLE => Err(TransportError::Unavailable),
        status if status.is_success() => Ok(()),
        _ => Err(TransportError::InvalidResponse),
    }
}

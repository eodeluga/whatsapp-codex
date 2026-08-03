//! Small OpenWA REST adapter.

use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OpenWaError {
    #[error("OpenWA request failed")]
    Transport,
    #[error("OpenWA rejected the request")]
    Unauthorized,
    #[error("OpenWA is rate limiting requests")]
    RateLimited,
    #[error("OpenWA server failed")]
    Server,
    #[error("OpenWA returned an invalid response")]
    InvalidResponse,
    #[error("WhatsApp text is too long")]
    TextTooLong,
}

/// OpenWA operations used by the coordinator.
pub trait OpenWaClient: Send + Sync {
    fn session_status(
        &self,
    ) -> impl std::future::Future<Output = Result<OpenWaSession, OpenWaError>> + Send;

    fn send_text(
        &self,
        chat_id: String,
        text: String,
    ) -> impl std::future::Future<Output = Result<String, OpenWaError>> + Send;

    fn edit_text(
        &self,
        chat_id: String,
        message_id: String,
        text: String,
    ) -> impl std::future::Future<Output = Result<(), OpenWaError>> + Send;

    fn register_webhook(
        &self,
        url: String,
        secret: String,
    ) -> impl std::future::Future<Output = Result<(), OpenWaError>> + Send;

    fn pairing_qr(
        &self,
    ) -> impl std::future::Future<Output = Result<serde_json::Value, OpenWaError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenWaSession {
    pub status: String,
    #[serde(alias = "phoneNumber")]
    pub phone: Option<String>,
}

#[derive(Clone)]
pub struct HttpOpenWaClient {
    client: reqwest::Client,
    api_base_url: String,
    session_id: String,
    api_key: String,
}

impl HttpOpenWaClient {
    pub fn new(
        api_base_url: String,
        session_id: String,
        api_key: String,
    ) -> Result<Self, OpenWaError> {
        Self::with_timeout(api_base_url, session_id, api_key, Duration::from_secs(10))
    }

    fn with_timeout(
        api_base_url: String,
        session_id: String,
        api_key: String,
        timeout: Duration,
    ) -> Result<Self, OpenWaError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|_| OpenWaError::Transport)?;
        Ok(Self {
            client,
            api_base_url: api_base_url.trim_end_matches('/').to_string(),
            session_id,
            api_key,
        })
    }
}

/// Creates and starts the private OpenWA session, returning its one-time
/// session-scoped operator key. OpenWA owns the administrator credential; the
/// bridge reads it only from the Docker-managed data volume.
pub async fn provision_session(
    api_base_url: &str,
    session_id: &str,
    administrator_key: &str,
) -> Result<String, OpenWaError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| OpenWaError::Transport)?;
    let api_base_url = api_base_url.trim_end_matches('/');
    let create = client
        .post(format!("{api_base_url}/sessions"))
        .header("X-API-Key", administrator_key)
        .json(&json!({ "name": session_id }))
        .send()
        .await
        .map_err(|_| OpenWaError::Transport)?;
    if !create.status().is_success() && create.status() != StatusCode::CONFLICT {
        ensure_success(create.status())?;
    }
    let start = client
        .post(format!("{api_base_url}/sessions/{session_id}/start"))
        .header("X-API-Key", administrator_key)
        .send()
        .await
        .map_err(|_| OpenWaError::Transport)?;
    if !start.status().is_success() && start.status() != StatusCode::CONFLICT {
        ensure_success(start.status())?;
    }
    let response = client
        .post(format!("{api_base_url}/auth/api-keys"))
        .header("X-API-Key", administrator_key)
        .json(&json!({
            "name": "WhatsApp Codex bridge",
            "role": "operator",
            "allowedSessions": [session_id],
        }))
        .send()
        .await
        .map_err(|_| OpenWaError::Transport)?;
    ensure_success(response.status())?;
    response
        .json::<ProvisionedApiKey>()
        .await
        .map_err(|_| OpenWaError::InvalidResponse)
        .map(|response| response.api_key)
}

impl OpenWaClient for HttpOpenWaClient {
    async fn session_status(&self) -> Result<OpenWaSession, OpenWaError> {
        let response = self
            .client
            .get(format!(
                "{}/sessions/{}",
                self.api_base_url, self.session_id
            ))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .map_err(|_| OpenWaError::Transport)?;
        ensure_success(response.status())?;
        response
            .json()
            .await
            .map_err(|_| OpenWaError::InvalidResponse)
    }

    async fn send_text(&self, chat_id: String, text: String) -> Result<String, OpenWaError> {
        if text.chars().count() > 4096 {
            return Err(OpenWaError::TextTooLong);
        }
        for attempt in 0..3 {
            let response = self
                .client
                .post(format!(
                    "{}/sessions/{}/messages/send-text",
                    self.api_base_url, self.session_id
                ))
                .header("X-API-Key", &self.api_key)
                .json(&SendTextRequest {
                    chat_id: chat_id.clone(),
                    text: text.clone(),
                })
                .send()
                .await
                .map_err(|_| OpenWaError::Transport)?;
            match ensure_success(response.status()) {
                Ok(()) => {
                    return response
                        .json::<SendTextResponse>()
                        .await
                        .map_err(|_| OpenWaError::InvalidResponse)
                        .map(|response| response.message_id);
                }
                Err(OpenWaError::RateLimited | OpenWaError::Server) if attempt < 2 => {
                    retry_delay(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(OpenWaError::Server)
    }

    async fn edit_text(
        &self,
        chat_id: String,
        message_id: String,
        text: String,
    ) -> Result<(), OpenWaError> {
        if text.chars().count() > 4096 {
            return Err(OpenWaError::TextTooLong);
        }
        for attempt in 0..3 {
            let response = self
                .client
                .post(format!(
                    "{}/sessions/{}/messages/edit",
                    self.api_base_url, self.session_id
                ))
                .header("X-API-Key", &self.api_key)
                .json(&EditTextRequest {
                    chat_id: chat_id.clone(),
                    message_id: message_id.clone(),
                    body: text.clone(),
                })
                .send()
                .await
                .map_err(|_| OpenWaError::Transport)?;
            match ensure_success(response.status()) {
                Err(OpenWaError::RateLimited | OpenWaError::Server) if attempt < 2 => {
                    retry_delay(attempt).await;
                }
                result => return result,
            }
        }
        Err(OpenWaError::Server)
    }

    async fn register_webhook(&self, url: String, secret: String) -> Result<(), OpenWaError> {
        for attempt in 0..3 {
            let list_response = self
                .client
                .get(format!(
                    "{}/sessions/{}/webhooks",
                    self.api_base_url, self.session_id
                ))
                .header("X-API-Key", &self.api_key)
                .send()
                .await
                .map_err(|_| OpenWaError::Transport)?;
            match ensure_success(list_response.status()) {
                Err(OpenWaError::RateLimited | OpenWaError::Server) if attempt < 2 => {
                    retry_delay(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
                Ok(()) => {}
            }
            let webhooks = list_response
                .json::<Vec<WebhookResponse>>()
                .await
                .map_err(|_| OpenWaError::InvalidResponse)?;
            let request =
                if let Some(existing) = webhooks.into_iter().find(|webhook| webhook.url == url) {
                    self.client.put(format!(
                        "{}/sessions/{}/webhooks/{}",
                        self.api_base_url, self.session_id, existing.id
                    ))
                } else {
                    self.client.post(format!(
                        "{}/sessions/{}/webhooks",
                        self.api_base_url, self.session_id
                    ))
                };
            let response = request
                .header("X-API-Key", &self.api_key)
                .json(&RegisterWebhookRequest {
                    url: url.clone(),
                    events: vec![
                        "message.received".to_string(),
                        "message.sent".to_string(),
                        "session.status".to_string(),
                    ],
                    secret: secret.clone(),
                })
                .send()
                .await
                .map_err(|_| OpenWaError::Transport)?;
            match ensure_success(response.status()) {
                Err(OpenWaError::RateLimited | OpenWaError::Server) if attempt < 2 => {
                    retry_delay(attempt).await;
                }
                result => return result,
            }
        }
        Err(OpenWaError::Server)
    }

    async fn pairing_qr(&self) -> Result<serde_json::Value, OpenWaError> {
        let response = self
            .client
            .get(format!(
                "{}/sessions/{}/qr",
                self.api_base_url, self.session_id
            ))
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .map_err(|_| OpenWaError::Transport)?;
        ensure_success(response.status())?;
        response
            .json()
            .await
            .map_err(|_| OpenWaError::InvalidResponse)
    }
}

async fn retry_delay(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))).await;
}

fn ensure_success(status: StatusCode) -> Result<(), OpenWaError> {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(OpenWaError::Unauthorized),
        StatusCode::TOO_MANY_REQUESTS => Err(OpenWaError::RateLimited),
        status if status.is_server_error() => Err(OpenWaError::Server),
        status if !status.is_success() => Err(OpenWaError::InvalidResponse),
        _ => Ok(()),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SendTextRequest {
    chat_id: String,
    text: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EditTextRequest {
    chat_id: String,
    message_id: String,
    body: String,
}

#[derive(Serialize)]
struct RegisterWebhookRequest {
    url: String,
    events: Vec<String>,
    secret: String,
}

#[derive(Deserialize)]
struct WebhookResponse {
    id: String,
    url: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendTextResponse {
    message_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProvisionedApiKey {
    api_key: String,
}

#[cfg(test)]
#[path = "openwa_tests.rs"]
mod tests;

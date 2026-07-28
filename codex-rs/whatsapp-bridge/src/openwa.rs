//! Small OpenWA REST adapter.

use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
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
    fn send_text(
        &self,
        chat_id: String,
        text: String,
    ) -> impl std::future::Future<Output = Result<String, OpenWaError>> + Send;
}

#[derive(Clone)]
pub struct HttpOpenWaClient {
    client: reqwest::Client,
    api_base_url: String,
    session_id: String,
    api_key: String,
}

impl HttpOpenWaClient {
    pub fn new(api_base_url: String, session_id: String, api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_base_url: api_base_url.trim_end_matches('/').to_string(),
            session_id,
            api_key,
        }
    }
}

impl OpenWaClient for HttpOpenWaClient {
    async fn send_text(&self, chat_id: String, text: String) -> Result<String, OpenWaError> {
        if text.chars().count() > 4096 {
            return Err(OpenWaError::TextTooLong);
        }
        let response = self
            .client
            .post(format!(
                "{}/sessions/{}/messages/send-text",
                self.api_base_url, self.session_id
            ))
            .header("X-API-Key", &self.api_key)
            .json(&SendTextRequest { chat_id, text })
            .send()
            .await
            .map_err(|_| OpenWaError::Transport)?;
        match response.status() {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                return Err(OpenWaError::Unauthorized);
            }
            StatusCode::TOO_MANY_REQUESTS => return Err(OpenWaError::RateLimited),
            status if status.is_server_error() => return Err(OpenWaError::Server),
            status if !status.is_success() => return Err(OpenWaError::InvalidResponse),
            _ => {}
        }
        response
            .json::<SendTextResponse>()
            .await
            .map_err(|_| OpenWaError::InvalidResponse)
            .map(|response| response.id)
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

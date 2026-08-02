//! Narrow adapter over Codex's normal app-server client protocol.

use codex_app_server_client::AppServerEvent;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
use codex_app_server_protocol::ApprovalsReviewer;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnInterruptParams;
use codex_app_server_protocol::TurnInterruptResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput;
use codex_utils_absolute_path::AbsolutePathBuf;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CodexError {
    #[error("failed to communicate with Codex app-server")]
    Transport,
}

/// Codex operations used by the coordinator.
///
/// Implementations preserve the app-server protocol's request and event
/// semantics while allowing coordinator tests to use an in-memory fake.
pub trait CodexClient: Send {
    fn start_thread(&self) -> impl std::future::Future<Output = Result<String, CodexError>> + Send;

    fn resume_thread(
        &self,
        thread_id: String,
    ) -> impl std::future::Future<Output = Result<ThreadResumeResponse, CodexError>> + Send;

    fn start_turn(
        &self,
        thread_id: String,
        message_id: String,
        prompt: String,
    ) -> impl std::future::Future<Output = Result<String, CodexError>> + Send;

    fn interrupt_turn(
        &self,
        thread_id: String,
        turn_id: String,
    ) -> impl std::future::Future<Output = Result<(), CodexError>> + Send;

    fn next_event(&mut self) -> impl std::future::Future<Output = Option<AppServerEvent>> + Send;

    fn reject_server_request(
        &self,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = Result<(), CodexError>> + Send;

    fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: serde_json::Value,
    ) -> impl std::future::Future<Output = Result<(), CodexError>> + Send;

    fn reconnect(&mut self) -> impl std::future::Future<Output = Result<(), CodexError>> + Send;

    fn shutdown(&mut self) -> impl std::future::Future<Output = Result<(), CodexError>> + Send;
}

pub struct RemoteCodexClient {
    client: Option<RemoteAppServerClient>,
    endpoint: RemoteAppServerEndpoint,
    next_request_id: AtomicI64,
}

impl RemoteCodexClient {
    pub async fn connect_unix(socket_path: AbsolutePathBuf) -> Result<Self, CodexError> {
        Self::connect(RemoteAppServerEndpoint::UnixSocket { socket_path }).await
    }

    pub async fn connect_websocket(websocket_url: String) -> Result<Self, CodexError> {
        Self::connect(RemoteAppServerEndpoint::WebSocket {
            websocket_url,
            auth_token: None,
        })
        .await
    }

    async fn connect(endpoint: RemoteAppServerEndpoint) -> Result<Self, CodexError> {
        let client = connect_client(endpoint.clone()).await?;
        Ok(Self {
            client: Some(client),
            endpoint,
            next_request_id: AtomicI64::new(1),
        })
    }

    pub fn disconnected_unix(socket_path: AbsolutePathBuf) -> Self {
        Self::disconnected(RemoteAppServerEndpoint::UnixSocket { socket_path })
    }

    pub fn disconnected_websocket(websocket_url: String) -> Self {
        Self::disconnected(RemoteAppServerEndpoint::WebSocket {
            websocket_url,
            auth_token: None,
        })
    }

    fn disconnected(endpoint: RemoteAppServerEndpoint) -> Self {
        Self {
            client: None,
            endpoint,
            next_request_id: AtomicI64::new(1),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    fn client(&self) -> Result<&RemoteAppServerClient, CodexError> {
        self.client.as_ref().ok_or(CodexError::Transport)
    }

    fn request_id(&self) -> codex_app_server_protocol::RequestId {
        codex_app_server_protocol::RequestId::Integer(
            self.next_request_id.fetch_add(1, Ordering::Relaxed),
        )
    }
}

async fn connect_client(
    endpoint: RemoteAppServerEndpoint,
) -> Result<RemoteAppServerClient, CodexError> {
    RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint,
        client_name: "codex-whatsapp-bridge".to_string(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        experimental_api: false,
        mcp_server_openai_form_elicitation: false,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: 128,
    })
    .await
    .map_err(|_| CodexError::Transport)
}

impl CodexClient for RemoteCodexClient {
    async fn start_thread(&self) -> Result<String, CodexError> {
        let response: ThreadStartResponse = self
            .client()?
            .request_typed(ClientRequest::ThreadStart {
                request_id: self.request_id(),
                params: ThreadStartParams {
                    approvals_reviewer: Some(ApprovalsReviewer::User),
                    ephemeral: Some(false),
                    ..Default::default()
                },
            })
            .await
            .map_err(|_| CodexError::Transport)?;
        Ok(response.thread.id)
    }

    async fn resume_thread(&self, thread_id: String) -> Result<ThreadResumeResponse, CodexError> {
        let response: ThreadResumeResponse = self
            .client()?
            .request_typed(ClientRequest::ThreadResume {
                request_id: self.request_id(),
                params: ThreadResumeParams {
                    thread_id,
                    ..Default::default()
                },
            })
            .await
            .map_err(|_| CodexError::Transport)?;
        Ok(response)
    }

    async fn start_turn(
        &self,
        thread_id: String,
        message_id: String,
        prompt: String,
    ) -> Result<String, CodexError> {
        let response: TurnStartResponse = self
            .client()?
            .request_typed(ClientRequest::TurnStart {
                request_id: self.request_id(),
                params: TurnStartParams {
                    thread_id,
                    client_user_message_id: Some(message_id),
                    input: vec![UserInput::Text {
                        text: prompt,
                        text_elements: Vec::new(),
                    }],
                    ..Default::default()
                },
            })
            .await
            .map_err(|_| CodexError::Transport)?;
        Ok(response.turn.id)
    }

    async fn interrupt_turn(&self, thread_id: String, turn_id: String) -> Result<(), CodexError> {
        let _: TurnInterruptResponse = self
            .client()?
            .request_typed(ClientRequest::TurnInterrupt {
                request_id: self.request_id(),
                params: TurnInterruptParams { thread_id, turn_id },
            })
            .await
            .map_err(|_| CodexError::Transport)?;
        Ok(())
    }

    async fn next_event(&mut self) -> Option<AppServerEvent> {
        match self.client.as_mut() {
            Some(client) => client.next_event().await,
            None => None,
        }
    }

    async fn reject_server_request(&self, request_id: RequestId) -> Result<(), CodexError> {
        self.client()?
            .reject_server_request(
                request_id,
                JSONRPCErrorError {
                    code: -32000,
                    message: "WhatsApp bridge does not support this request".to_string(),
                    data: None,
                },
            )
            .await
            .map_err(|_| CodexError::Transport)
    }

    async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: serde_json::Value,
    ) -> Result<(), CodexError> {
        self.client()?
            .resolve_server_request(request_id, result)
            .await
            .map_err(|_| CodexError::Transport)
    }

    async fn reconnect(&mut self) -> Result<(), CodexError> {
        self.client = Some(connect_client(self.endpoint.clone()).await?);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), CodexError> {
        let Some(client) = self.client.take() else {
            return Ok(());
        };
        client.shutdown().await.map_err(|_| CodexError::Transport)
    }
}

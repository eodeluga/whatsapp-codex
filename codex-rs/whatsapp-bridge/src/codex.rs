//! Narrow adapter over Codex's normal app-server client protocol.

use codex_app_server_client::AppServerEvent;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_client::RemoteAppServerEndpoint;
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
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CodexError {
    #[error("failed to communicate with Codex app-server")]
    Transport,
}

pub struct RemoteCodexClient {
    client: RemoteAppServerClient,
    next_request_id: AtomicI64,
}

impl RemoteCodexClient {
    pub async fn connect_unix(socket_path: AbsolutePathBuf) -> Result<Self, CodexError> {
        let client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
            endpoint: RemoteAppServerEndpoint::UnixSocket { socket_path },
            client_name: "codex-whatsapp-bridge".to_string(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            experimental_api: false,
            mcp_server_openai_form_elicitation: false,
            opt_out_notification_methods: Vec::new(),
            channel_capacity: 128,
        })
        .await
        .map_err(|_| CodexError::Transport)?;
        Ok(Self {
            client,
            next_request_id: AtomicI64::new(1),
        })
    }

    pub async fn start_thread(&self, workspace: &Path) -> Result<String, CodexError> {
        let response: ThreadStartResponse = self
            .client
            .request_typed(ClientRequest::ThreadStart {
                request_id: self.request_id(),
                params: ThreadStartParams {
                    cwd: Some(workspace.display().to_string()),
                    ephemeral: Some(false),
                    ..Default::default()
                },
            })
            .await
            .map_err(|_| CodexError::Transport)?;
        Ok(response.thread.id)
    }

    pub async fn resume_thread(&self, thread_id: String) -> Result<String, CodexError> {
        let response: ThreadResumeResponse = self
            .client
            .request_typed(ClientRequest::ThreadResume {
                request_id: self.request_id(),
                params: ThreadResumeParams {
                    thread_id,
                    ..Default::default()
                },
            })
            .await
            .map_err(|_| CodexError::Transport)?;
        Ok(response.thread.id)
    }

    pub async fn start_turn(
        &self,
        thread_id: String,
        message_id: String,
        prompt: String,
    ) -> Result<String, CodexError> {
        let response: TurnStartResponse = self
            .client
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

    pub async fn interrupt_turn(
        &self,
        thread_id: String,
        turn_id: String,
    ) -> Result<(), CodexError> {
        let _: TurnInterruptResponse = self
            .client
            .request_typed(ClientRequest::TurnInterrupt {
                request_id: self.request_id(),
                params: TurnInterruptParams { thread_id, turn_id },
            })
            .await
            .map_err(|_| CodexError::Transport)?;
        Ok(())
    }

    pub async fn next_event(&mut self) -> Option<AppServerEvent> {
        self.client.next_event().await
    }

    pub async fn reject_server_request(&self, request_id: RequestId) -> Result<(), CodexError> {
        self.client
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

    pub async fn resolve_server_request(
        &self,
        request_id: RequestId,
        result: serde_json::Value,
    ) -> Result<(), CodexError> {
        self.client
            .resolve_server_request(request_id, result)
            .await
            .map_err(|_| CodexError::Transport)
    }

    fn request_id(&self) -> codex_app_server_protocol::RequestId {
        codex_app_server_protocol::RequestId::Integer(
            self.next_request_id.fetch_add(1, Ordering::Relaxed),
        )
    }
}

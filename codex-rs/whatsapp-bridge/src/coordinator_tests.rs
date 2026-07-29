use super::*;
use crate::codex::CodexClient;
use crate::codex::CodexError;
use crate::openwa::OpenWaError;
use crate::openwa::OpenWaSession;
use codex_app_server_protocol::ThreadResumeResponse;
use std::path::Path;
use std::sync::Mutex;
use tempfile::tempdir;

#[derive(Clone, Default)]
struct FakeCodex {
    turns: Arc<Mutex<Vec<(String, String, String)>>>,
}

impl CodexClient for FakeCodex {
    async fn start_thread(&self, _workspace: &Path) -> Result<String, CodexError> {
        Ok("thread-1".to_string())
    }

    async fn resume_thread(&self, _thread_id: String) -> Result<ThreadResumeResponse, CodexError> {
        Err(CodexError::Transport)
    }

    async fn start_turn(
        &self,
        thread_id: String,
        message_id: String,
        prompt: String,
    ) -> Result<String, CodexError> {
        self.turns
            .lock()
            .unwrap()
            .push((thread_id, message_id, prompt));
        Ok("turn-1".to_string())
    }

    async fn interrupt_turn(&self, _thread_id: String, _turn_id: String) -> Result<(), CodexError> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<AppServerEvent> {
        std::future::pending().await
    }

    async fn reject_server_request(&self, _request_id: RequestId) -> Result<(), CodexError> {
        Ok(())
    }

    async fn resolve_server_request(
        &self,
        _request_id: RequestId,
        _result: serde_json::Value,
    ) -> Result<(), CodexError> {
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<(), CodexError> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), CodexError> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct FakeOpenWa {
    sent: Arc<Mutex<Vec<String>>>,
}

impl OpenWaClient for FakeOpenWa {
    async fn session_status(&self) -> Result<OpenWaSession, OpenWaError> {
        Ok(OpenWaSession {
            status: "ready".to_string(),
            phone: Some("447700900000".to_string()),
        })
    }

    async fn send_text(&self, _chat_id: String, text: String) -> Result<String, OpenWaError> {
        let mut sent = self.sent.lock().unwrap();
        sent.push(text);
        Ok(format!("wa-{}", sent.len()))
    }

    async fn edit_text(
        &self,
        _chat_id: String,
        _message_id: String,
        _text: String,
    ) -> Result<(), OpenWaError> {
        Ok(())
    }

    async fn register_webhook(&self, _url: String, _secret: String) -> Result<(), OpenWaError> {
        Ok(())
    }
}

#[tokio::test]
async fn durable_deduplication_starts_one_turn_for_a_replayed_webhook() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let codex = FakeCodex::default();
    let recorded_turns = Arc::clone(&codex.turns);
    let ready = Arc::new(AtomicBool::new(true));
    let (commands, command_rx) = mpsc::channel(8);
    let coordinator = Coordinator::new(
        codex,
        FakeOpenWa::default(),
        BridgeState::empty(),
        state_path.clone(),
        directory.path().to_path_buf(),
        "personal".to_string(),
        "447700900000".to_string(),
        "http://bridge/webhooks/openwa".to_string(),
        "secret".to_string(),
        "447700900000@c.us".to_string(),
        "!codex ".to_string(),
        3_500,
        1_500,
        20,
        100,
        24,
        ready,
        true,
        true,
    );
    let task = tokio::spawn(coordinator.run(command_rx));
    let message = InboundMessage {
        idempotency_key: "event-1".to_string(),
        message_id: "message-1".to_string(),
        chat_id: "447700900000@c.us".to_string(),
        body: "!codex inspect the workspace".to_string(),
    };

    for _ in 0..2 {
        let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
        commands
            .send(CoordinatorCommand::Inbound {
                message: message.clone(),
                accepted,
            })
            .await
            .unwrap();
        assert!(accepted_rx.await.unwrap());
        let (status, status_rx) = tokio::sync::oneshot::channel();
        commands
            .send(CoordinatorCommand::Status(status))
            .await
            .unwrap();
        let _ = status_rx.await.unwrap();
    }

    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    commands
        .send(CoordinatorCommand::Shutdown(shutdown))
        .await
        .unwrap();
    shutdown_rx.await.unwrap();
    task.await.unwrap();

    assert_eq!(recorded_turns.lock().unwrap().len(), 1);
    let persisted = BridgeState::load(&state_path).unwrap();
    assert!(persisted.was_processed("event-1"));
    assert_eq!(
        persisted
            .active_turn
            .as_ref()
            .map(|turn| turn.codex_turn_id.as_str()),
        Some("turn-1")
    );
}

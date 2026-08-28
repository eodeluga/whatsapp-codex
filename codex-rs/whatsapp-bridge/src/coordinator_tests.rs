use super::*;
use crate::codex::CodexClient;
use crate::codex::CodexError;
use crate::codex::ThreadSummary;
use crate::transport::TransportError;
use crate::transport::TransportStatus;
use codex_app_server_protocol::ThreadResumeResponse;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tempfile::tempdir;

#[derive(Clone, Default)]
struct FakeCodex {
    turns: Arc<Mutex<Vec<(String, String, String)>>>,
    steers: Arc<Mutex<Vec<(String, String, String, String)>>>,
    start_thread_fails: Arc<AtomicBool>,
}

impl CodexClient for FakeCodex {
    async fn start_thread(&self) -> Result<String, CodexError> {
        if self.start_thread_fails.load(Ordering::Acquire) {
            Err(CodexError::Transport("test failure".to_string()))
        } else {
            Ok("thread-1".to_string())
        }
    }

    async fn resume_thread(&self, _thread_id: String) -> Result<ThreadResumeResponse, CodexError> {
        Err(CodexError::Transport("test failure".to_string()))
    }

    async fn list_threads(&self) -> Result<Vec<ThreadSummary>, CodexError> {
        Ok(Vec::new())
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

    async fn steer_turn(
        &self,
        thread_id: String,
        turn_id: String,
        message_id: String,
        prompt: String,
    ) -> Result<(), CodexError> {
        self.steers
            .lock()
            .unwrap()
            .push((thread_id, turn_id, message_id, prompt));
        Ok(())
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
struct FakeTransport {
    sent: Arc<Mutex<Vec<String>>>,
}

impl TransportClient for FakeTransport {
    async fn status(&self) -> Result<TransportStatus, TransportError> {
        Ok(TransportStatus {
            status: "ready".to_string(),
            account: Some("447700900000".to_string()),
        })
    }

    async fn send_text(&self, _chat_id: String, text: String) -> Result<String, TransportError> {
        let mut sent = self.sent.lock().unwrap();
        sent.push(text);
        Ok(format!("wa-{}", sent.len()))
    }

    async fn edit_text(
        &self,
        _chat_id: String,
        _message_id: String,
        _text: String,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    async fn pairing_qr(&self) -> Result<Option<String>, TransportError> {
        Ok(None)
    }
}

#[tokio::test]
async fn durable_deduplication_starts_one_turn_for_a_replayed_webhook() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let codex = FakeCodex::default();
    let recorded_turns = Arc::clone(&codex.turns);
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: true,
        state_healthy: true,
        app_server_connected: true,
        transport_healthy: true,
    }));
    let (commands, command_rx) = mpsc::channel(8);
    let (command_catalog, command_catalog_path) =
        CommandCatalog::load_or_create(directory.path()).unwrap();
    let coordinator = Coordinator::new(
        codex,
        FakeTransport::default(),
        BridgeState::empty(),
        state_path.clone(),
        "447700900000".to_string(),
        "447700900000@c.us".to_string(),
        3_500,
        1_500,
        20,
        100,
        24,
        command_catalog,
        command_catalog_path,
        Arc::clone(&readiness),
        true,
        true,
    );
    let task = tokio::spawn(coordinator.run(command_rx));
    let message = InboundMessage {
        idempotency_key: "event-1".to_string(),
        message_id: "message-1".to_string(),
        chat_id: "447700900000@c.us".to_string(),
        body: "inspect the current project".to_string(),
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

    assert_eq!(
        readiness.snapshot(),
        BridgeReadinessSnapshot {
            ready: false,
            state_healthy: true,
            app_server_connected: false,
            transport_healthy: true,
        }
    );
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

#[tokio::test]
async fn message_during_active_turn_uses_steer_without_queueing_a_second_turn() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let codex = FakeCodex::default();
    let recorded_steers = Arc::clone(&codex.steers);
    let mut state = BridgeState::empty();
    state.binding = Some(crate::state::ThreadBinding {
        self_chat_id: "447700900000@c.us".to_string(),
        codex_thread_id: "thread-1".to_string(),
    });
    state.active_turn = Some(crate::state::ActiveTurn {
        inbound_message_id: "message-1".to_string(),
        thread_id: "thread-1".to_string(),
        codex_turn_id: "turn-1".to_string(),
        working_output_message_id: None,
    });
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: true,
        state_healthy: true,
        app_server_connected: true,
        transport_healthy: true,
    }));
    let (commands, command_rx) = mpsc::channel(8);
    let (command_catalog, command_catalog_path) =
        CommandCatalog::load_or_create(directory.path()).unwrap();
    let coordinator = Coordinator::new(
        codex,
        FakeTransport::default(),
        state,
        state_path.clone(),
        "447700900000".to_string(),
        "447700900000@c.us".to_string(),
        3_500,
        1_500,
        20,
        100,
        24,
        command_catalog,
        command_catalog_path,
        readiness,
        true,
        true,
    );
    let task = tokio::spawn(coordinator.run(command_rx));
    let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
    commands
        .send(CoordinatorCommand::Inbound {
            message: InboundMessage {
                idempotency_key: "event-steer".to_string(),
                message_id: "message-steer".to_string(),
                chat_id: "447700900000@c.us".to_string(),
                body: "keep checking".to_string(),
            },
            accepted,
        })
        .await
        .unwrap();
    assert!(accepted_rx.await.unwrap());
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    commands
        .send(CoordinatorCommand::Shutdown(shutdown))
        .await
        .unwrap();
    shutdown_rx.await.unwrap();
    task.await.unwrap();

    assert_eq!(
        *recorded_steers.lock().unwrap(),
        vec![(
            "thread-1".to_string(),
            "turn-1".to_string(),
            "message-steer".to_string(),
            "keep checking".to_string(),
        )]
    );
    let persisted = BridgeState::load(&state_path).unwrap();
    assert!(persisted.pending_steers.is_empty());
    assert!(persisted.queued_prompts.is_empty());
}

#[tokio::test]
async fn help_reloads_the_user_command_catalog() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let codex = FakeCodex::default();
    let transport = FakeTransport::default();
    let sent = Arc::clone(&transport.sent);
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: true,
        state_healthy: true,
        app_server_connected: true,
        transport_healthy: true,
    }));
    let (commands, command_rx) = mpsc::channel(8);
    let (command_catalog, command_catalog_path) =
        CommandCatalog::load_or_create(directory.path()).unwrap();
    std::fs::write(
        &command_catalog_path,
        r#"{
          "schemaVersion": 1,
          "responsePrefix": "[remote]",
          "helpHeading": "My commands",
          "helpFooter": "Configured on disk.",
          "groups": [{
            "heading": "Session",
            "commands": [{"usage": "/status", "description": "show status"}]
          }]
        }"#,
    )
    .unwrap();
    let coordinator = Coordinator::new(
        codex,
        transport,
        BridgeState::empty(),
        state_path,
        "447700900000".to_string(),
        "447700900000@c.us".to_string(),
        3_500,
        1_500,
        20,
        100,
        24,
        command_catalog,
        command_catalog_path,
        readiness,
        true,
        true,
    );
    let task = tokio::spawn(coordinator.run(command_rx));
    let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
    commands
        .send(CoordinatorCommand::Inbound {
            message: InboundMessage {
                idempotency_key: "event-help".to_string(),
                message_id: "message-help".to_string(),
                chat_id: "447700900000@c.us".to_string(),
                body: "/help".to_string(),
            },
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
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    commands
        .send(CoordinatorCommand::Shutdown(shutdown))
        .await
        .unwrap();
    shutdown_rx.await.unwrap();
    task.await.unwrap();

    assert_eq!(
        *sent.lock().unwrap(),
        vec![
            "[remote] My commands\n\nSession\n• `/status` — show status\n\nConfigured on disk."
                .to_string()
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn app_server_retries_notify_once_for_a_queued_prompt() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let codex = FakeCodex::default();
    codex.start_thread_fails.store(true, Ordering::Release);
    let transport = FakeTransport::default();
    let sent = Arc::clone(&transport.sent);
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: true,
        state_healthy: true,
        app_server_connected: true,
        transport_healthy: true,
    }));
    let (commands, command_rx) = mpsc::channel(8);
    let (command_catalog, command_catalog_path) =
        CommandCatalog::load_or_create(directory.path()).unwrap();
    let coordinator = Coordinator::new(
        codex,
        transport,
        BridgeState::empty(),
        state_path.clone(),
        "447700900000".to_string(),
        "447700900000@c.us".to_string(),
        3_500,
        1_500,
        20,
        100,
        24,
        command_catalog,
        command_catalog_path,
        readiness,
        true,
        true,
    );
    let task = tokio::spawn(coordinator.run(command_rx));
    let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
    commands
        .send(CoordinatorCommand::Inbound {
            message: InboundMessage {
                idempotency_key: "event-1".to_string(),
                message_id: "message-1".to_string(),
                chat_id: "447700900000@c.us".to_string(),
                body: "inspect the current project".to_string(),
            },
            accepted,
        })
        .await
        .unwrap();
    assert!(accepted_rx.await.unwrap());

    tokio::time::advance(std::time::Duration::from_secs(120)).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }

    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    commands
        .send(CoordinatorCommand::Shutdown(shutdown))
        .await
        .unwrap();
    shutdown_rx.await.unwrap();
    task.await.unwrap();

    assert_eq!(
        sent.lock()
            .unwrap()
            .iter()
            .filter(|message| message.contains("app-server is unavailable"))
            .count(),
        1
    );
    assert!(BridgeState::load(&state_path).unwrap().queued_prompts[0].failure_notified);
}

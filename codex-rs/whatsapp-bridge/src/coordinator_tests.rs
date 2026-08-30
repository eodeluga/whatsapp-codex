use super::*;
use crate::attachment::InboundAttachment;
use crate::codex::CodexClient;
use crate::codex::CodexError;
use crate::codex::ThreadSummary;
use crate::transport::TransportError;
use crate::transport::TransportStatus;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ToolRequestUserInputParams;
use codex_app_server_protocol::ToolRequestUserInputQuestion;
use codex_app_server_protocol::UserInput;
use codex_messaging::DeliveryWorker;
use codex_messaging::FileDeliveryStore;
use codex_messaging::ProviderAdapter;
use codex_messaging::ProviderCapabilities;
use codex_messaging::ProviderConversationId;
use codex_messaging::ProviderError;
use codex_messaging::ProviderMessageId;
use codex_messaging::ProviderStatus;
use codex_transcript::EntryOrigin;
use codex_transcript::ProjectionEvent;
use codex_transcript::TranscriptEntry;
use codex_transcript::TranscriptKey;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tempfile::tempdir;

#[test]
fn image_attachment_is_encoded_as_codex_image_input() {
    assert_eq!(
        user_inputs(
            "describe this".to_string(),
            Some(InboundAttachment::Image {
                mime_type: "image/png".to_string(),
                data_base64: "aW1hZ2U=".to_string(),
            }),
        ),
        vec![
            UserInput::Image {
                detail: None,
                url: "data:image/png;base64,aW1hZ2U=".to_string(),
            },
            UserInput::Text {
                text: "describe this".to_string(),
                text_elements: Vec::new(),
            },
        ]
    );
}

#[tokio::test]
async fn audio_attachment_is_rejected_with_session_message() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let codex = FakeCodex::default();
    let recorded_turns = Arc::clone(&codex.turns);
    let transport = FakeTransport::default();
    let sent = Arc::clone(&transport.sent);
    let delivery_transport = transport.clone();
    let (command_catalog, command_catalog_path) =
        CommandCatalog::load_or_create(directory.path()).unwrap();
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: true,
        state_healthy: true,
        app_server_connected: true,
        transport_healthy: true,
    }));
    let mut coordinator = Coordinator::new(
        codex,
        transport,
        BridgeState::empty(),
        state_path,
        directory.path().join("attachments"),
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
    let (delivery_events, _delivery_event_rx) = mpsc::channel(16);
    let delivery_worker = DeliveryWorker::new(
        delivery_transport,
        FileDeliveryStore::new(directory.path().join("delivery.json")),
        delivery_events,
        std::time::Duration::from_millis(1),
    )
    .await
    .unwrap();
    let (delivery, delivery_commands) =
        DeliveryWorker::<FakeTransport, FileDeliveryStore>::channel(8);
    coordinator.delivery = Some(delivery.clone());
    let delivery_task = tokio::spawn(delivery_worker.run(delivery_commands));
    let action = coordinator
        .accept_inbound(InboundMessage {
            idempotency_key: "event-audio".to_string(),
            message_id: "message-audio".to_string(),
            chat_id: "447700900000@c.us".to_string(),
            body: String::new(),
            attachment: Some(InboundAttachment::Unsupported {
                kind: "audio attachment".to_string(),
            }),
        })
        .unwrap()
        .unwrap();

    coordinator.handle_accepted_action(action).await;

    wait_for_sent(&sent, 1).await;
    delivery.shutdown().await;
    delivery_task.await.unwrap();

    assert!(recorded_turns.lock().unwrap().is_empty());
    assert_eq!(
        *sent.lock().unwrap(),
        vec!["[codex] Audio attachments are unsupported in this session".to_string()]
    );
}

#[tokio::test]
async fn lagged_app_server_events_do_not_emit_recovery_narration() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let transport = FakeTransport::default();
    let sent = Arc::clone(&transport.sent);
    let (command_catalog, command_catalog_path) =
        CommandCatalog::load_or_create(directory.path()).unwrap();
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: true,
        state_healthy: true,
        app_server_connected: true,
        transport_healthy: true,
    }));
    let mut coordinator = Coordinator::new(
        FakeCodex::default(),
        transport,
        BridgeState::empty(),
        state_path,
        directory.path().join("attachments"),
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
    coordinator
        .handle_event(AppServerEvent::Lagged { skipped: 4 })
        .await;

    assert!(sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn user_input_questions_are_collected_in_order() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let codex = FakeCodex::default();
    let resolves = Arc::clone(&codex.resolves);
    let transport = FakeTransport::default();
    let sent = Arc::clone(&transport.sent);
    let delivery_transport = transport.clone();
    let mut state = BridgeState::empty();
    state.binding = Some(crate::state::ThreadBinding {
        self_chat_id: "447700900000@c.us".to_string(),
        codex_thread_id: "thread-1".to_string(),
    });
    state.active_turn = Some(crate::state::ActiveTurn {
        inbound_message_id: "message-1".to_string(),
        thread_id: "thread-1".to_string(),
        codex_turn_id: "turn-1".to_string(),
        legacy_working_output_message_id: None,
        attachment_paths: Vec::new(),
    });
    let (command_catalog, command_catalog_path) =
        CommandCatalog::load_or_create(directory.path()).unwrap();
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: true,
        state_healthy: true,
        app_server_connected: true,
        transport_healthy: true,
    }));
    let mut coordinator = Coordinator::new(
        codex,
        transport,
        state,
        state_path,
        directory.path().join("attachments"),
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
    let (delivery_events, _delivery_event_rx) = mpsc::channel(16);
    let delivery_worker = DeliveryWorker::new(
        delivery_transport,
        FileDeliveryStore::new(directory.path().join("delivery.json")),
        delivery_events,
        std::time::Duration::from_millis(1),
    )
    .await
    .unwrap();
    let (delivery, delivery_commands) =
        DeliveryWorker::<FakeTransport, FileDeliveryStore>::channel(8);
    coordinator.delivery = Some(delivery.clone());
    let delivery_task = tokio::spawn(delivery_worker.run(delivery_commands));
    coordinator
        .handle_server_request(ServerRequest::ToolRequestUserInput {
            request_id: RequestId::String("request-1".to_string()),
            params: ToolRequestUserInputParams {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "item-1".to_string(),
                questions: vec![
                    ToolRequestUserInputQuestion {
                        id: "first".to_string(),
                        header: "First".to_string(),
                        question: "First answer?".to_string(),
                        is_other: false,
                        is_secret: false,
                        options: None,
                    },
                    ToolRequestUserInputQuestion {
                        id: "second".to_string(),
                        header: "Second".to_string(),
                        question: "Second answer?".to_string(),
                        is_other: false,
                        is_secret: false,
                        options: None,
                    },
                ],
                auto_resolution_ms: None,
            },
        })
        .await;
    wait_for_sent(&sent, 1).await;
    let first = sent.lock().unwrap()[0].clone();
    let token = first
        .split_once('(')
        .and_then(|(_, value)| value.split_once(')'))
        .map(|(token, _)| token)
        .unwrap()
        .to_string();
    coordinator.answer_request(&token, "one".to_string()).await;
    wait_for_sent(&sent, 2).await;
    assert!(sent.lock().unwrap()[1].contains("Question 2/2"));
    coordinator.answer_request(&token, "two".to_string()).await;
    wait_for_sent(&sent, 3).await;
    delivery.shutdown().await;
    delivery_task.await.unwrap();
    assert_eq!(resolves.lock().unwrap().len(), 1);
    let resolved = &resolves.lock().unwrap()[0];
    assert_eq!(resolved["answers"]["first"]["answers"][0], "one");
    assert_eq!(resolved["answers"]["second"]["answers"][0], "two");
}

#[tokio::test]
async fn projected_codex_text_is_delivered_without_bridge_prefix() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let transport = FakeTransport::default();
    let sent = Arc::clone(&transport.sent);
    let delivery_transport = transport.clone();
    let (command_catalog, command_catalog_path) =
        CommandCatalog::load_or_create(directory.path()).unwrap();
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: true,
        state_healthy: true,
        app_server_connected: true,
        transport_healthy: true,
    }));
    let mut coordinator = Coordinator::new(
        FakeCodex::default(),
        transport,
        BridgeState::empty(),
        state_path,
        directory.path().join("attachments"),
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
    let (delivery_events, _delivery_event_rx) = mpsc::channel(16);
    let delivery_worker = DeliveryWorker::new(
        delivery_transport,
        FileDeliveryStore::new(directory.path().join("delivery.json")),
        delivery_events,
        std::time::Duration::from_millis(1),
    )
    .await
    .unwrap();
    let (delivery, delivery_commands) =
        DeliveryWorker::<FakeTransport, FileDeliveryStore>::channel(8);
    coordinator.delivery = Some(delivery.clone());
    let delivery_task = tokio::spawn(delivery_worker.run(delivery_commands));

    coordinator
        .enqueue_projection(vec![ProjectionEvent::Entry(Box::new(TranscriptEntry {
            key: TranscriptKey::new("thread-1", "turn-1", "item-1"),
            item: ThreadItem::AgentMessage {
                id: "item-1".to_string(),
                text: "plain Codex output".to_string(),
                phase: None,
                memory_citation: None,
            },
            origin: EntryOrigin::CodexTranscript,
            revision: 1,
            committed: true,
        }))])
        .await;
    wait_for_sent(&sent, 1).await;

    assert_eq!(
        *sent.lock().unwrap(),
        vec!["plain Codex output".to_string()]
    );
    delivery.shutdown().await;
    delivery_task.await.unwrap();
}

async fn wait_for_sent(sent: &Arc<Mutex<Vec<String>>>, count: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if sent.lock().unwrap().len() >= count {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[derive(Clone, Default)]
struct FakeCodex {
    turns: Arc<Mutex<Vec<(String, String, String)>>>,
    steers: Arc<Mutex<Vec<(String, String, String, String)>>>,
    resolves: Arc<Mutex<Vec<serde_json::Value>>>,
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
        input: Vec<UserInput>,
    ) -> Result<String, CodexError> {
        let prompt = input
            .into_iter()
            .find_map(|input| match input {
                UserInput::Text { text, .. } => Some(text),
                _ => None,
            })
            .unwrap_or_default();
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
        input: Vec<UserInput>,
    ) -> Result<(), CodexError> {
        let prompt = input
            .into_iter()
            .find_map(|input| match input {
                UserInput::Text { text, .. } => Some(text),
                _ => None,
            })
            .unwrap_or_default();
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
        result: serde_json::Value,
    ) -> Result<(), CodexError> {
        self.resolves.lock().unwrap().push(result);
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

impl ProviderAdapter for FakeTransport {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            message_limit: 4_096,
            edit_support: true,
            attachment_support: true,
            supports_markdown: false,
            rich_interaction_support: false,
        }
    }

    async fn status(&self) -> Result<ProviderStatus, ProviderError> {
        TransportClient::status(self)
            .await
            .map(|status| ProviderStatus {
                ready: status.status == "ready",
                account: status.account,
            })
            .map_err(|_| ProviderError::Transport)
    }

    async fn send_text(
        &self,
        conversation_id: ProviderConversationId,
        text: String,
    ) -> Result<ProviderMessageId, ProviderError> {
        TransportClient::send_text(self, conversation_id.as_str().to_string(), text)
            .await
            .map(ProviderMessageId::new)
            .map_err(|_| ProviderError::Transport)
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
        .map_err(|_| ProviderError::Transport)
    }
}

#[tokio::test]
async fn durable_deduplication_starts_one_turn_for_a_replayed_webhook() {
    let directory = tempdir().unwrap();
    let state_path = directory.path().join("state.json");
    let attachment_dir = directory.path().join("attachments");
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
        attachment_dir,
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
        attachment: None,
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
    let attachment_dir = directory.path().join("attachments");
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
        legacy_working_output_message_id: None,
        attachment_paths: Vec::new(),
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
        attachment_dir,
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
                attachment: None,
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
    let attachment_dir = directory.path().join("attachments");
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
        attachment_dir,
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
                attachment: None,
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
    let attachment_dir = directory.path().join("attachments");
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
        attachment_dir,
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
                attachment: None,
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

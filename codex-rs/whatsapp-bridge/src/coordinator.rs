//! Single-owner conversation state machine for the WhatsApp bridge.

use crate::codex::CodexClient;
use crate::codex::CodexError;
use crate::commands::BridgeCommand;
use crate::commands::parse_command;
use crate::openwa::OpenWaClient;
use crate::output::OutputAggregator;
use crate::output::labelled_chunks;
use crate::state::BridgeState;
use crate::state::OutboundMessage;
use crate::state::QueuedPrompt;
use crate::state::unix_timestamp;
use crate::webhook::InboundMessage;
use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::CommandExecutionApprovalDecision;
use codex_app_server_protocol::CommandExecutionRequestApprovalResponse;
use codex_app_server_protocol::FileChangeApprovalDecision;
use codex_app_server_protocol::FileChangeRequestApprovalResponse;
use codex_app_server_protocol::GrantedPermissionProfile;
use codex_app_server_protocol::PermissionGrantScope;
use codex_app_server_protocol::PermissionsRequestApprovalResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::RequestPermissionProfile;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ToolRequestUserInputAnswer;
use codex_app_server_protocol::ToolRequestUserInputResponse;
use codex_app_server_protocol::TurnStatus;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use uuid::Uuid;

const MAX_PROMPT_BYTES: usize = 10_000;
const MAX_OUTBOX_MESSAGES: usize = 100;
const MAX_PENDING_REQUESTS: usize = 32;
const MAX_FILE_CHANGE_ITEMS: usize = 128;
const MAX_FILE_CHANGE_PATHS: usize = 128;
const MAX_FILE_CHANGE_PATH_CHARS: usize = 512;
const APPROVAL_TIMEOUT_SECONDS: u64 = 300;

pub enum CoordinatorCommand {
    Inbound {
        message: InboundMessage,
        accepted: tokio::sync::oneshot::Sender<bool>,
    },
    Status(tokio::sync::oneshot::Sender<String>),
    Shutdown(tokio::sync::oneshot::Sender<()>),
}

enum AcceptedAction {
    Command(BridgeCommand),
    PromptTooLong,
    QueueFull,
    StartQueuedPrompt,
}

#[derive(Clone)]
enum PendingRequest {
    Command {
        request_id: RequestId,
        available_decisions: Option<Vec<CommandExecutionApprovalDecision>>,
        expires_at: u64,
    },
    FileChange {
        request_id: RequestId,
        expires_at: u64,
    },
    UserInput {
        request_id: RequestId,
        question_id: String,
        expires_at: u64,
    },
    Permissions {
        request_id: RequestId,
        permissions: RequestPermissionProfile,
        expires_at: u64,
    },
}

impl PendingRequest {
    fn request_id(&self) -> &RequestId {
        match self {
            Self::Command { request_id, .. }
            | Self::FileChange { request_id, .. }
            | Self::UserInput { request_id, .. }
            | Self::Permissions { request_id, .. } => request_id,
        }
    }

    fn expires_at(&self) -> u64 {
        match self {
            Self::Command { expires_at, .. }
            | Self::FileChange { expires_at, .. }
            | Self::UserInput { expires_at, .. }
            | Self::Permissions { expires_at, .. } => *expires_at,
        }
    }
}

pub struct Coordinator<C, O> {
    codex: C,
    openwa: O,
    state: BridgeState,
    state_path: PathBuf,
    openwa_session_id: String,
    configured_phone: String,
    webhook_url: String,
    webhook_secret: String,
    self_chat_id: String,
    output_chunk_chars: usize,
    edit_interval: std::time::Duration,
    max_queued_prompts: usize,
    dedupe_capacity: usize,
    dedupe_ttl_hours: u64,
    output: OutputAggregator,
    file_change_paths: HashMap<(String, String, String), Vec<String>>,
    pending_requests: HashMap<String, PendingRequest>,
    state_healthy: bool,
    app_server_connected: bool,
    openwa_healthy: bool,
    webhook_registered: bool,
    ready: Arc<AtomicBool>,
    stream_degraded: bool,
    last_edit_at: Option<tokio::time::Instant>,
    live_edit_disabled: bool,
    resume_failures: u8,
}

impl<C: CodexClient, O: OpenWaClient> Coordinator<C, O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        codex: C,
        openwa: O,
        state: BridgeState,
        state_path: PathBuf,
        openwa_session_id: String,
        configured_phone: String,
        webhook_url: String,
        webhook_secret: String,
        self_chat_id: String,
        output_chunk_chars: usize,
        edit_interval_ms: u64,
        max_queued_prompts: usize,
        dedupe_capacity: usize,
        dedupe_ttl_hours: u64,
        ready: Arc<AtomicBool>,
        app_server_connected: bool,
        openwa_healthy: bool,
    ) -> Self {
        Self {
            codex,
            openwa,
            state,
            state_path,
            openwa_session_id,
            configured_phone,
            webhook_url,
            webhook_secret,
            self_chat_id,
            output_chunk_chars,
            edit_interval: std::time::Duration::from_millis(edit_interval_ms),
            max_queued_prompts,
            dedupe_capacity,
            dedupe_ttl_hours,
            output: OutputAggregator::default(),
            file_change_paths: HashMap::new(),
            pending_requests: HashMap::new(),
            state_healthy: true,
            app_server_connected,
            openwa_healthy,
            webhook_registered: openwa_healthy,
            ready,
            stream_degraded: false,
            last_edit_at: None,
            live_edit_disabled: false,
            resume_failures: 0,
        }
    }

    pub async fn run(mut self, mut commands: mpsc::Receiver<CoordinatorCommand>) {
        let _ = self.flush_outbox().await;
        if self.app_server_connected && self.state.binding.is_some() {
            self.app_server_connected = self.resume_after_reconnect().await;
            self.refresh_readiness();
        }
        if self.app_server_connected
            && self.state.active_turn.is_none()
            && !self.state.queued_prompts.is_empty()
        {
            self.advance_queue().await;
        }
        let mut retry_outbox = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut recover_state = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut expire_approvals = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut reconcile_turn_start = tokio::time::interval(std::time::Duration::from_secs(5));
        let openwa_check_interval = std::time::Duration::from_secs(30);
        let mut check_openwa = tokio::time::interval_at(
            tokio::time::Instant::now() + openwa_check_interval,
            openwa_check_interval,
        );
        let mut connected = self.app_server_connected;
        let mut reconnect_delay = std::time::Duration::from_secs(1);
        loop {
            if connected {
                tokio::select! {
                    Some(command) = commands.recv() => {
                        if self.handle_command(command).await {
                            break;
                        }
                        if !self.app_server_connected {
                            connected = false;
                        }
                    },
                    event = self.codex.next_event() => match event {
                        Some(AppServerEvent::Disconnected { .. }) | None => {
                            self.mark_disconnected().await;
                            connected = false;
                        }
                        Some(event) => {
                            self.handle_event(event).await;
                            if !self.app_server_connected {
                                connected = false;
                            } else if self.state.active_turn.is_none()
                                && !self.state.queued_prompts.is_empty()
                            {
                                self.advance_queue().await;
                            }
                        },
                    },
                    _ = retry_outbox.tick(), if !self.state.outbox.is_empty() => {
                        let _ = self.flush_outbox().await;
                    },
                    _ = recover_state.tick(), if !self.state_healthy => {
                        self.recover_state_storage();
                    },
                    _ = expire_approvals.tick(), if !self.pending_requests.is_empty() => {
                        self.expire_pending_requests().await;
                    },
                    _ = reconcile_turn_start.tick(), if self.has_uncertain_turn_start() => {
                        self.reconcile_uncertain_turn_start().await;
                    },
                    _ = check_openwa.tick() => self.check_openwa().await,
                    else => break,
                }
            } else {
                tokio::select! {
                    Some(command) = commands.recv() => {
                        if self.handle_command(command).await {
                            break;
                        }
                    },
                    _ = tokio::time::sleep(reconnect_delay_with_jitter(reconnect_delay)) => {
                        if self.codex.reconnect().await.is_ok() && self.resume_after_reconnect().await {
                            connected = true;
                            self.app_server_connected = true;
                            reconnect_delay = std::time::Duration::from_secs(1);
                            self.refresh_readiness();
                            if self.state.active_turn.is_none()
                                && !self.state.queued_prompts.is_empty()
                            {
                                self.advance_queue().await;
                            }
                        } else {
                            reconnect_delay =
                                (reconnect_delay * 2).min(std::time::Duration::from_secs(30));
                        }
                    }
                    _ = retry_outbox.tick(), if !self.state.outbox.is_empty() => {
                        let _ = self.flush_outbox().await;
                    },
                    _ = recover_state.tick(), if !self.state_healthy => {
                        self.recover_state_storage();
                    },
                    _ = check_openwa.tick() => self.check_openwa().await,
                    else => break,
                }
            }
        }
        self.ready.store(false, Ordering::Release);
    }

    async fn check_openwa(&mut self) {
        let should_register_webhook = !self.webhook_registered;
        let healthy = match self.openwa.session_status().await {
            Ok(session) => {
                let session_ready = session.status.eq_ignore_ascii_case("ready")
                    && session
                        .phone
                        .as_deref()
                        .is_none_or(|phone| normalize_phone(phone) == self.configured_phone);
                if session_ready
                    && should_register_webhook
                    && self
                        .openwa
                        .register_webhook(self.webhook_url.clone(), self.webhook_secret.clone())
                        .await
                        .is_ok()
                {
                    self.webhook_registered = true;
                }
                session_ready && self.webhook_registered
            }
            Err(_) => false,
        };
        self.openwa_healthy = healthy;
        self.refresh_readiness();
        if healthy && !self.state.outbox.is_empty() {
            let _ = self.flush_outbox().await;
        }
    }

    async fn mark_disconnected(&mut self) {
        self.app_server_connected = false;
        self.refresh_readiness();
        self.pending_requests.clear();
        self.stream_degraded = true;
        self.send(
            "[codex] Codex app-server disconnected; queued prompts will resume after reconnection.",
        )
        .await;
    }

    async fn resume_after_reconnect(&mut self) -> bool {
        let Some(binding) = self.state.binding.clone() else {
            return true;
        };
        match self.codex.resume_thread(binding.codex_thread_id).await {
            Ok(response) => {
                self.resume_failures = 0;
                if let Some(active) = self.state.active_turn.clone() {
                    let matching_turn = response
                        .thread
                        .turns
                        .iter()
                        .find(|turn| turn.id == active.codex_turn_id);
                    if let Some(turn) =
                        matching_turn.filter(|turn| turn.status == TurnStatus::InProgress)
                    {
                        for item in &turn.items {
                            if let ThreadItem::AgentMessage { id, text, .. } = item {
                                self.output.complete_item(
                                    active.thread_id.clone(),
                                    active.codex_turn_id.clone(),
                                    id.clone(),
                                    text.clone(),
                                );
                            }
                        }
                        return true;
                    }
                    let recovered = matching_turn.map(|turn| {
                        let output = turn
                            .items
                            .iter()
                            .filter_map(|item| match item {
                                ThreadItem::AgentMessage { text, .. } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<String>();
                        (
                            turn.status.clone(),
                            turn.error.as_ref().map(|error| error.message.clone()),
                            output,
                        )
                    });
                    self.state.active_turn = None;
                    self.pending_requests.clear();
                    if self.state.save(&self.state_path).is_err() {
                        self.state_healthy = false;
                        self.refresh_readiness();
                        return false;
                    }
                    if let Some((status, error, output)) = recovered {
                        self.deliver_turn_output(
                            status,
                            error,
                            output,
                            active.working_output_message_id,
                        )
                        .await;
                    } else {
                        self.send("[codex] The previous turn ended while the bridge was disconnected; its final response could not be reconstructed.")
                            .await;
                    }
                }
                true
            }
            Err(_) => {
                self.resume_failures = self.resume_failures.saturating_add(1);
                if self.resume_failures < 3 {
                    return false;
                }
                self.state.orphaned_thread_id = self
                    .state
                    .binding
                    .take()
                    .map(|binding| binding.codex_thread_id);
                self.state.active_turn = None;
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                    return false;
                }
                self.send("[codex] The previous Codex thread could not be resumed; the next prompt will create a new context.")
                    .await;
                self.resume_failures = 0;
                true
            }
        }
    }

    async fn handle_command(&mut self, command: CoordinatorCommand) -> bool {
        match command {
            CoordinatorCommand::Inbound { message, accepted } => {
                let action = self.accept_inbound(message);
                let _ = accepted.send(action.is_ok());
                if let Ok(Some(action)) = action {
                    self.handle_accepted_action(action).await;
                }
            }
            CoordinatorCommand::Status(reply) => {
                let _ = reply.send(self.status_message());
            }
            CoordinatorCommand::Shutdown(done) => {
                let requests = self
                    .pending_requests
                    .drain()
                    .map(|(_, request)| request.request_id().clone())
                    .collect::<Vec<_>>();
                for request_id in requests {
                    let _ = self.codex.reject_server_request(request_id).await;
                }
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(10), self.flush_outbox())
                        .await;
                let _ = self.state.save(&self.state_path);
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(5), self.codex.shutdown())
                        .await;
                self.ready.store(false, Ordering::Release);
                let _ = done.send(());
                return true;
            }
        }
        false
    }

    fn accept_inbound(
        &mut self,
        message: InboundMessage,
    ) -> Result<Option<AcceptedAction>, crate::state::StateError> {
        if !self.state_healthy {
            return Err(crate::state::StateError::Write);
        }
        if self.state.was_processed(&message.idempotency_key) {
            return Ok(None);
        }
        if self.state.was_sent_by_bridge(&message.message_id) {
            return Ok(None);
        }
        let command = parse_command(&message.body);
        let previous_state = self.state.clone();
        let now = unix_timestamp();
        self.state
            .mark_processed(message.idempotency_key.clone(), now);
        self.state
            .prune(now, self.dedupe_ttl_hours, self.dedupe_capacity);
        let action = match command {
            BridgeCommand::Prompt(body) => {
                if body.len() > MAX_PROMPT_BYTES {
                    AcceptedAction::PromptTooLong
                } else if self.state.queued_prompts.len() >= self.max_queued_prompts {
                    AcceptedAction::QueueFull
                } else {
                    let prompt = QueuedPrompt {
                        idempotency_key: message.idempotency_key,
                        message_id: message.message_id,
                        body,
                        accepted_at: now,
                        submission_uncertain: false,
                    };
                    self.state.queued_prompts.push(prompt);
                    if self.state.active_turn.is_some() {
                        return self.persist_accepted(previous_state, None);
                    }
                    AcceptedAction::StartQueuedPrompt
                }
            }
            command => AcceptedAction::Command(command),
        };
        self.persist_accepted(previous_state, Some(action))
    }

    fn persist_accepted(
        &mut self,
        previous_state: BridgeState,
        action: Option<AcceptedAction>,
    ) -> Result<Option<AcceptedAction>, crate::state::StateError> {
        if let Err(error) = self.state.save(&self.state_path) {
            self.state = previous_state;
            self.state_healthy = false;
            self.refresh_readiness();
            return Err(error);
        }
        Ok(action)
    }

    async fn handle_accepted_action(&mut self, action: AcceptedAction) {
        match action {
            AcceptedAction::StartQueuedPrompt => self.start_next_prompt().await,
            AcceptedAction::QueueFull => {
                self.send("[codex] Queue is full; try again after the current turn.")
                    .await;
            }
            AcceptedAction::PromptTooLong => {
                self.send("[codex] That prompt is too long; keep it under 10,000 UTF-8 bytes.")
                    .await;
            }
            AcceptedAction::Command(command) => self.handle_bridge_command(command).await,
        }
    }

    async fn handle_bridge_command(&mut self, command: BridgeCommand) {
        match command {
            BridgeCommand::Prompt(_) => {}
            BridgeCommand::New if self.state.active_turn.is_some() => {
                self.send("[codex] Stop or wait for the active turn before starting a new thread.")
                    .await;
            }
            BridgeCommand::New => match self.codex.start_thread().await {
                Ok(thread_id) => {
                    if self.bind_thread(thread_id) {
                        self.send("[codex] Started a new thread.").await;
                        if !self.state.queued_prompts.is_empty() {
                            self.advance_queue().await;
                        }
                    } else {
                        self.send("[codex] The new thread could not be persisted.")
                            .await;
                    }
                }
                Err(_) => {
                    self.app_server_connected = false;
                    self.refresh_readiness();
                    self.send("[codex] Could not start a new thread.").await;
                }
            },
            BridgeCommand::Status => self.send(&self.status_message()).await,
            BridgeCommand::Stop => self.stop_active_turn().await,
            BridgeCommand::WhatsAppListThreads => self.list_threads().await,
            BridgeCommand::WhatsAppAttach(token) => self.attach_thread(token).await,
            BridgeCommand::Help => self.send_help().await,
            BridgeCommand::Approve { token, session } => {
                self.approve_request(&token, session).await
            }
            BridgeCommand::Deny { token } => self.deny_request(&token).await,
            BridgeCommand::Answer { token: _, answer } if answer.len() > MAX_PROMPT_BYTES => {
                self.send("[codex] That answer is too long; keep it under 10,000 UTF-8 bytes.")
                    .await;
            }
            BridgeCommand::Answer { token, answer } => self.answer_request(&token, answer).await,
        }
    }

    async fn list_threads(&mut self) {
        match self.codex.list_threads().await {
            Ok(threads) if threads.is_empty() => {
                self.send("[codex] No resumable Codex threads are available.")
                    .await;
            }
            Ok(threads) => {
                let rendered = threads
                    .into_iter()
                    .map(|thread| format!("• `{}` — {}", thread.id, thread.preview))
                    .collect::<Vec<_>>()
                    .join("\n");
                self.send(&format!(
                    "[codex] Recent threads:\n{rendered}\nAttach one with `/whatsapp attach <thread-id>`.",
                ))
                .await;
            }
            Err(_) => self.send("[codex] Could not list Codex threads.").await,
        }
    }

    async fn attach_thread(&mut self, thread_id: String) {
        if self.state.active_turn.is_some() {
            self.send("[codex] Stop or wait for the active turn before attaching another thread.")
                .await;
            return;
        }
        match self.codex.resume_thread(thread_id.clone()).await {
            Ok(_) if self.bind_thread(thread_id) => {
                self.send("[codex] Attached the selected Codex thread.")
                    .await;
            }
            Ok(_) => {
                self.send("[codex] The selected thread could not be persisted.")
                    .await
            }
            Err(_) => {
                self.send("[codex] Could not resume that Codex thread.")
                    .await
            }
        }
    }

    async fn start_next_prompt(&mut self) {
        let Some(prompt) = self.state.queued_prompts.first().cloned() else {
            return;
        };
        let thread_id = match self.state.binding.as_ref() {
            Some(binding) => binding.codex_thread_id.clone(),
            None => match self.codex.start_thread().await {
                Ok(thread_id) => {
                    if self.bind_thread(thread_id.clone()) {
                        thread_id
                    } else {
                        return;
                    }
                }
                Err(_) => {
                    self.app_server_connected = false;
                    self.refresh_readiness();
                    self.send("[codex] Codex app-server is unavailable.").await;
                    return;
                }
            },
        };
        if let Some(queued) = self.state.queued_prompts.first_mut()
            && queued.message_id == prompt.message_id
        {
            queued.submission_uncertain = true;
        }
        if self.state.save(&self.state_path).is_err() {
            self.state_healthy = false;
            self.refresh_readiness();
            return;
        }
        match self
            .codex
            .start_turn(thread_id.clone(), prompt.message_id.clone(), prompt.body)
            .await
        {
            Ok(turn_id) => {
                self.state.queued_prompts.remove(0);
                self.state.active_turn = Some(crate::state::ActiveTurn {
                    inbound_message_id: prompt.message_id,
                    thread_id,
                    codex_turn_id: turn_id,
                    working_output_message_id: None,
                });
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                    return;
                }
                if let Some(message_id) = self.send_tracked("[codex] Working…").await {
                    if let Some(active_turn) = self.state.active_turn.as_mut() {
                        active_turn.working_output_message_id = Some(message_id);
                    }
                    if self.state.save(&self.state_path).is_err() {
                        self.state_healthy = false;
                        self.refresh_readiness();
                    }
                }
            }
            Err(_) => {
                self.app_server_connected = false;
                self.refresh_readiness();
                self.send("[codex] Turn submission was interrupted; Codex will reconcile it before retrying.")
                    .await;
            }
        }
    }

    async fn advance_queue(&mut self) {
        if self.has_uncertain_turn_start() {
            self.reconcile_uncertain_turn_start().await;
        } else {
            self.start_next_prompt().await;
        }
    }

    fn has_uncertain_turn_start(&self) -> bool {
        self.state.active_turn.is_none()
            && self
                .state
                .queued_prompts
                .first()
                .is_some_and(|prompt| prompt.submission_uncertain)
    }

    async fn reconcile_uncertain_turn_start(&mut self) {
        let (Some(binding), Some(prompt)) = (
            self.state.binding.clone(),
            self.state.queued_prompts.first().cloned(),
        ) else {
            return;
        };
        let Ok(response) = self
            .codex
            .resume_thread(binding.codex_thread_id.clone())
            .await
        else {
            return;
        };
        let matching_turn = response.thread.turns.iter().find(|turn| {
            turn.items.iter().any(|item| {
                matches!(
                    item,
                    ThreadItem::UserMessage {
                        client_id: Some(client_id),
                        ..
                    } if client_id == &prompt.message_id
                )
            })
        });
        if let Some(turn) = matching_turn {
            self.state.queued_prompts.remove(0);
            if turn.status == TurnStatus::InProgress {
                self.state.active_turn = Some(crate::state::ActiveTurn {
                    inbound_message_id: prompt.message_id,
                    thread_id: binding.codex_thread_id,
                    codex_turn_id: turn.id.clone(),
                    working_output_message_id: None,
                });
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                    return;
                }
                if let Some(message_id) = self.send_tracked("[codex] Working…").await {
                    if let Some(active) = self.state.active_turn.as_mut() {
                        active.working_output_message_id = Some(message_id);
                    }
                    let _ = self.state.save(&self.state_path);
                }
            } else {
                let output = turn
                    .items
                    .iter()
                    .filter_map(|item| match item {
                        ThreadItem::AgentMessage { text, .. } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<String>();
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                    return;
                }
                self.deliver_turn_output(
                    turn.status.clone(),
                    turn.error.as_ref().map(|error| error.message.clone()),
                    output,
                    None,
                )
                .await;
                if !self.state.queued_prompts.is_empty() {
                    self.start_next_prompt().await;
                }
            }
            return;
        }
        if let Some(queued) = self.state.queued_prompts.first_mut()
            && queued.message_id == prompt.message_id
        {
            queued.submission_uncertain = false;
        }
        if self.state.save(&self.state_path).is_err() {
            self.state_healthy = false;
            self.refresh_readiness();
            return;
        }
        self.app_server_connected = true;
        self.refresh_readiness();
        self.start_next_prompt().await;
    }

    fn bind_thread(&mut self, thread_id: String) -> bool {
        let previous_binding = self.state.binding.clone();
        self.state.binding = Some(crate::state::ThreadBinding {
            openwa_session_id: self.openwa_session_id.clone(),
            self_chat_id: self.self_chat_id.clone(),
            codex_thread_id: thread_id,
        });
        if self.state.save(&self.state_path).is_err() {
            self.state.binding = previous_binding;
            self.state_healthy = false;
            self.refresh_readiness();
            false
        } else {
            true
        }
    }

    async fn stop_active_turn(&mut self) {
        let Some(active_turn) = &self.state.active_turn else {
            self.send("[codex] No turn is running.").await;
            return;
        };
        match self
            .codex
            .interrupt_turn(
                active_turn.thread_id.clone(),
                active_turn.codex_turn_id.clone(),
            )
            .await
        {
            Ok(()) => self.send("[codex] Interrupt requested.").await,
            Err(CodexError::Transport) => {
                self.app_server_connected = false;
                self.refresh_readiness();
                self.send("[codex] Could not interrupt the turn.").await;
            }
        }
    }

    async fn handle_event(&mut self, event: AppServerEvent) {
        let notification = match event {
            AppServerEvent::ServerNotification(notification) => notification,
            AppServerEvent::ServerRequest(request) => {
                self.handle_server_request(request).await;
                return;
            }
            AppServerEvent::Lagged { .. } => {
                self.stream_degraded = true;
                if !self.resume_after_reconnect().await {
                    self.app_server_connected = false;
                    self.refresh_readiness();
                }
                return;
            }
            AppServerEvent::Disconnected { .. } => return,
        };
        match notification {
            ServerNotification::AgentMessageDelta(delta) => {
                if !self.is_active_request(&delta.thread_id, &delta.turn_id) {
                    return;
                }
                let thread_id = delta.thread_id;
                let turn_id = delta.turn_id;
                self.output.push_delta(
                    thread_id.clone(),
                    turn_id.clone(),
                    delta.item_id,
                    &delta.delta,
                );
                self.update_working_message(&thread_id, &turn_id).await;
            }
            ServerNotification::ItemStarted(started) => {
                if self.is_active_request(&started.thread_id, &started.turn_id)
                    && self.file_change_paths.len() < MAX_FILE_CHANGE_ITEMS
                    && let ThreadItem::FileChange { id, changes, .. } = started.item
                {
                    self.file_change_paths.insert(
                        (started.thread_id, started.turn_id, id),
                        changes
                            .into_iter()
                            .take(MAX_FILE_CHANGE_PATHS)
                            .map(|change| {
                                change
                                    .path
                                    .chars()
                                    .take(MAX_FILE_CHANGE_PATH_CHARS)
                                    .collect()
                            })
                            .collect(),
                    );
                }
            }
            ServerNotification::ItemCompleted(completed) => {
                if self.is_active_request(&completed.thread_id, &completed.turn_id)
                    && let ThreadItem::AgentMessage { id, text, .. } = completed.item
                {
                    self.output
                        .complete_item(completed.thread_id, completed.turn_id, id, text);
                }
            }
            ServerNotification::TurnCompleted(completed) => {
                let is_active = self.state.active_turn.as_ref().is_some_and(|active| {
                    active.thread_id == completed.thread_id
                        && active.codex_turn_id == completed.turn.id
                });
                if !is_active {
                    return;
                }
                let output = self
                    .output
                    .finish_turn(&completed.thread_id, &completed.turn.id);
                let working_message_id = self
                    .state
                    .active_turn
                    .as_ref()
                    .and_then(|active| active.working_output_message_id.clone());
                self.deliver_turn_output(
                    completed.turn.status,
                    completed.turn.error.map(|error| error.message),
                    output,
                    working_message_id,
                )
                .await;
                self.state.active_turn = None;
                self.pending_requests.clear();
                self.file_change_paths.retain(|(thread_id, turn_id, _), _| {
                    thread_id != &completed.thread_id || turn_id != &completed.turn.id
                });
                self.stream_degraded = false;
                self.last_edit_at = None;
                self.live_edit_disabled = false;
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                    return;
                }
                if !self.state.queued_prompts.is_empty() {
                    self.start_next_prompt().await;
                }
            }
            ServerNotification::Error(error)
                if !error.will_retry
                    && self.state.active_turn.as_ref().is_some_and(|active| {
                        active.thread_id == error.thread_id && active.codex_turn_id == error.turn_id
                    }) =>
            {
                self.send(&format!("[codex] Codex error: {}", error.error.message))
                    .await;
            }
            ServerNotification::ServerRequestResolved(resolved) => {
                self.pending_requests
                    .retain(|_, request| request.request_id() != &resolved.request_id);
            }
            _ => {}
        }
    }

    async fn update_working_message(&mut self, thread_id: &str, turn_id: &str) {
        if self.live_edit_disabled
            || self
                .last_edit_at
                .is_some_and(|last| last.elapsed() < self.edit_interval)
        {
            return;
        }
        let Some(active) = self
            .state
            .active_turn
            .as_ref()
            .filter(|active| active.thread_id == thread_id && active.codex_turn_id == turn_id)
        else {
            return;
        };
        let Some(message_id) = active.working_output_message_id.clone() else {
            return;
        };
        let text = self.output.turn_text(thread_id, turn_id);
        let Some(chunk) = labelled_chunks(&text, self.output_chunk_chars)
            .into_iter()
            .next()
            .filter(|_| text.chars().count() <= self.output_chunk_chars)
        else {
            self.live_edit_disabled = true;
            return;
        };
        self.last_edit_at = Some(tokio::time::Instant::now());
        if self
            .openwa
            .edit_text(self.self_chat_id.clone(), message_id, chunk)
            .await
            .is_err()
        {
            self.live_edit_disabled = true;
        }
    }

    async fn deliver_turn_output(
        &mut self,
        status: TurnStatus,
        error: Option<String>,
        output: String,
        working_message_id: Option<String>,
    ) {
        let final_text = match status {
            TurnStatus::Completed if output.is_empty() => "[codex] Turn completed.".to_string(),
            TurnStatus::Completed => output,
            TurnStatus::Interrupted => "[codex] Turn interrupted.".to_string(),
            TurnStatus::Failed => format!(
                "[codex] Turn failed: {}",
                error.unwrap_or_else(|| "unknown error".to_string())
            ),
            TurnStatus::InProgress => "[codex] Turn ended with an invalid status.".to_string(),
        };
        let mut chunks = if final_text.starts_with("[codex] ") {
            vec![final_text]
        } else {
            labelled_chunks(&final_text, self.output_chunk_chars)
        };
        if self.stream_degraded {
            chunks.push(
                "[codex] Some streaming events were missed; completed output was reconstructed from authoritative events."
                    .to_string(),
            );
        }
        let mut chunks = chunks.into_iter();
        let Some(first) = chunks.next() else {
            return;
        };
        let edited = if let Some(message_id) = working_message_id {
            self.openwa
                .edit_text(self.self_chat_id.clone(), message_id, first.clone())
                .await
                .is_ok()
        } else {
            false
        };
        if !edited {
            self.send(&first).await;
        }
        for chunk in chunks {
            self.send(&chunk).await;
        }
    }

    async fn handle_server_request(&mut self, request: ServerRequest) {
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            let _ = self.codex.reject_server_request(request.id().clone()).await;
            self.send(
                "[codex] Too many approval requests are pending; the newest request was rejected.",
            )
            .await;
            return;
        }
        let token = Uuid::new_v4().simple().to_string()[..8].to_string();
        let expires_at = unix_timestamp().saturating_add(APPROVAL_TIMEOUT_SECONDS);
        match request {
            ServerRequest::CommandExecutionRequestApproval { request_id, params } => {
                if !self.is_active_request(&params.thread_id, &params.turn_id) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                let command = params
                    .command
                    .unwrap_or_else(|| "an unknown command".to_string());
                let reason = params.reason.unwrap_or_default();
                self.pending_requests.insert(
                    token.clone(),
                    PendingRequest::Command {
                        request_id,
                        available_decisions: params.available_decisions,
                        expires_at,
                    },
                );
                self.send(&format!(
                    "[codex] Approval {token}: run `{command}`{}{}\nReply `/approve {token}` or `/deny {token}`.",
                    if reason.is_empty() { String::new() } else { format!(" ({reason})") },
                    params.cwd.map_or_else(String::new, |cwd| format!(" in `{cwd}`")),
                )).await;
            }
            ServerRequest::FileChangeRequestApproval { request_id, params } => {
                if !self.is_active_request(&params.thread_id, &params.turn_id) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                let reason = params.reason.unwrap_or_else(|| "file changes".to_string());
                let paths = self
                    .file_change_paths
                    .get(&(
                        params.thread_id.clone(),
                        params.turn_id.clone(),
                        params.item_id.clone(),
                    ))
                    .filter(|paths| !paths.is_empty())
                    .map_or_else(String::new, |paths| format!(" to {}", paths.join(", ")));
                self.pending_requests.insert(
                    token.clone(),
                    PendingRequest::FileChange {
                        request_id,
                        expires_at,
                    },
                );
                self.send(&format!(
                    "[codex] Approval {token}: allow {reason}{paths}{}.\nReply `/approve {token}` or `/deny {token}`.",
                    params.grant_root.map_or_else(String::new, |root| format!(" under `{}`", root.display())),
                )).await;
            }
            ServerRequest::ToolRequestUserInput { request_id, params } => {
                if !self.is_active_request(&params.thread_id, &params.turn_id) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                let [question] = params.questions.as_slice() else {
                    let _ = self.codex.reject_server_request(request_id).await;
                    self.send("[codex] This tool asked multiple questions, which WhatsApp v1 cannot render.").await;
                    return;
                };
                if question.is_secret {
                    let _ = self.codex.reject_server_request(request_id).await;
                    self.send(
                        "[codex] A secret answer was requested and cannot be sent over WhatsApp.",
                    )
                    .await;
                    return;
                }
                self.pending_requests.insert(
                    token.clone(),
                    PendingRequest::UserInput {
                        request_id,
                        question_id: question.id.clone(),
                        expires_at,
                    },
                );
                let options = question
                    .options
                    .as_ref()
                    .map_or_else(String::new, |options| {
                        let rendered = options
                            .iter()
                            .map(|option| format!("• {} — {}", option.label, option.description))
                            .collect::<Vec<_>>()
                            .join("\n");
                        format!("\nOptions:\n{rendered}")
                    });
                self.send(&format!(
                    "[codex] Question {token}: {}{}\nReply `/answer {token} <your answer>`.",
                    question.question, options,
                ))
                .await;
            }
            ServerRequest::PermissionsRequestApproval { request_id, params } => {
                if !self.is_active_request(&params.thread_id, &params.turn_id) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                let summary = serde_json::to_string(&params.permissions)
                    .unwrap_or_else(|_| "additional access".to_string());
                self.pending_requests.insert(
                    token.clone(),
                    PendingRequest::Permissions {
                        request_id,
                        permissions: params.permissions,
                        expires_at,
                    },
                );
                self.send(&format!(
                    "[codex] Approval {token}: allow permissions {summary} for `{}`{}.\nReply `/approve {token}` or `/deny {token}`.",
                    params.cwd.as_path().display(),
                    params.reason.map_or_else(String::new, |reason| format!(" ({reason})")),
                ))
                .await;
            }
            request => {
                let _ = self.codex.reject_server_request(request.id().clone()).await;
                self.send("[codex] Codex requested an unsupported approval.")
                    .await;
            }
        }
    }

    fn is_active_request(&self, thread_id: &str, turn_id: &str) -> bool {
        self.state
            .active_turn
            .as_ref()
            .is_some_and(|active| active.thread_id == thread_id && active.codex_turn_id == turn_id)
    }

    async fn approve_request(&mut self, token: &str, session: bool) {
        let Some(request) = self.pending_requests.get(token).cloned() else {
            self.send("[codex] That approval token is unknown or expired.")
                .await;
            return;
        };
        let result = match request {
            PendingRequest::Command {
                request_id,
                available_decisions,
                ..
            } => {
                let decision = if session {
                    CommandExecutionApprovalDecision::AcceptForSession
                } else {
                    CommandExecutionApprovalDecision::Accept
                };
                if available_decisions
                    .as_ref()
                    .is_some_and(|decisions| !decisions.contains(&decision))
                {
                    self.send("[codex] That approval scope is not available for this request.")
                        .await;
                    return;
                }
                serde_json::to_value(CommandExecutionRequestApprovalResponse { decision })
                    .map(|result| (request_id, result))
            }
            PendingRequest::FileChange { request_id, .. } => {
                let decision = if session {
                    FileChangeApprovalDecision::AcceptForSession
                } else {
                    FileChangeApprovalDecision::Accept
                };
                serde_json::to_value(FileChangeRequestApprovalResponse { decision })
                    .map(|result| (request_id, result))
            }
            PendingRequest::UserInput { .. } => {
                self.send("[codex] Use `answer` for that token.").await;
                return;
            }
            PendingRequest::Permissions {
                request_id,
                permissions,
                ..
            } => {
                if session {
                    self.send("[codex] Session-wide permission approval is not available for this request.")
                        .await;
                    return;
                }
                serde_json::to_value(PermissionsRequestApprovalResponse {
                    permissions: GrantedPermissionProfile {
                        network: permissions.network,
                        file_system: permissions.file_system,
                    },
                    scope: PermissionGrantScope::Turn,
                    strict_auto_review: None,
                })
                .map(|result| (request_id, result))
            }
        };
        if let Ok((request_id, result)) = result {
            if self
                .codex
                .resolve_server_request(request_id, result)
                .await
                .is_ok()
            {
                self.pending_requests.remove(token);
                self.send("[codex] Approved.").await;
            } else {
                self.send("[codex] Could not deliver that approval.").await;
            }
        }
    }

    async fn deny_request(&mut self, token: &str) {
        let Some(request) = self.pending_requests.get(token).cloned() else {
            self.send("[codex] That approval token is unknown or expired.")
                .await;
            return;
        };
        let result = match request {
            PendingRequest::Command {
                request_id,
                available_decisions,
                ..
            } => {
                if available_decisions.as_ref().is_some_and(|decisions| {
                    !decisions.contains(&CommandExecutionApprovalDecision::Decline)
                }) {
                    self.send("[codex] Decline is not available for this request.")
                        .await;
                    return;
                }
                serde_json::to_value(CommandExecutionRequestApprovalResponse {
                    decision: CommandExecutionApprovalDecision::Decline,
                })
                .map(|result| (request_id, result))
            }
            PendingRequest::FileChange { request_id, .. } => {
                serde_json::to_value(FileChangeRequestApprovalResponse {
                    decision: FileChangeApprovalDecision::Decline,
                })
                .map(|result| (request_id, result))
            }
            PendingRequest::UserInput { .. } => {
                self.send("[codex] Use `answer` for that token.").await;
                return;
            }
            PendingRequest::Permissions { request_id, .. } => {
                serde_json::to_value(PermissionsRequestApprovalResponse {
                    permissions: GrantedPermissionProfile::default(),
                    scope: PermissionGrantScope::Turn,
                    strict_auto_review: None,
                })
                .map(|result| (request_id, result))
            }
        };
        if let Ok((request_id, result)) = result {
            if self
                .codex
                .resolve_server_request(request_id, result)
                .await
                .is_ok()
            {
                self.pending_requests.remove(token);
                self.send("[codex] Denied.").await;
            } else {
                self.send("[codex] Could not deliver that decision.").await;
            }
        }
    }

    async fn answer_request(&mut self, token: &str, answer: String) {
        let Some(request) = self.pending_requests.get(token).cloned() else {
            self.send("[codex] That question token is unknown or expired.")
                .await;
            return;
        };
        let PendingRequest::UserInput {
            request_id,
            question_id,
            ..
        } = request
        else {
            self.send("[codex] Use `approve` or `deny` for that token.")
                .await;
            return;
        };
        let response = ToolRequestUserInputResponse {
            answers: HashMap::from([(
                question_id,
                ToolRequestUserInputAnswer {
                    answers: vec![answer],
                },
            )]),
        };
        if let Ok(result) = serde_json::to_value(response) {
            if self
                .codex
                .resolve_server_request(request_id, result)
                .await
                .is_ok()
            {
                self.pending_requests.remove(token);
                self.send("[codex] Answer sent.").await;
            } else {
                self.send("[codex] Could not deliver that answer.").await;
            }
        }
    }

    async fn expire_pending_requests(&mut self) {
        let now = unix_timestamp();
        let expired = self
            .pending_requests
            .iter()
            .filter(|(_, request)| request.expires_at() <= now)
            .map(|(token, _)| token.clone())
            .collect::<Vec<_>>();
        for token in expired {
            let Some(request) = self.pending_requests.remove(&token) else {
                continue;
            };
            let _ = match request {
                PendingRequest::Command { request_id, .. } => {
                    let result = serde_json::to_value(CommandExecutionRequestApprovalResponse {
                        decision: CommandExecutionApprovalDecision::Decline,
                    });
                    match result {
                        Ok(result) => self.codex.resolve_server_request(request_id, result).await,
                        Err(_) => self.codex.reject_server_request(request_id).await,
                    }
                }
                PendingRequest::FileChange { request_id, .. } => {
                    let result = serde_json::to_value(FileChangeRequestApprovalResponse {
                        decision: FileChangeApprovalDecision::Decline,
                    });
                    match result {
                        Ok(result) => self.codex.resolve_server_request(request_id, result).await,
                        Err(_) => self.codex.reject_server_request(request_id).await,
                    }
                }
                PendingRequest::UserInput { request_id, .. } => {
                    self.codex.reject_server_request(request_id).await
                }
                PendingRequest::Permissions { request_id, .. } => {
                    let result = serde_json::to_value(PermissionsRequestApprovalResponse {
                        permissions: GrantedPermissionProfile::default(),
                        scope: PermissionGrantScope::Turn,
                        strict_auto_review: None,
                    });
                    match result {
                        Ok(result) => self.codex.resolve_server_request(request_id, result).await,
                        Err(_) => self.codex.reject_server_request(request_id).await,
                    }
                }
            };
            self.send(&format!("[codex] Approval token {token} expired."))
                .await;
        }
    }

    async fn send_help(&mut self) {
        self.send(
            "[codex] Commands: `/new`, `/status`, `/stop`, `/approve <token>`, \
             `/approve-session <token>`, `/deny <token>`, `/answer <token> <answer>`, \
             `/whatsapp list-threads`, and `/whatsapp attach <thread-id>`. Any other \
             message starts a Codex turn.",
        )
        .await;
    }

    async fn send(&mut self, text: &str) {
        if text.chars().count() <= 4_096 {
            let _ = self.send_tracked(text).await;
            return;
        }
        let content = text.strip_prefix("[codex] ").unwrap_or(text);
        for chunk in labelled_chunks(content, self.output_chunk_chars) {
            let _ = self.send_tracked(&chunk).await;
        }
    }

    async fn send_tracked(&mut self, text: &str) -> Option<String> {
        if !self.state_healthy || self.state.outbox.len() >= MAX_OUTBOX_MESSAGES {
            if self.state.outbox.len() >= MAX_OUTBOX_MESSAGES {
                self.openwa_healthy = false;
                self.refresh_readiness();
            }
            return None;
        }
        let response_id = Uuid::new_v4().simple().to_string();
        self.state.outbox.push(OutboundMessage {
            response_id: response_id.clone(),
            chat_id: self.self_chat_id.clone(),
            body: text.to_string(),
            attempts: 0,
        });
        if self.state.save(&self.state_path).is_err() {
            self.state.outbox.pop();
            self.state_healthy = false;
            self.refresh_readiness();
            return None;
        }
        self.flush_outbox().await.remove(&response_id)
    }

    async fn flush_outbox(&mut self) -> HashMap<String, String> {
        let mut delivered = HashMap::new();
        let mut delivered_any = false;
        while let Some(message) = self.state.outbox.first().cloned() {
            match self.openwa.send_text(message.chat_id, message.body).await {
                Ok(message_id) => {
                    delivered_any = true;
                    self.state.outbox.remove(0);
                    self.state
                        .mark_outbound(message_id.clone(), unix_timestamp());
                    delivered.insert(message.response_id, message_id);
                    self.state.prune(
                        unix_timestamp(),
                        self.dedupe_ttl_hours,
                        self.dedupe_capacity,
                    );
                    if self.state.save(&self.state_path).is_err() {
                        self.state_healthy = false;
                        self.refresh_readiness();
                        break;
                    }
                }
                Err(_) => {
                    self.openwa_healthy = false;
                    self.refresh_readiness();
                    if let Some(queued) = self.state.outbox.first_mut() {
                        queued.attempts = queued.attempts.saturating_add(1);
                    }
                    if self.state.save(&self.state_path).is_err() {
                        self.state_healthy = false;
                        self.refresh_readiness();
                    }
                    break;
                }
            }
        }
        if delivered_any {
            self.openwa_healthy = true;
            self.refresh_readiness();
        }
        delivered
    }

    fn recover_state_storage(&mut self) {
        if self.state.save(&self.state_path).is_ok() {
            self.state_healthy = true;
            self.refresh_readiness();
        }
    }

    fn refresh_readiness(&self) {
        self.ready.store(
            self.state_healthy
                && self.app_server_connected
                && self.openwa_healthy
                && self.webhook_registered,
            Ordering::Release,
        );
    }

    fn status_message(&self) -> String {
        let thread = self
            .state
            .binding
            .as_ref()
            .map_or("none", |binding| binding.codex_thread_id.as_str());
        let active_turn = self
            .state
            .active_turn
            .as_ref()
            .map_or("none", |turn| turn.codex_turn_id.as_str());
        let pending =
            self.pending_requests
                .values()
                .next()
                .map_or("none", |request| match request {
                    PendingRequest::Command { .. } => "command approval",
                    PendingRequest::FileChange { .. } => "file-change approval",
                    PendingRequest::UserInput { .. } => "user input",
                    PendingRequest::Permissions { .. } => "permission approval",
                });
        format!(
            "[codex] app-server: {}; OpenWA: {}; state: {}; thread: {thread}; active turn: {active_turn}; queued: {}; pending: {pending}",
            if self.app_server_connected {
                "connected"
            } else {
                "disconnected"
            },
            if self.openwa_healthy && self.webhook_registered {
                "ready"
            } else {
                "degraded"
            },
            if self.state_healthy {
                "durable"
            } else {
                "failed"
            },
            self.state.queued_prompts.len(),
        )
    }
}

fn normalize_phone(value: &str) -> String {
    value.chars().filter(char::is_ascii_digit).collect()
}

fn reconnect_delay_with_jitter(base: std::time::Duration) -> std::time::Duration {
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| u64::from(duration.subsec_millis()) % 250);
    base.saturating_add(std::time::Duration::from_millis(jitter))
}

#[cfg(test)]
#[path = "coordinator_tests.rs"]
mod tests;

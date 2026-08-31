//! Single-owner conversation state machine for the WhatsApp bridge.

use crate::CommandCatalog;
use crate::attachment::InboundAttachment;
use crate::codex::CodexClient;
use crate::codex::CodexError;
use crate::commands::BridgeCommand;
use crate::commands::parse_command;
use crate::health::BridgeReadiness;
use crate::health::BridgeReadinessSnapshot;
use crate::notifications::notification_thread_id;
use crate::notifications::server_request_thread_id;
use crate::state::BridgeState;
use crate::state::PendingSteer;
use crate::state::PendingUserInput;
use crate::state::PendingUserInputOption;
use crate::state::PendingUserInputQuestion;
use crate::state::QueuedPrompt;
use crate::state::unix_timestamp;
use crate::transport::TransportClient;
use crate::transport_webhook::InboundMessage;
use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::CommandExecutionApprovalDecision;
use codex_app_server_protocol::CommandExecutionRequestApprovalResponse;
use codex_app_server_protocol::FileChangeApprovalDecision;
use codex_app_server_protocol::FileChangeRequestApprovalResponse;
use codex_app_server_protocol::GrantedPermissionProfile;
use codex_app_server_protocol::PermissionGrantScope;
use codex_app_server_protocol::PermissionsRequestApprovalResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ToolRequestUserInputAnswer;
use codex_app_server_protocol::ToolRequestUserInputResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_messaging::DeliveryIntent;
use codex_messaging::DeliveryWorker;
use codex_messaging::DeliveryWorkerEvent;
use codex_messaging::DeliveryWorkerHandle;
use codex_messaging::FileDeliveryStore;
use codex_messaging::ProviderAdapter;
use codex_messaging::ProviderConversationId;
use codex_messaging::segment_text;
use codex_transcript::EntryOrigin;
use codex_transcript::ProjectionEvent;
use codex_transcript::TranscriptKey;
use codex_transcript::TranscriptProjectionOptions;
use codex_transcript::TranscriptProjector;
use codex_transcript::item_is_tool_call;
use codex_utils_approval_presentation::ApprovalDecision;
use codex_utils_approval_presentation::ApprovalPresentation;
use codex_utils_approval_presentation::command_execution_presentation;
use codex_utils_approval_presentation::file_change_presentation;
use codex_utils_approval_presentation::permissions_presentation;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

const MAX_PROMPT_BYTES: usize = 10_000;
const MAX_DELIVERY_QUEUE: usize = 100;
const MAX_PENDING_REQUESTS: usize = 32;
const USER_INPUT_TIMEOUT_SECONDS: u64 = 300;
const MAX_FILE_CHANGE_ITEMS: usize = 128;
const MAX_FILE_CHANGE_PATHS: usize = 128;
const MAX_FILE_CHANGE_PATH_CHARS: usize = 512;
const LOCAL_RECOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const ATTACHMENT_RETENTION: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Clone, Copy, Debug)]
enum BridgeTurnStatus {
    Working,
    Reasoning,
    Tooling,
}

impl BridgeTurnStatus {
    const fn item_id(self) -> &'static str {
        match self {
            Self::Working => "bridge-status-working",
            Self::Reasoning => "bridge-status-reasoning",
            Self::Tooling => "bridge-status-tooling",
        }
    }

    const fn text(self) -> &'static str {
        match self {
            Self::Working => "[codex working...]",
            Self::Reasoning => "[codex reasoning...]",
            Self::Tooling => "[codex tooling]",
        }
    }
}

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
    UnsupportedAttachment(String),
    PromptTooLong,
    QueueFull,
    StartQueuedPrompt,
    Steer(PendingSteer),
    ApprovalChoice(usize),
    ApprovalInputBlocked,
}

#[derive(Clone)]
enum PendingRequest {
    UserInput {
        request_id: RequestId,
        questions: Vec<PendingQuestion>,
        answers: HashMap<String, Vec<String>>,
        expires_at: u64,
    },
}

#[derive(Clone)]
struct PendingQuestion {
    id: String,
    question: String,
    options: Option<Vec<codex_app_server_protocol::ToolRequestUserInputOption>>,
}

impl PendingRequest {
    fn request_id(&self) -> &RequestId {
        let Self::UserInput { request_id, .. } = self;
        request_id
    }

    fn expires_at(&self) -> u64 {
        let Self::UserInput { expires_at, .. } = self;
        *expires_at
    }
}

#[derive(Clone)]
enum PendingApproval {
    Command {
        request_id: RequestId,
        presentation: ApprovalPresentation,
    },
    FileChange {
        request_id: RequestId,
        presentation: ApprovalPresentation,
    },
    Permissions {
        request_id: RequestId,
        presentation: ApprovalPresentation,
    },
}

impl PendingApproval {
    fn request_id(&self) -> &RequestId {
        match self {
            Self::Command { request_id, .. }
            | Self::FileChange { request_id, .. }
            | Self::Permissions { request_id, .. } => request_id,
        }
    }

    fn presentation(&self) -> &ApprovalPresentation {
        match self {
            Self::Command { presentation, .. }
            | Self::FileChange { presentation, .. }
            | Self::Permissions { presentation, .. } => presentation,
        }
    }
}

pub struct Coordinator<C, O> {
    codex: C,
    transport: O,
    state: BridgeState,
    state_path: PathBuf,
    attachment_dir: PathBuf,
    configured_phone: String,
    self_chat_id: String,
    max_queued_prompts: usize,
    dedupe_capacity: usize,
    dedupe_ttl_hours: u64,
    command_catalog: CommandCatalog,
    command_catalog_path: PathBuf,
    projector: TranscriptProjector,
    include_approval_notices: bool,
    delivery: Option<DeliveryWorkerHandle>,
    delivery_generation: u64,
    file_change_paths: HashMap<(String, String, String), Vec<String>>,
    pending_requests: HashMap<String, PendingRequest>,
    pending_approvals: VecDeque<PendingApproval>,
    state_healthy: bool,
    app_server_connected: bool,
    transport_healthy: bool,
    readiness: Arc<BridgeReadiness>,
    stream_degraded: bool,
    resume_failures: u8,
}

impl<C: CodexClient, O: TransportClient + ProviderAdapter + Clone + 'static> Coordinator<C, O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        codex: C,
        transport: O,
        state: BridgeState,
        state_path: PathBuf,
        attachment_dir: PathBuf,
        configured_phone: String,
        self_chat_id: String,
        _output_chunk_chars: usize,
        _edit_interval_ms: u64,
        max_queued_prompts: usize,
        dedupe_capacity: usize,
        dedupe_ttl_hours: u64,
        command_catalog: CommandCatalog,
        command_catalog_path: PathBuf,
        readiness: Arc<BridgeReadiness>,
        app_server_connected: bool,
        transport_healthy: bool,
    ) -> Self {
        Self {
            codex,
            transport,
            state,
            state_path,
            attachment_dir,
            configured_phone,
            self_chat_id,
            max_queued_prompts,
            dedupe_capacity,
            dedupe_ttl_hours,
            command_catalog,
            command_catalog_path,
            projector: TranscriptProjector::default(),
            include_approval_notices: false,
            delivery: None,
            delivery_generation: 0,
            file_change_paths: HashMap::new(),
            pending_requests: HashMap::new(),
            pending_approvals: VecDeque::new(),
            state_healthy: true,
            app_server_connected,
            transport_healthy,
            readiness,
            stream_degraded: false,
            resume_failures: 0,
        }
    }

    /// Configures the transcript categories exposed to every provider adapter.
    pub fn with_transcript_options(mut self, options: TranscriptProjectionOptions) -> Self {
        self.projector = TranscriptProjector::new(options);
        self
    }

    /// Enables or disables command and file-change approval notices.
    /// Permission requests remain available regardless of this setting.
    pub fn with_approval_notices(mut self, enabled: bool) -> Self {
        self.include_approval_notices = enabled;
        self
    }

    pub async fn run(mut self, mut commands: mpsc::Receiver<CoordinatorCommand>) {
        self.cleanup_stale_attachments();
        let (delivery, delivery_commands) =
            DeliveryWorker::<O, FileDeliveryStore>::channel(MAX_DELIVERY_QUEUE);
        let (delivery_events, mut delivery_event_rx) = mpsc::channel(128);
        let delivery_path = self.state_path.with_extension("delivery.json");
        let delivery_task = match DeliveryWorker::new(
            self.transport.clone(),
            FileDeliveryStore::new(delivery_path),
            delivery_events,
            std::time::Duration::from_secs(5),
        )
        .await
        {
            Ok(worker) => {
                self.delivery = Some(delivery.clone());
                Some(tokio::spawn(worker.run(delivery_commands)))
            }
            Err(error) => {
                tracing::error!(%error, "failed to load the durable delivery journal");
                self.state_healthy = false;
                self.refresh_readiness();
                None
            }
        };
        for pending in self.state.pending_user_inputs.clone() {
            self.pending_requests.insert(
                pending.token,
                PendingRequest::UserInput {
                    request_id: pending.request_id,
                    questions: pending
                        .questions
                        .into_iter()
                        .map(|question| PendingQuestion {
                            id: question.id,
                            question: question.question,
                            options: question.options.map(|options| {
                                options
                                    .into_iter()
                                    .map(|option| {
                                        codex_app_server_protocol::ToolRequestUserInputOption {
                                            label: option.label,
                                            description: option.description,
                                        }
                                    })
                                    .collect()
                            }),
                        })
                        .collect(),
                    answers: pending.answers.into_iter().collect(),
                    expires_at: pending.expires_at,
                },
            );
        }
        let discarded_legacy_messages = self.state.legacy_outbox.len();
        if discarded_legacy_messages > 0 {
            self.state.legacy_outbox.clear();
            if self.state.save(&self.state_path).is_err() {
                self.state_healthy = false;
                self.refresh_readiness();
            } else {
                self.send(&format!(
                    "[codex] Discarded {discarded_legacy_messages} legacy queued message(s); new transcript delivery uses the experimental journal."
                ))
                .await;
            }
        }
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
        let mut recover_state = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut expire_requests = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut reconcile_turn_start = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut reconcile_active_turn = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut cleanup_attachments = tokio::time::interval(ATTACHMENT_RETENTION);
        let mut connected = self.app_server_connected;
        let mut reconnect_delay = std::time::Duration::from_secs(1);
        loop {
            if connected {
                tokio::select! {
                    // Keep inbound WhatsApp input ahead of the continuously-ready app-server
                    // stream. This is the bridge equivalent of the TUI's immediate steer path:
                    // a follow-up message must not wait behind a burst of protocol events.
                    biased;
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
                    delivery_event = delivery_event_rx.recv() => {
                        if let Some(delivery_event) = delivery_event {
                            self.handle_delivery_event(delivery_event);
                        }
                    },
                    _ = recover_state.tick(), if !self.state_healthy => {
                        self.recover_state_storage();
                    },
                    _ = cleanup_attachments.tick() => {
                        self.cleanup_stale_attachments();
                    },
                    _ = expire_requests.tick(), if !self.pending_requests.is_empty() => {
                        self.expire_pending_requests().await;
                    },
                    _ = reconcile_turn_start.tick(), if self.has_uncertain_turn_start() => {
                        self.reconcile_uncertain_turn_start().await;
                    },
                    _ = reconcile_active_turn.tick(), if self.state.active_turn.is_some() => {
                        self.reconcile_active_turn().await;
                    },
                    else => break,
                }
            } else {
                tokio::select! {
                    biased;
                    Some(command) = commands.recv() => {
                        if self.handle_command(command).await {
                            break;
                        }
                    },
                    delivery_event = delivery_event_rx.recv() => {
                        if let Some(delivery_event) = delivery_event {
                            self.handle_delivery_event(delivery_event);
                        }
                    },
                    _ = tokio::time::sleep(reconnect_delay_with_jitter(reconnect_delay)) => {
                        match self.codex.reconnect().await {
                            Ok(()) => {
                                self.delivery_generation =
                                    self.delivery_generation.saturating_add(1);
                                if self.resume_after_reconnect().await {
                                    connected = true;
                                    self.app_server_connected = true;
                                    reconnect_delay = std::time::Duration::from_secs(1);
                                    self.refresh_readiness();
                                    tracing::info!(
                                        generation = self.delivery_generation,
                                        "connected to Codex app-server"
                                    );
                                    if self.state.active_turn.is_none()
                                        && !self.state.queued_prompts.is_empty()
                                    {
                                        self.advance_queue().await;
                                    }
                                } else {
                                    tracing::warn!(
                                        generation = self.delivery_generation,
                                        "connected to Codex app-server but thread resume failed"
                                    );
                                    reconnect_delay =
                                        (reconnect_delay * 2).min(LOCAL_RECOVERY_INTERVAL);
                                }
                            }
                            Err(error) => {
                                tracing::debug!(%error, "Codex app-server reconnect attempt failed");
                                reconnect_delay =
                                    (reconnect_delay * 2).min(LOCAL_RECOVERY_INTERVAL);
                            }
                        }
                    }
                    _ = recover_state.tick(), if !self.state_healthy => {
                        self.recover_state_storage();
                    },
                    _ = cleanup_attachments.tick() => {
                        self.cleanup_stale_attachments();
                    },
                    else => break,
                }
            }
        }
        if let Some(delivery) = self.delivery.take() {
            delivery.shutdown().await;
        }
        if let Some(task) = delivery_task {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(15), task).await;
        }
        self.readiness.mark_stopped();
    }

    fn handle_delivery_event(&mut self, event: DeliveryWorkerEvent) {
        match event {
            DeliveryWorkerEvent::Status(status) => {
                self.transport_healthy = status.ready
                    && status
                        .account
                        .as_deref()
                        .is_none_or(|phone| normalize_phone(phone) == self.configured_phone);
                self.refresh_readiness();
            }
            DeliveryWorkerEvent::Enqueued { key, queue_depth } => {
                tracing::debug!(key = ?key, queue_depth, "transcript delivery queued");
            }
            DeliveryWorkerEvent::Sent { key, segment } => {
                tracing::debug!(key = ?key, segment, "transcript delivery sent");
            }
            DeliveryWorkerEvent::Edited {
                key,
                segment,
                revisions,
            } => {
                tracing::debug!(
                    key = ?key,
                    segment,
                    coalesced_revisions = revisions,
                    "transcript delivery edited"
                );
            }
            DeliveryWorkerEvent::Failed {
                key,
                segment,
                error,
            } => {
                tracing::warn!(%error, key = ?key, segment, "transcript delivery failed");
                self.transport_healthy = false;
                self.refresh_readiness();
            }
            DeliveryWorkerEvent::StoreFailed => {
                tracing::error!("transcript delivery journal persistence failed");
                self.transport_healthy = false;
                self.refresh_readiness();
            }
        }
    }

    fn cleanup_stale_attachments(&self) {
        let Ok(entries) = std::fs::read_dir(&self.attachment_dir) else {
            return;
        };
        let referenced = self.referenced_attachment_paths();
        let now = std::time::SystemTime::now();
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() || referenced.contains(&path) {
                continue;
            }
            let Ok(modified) = entry.metadata().and_then(|metadata| metadata.modified()) else {
                continue;
            };
            if now
                .duration_since(modified)
                .is_ok_and(|age| age >= ATTACHMENT_RETENTION)
                && std::fs::remove_file(&path).is_err()
            {
                tracing::debug!(path = %path.display(), "failed to remove stale WhatsApp attachment");
            }
        }
    }

    fn referenced_attachment_paths(&self) -> std::collections::HashSet<PathBuf> {
        let mut paths = std::collections::HashSet::new();
        if let Some(active) = &self.state.active_turn {
            paths.extend(active.attachment_paths.iter().cloned());
        }
        paths.extend(
            self.state
                .queued_prompts
                .iter()
                .filter_map(|prompt| prompt.attachment.as_ref().and_then(InboundAttachment::path))
                .map(PathBuf::from),
        );
        paths.extend(
            self.state
                .pending_steers
                .iter()
                .filter_map(|steer| steer.attachment.as_ref().and_then(InboundAttachment::path))
                .map(PathBuf::from),
        );
        paths
    }

    fn remove_attachment_paths(&self, paths: &[PathBuf]) {
        for path in paths {
            if path.parent() == Some(self.attachment_dir.as_path())
                && std::fs::remove_file(path).is_err()
            {
                tracing::debug!(path = %path.display(), "failed to remove WhatsApp attachment");
            }
        }
    }

    fn retain_attachment_for_active_turn(&mut self, attachment: Option<&InboundAttachment>) {
        let Some(path) = attachment.and_then(InboundAttachment::path) else {
            return;
        };
        if let Some(active) = self.state.active_turn.as_mut()
            && !active
                .attachment_paths
                .iter()
                .any(|existing| existing == path)
        {
            active.attachment_paths.push(path.to_path_buf());
        }
    }

    async fn mark_disconnected(&mut self) {
        self.app_server_connected = false;
        self.refresh_readiness();
        self.pending_requests.clear();
        self.pending_approvals.clear();
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
                self.reconcile_pending_steers(&response).await;
                self.retry_pending_steers().await;
                if let Some(active) = self.state.active_turn.clone() {
                    let matching_turn = response
                        .thread
                        .turns
                        .iter()
                        .find(|turn| turn.id == active.codex_turn_id);
                    if let Some(turn) =
                        matching_turn.filter(|turn| turn.status == TurnStatus::InProgress)
                    {
                        let projection = self.projector.reconcile_turn(&active.thread_id, turn);
                        self.enqueue_projection(projection).await;
                        return true;
                    }
                    let recovered = matching_turn
                        .map(|turn| self.projector.reconcile_turn(&active.thread_id, turn));
                    self.state.active_turn = None;
                    self.pending_requests.clear();
                    if self.state.save(&self.state_path).is_err() {
                        self.state_healthy = false;
                        self.refresh_readiness();
                        return false;
                    }
                    if let Some(projection) = recovered {
                        self.enqueue_projection(projection).await;
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

    async fn reconcile_active_turn(&mut self) {
        let Some(active) = self.state.active_turn.clone() else {
            return;
        };
        let Some(binding) = self.state.binding.clone() else {
            self.state.active_turn = None;
            if self.state.save(&self.state_path).is_err() {
                self.state_healthy = false;
                self.refresh_readiness();
            }
            return;
        };
        let response = match self.codex.resume_thread(binding.codex_thread_id).await {
            Ok(response) => response,
            Err(error) => {
                tracing::debug!(%error, "active WhatsApp turn reconciliation failed");
                return;
            }
        };
        self.reconcile_pending_steers(&response).await;
        self.retry_pending_steers().await;
        let matching_turn = response
            .thread
            .turns
            .iter()
            .find(|turn| turn.id == active.codex_turn_id);
        if let Some(turn) = matching_turn.filter(|turn| turn.status == TurnStatus::InProgress) {
            let projection = self.projector.reconcile_turn(&active.thread_id, turn);
            self.enqueue_projection(projection).await;
            return;
        }

        let recovered =
            matching_turn.map(|turn| self.projector.reconcile_turn(&active.thread_id, turn));
        let request_ids = self
            .pending_requests
            .drain()
            .map(|(_, request)| request.request_id().clone())
            .chain(
                self.pending_approvals
                    .drain(..)
                    .map(|approval| approval.request_id().clone()),
            )
            .collect::<Vec<_>>();
        for request_id in request_ids {
            let _ = self.codex.reject_server_request(request_id).await;
        }
        self.state.active_turn = None;
        self.file_change_paths.retain(|(thread_id, turn_id, _), _| {
            thread_id != &active.thread_id || turn_id != &active.codex_turn_id
        });
        self.stream_degraded = false;
        if self.state.save(&self.state_path).is_err() {
            self.state_healthy = false;
            self.refresh_readiness();
            return;
        }
        if let Some(projection) = recovered {
            self.enqueue_projection(projection).await;
        }
        if !self.state.queued_prompts.is_empty() {
            self.advance_queue().await;
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
                for approval in self.pending_approvals.drain(..) {
                    let _ = self
                        .codex
                        .reject_server_request(approval.request_id().clone())
                        .await;
                }
                self.state.pending_user_inputs.clear();
                let _ = self.state.save(&self.state_path);
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(5), self.codex.shutdown())
                        .await;
                self.readiness.mark_stopped();
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
        let envelope = message.as_envelope();
        if !self.state_healthy {
            return Err(crate::state::StateError::Write);
        }
        if self.state.was_processed(&envelope.idempotency_key) {
            return Ok(None);
        }
        if self
            .state
            .was_sent_by_bridge(envelope.provider_message_id.as_str())
        {
            return Ok(None);
        }
        let attachment = message.attachment;
        let unsupported_attachment = match attachment.as_ref() {
            Some(InboundAttachment::Unsupported { kind }) => Some(kind.clone()),
            None
            | Some(InboundAttachment::Image { .. })
            | Some(InboundAttachment::Document { .. }) => None,
        };
        let command = if attachment.is_some() {
            BridgeCommand::Prompt(envelope.body.clone())
        } else {
            parse_command(&envelope.body)
        };
        let previous_state = self.state.clone();
        let now = unix_timestamp();
        self.state.mark_processed(envelope.idempotency_key, now);
        self.state
            .prune(now, self.dedupe_ttl_hours, self.dedupe_capacity);
        let action = match unsupported_attachment {
            Some(kind) => AcceptedAction::UnsupportedAttachment(kind),
            None => match command {
                BridgeCommand::Prompt(body) => {
                    if body.len() > MAX_PROMPT_BYTES {
                        AcceptedAction::PromptTooLong
                    } else if self.pending_approvals.front().is_some() {
                        if attachment.is_none()
                            && let Some(choice) = parse_approval_choice(&body)
                        {
                            AcceptedAction::ApprovalChoice(choice)
                        } else {
                            AcceptedAction::ApprovalInputBlocked
                        }
                    } else if self.state.pending_steers.len() >= self.max_queued_prompts {
                        AcceptedAction::QueueFull
                    } else if let Some(active) = self.state.active_turn.clone() {
                        let steer = PendingSteer {
                            idempotency_key: message.idempotency_key,
                            message_id: message.message_id,
                            body,
                            attachment,
                            thread_id: active.thread_id,
                            expected_turn_id: active.codex_turn_id,
                            accepted_at: now,
                            submission_uncertain: false,
                        };
                        self.state.pending_steers.push(steer.clone());
                        AcceptedAction::Steer(steer)
                    } else if self.state.queued_prompts.len() >= self.max_queued_prompts {
                        AcceptedAction::QueueFull
                    } else {
                        let prompt = QueuedPrompt {
                            idempotency_key: message.idempotency_key,
                            message_id: message.message_id,
                            body,
                            attachment,
                            accepted_at: now,
                            submission_uncertain: false,
                            failure_notified: false,
                        };
                        self.state.queued_prompts.push(prompt);
                        if self.state.active_turn.is_some() {
                            return self.persist_accepted(previous_state, None);
                        }
                        AcceptedAction::StartQueuedPrompt
                    }
                }
                command => AcceptedAction::Command(command),
            },
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
            AcceptedAction::Steer(steer) => self.submit_steer(steer).await,
            AcceptedAction::ApprovalChoice(choice) => self.resolve_approval_choice(choice).await,
            AcceptedAction::ApprovalInputBlocked => {
                self.send(
                    "[codex] An approval is displayed. Reply with one of its numbers, or /stop.",
                )
                .await;
            }
            AcceptedAction::QueueFull => {
                self.send("[codex] Queue is full; try again after the current turn.")
                    .await;
            }
            AcceptedAction::PromptTooLong => {
                self.send("[codex] That prompt is too long; keep it under 10,000 UTF-8 bytes.")
                    .await;
            }
            AcceptedAction::Command(command) => self.handle_bridge_command(command).await,
            AcceptedAction::UnsupportedAttachment(kind) => {
                if kind == "audio attachment" {
                    self.send("[codex] Audio attachments are unsupported in this session")
                        .await;
                } else {
                    self.send(&format!(
                        "[codex] This WhatsApp {kind} cannot be used as Codex input."
                    ))
                    .await;
                }
            }
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
                Err(error) => {
                    self.app_server_connected = false;
                    self.refresh_readiness();
                    tracing::warn!(%error, "failed to start a Codex thread for a queued WhatsApp prompt");
                    let should_notify = self
                        .state
                        .queued_prompts
                        .first_mut()
                        .filter(|queued| queued.message_id == prompt.message_id)
                        .is_some_and(|queued| {
                            let should_notify = !queued.failure_notified;
                            queued.failure_notified = true;
                            should_notify
                        });
                    if should_notify {
                        if self.state.save(&self.state_path).is_err() {
                            self.state_healthy = false;
                            self.refresh_readiness();
                            return;
                        }
                        self.send("[codex] Codex app-server is unavailable; your prompt is queued and will retry automatically.")
                            .await;
                    }
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
            .start_turn(
                thread_id.clone(),
                prompt.message_id.clone(),
                user_inputs(prompt.body, prompt.attachment.clone()),
            )
            .await
        {
            Ok(turn_id) => {
                let status_thread_id = thread_id.clone();
                let status_turn_id = turn_id.clone();
                self.state.queued_prompts.remove(0);
                self.state.active_turn = Some(crate::state::ActiveTurn {
                    inbound_message_id: prompt.message_id,
                    thread_id,
                    codex_turn_id: turn_id,
                    legacy_working_output_message_id: None,
                    attachment_paths: prompt
                        .attachment
                        .as_ref()
                        .and_then(InboundAttachment::path)
                        .map(|path| vec![path.to_path_buf()])
                        .unwrap_or_default(),
                });
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                } else {
                    self.enqueue_turn_status(
                        BridgeTurnStatus::Working,
                        &status_thread_id,
                        &status_turn_id,
                    )
                    .await;
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

    async fn submit_steer(&mut self, steer: PendingSteer) {
        let Some(active) = self.state.active_turn.clone() else {
            self.queue_steer_for_next_turn(&steer).await;
            return;
        };
        if let Some(pending) = self
            .state
            .pending_steers
            .iter_mut()
            .find(|pending| pending.message_id == steer.message_id)
        {
            pending.submission_uncertain = true;
        }
        if self.state.save(&self.state_path).is_err() {
            self.state_healthy = false;
            self.refresh_readiness();
            return;
        }

        let mut expected_turn_id = steer.expected_turn_id.clone();
        let mut retried_after_mismatch = false;
        loop {
            match self
                .codex
                .steer_turn(
                    active.thread_id.clone(),
                    expected_turn_id.clone(),
                    steer.message_id.clone(),
                    user_inputs(steer.body.clone(), steer.attachment.clone()),
                )
                .await
            {
                Ok(()) => {
                    self.retain_attachment_for_active_turn(steer.attachment.as_ref());
                    self.remove_pending_steer(&steer.message_id);
                    let _ = self.state.save(&self.state_path);
                    return;
                }
                Err(error) if error.is_no_active_turn() => {
                    self.state.active_turn = None;
                    self.queue_steer_for_next_turn(&steer).await;
                    self.start_next_prompt().await;
                    return;
                }
                Err(error) if error.is_non_steerable() => {
                    self.queue_steer_for_next_turn(&steer).await;
                    self.send("[codex] This turn cannot be steered; your message was queued for the next turn.")
                        .await;
                    return;
                }
                Err(CodexError::Transport(_)) => {
                    self.app_server_connected = false;
                    self.refresh_readiness();
                    self.send("[codex] Steer submission is uncertain; it will be reconciled after reconnecting.")
                        .await;
                    return;
                }
                Err(error) if !retried_after_mismatch => {
                    let Some(actual_turn_id) = error.actual_turn_id() else {
                        self.queue_steer_for_next_turn(&steer).await;
                        self.send("[codex] The message could not be steered; it was queued for the next turn.")
                            .await;
                        return;
                    };
                    expected_turn_id = actual_turn_id.clone();
                    retried_after_mismatch = true;
                    if let Some(pending) = self
                        .state
                        .pending_steers
                        .iter_mut()
                        .find(|pending| pending.message_id == steer.message_id)
                    {
                        pending.expected_turn_id = actual_turn_id.clone();
                    }
                    if let Some(active_turn) = self.state.active_turn.as_mut() {
                        active_turn.codex_turn_id = actual_turn_id;
                    }
                    if self.state.save(&self.state_path).is_err() {
                        self.state_healthy = false;
                        self.refresh_readiness();
                        return;
                    }
                }
                Err(_) => {
                    self.queue_steer_for_next_turn(&steer).await;
                    self.send("[codex] The message could not be steered; it was queued for the next turn.")
                        .await;
                    return;
                }
            }
        }
    }

    async fn queue_steer_for_next_turn(&mut self, steer: &PendingSteer) {
        self.remove_pending_steer(&steer.message_id);
        self.state.queued_prompts.push(QueuedPrompt {
            idempotency_key: steer.idempotency_key.clone(),
            message_id: steer.message_id.clone(),
            body: steer.body.clone(),
            attachment: steer.attachment.clone(),
            accepted_at: steer.accepted_at,
            submission_uncertain: false,
            failure_notified: false,
        });
        if self.state.save(&self.state_path).is_err() {
            self.state_healthy = false;
            self.refresh_readiness();
        }
    }

    fn remove_pending_steer(&mut self, message_id: &str) {
        self.state
            .pending_steers
            .retain(|pending| pending.message_id != message_id);
    }

    async fn requeue_pending_steers_for_turn(&mut self, thread_id: &str, turn_id: &str) {
        let steers = self
            .state
            .pending_steers
            .iter()
            .filter(|steer| steer.thread_id == thread_id && steer.expected_turn_id == turn_id)
            .cloned()
            .collect::<Vec<_>>();
        for steer in steers {
            self.queue_steer_for_next_turn(&steer).await;
        }
    }

    async fn reconcile_pending_steers(&mut self, response: &ThreadResumeResponse) {
        let pending = self.state.pending_steers.clone();
        let mut changed = false;
        for steer in pending {
            let Some(turn) = response
                .thread
                .turns
                .iter()
                .find(|turn| turn.id == steer.expected_turn_id)
            else {
                let target_is_active = self.state.active_turn.as_ref().is_some_and(|active| {
                    active.thread_id == steer.thread_id
                        && active.codex_turn_id == steer.expected_turn_id
                });
                if !target_is_active {
                    self.queue_steer_for_next_turn(&steer).await;
                }
                continue;
            };
            let accepted = turn.items.iter().any(|item| {
                matches!(
                    item,
                    ThreadItem::UserMessage {
                        client_id: Some(client_id),
                        ..
                    } if client_id == &steer.message_id
                )
            });
            if accepted {
                self.retain_attachment_for_active_turn(steer.attachment.as_ref());
                self.remove_pending_steer(&steer.message_id);
                changed = true;
            } else if turn.status != TurnStatus::InProgress {
                self.queue_steer_for_next_turn(&steer).await;
                changed = true;
            }
        }
        if changed && self.state.save(&self.state_path).is_err() {
            self.state_healthy = false;
            self.refresh_readiness();
        }
    }

    async fn retry_pending_steers(&mut self) {
        let Some(active) = self.state.active_turn.clone() else {
            return;
        };
        let pending = self.state.pending_steers.clone();
        for steer in pending {
            if steer.thread_id == active.thread_id && steer.expected_turn_id == active.codex_turn_id
            {
                self.submit_steer(steer).await;
            }
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
                    legacy_working_output_message_id: None,
                    attachment_paths: prompt
                        .attachment
                        .as_ref()
                        .and_then(InboundAttachment::path)
                        .map(|path| vec![path.to_path_buf()])
                        .unwrap_or_default(),
                });
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                    return;
                }
            } else {
                let projection = self
                    .projector
                    .reconcile_turn(&binding.codex_thread_id, turn);
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                    return;
                }
                self.enqueue_projection(projection).await;
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
        if self.pending_approvals.front().is_some() {
            self.resolve_current_approval_for_stop().await;
        }
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
            Err(_) => {
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
            AppServerEvent::Lagged { skipped } => {
                self.stream_degraded = true;
                tracing::warn!(
                    skipped,
                    "app-server event stream lagged; reconciling the current turn"
                );
                if !self.resume_after_reconnect().await {
                    self.app_server_connected = false;
                    self.refresh_readiness();
                }
                return;
            }
            AppServerEvent::Disconnected { .. } => return,
        };
        if let Some(thread_id) = notification_thread_id(&notification)
            && self
                .state
                .binding
                .as_ref()
                .is_some_and(|binding| binding.codex_thread_id != thread_id)
        {
            return;
        }
        let projection = self.projector.apply(notification.clone());
        let (reasoning_status, tooling_status) = notification_statuses(&notification);
        if let Some((thread_id, turn_id)) = notification_turn_scope(&notification) {
            if reasoning_status {
                self.enqueue_turn_status(BridgeTurnStatus::Reasoning, thread_id, turn_id)
                    .await;
            }
            if tooling_status {
                self.enqueue_turn_status(BridgeTurnStatus::Tooling, thread_id, turn_id)
                    .await;
            }
        }
        if projection
            .iter()
            .any(|event| matches!(event, ProjectionEvent::Suppressed))
        {
            tracing::trace!("suppressed app-server notification from provider output");
        }
        let should_enqueue = match &notification {
            ServerNotification::AgentMessageDelta(delta) => {
                self.is_active_request(&delta.thread_id, &delta.turn_id)
            }
            ServerNotification::PlanDelta(delta) => {
                self.is_active_request(&delta.thread_id, &delta.turn_id)
            }
            ServerNotification::ReasoningSummaryTextDelta(delta) => {
                self.is_active_request(&delta.thread_id, &delta.turn_id)
            }
            ServerNotification::ReasoningTextDelta(delta) => {
                self.is_active_request(&delta.thread_id, &delta.turn_id)
            }
            ServerNotification::CommandExecutionOutputDelta(delta) => {
                self.is_active_request(&delta.thread_id, &delta.turn_id)
            }
            ServerNotification::FileChangeOutputDelta(delta) => {
                self.is_active_request(&delta.thread_id, &delta.turn_id)
            }
            ServerNotification::FileChangePatchUpdated(delta) => {
                self.is_active_request(&delta.thread_id, &delta.turn_id)
            }
            ServerNotification::ItemStarted(item) => {
                self.is_active_request(&item.thread_id, &item.turn_id)
            }
            ServerNotification::ItemCompleted(item) => {
                self.is_active_request(&item.thread_id, &item.turn_id)
            }
            ServerNotification::TurnCompleted(completed) => {
                self.state.active_turn.as_ref().is_some_and(|active| {
                    active.thread_id == completed.thread_id
                        && active.codex_turn_id == completed.turn.id
                })
            }
            _ => true,
        };
        if should_enqueue {
            self.enqueue_projection(projection).await;
        }
        match notification {
            ServerNotification::AgentMessageDelta(delta)
                if !self.is_active_request(&delta.thread_id, &delta.turn_id) => {}
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
                let _ = completed;
            }
            ServerNotification::TurnCompleted(completed) => {
                let is_active = self.state.active_turn.as_ref().is_some_and(|active| {
                    active.thread_id == completed.thread_id
                        && active.codex_turn_id == completed.turn.id
                });
                if !is_active {
                    if self
                        .state
                        .active_turn
                        .as_ref()
                        .is_some_and(|active| active.thread_id == completed.thread_id)
                    {
                        self.reconcile_active_turn().await;
                    }
                    return;
                }
                let attachment_paths = self
                    .state
                    .active_turn
                    .as_ref()
                    .map(|active| active.attachment_paths.clone())
                    .unwrap_or_default();
                self.requeue_pending_steers_for_turn(&completed.thread_id, &completed.turn.id)
                    .await;
                self.state.active_turn = None;
                self.pending_requests.clear();
                self.state.pending_user_inputs.retain(|pending| {
                    pending.thread_id != completed.thread_id || pending.turn_id != completed.turn.id
                });
                self.pending_approvals.clear();
                self.file_change_paths.retain(|(thread_id, turn_id, _), _| {
                    thread_id != &completed.thread_id || turn_id != &completed.turn.id
                });
                self.stream_degraded = false;
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                    return;
                }
                self.remove_attachment_paths(&attachment_paths);
                if !self.state.queued_prompts.is_empty() {
                    self.start_next_prompt().await;
                }
            }
            ServerNotification::Error(error) => {
                let _ = error;
            }
            ServerNotification::ServerRequestResolved(resolved) => {
                let was_front = self
                    .pending_approvals
                    .front()
                    .is_some_and(|approval| approval.request_id() == &resolved.request_id);
                self.pending_approvals
                    .retain(|approval| approval.request_id() != &resolved.request_id);
                if was_front {
                    self.send_front_approval().await;
                }
                self.pending_requests
                    .retain(|_, request| request.request_id() != &resolved.request_id);
                self.state
                    .pending_user_inputs
                    .retain(|pending| pending.request_id != resolved.request_id);
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                }
            }
            _ => {}
        }
    }

    async fn enqueue_projection(&mut self, projection: Vec<ProjectionEvent>) {
        for event in projection {
            match event {
                ProjectionEvent::Entry(entry) => {
                    if entry.origin == codex_transcript::EntryOrigin::Internal {
                        continue;
                    }
                    let Some(text) = entry.plain_text() else {
                        continue;
                    };
                    self.enqueue_intent(DeliveryIntent {
                        conversation_id: ProviderConversationId::new(self.self_chat_id.clone()),
                        generation: self.delivery_generation,
                        key: entry.key.clone(),
                        origin: entry.origin,
                        text,
                        revision: entry.revision,
                        committed: entry.committed,
                    })
                    .await;
                }
                ProjectionEvent::Notice(notice) => {
                    if notice.origin == codex_transcript::EntryOrigin::Internal {
                        continue;
                    }
                    if notice.text.trim().is_empty() {
                        continue;
                    }
                    self.enqueue_intent(DeliveryIntent {
                        conversation_id: ProviderConversationId::new(self.self_chat_id.clone()),
                        generation: self.delivery_generation,
                        key: codex_transcript::TranscriptKey::new(
                            notice.key.thread_id.clone(),
                            notice.key.turn_id.clone(),
                            notice.key.item_id.clone(),
                        ),
                        origin: notice.origin,
                        text: notice.text,
                        revision: 1,
                        committed: true,
                    })
                    .await;
                }
                ProjectionEvent::Request(_) => {}
                ProjectionEvent::Suppressed => {}
            }
        }
    }

    async fn enqueue_turn_status(
        &mut self,
        status: BridgeTurnStatus,
        thread_id: &str,
        turn_id: &str,
    ) {
        if !self
            .state
            .active_turn
            .as_ref()
            .is_some_and(|active| active.thread_id == thread_id && active.codex_turn_id == turn_id)
        {
            return;
        }
        self.enqueue_intent(DeliveryIntent {
            conversation_id: ProviderConversationId::new(self.self_chat_id.clone()),
            generation: self.delivery_generation,
            key: TranscriptKey::new(thread_id, turn_id, status.item_id()),
            origin: EntryOrigin::BridgeNotice,
            text: status.text().to_string(),
            revision: 1,
            committed: true,
        })
        .await;
    }

    async fn enqueue_intent(&mut self, intent: DeliveryIntent) {
        if let Some(delivery) = &self.delivery {
            if delivery.try_apply(intent).is_err() {
                self.transport_healthy = false;
                self.refresh_readiness();
            }
            return;
        }
        tracing::error!(key = ?intent.key, "transcript delivery worker is unavailable");
        self.state_healthy = false;
        self.refresh_readiness();
    }

    async fn handle_server_request(&mut self, request: ServerRequest) {
        if let Some(thread_id) = server_request_thread_id(&request)
            && self
                .state
                .binding
                .as_ref()
                .is_some_and(|binding| binding.codex_thread_id != thread_id)
        {
            let _ = self.codex.reject_server_request(request.id().clone()).await;
            return;
        }
        if self.pending_requests.len() + self.pending_approvals.len() >= MAX_PENDING_REQUESTS {
            let _ = self.codex.reject_server_request(request.id().clone()).await;
            self.send("[codex] Too many interactive requests are pending; the newest request was rejected.")
                .await;
            return;
        }
        match request {
            ServerRequest::CommandExecutionRequestApproval { request_id, params } => {
                if !self.is_active_request(&params.thread_id, &params.turn_id) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                let presentation = command_execution_presentation(&params);
                self.enqueue_approval(PendingApproval::Command {
                    request_id: request_id.clone(),
                    presentation,
                })
                .await;
            }
            ServerRequest::FileChangeRequestApproval { request_id, params } => {
                if !self.is_active_request(&params.thread_id, &params.turn_id) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                let paths = self
                    .file_change_paths
                    .get(&(
                        params.thread_id.clone(),
                        params.turn_id.clone(),
                        params.item_id.clone(),
                    ))
                    .filter(|paths| !paths.is_empty())
                    .cloned()
                    .unwrap_or_default();
                let presentation = file_change_presentation(&params, &paths);
                self.enqueue_approval(PendingApproval::FileChange {
                    request_id,
                    presentation,
                })
                .await;
            }
            ServerRequest::ToolRequestUserInput { request_id, params } => {
                let token = Uuid::new_v4().simple().to_string()[..8].to_string();
                let expires_at = unix_timestamp().saturating_add(USER_INPUT_TIMEOUT_SECONDS);
                let thread_id = params.thread_id.clone();
                let turn_id = params.turn_id.clone();
                if !self.is_active_request(&thread_id, &turn_id) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                if params.questions.is_empty() {
                    let _ = self.codex.reject_server_request(request_id).await;
                    self.send("[codex] This tool did not provide a question.")
                        .await;
                    return;
                }
                if params.questions.iter().any(|question| question.is_secret) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    self.send(
                        "[codex] A secret answer was requested and cannot be sent over WhatsApp.",
                    )
                    .await;
                    return;
                }
                let ProjectionEvent::Request(presentation) = self
                    .projector
                    .apply_user_input_request(&request_id, &params)
                else {
                    unreachable!("user input projection always returns a request")
                };
                let questions = presentation
                    .questions
                    .into_iter()
                    .map(|question| PendingQuestion {
                        id: question.id,
                        question: question.question,
                        options: question.options.map(|options| {
                            options
                                .into_iter()
                                .map(|option| {
                                    codex_app_server_protocol::ToolRequestUserInputOption {
                                        label: option.label,
                                        description: option.description,
                                    }
                                })
                                .collect()
                        }),
                    })
                    .collect::<Vec<_>>();
                let persisted = PendingUserInput {
                    token: token.clone(),
                    request_id: request_id.clone(),
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    questions: questions
                        .iter()
                        .map(|question| PendingUserInputQuestion {
                            id: question.id.clone(),
                            question: question.question.clone(),
                            options: question.options.clone().map(|options| {
                                options
                                    .into_iter()
                                    .map(|option| PendingUserInputOption {
                                        label: option.label,
                                        description: option.description,
                                    })
                                    .collect()
                            }),
                        })
                        .collect(),
                    answers: BTreeMap::new(),
                    expires_at,
                };
                self.pending_requests.insert(
                    token.clone(),
                    PendingRequest::UserInput {
                        request_id: request_id.clone(),
                        questions,
                        answers: HashMap::new(),
                        expires_at,
                    },
                );
                self.state.pending_user_inputs.push(persisted);
                if self.state.save(&self.state_path).is_err() {
                    self.pending_requests.remove(&token);
                    self.state.pending_user_inputs.pop();
                    self.state_healthy = false;
                    self.refresh_readiness();
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                self.send_user_question(&token, 0).await;
            }
            ServerRequest::PermissionsRequestApproval { request_id, params } => {
                if !self.is_active_request(&params.thread_id, &params.turn_id) {
                    let _ = self.codex.reject_server_request(request_id).await;
                    return;
                }
                let presentation = permissions_presentation(&params);
                self.enqueue_approval(PendingApproval::Permissions {
                    request_id,
                    presentation,
                })
                .await;
            }
            request => {
                let _ = self.codex.reject_server_request(request.id().clone()).await;
                self.send("[codex] This app-server request is not supported by the WhatsApp bridge; it was rejected.")
                    .await;
            }
        }
    }

    async fn enqueue_approval(&mut self, approval: PendingApproval) {
        if !self.include_approval_notices
            && !matches!(&approval, PendingApproval::Permissions { .. })
        {
            let _ = self
                .codex
                .reject_server_request(approval.request_id().clone())
                .await;
            return;
        }
        let display = self.pending_approvals.is_empty();
        self.pending_approvals.push_back(approval);
        if display {
            self.send_front_approval().await;
        }
    }

    async fn send_front_approval(&mut self) {
        let Some(presentation) = self
            .pending_approvals
            .front()
            .map(PendingApproval::presentation)
            .cloned()
        else {
            return;
        };
        let mut lines = vec![format!("[codex] {}", presentation.title), String::new()];
        lines.extend(presentation.details.iter().cloned());
        if !presentation.details.is_empty() {
            lines.push(String::new());
        }
        lines.extend(
            presentation
                .choices
                .iter()
                .enumerate()
                .map(|(index, choice)| format!("{}. {}", index + 1, choice.label)),
        );
        lines.push(String::new());
        let choice_count = presentation.choices.len();
        lines.push(format!("Reply with 1-{choice_count}, or /stop."));
        self.send(&lines.join("\n")).await;
    }

    fn is_active_request(&self, thread_id: &str, turn_id: &str) -> bool {
        self.state
            .active_turn
            .as_ref()
            .is_some_and(|active| active.thread_id == thread_id && active.codex_turn_id == turn_id)
    }

    async fn resolve_approval_choice(&mut self, choice: usize) {
        let Some(approval) = self.pending_approvals.front().cloned() else {
            return;
        };
        let Some(selected) = approval
            .presentation()
            .choices
            .get(choice.saturating_sub(1))
            .cloned()
        else {
            self.send("[codex] That approval number is not available. Reply with one of the displayed numbers, or /stop.").await;
            return;
        };
        self.resolve_approval(approval, selected.decision.clone())
            .await;
    }

    async fn resolve_current_approval_for_stop(&mut self) {
        let Some(approval) = self.pending_approvals.front().cloned() else {
            return;
        };
        let decision =
            match &approval {
                PendingApproval::Command { presentation, .. } => presentation
                    .choices
                    .iter()
                    .find_map(|choice| match &choice.decision {
                        ApprovalDecision::Command(CommandExecutionApprovalDecision::Cancel) => {
                            Some(choice.decision.clone())
                        }
                        _ => None,
                    }),
                PendingApproval::FileChange { .. } => Some(ApprovalDecision::FileChange(
                    FileChangeApprovalDecision::Cancel,
                )),
                PendingApproval::Permissions { .. } => Some(ApprovalDecision::Permissions {
                    permissions: GrantedPermissionProfile::default(),
                    scope: PermissionGrantScope::Turn,
                    strict_auto_review: None,
                }),
            };
        if let Some(decision) = decision {
            self.resolve_approval(approval, decision).await;
        }
    }

    async fn resolve_approval(&mut self, approval: PendingApproval, decision: ApprovalDecision) {
        let result = match &decision {
            ApprovalDecision::Command(decision) => {
                serde_json::to_value(CommandExecutionRequestApprovalResponse {
                    decision: decision.clone(),
                })
            }
            ApprovalDecision::FileChange(decision) => {
                serde_json::to_value(FileChangeRequestApprovalResponse {
                    decision: decision.clone(),
                })
            }
            ApprovalDecision::Permissions {
                permissions,
                scope,
                strict_auto_review,
            } => serde_json::to_value(PermissionsRequestApprovalResponse {
                permissions: permissions.clone(),
                scope: *scope,
                strict_auto_review: *strict_auto_review,
            }),
        };
        let Ok(result) = result else {
            self.send("[codex] Could not encode that approval decision.")
                .await;
            return;
        };
        if self
            .codex
            .resolve_server_request(approval.request_id().clone(), result)
            .await
            .is_err()
        {
            self.send("[codex] Could not deliver that approval decision.")
                .await;
            return;
        }
        self.pending_approvals
            .retain(|pending| pending.request_id() != approval.request_id());
        self.send(&format!(
            "[codex] Selected: {}.",
            approval
                .presentation()
                .choices
                .iter()
                .find(|choice| choice.decision == decision)
                .map_or("the selected option", |choice| choice.label.as_str())
        ))
        .await;
        self.send_front_approval().await;
    }

    async fn answer_request(&mut self, token: &str, answer: String) {
        let Some(request) = self.pending_requests.get(token).cloned() else {
            self.send("[codex] That question token is unknown or expired.")
                .await;
            return;
        };
        let PendingRequest::UserInput {
            request_id,
            questions,
            mut answers,
            ..
        } = request;
        let question_index = answers.len();
        let Some(question) = questions.get(question_index) else {
            self.send("[codex] That question token is unknown or expired.")
                .await;
            return;
        };
        answers.insert(question.id.clone(), vec![answer]);
        if question_index + 1 < questions.len() {
            if let Some(PendingRequest::UserInput {
                answers: pending_answers,
                ..
            }) = self.pending_requests.get_mut(token)
            {
                *pending_answers = answers.clone();
            }
            if let Some(pending) = self
                .state
                .pending_user_inputs
                .iter_mut()
                .find(|pending| pending.token == token)
            {
                pending.answers = answers.clone().into_iter().collect();
            }
            if self.state.save(&self.state_path).is_err() {
                self.state_healthy = false;
                self.refresh_readiness();
            }
            self.send_user_question(token, question_index + 1).await;
            return;
        }
        let response = ToolRequestUserInputResponse {
            answers: answers
                .into_iter()
                .map(|(question_id, answers)| (question_id, ToolRequestUserInputAnswer { answers }))
                .collect(),
        };
        if let Ok(result) = serde_json::to_value(response) {
            if self
                .codex
                .resolve_server_request(request_id, result)
                .await
                .is_ok()
            {
                self.pending_requests.remove(token);
                self.state
                    .pending_user_inputs
                    .retain(|pending| pending.token != token);
                if self.state.save(&self.state_path).is_err() {
                    self.state_healthy = false;
                    self.refresh_readiness();
                }
                self.send("[codex] Answer sent.").await;
            } else {
                self.send("[codex] Could not deliver that answer.").await;
            }
        }
    }

    async fn send_user_question(&mut self, token: &str, question_index: usize) {
        let Some(PendingRequest::UserInput { questions, .. }) = self.pending_requests.get(token)
        else {
            return;
        };
        let Some(question) = questions.get(question_index) else {
            return;
        };
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
            "[codex] Question {}/{} ({token}): {}{}\nReply `/answer {token} <your answer>`. ",
            question_index + 1,
            questions.len(),
            question.question,
            options,
        ))
        .await;
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
            let PendingRequest::UserInput { request_id, .. } = request;
            let _ = self.codex.reject_server_request(request_id).await;
            self.state
                .pending_user_inputs
                .retain(|pending| pending.token != token);
            self.send(&format!("[codex] Question token {token} expired."))
                .await;
        }
        if self.state.save(&self.state_path).is_err() {
            self.state_healthy = false;
            self.refresh_readiness();
        }
    }

    async fn send_help(&mut self) {
        match CommandCatalog::load(&self.command_catalog_path) {
            Ok(catalog) => {
                self.command_catalog = catalog;
                let help = self.command_catalog.render_help();
                self.send(&format!("[codex] {help}")).await;
            }
            Err(error) => {
                tracing::warn!(
                    path = %self.command_catalog_path.display(),
                    %error,
                    "failed to reload WhatsApp command catalogue"
                );
                self.send(&format!(
                    "[codex] Could not load the command catalogue at `{}`: {error}.",
                    self.command_catalog_path.display()
                ))
                .await;
            }
        }
    }

    async fn send(&mut self, text: &str) {
        let message_limit = self.transport.capabilities().message_limit;
        if text.chars().count() <= message_limit {
            let _ = self.send_tracked(text).await;
            return;
        }
        for chunk in segment_text(text, message_limit) {
            let _ = self.send_tracked(&chunk).await;
        }
    }

    async fn send_tracked(&mut self, text: &str) -> Option<String> {
        if !self.state_healthy {
            return None;
        }
        let text = self.command_catalog.rewrite_legacy_prefix(text);
        let response_id = Uuid::new_v4().simple().to_string();
        if let Some(delivery) = &self.delivery {
            let intent = DeliveryIntent {
                conversation_id: ProviderConversationId::new(self.self_chat_id.clone()),
                generation: self.delivery_generation,
                key: codex_transcript::TranscriptKey::new("bridge", "notice", response_id),
                origin: codex_transcript::EntryOrigin::BridgeNotice,
                text,
                revision: 1,
                committed: true,
            };
            if delivery.try_apply(intent).is_err() {
                self.transport_healthy = false;
                self.refresh_readiness();
            }
            return None;
        }
        tracing::error!("bridge notice delivery worker is unavailable");
        self.state_healthy = false;
        self.refresh_readiness();
        None
    }

    fn recover_state_storage(&mut self) {
        if self.state.save(&self.state_path).is_ok() {
            self.state_healthy = true;
            self.refresh_readiness();
        }
    }

    fn refresh_readiness(&self) {
        self.readiness.update(BridgeReadinessSnapshot {
            ready: self.state_healthy && self.app_server_connected && self.transport_healthy,
            state_healthy: self.state_healthy,
            app_server_connected: self.app_server_connected,
            transport_healthy: self.transport_healthy,
        });
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
        let pending = if self.pending_approvals.is_empty() && self.pending_requests.is_empty() {
            "none".to_string()
        } else {
            let mut kinds = Vec::new();
            if !self.pending_approvals.is_empty() {
                kinds.push(format!("{} approval", self.pending_approvals.len()));
            }
            if !self.pending_requests.is_empty() {
                kinds.push(format!("{} user input", self.pending_requests.len()));
            }
            kinds.join(", ")
        };
        format!(
            "[codex] app-server: {}; transport: {}; state: {}; thread: {thread}; active turn: {active_turn}; queued: {}; pending: {pending}",
            if self.app_server_connected {
                "connected"
            } else {
                "disconnected"
            },
            if self.transport_healthy {
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

fn notification_turn_scope(notification: &ServerNotification) -> Option<(&str, &str)> {
    match notification {
        ServerNotification::AgentMessageDelta(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::PlanDelta(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::ReasoningSummaryTextDelta(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::ReasoningTextDelta(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::CommandExecutionOutputDelta(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::FileChangeOutputDelta(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::FileChangePatchUpdated(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::ItemStarted(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::ItemCompleted(notification) => {
            Some((&notification.thread_id, &notification.turn_id))
        }
        ServerNotification::TurnCompleted(notification) => {
            Some((&notification.thread_id, &notification.turn.id))
        }
        _ => None,
    }
}

fn notification_statuses(notification: &ServerNotification) -> (bool, bool) {
    match notification {
        ServerNotification::ReasoningSummaryTextDelta(_)
        | ServerNotification::ReasoningTextDelta(_) => (true, false),
        ServerNotification::CommandExecutionOutputDelta(_)
        | ServerNotification::FileChangeOutputDelta(_)
        | ServerNotification::FileChangePatchUpdated(_) => (false, true),
        ServerNotification::ItemStarted(notification) => (
            matches!(&notification.item, ThreadItem::Reasoning { .. }),
            item_is_tool_call(&notification.item),
        ),
        ServerNotification::ItemCompleted(notification) => (
            matches!(&notification.item, ThreadItem::Reasoning { .. }),
            item_is_tool_call(&notification.item),
        ),
        ServerNotification::TurnCompleted(notification) => (
            notification
                .turn
                .items
                .iter()
                .any(|item| matches!(item, ThreadItem::Reasoning { .. })),
            notification.turn.items.iter().any(item_is_tool_call),
        ),
        _ => (false, false),
    }
}

fn user_inputs(body: String, attachment: Option<InboundAttachment>) -> Vec<UserInput> {
    let mut inputs = Vec::new();
    match attachment {
        Some(InboundAttachment::Image {
            mime_type,
            data_base64,
        }) => inputs.push(UserInput::Image {
            detail: None,
            url: format!("data:{mime_type};base64,{data_base64}"),
        }),
        Some(InboundAttachment::Document {
            path, file_name, ..
        }) => {
            let name = file_name.as_deref().unwrap_or("attachment");
            inputs.push(UserInput::Text {
                text: format!(
                    "The user is providing `{name}` for added context.\nFile location: {}",
                    path.display()
                ),
                text_elements: Vec::new(),
            });
        }
        Some(InboundAttachment::Unsupported { .. }) | None => {}
    }
    if !body.is_empty() {
        inputs.push(UserInput::Text {
            text: body,
            text_elements: Vec::new(),
        });
    }
    inputs
}

fn parse_approval_choice(body: &str) -> Option<usize> {
    let body = body.trim();
    (!body.is_empty() && body.chars().all(|character| character.is_ascii_digit()))
        .then(|| body.parse::<usize>().ok())
        .flatten()
        .filter(|choice| *choice > 0)
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

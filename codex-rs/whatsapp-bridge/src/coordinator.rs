//! Single-owner conversation state machine for the WhatsApp bridge.

use crate::codex::CodexError;
use crate::codex::RemoteCodexClient;
use crate::commands::BridgeCommand;
use crate::commands::parse_command;
use crate::openwa::OpenWaClient;
use crate::output::OutputAggregator;
use crate::output::labelled_chunks;
use crate::state::BridgeState;
use crate::state::QueuedPrompt;
use crate::state::unix_timestamp;
use crate::webhook::InboundMessage;
use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::CommandExecutionApprovalDecision;
use codex_app_server_protocol::CommandExecutionRequestApprovalResponse;
use codex_app_server_protocol::FileChangeApprovalDecision;
use codex_app_server_protocol::FileChangeRequestApprovalResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use codex_app_server_protocol::ToolRequestUserInputAnswer;
use codex_app_server_protocol::ToolRequestUserInputResponse;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::mpsc;
use uuid::Uuid;

pub enum CoordinatorCommand {
    Inbound {
        message: InboundMessage,
        accepted: tokio::sync::oneshot::Sender<bool>,
    },
    Status(tokio::sync::oneshot::Sender<String>),
}

enum PendingRequest {
    Command {
        request_id: RequestId,
        available_decisions: Option<Vec<CommandExecutionApprovalDecision>>,
    },
    FileChange {
        request_id: RequestId,
    },
    UserInput {
        request_id: RequestId,
        question_id: String,
    },
}

pub struct Coordinator<O> {
    codex: RemoteCodexClient,
    openwa: O,
    state: BridgeState,
    state_path: PathBuf,
    workspace: PathBuf,
    openwa_session_id: String,
    self_chat_id: String,
    trigger_prefix: String,
    output_chunk_chars: usize,
    max_queued_prompts: usize,
    output: OutputAggregator,
    pending_requests: HashMap<String, PendingRequest>,
}

impl<O: OpenWaClient> Coordinator<O> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        codex: RemoteCodexClient,
        openwa: O,
        state: BridgeState,
        state_path: PathBuf,
        workspace: PathBuf,
        openwa_session_id: String,
        self_chat_id: String,
        trigger_prefix: String,
        output_chunk_chars: usize,
        max_queued_prompts: usize,
    ) -> Self {
        Self {
            codex,
            openwa,
            state,
            state_path,
            workspace,
            openwa_session_id,
            self_chat_id,
            trigger_prefix,
            output_chunk_chars,
            max_queued_prompts,
            output: OutputAggregator::default(),
            pending_requests: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut commands: mpsc::Receiver<CoordinatorCommand>) {
        loop {
            tokio::select! {
                Some(command) = commands.recv() => self.handle_command(command).await,
                event = self.codex.next_event() => match event {
                    Some(event) => self.handle_event(event).await,
                    None => break,
                },
                else => break,
            }
        }
    }

    async fn handle_command(&mut self, command: CoordinatorCommand) {
        match command {
            CoordinatorCommand::Inbound { message, accepted } => {
                let accepted_event = self.handle_inbound(message).await;
                let _ = accepted.send(accepted_event);
            }
            CoordinatorCommand::Status(reply) => {
                let _ = reply.send(self.status_message());
            }
        }
    }

    async fn handle_inbound(&mut self, message: InboundMessage) -> bool {
        if self.state.was_processed(&message.idempotency_key) {
            return true;
        }
        self.state
            .mark_processed(message.idempotency_key.clone(), unix_timestamp());
        if self.state.save(&self.state_path).is_err() {
            return false;
        }
        let Some(command) = parse_command(&self.trigger_prefix, &message.body) else {
            return true;
        };
        match command {
            BridgeCommand::Prompt(body) => {
                let prompt = QueuedPrompt {
                    idempotency_key: message.idempotency_key,
                    message_id: message.message_id,
                    body,
                    accepted_at: unix_timestamp(),
                };
                if self.state.active_turn.is_some() {
                    if self.state.queued_prompts.len() >= self.max_queued_prompts {
                        self.send("[codex] Queue is full; try again after the current turn.")
                            .await;
                    } else {
                        self.state.queued_prompts.push(prompt);
                        let _ = self.state.save(&self.state_path);
                    }
                } else {
                    self.start_prompt(prompt).await;
                }
            }
            BridgeCommand::New => match self.codex.start_thread(&self.workspace).await {
                Ok(thread_id) => {
                    self.bind_thread(thread_id);
                    self.send("[codex] Started a new thread.").await;
                }
                Err(_) => self.send("[codex] Could not start a new thread.").await,
            },
            BridgeCommand::Status => self.send(&self.status_message()).await,
            BridgeCommand::Stop => self.stop_active_turn().await,
            BridgeCommand::Help => self.send_help().await,
            BridgeCommand::Approve { token, session } => {
                self.approve_request(&token, session).await
            }
            BridgeCommand::Deny { token } => self.deny_request(&token).await,
            BridgeCommand::Answer { token, answer } => self.answer_request(&token, answer).await,
        }
        true
    }

    async fn start_prompt(&mut self, prompt: QueuedPrompt) {
        let thread_id = match self.state.binding.as_ref() {
            Some(binding) => binding.codex_thread_id.clone(),
            None => match self.codex.start_thread(&self.workspace).await {
                Ok(thread_id) => {
                    self.bind_thread(thread_id.clone());
                    thread_id
                }
                Err(_) => {
                    self.send("[codex] Codex app-server is unavailable.").await;
                    self.state.queued_prompts.insert(0, prompt);
                    let _ = self.state.save(&self.state_path);
                    return;
                }
            },
        };
        match self
            .codex
            .start_turn(thread_id, prompt.message_id.clone(), prompt.body)
            .await
        {
            Ok(turn_id) => {
                self.state.active_turn = Some(crate::state::ActiveTurn {
                    inbound_message_id: prompt.message_id,
                    codex_turn_id: turn_id,
                    working_output_message_id: None,
                });
                let _ = self.state.save(&self.state_path);
                self.send("[codex] Working…").await;
            }
            Err(_) => self.send("[codex] Could not start that turn.").await,
        }
    }

    fn bind_thread(&mut self, thread_id: String) {
        self.state.binding = Some(crate::state::ThreadBinding {
            openwa_session_id: self.openwa_session_id.clone(),
            self_chat_id: self.self_chat_id.clone(),
            codex_thread_id: thread_id,
            workspace: self.workspace.clone(),
        });
        let _ = self.state.save(&self.state_path);
    }

    async fn stop_active_turn(&mut self) {
        let (Some(binding), Some(active_turn)) = (&self.state.binding, &self.state.active_turn)
        else {
            self.send("[codex] No turn is running.").await;
            return;
        };
        match self
            .codex
            .interrupt_turn(
                binding.codex_thread_id.clone(),
                active_turn.codex_turn_id.clone(),
            )
            .await
        {
            Ok(()) => self.send("[codex] Interrupt requested.").await,
            Err(CodexError::Transport) => self.send("[codex] Could not interrupt the turn.").await,
        }
    }

    async fn handle_event(&mut self, event: AppServerEvent) {
        let AppServerEvent::ServerNotification(notification) = event else {
            if let AppServerEvent::ServerRequest(request) = event {
                self.handle_server_request(request).await;
            }
            return;
        };
        match notification {
            ServerNotification::AgentMessageDelta(delta) => {
                self.output
                    .push_delta(delta.thread_id, delta.turn_id, delta.item_id, &delta.delta)
            }
            ServerNotification::TurnCompleted(completed) => {
                let output = self
                    .output
                    .finish_turn(&completed.thread_id, &completed.turn.id);
                if output.is_empty() {
                    self.send("[codex] Turn completed.").await;
                } else {
                    for chunk in labelled_chunks(&output, self.output_chunk_chars) {
                        self.send(&chunk).await;
                    }
                }
                self.state.active_turn = None;
                self.pending_requests.clear();
                let _ = self.state.save(&self.state_path);
                if let Some(next) = self.state.queued_prompts.first().cloned() {
                    self.state.queued_prompts.remove(0);
                    self.start_prompt(next).await;
                }
            }
            _ => {}
        }
    }

    async fn handle_server_request(&mut self, request: ServerRequest) {
        let token = Uuid::new_v4().simple().to_string()[..8].to_string();
        match request {
            ServerRequest::CommandExecutionRequestApproval { request_id, params } => {
                let command = params
                    .command
                    .unwrap_or_else(|| "an unknown command".to_string());
                let reason = params.reason.unwrap_or_default();
                self.pending_requests.insert(
                    token.clone(),
                    PendingRequest::Command {
                        request_id,
                        available_decisions: params.available_decisions,
                    },
                );
                self.send(&format!(
                    "[codex] Approval {token}: run `{command}`{}\nReply `{}approve {token}` or `{}deny {token}`.",
                    if reason.is_empty() { String::new() } else { format!(" ({reason})") },
                    self.trigger_prefix,
                    self.trigger_prefix,
                )).await;
            }
            ServerRequest::FileChangeRequestApproval { request_id, params } => {
                let reason = params.reason.unwrap_or_else(|| "file changes".to_string());
                self.pending_requests
                    .insert(token.clone(), PendingRequest::FileChange { request_id });
                self.send(&format!(
                    "[codex] Approval {token}: allow {reason}.\nReply `{}approve {token}` or `{}deny {token}`.",
                    self.trigger_prefix, self.trigger_prefix,
                )).await;
            }
            ServerRequest::ToolRequestUserInput { request_id, params } => {
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
                    },
                );
                self.send(&format!(
                    "[codex] Question {token}: {}\nReply `{}answer {token} <your answer>`.",
                    question.question, self.trigger_prefix,
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

    async fn approve_request(&mut self, token: &str, session: bool) {
        let Some(request) = self.pending_requests.remove(token) else {
            self.send("[codex] That approval token is unknown or expired.")
                .await;
            return;
        };
        let result = match request {
            PendingRequest::Command {
                request_id,
                available_decisions,
            } => {
                let decision = if session
                    && available_decisions.as_ref().is_some_and(|decisions| {
                        decisions.contains(&CommandExecutionApprovalDecision::AcceptForSession)
                    }) {
                    CommandExecutionApprovalDecision::AcceptForSession
                } else {
                    CommandExecutionApprovalDecision::Accept
                };
                serde_json::to_value(CommandExecutionRequestApprovalResponse { decision })
                    .map(|result| (request_id, result))
            }
            PendingRequest::FileChange { request_id } => {
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
        };
        if let Ok((request_id, result)) = result {
            if self
                .codex
                .resolve_server_request(request_id, result)
                .await
                .is_ok()
            {
                self.send("[codex] Approved.").await;
            } else {
                self.send("[codex] Could not deliver that approval.").await;
            }
        }
    }

    async fn deny_request(&mut self, token: &str) {
        let Some(request) = self.pending_requests.remove(token) else {
            self.send("[codex] That approval token is unknown or expired.")
                .await;
            return;
        };
        let result = match request {
            PendingRequest::Command { request_id, .. } => {
                serde_json::to_value(CommandExecutionRequestApprovalResponse {
                    decision: CommandExecutionApprovalDecision::Decline,
                })
                .map(|result| (request_id, result))
            }
            PendingRequest::FileChange { request_id } => {
                serde_json::to_value(FileChangeRequestApprovalResponse {
                    decision: FileChangeApprovalDecision::Decline,
                })
                .map(|result| (request_id, result))
            }
            PendingRequest::UserInput { .. } => {
                self.send("[codex] Use `answer` for that token.").await;
                return;
            }
        };
        if let Ok((request_id, result)) = result {
            if self
                .codex
                .resolve_server_request(request_id, result)
                .await
                .is_ok()
            {
                self.send("[codex] Denied.").await;
            } else {
                self.send("[codex] Could not deliver that decision.").await;
            }
        }
    }

    async fn answer_request(&mut self, token: &str, answer: String) {
        let Some(request) = self.pending_requests.remove(token) else {
            self.send("[codex] That question token is unknown or expired.")
                .await;
            return;
        };
        let PendingRequest::UserInput {
            request_id,
            question_id,
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
                self.send("[codex] Answer sent.").await;
            } else {
                self.send("[codex] Could not deliver that answer.").await;
            }
        }
    }

    async fn send_help(&self) {
        self.send(&format!(
            "[codex] Commands: `{0}new`, `{0}status`, `{0}stop`, `{0}approve <token>`, `{0}approve-session <token>`, `{0}deny <token>`, and `{0}answer <token> <answer>`. Any other message beginning with `{0}` starts a Codex turn.",
            self.trigger_prefix,
        )).await;
    }

    async fn send(&self, text: &str) {
        let _ = self
            .openwa
            .send_text(self.self_chat_id.clone(), text.to_string())
            .await;
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
        format!(
            "[codex] thread: {thread}; active turn: {active_turn}; queued: {}; workspace: {}",
            self.state.queued_prompts.len(),
            self.workspace.display()
        )
    }
}

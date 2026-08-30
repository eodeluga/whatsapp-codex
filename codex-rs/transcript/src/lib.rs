//! Transport-neutral semantic projection of a Codex conversation.
//!
//! This crate deliberately stores protocol items instead of rendering provider
//! text. Consumers such as the TUI and remote providers can apply their own
//! layout while sharing item identity, ordering, and lifecycle semantics.

mod model;

use codex_app_server_protocol::CommandExecutionOutputDeltaNotification;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::ErrorNotification;
use codex_app_server_protocol::FileChangeOutputDeltaNotification;
use codex_app_server_protocol::FileChangePatchUpdatedNotification;
use codex_app_server_protocol::GuardianWarningNotification;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::ReasoningSummaryTextDeltaNotification;
use codex_app_server_protocol::ReasoningTextDeltaNotification;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ToolRequestUserInputParams;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::WarningNotification;
use std::collections::HashMap;

pub use model::EntryOrigin;
pub use model::ProjectionEvent;
pub use model::TranscriptEntry;
pub use model::TranscriptKey;
pub use model::TranscriptNotice;
pub use model::UserInputOptionPresentation;
pub use model::UserInputPresentation;
pub use model::UserInputQuestionPresentation;

/// Item-keyed, append-ordered projection of a Codex thread.
#[derive(Debug, Default)]
pub struct TranscriptProjector {
    entries: Vec<TranscriptEntry>,
    entry_indexes: HashMap<TranscriptKey, usize>,
    user_input_requests: HashMap<TranscriptKey, UserInputPresentation>,
    next_revision: u64,
}

impl TranscriptProjector {
    /// Applies one app-server notification and returns only newly-created or
    /// changed user-visible projection events.
    pub fn apply(&mut self, notification: ServerNotification) -> Vec<ProjectionEvent> {
        match notification {
            ServerNotification::ItemStarted(notification) => {
                self.apply_item_started(notification).into_iter().collect()
            }
            ServerNotification::ItemCompleted(notification) => self
                .apply_item_completed(notification)
                .into_iter()
                .collect(),
            ServerNotification::AgentMessageDelta(notification) => self
                .apply_text_delta(
                    &notification.thread_id,
                    &notification.turn_id,
                    &notification.item_id,
                    &notification.delta,
                    StreamedItem::AgentMessage,
                )
                .into_iter()
                .collect(),
            ServerNotification::PlanDelta(notification) => self
                .apply_text_delta(
                    &notification.thread_id,
                    &notification.turn_id,
                    &notification.item_id,
                    &notification.delta,
                    StreamedItem::Plan,
                )
                .into_iter()
                .collect(),
            ServerNotification::ReasoningSummaryTextDelta(notification) => self
                .apply_reasoning_delta(notification, ReasoningPart::Summary)
                .into_iter()
                .collect(),
            ServerNotification::ReasoningTextDelta(notification) => self
                .apply_reasoning_delta(notification, ReasoningPart::Content)
                .into_iter()
                .collect(),
            ServerNotification::CommandExecutionOutputDelta(notification) => self
                .apply_command_output_delta(notification)
                .into_iter()
                .collect(),
            ServerNotification::FileChangeOutputDelta(notification) => self
                .apply_file_change_output_delta(notification)
                .into_iter()
                .collect(),
            ServerNotification::FileChangePatchUpdated(notification) => self
                .apply_file_change_patch_updated(notification)
                .into_iter()
                .collect(),
            ServerNotification::TurnCompleted(notification) => {
                self.apply_turn_completed(notification)
            }
            ServerNotification::Error(notification) => self.apply_error(notification),
            ServerNotification::Warning(notification) => self.apply_warning(notification),
            ServerNotification::GuardianWarning(notification) => {
                self.apply_guardian_warning(notification)
            }
            ServerNotification::ConfigWarning(notification) => {
                self.apply_config_warning(notification)
            }
            ServerNotification::DeprecationNotice(notification) => {
                let text = match notification.details {
                    Some(details) => format!("{}\n{details}", notification.summary),
                    None => notification.summary,
                };
                vec![ProjectionEvent::Notice(TranscriptNotice {
                    origin: EntryOrigin::CodexTranscript,
                    thread_id: String::new(),
                    turn_id: String::new(),
                    text,
                })]
            }
            // This is an explicit output allowlist. New protocol notifications
            // land here until they are deliberately classified above; they are
            // never serialized as a provider message.
            _ => {
                tracing::debug!("suppressed non-transcript app-server notification");
                vec![ProjectionEvent::Suppressed]
            }
        }
    }

    pub fn entries(&self) -> &[TranscriptEntry] {
        &self.entries
    }

    /// Registers an interactive request using the shared semantic shape.
    /// Provider surfaces own reply tokens, persistence, and input controls.
    pub fn apply_user_input_request(
        &mut self,
        request_id: &RequestId,
        params: &ToolRequestUserInputParams,
    ) -> ProjectionEvent {
        let presentation = UserInputPresentation::from_request(request_id, params);
        self.user_input_requests
            .insert(presentation.key.clone(), presentation.clone());
        ProjectionEvent::Request(Box::new(presentation))
    }

    /// Reconciles a resumed turn from its authoritative item list.
    pub fn reconcile_items(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        items: &[ThreadItem],
    ) -> Vec<ProjectionEvent> {
        items
            .iter()
            .cloned()
            .filter_map(|item| {
                self.upsert(
                    TranscriptKey::new(thread_id, turn_id, item.id().to_owned()),
                    item,
                    true,
                )
            })
            .collect()
    }

    fn apply_item_started(
        &mut self,
        notification: ItemStartedNotification,
    ) -> Option<ProjectionEvent> {
        self.upsert(
            TranscriptKey::new(
                notification.thread_id,
                notification.turn_id,
                notification.item.id().to_owned(),
            ),
            notification.item,
            false,
        )
    }

    fn apply_item_completed(
        &mut self,
        notification: ItemCompletedNotification,
    ) -> Option<ProjectionEvent> {
        self.upsert(
            TranscriptKey::new(
                notification.thread_id,
                notification.turn_id,
                notification.item.id().to_owned(),
            ),
            notification.item,
            true,
        )
    }

    fn apply_turn_completed(
        &mut self,
        notification: TurnCompletedNotification,
    ) -> Vec<ProjectionEvent> {
        notification
            .turn
            .items
            .into_iter()
            .filter_map(|item| {
                self.upsert(
                    TranscriptKey::new(
                        notification.thread_id.clone(),
                        notification.turn.id.clone(),
                        item.id().to_owned(),
                    ),
                    item,
                    true,
                )
            })
            .collect()
    }

    fn apply_text_delta(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        delta: &str,
        item_kind: StreamedItem,
    ) -> Option<ProjectionEvent> {
        let key = TranscriptKey::new(thread_id, turn_id, item_id);
        if self
            .entry_indexes
            .get(&key)
            .and_then(|index| self.entries.get(*index))
            .is_some_and(|entry| entry.committed)
        {
            return None;
        }
        let item = match self.entry_indexes.get(&key).copied() {
            Some(index) => match (&self.entries[index].item, item_kind) {
                (ThreadItem::AgentMessage { text, phase, .. }, StreamedItem::AgentMessage) => {
                    ThreadItem::AgentMessage {
                        id: item_id.to_owned(),
                        text: format!("{text}{delta}"),
                        phase: phase.clone(),
                        memory_citation: None,
                    }
                }
                (ThreadItem::Plan { text, .. }, StreamedItem::Plan) => ThreadItem::Plan {
                    id: item_id.to_owned(),
                    text: format!("{text}{delta}"),
                },
                _ => return None,
            },
            None => match item_kind {
                StreamedItem::AgentMessage => ThreadItem::AgentMessage {
                    id: item_id.to_owned(),
                    text: delta.to_owned(),
                    phase: None,
                    memory_citation: None,
                },
                StreamedItem::Plan => ThreadItem::Plan {
                    id: item_id.to_owned(),
                    text: delta.to_owned(),
                },
            },
        };
        self.upsert(key, item, false)
    }

    fn apply_reasoning_delta(
        &mut self,
        notification: impl ReasoningDelta,
        part: ReasoningPart,
    ) -> Option<ProjectionEvent> {
        let key = TranscriptKey::new(
            notification.thread_id(),
            notification.turn_id(),
            notification.item_id(),
        );
        let (mut summary, mut content) = self
            .entry_indexes
            .get(&key)
            .and_then(|index| self.entries.get(*index))
            .and_then(|entry| match &entry.item {
                ThreadItem::Reasoning {
                    summary, content, ..
                } => Some((summary.clone(), content.clone())),
                _ => None,
            })
            .unwrap_or_default();
        let target = match part {
            ReasoningPart::Summary => &mut summary,
            ReasoningPart::Content => &mut content,
        };
        let index = notification.part_index();
        while target.len() <= index {
            target.push(String::new());
        }
        target[index].push_str(notification.delta());
        self.upsert(
            key,
            ThreadItem::Reasoning {
                id: notification.item_id().to_owned(),
                summary,
                content,
            },
            false,
        )
    }

    fn apply_command_output_delta(
        &mut self,
        notification: CommandExecutionOutputDeltaNotification,
    ) -> Option<ProjectionEvent> {
        let key = TranscriptKey::new(
            &notification.thread_id,
            &notification.turn_id,
            &notification.item_id,
        );
        let index = self.entry_indexes.get(&key).copied()?;
        let ThreadItem::CommandExecution {
            id,
            plugin_id,
            script_path,
            command,
            cwd,
            process_id,
            source,
            status,
            command_actions,
            aggregated_output,
            exit_code,
            duration_ms,
        } = &self.entries[index].item
        else {
            return None;
        };
        let mut output = aggregated_output.clone().unwrap_or_default();
        output.push_str(&notification.delta);
        self.upsert(
            key,
            ThreadItem::CommandExecution {
                id: id.clone(),
                plugin_id: plugin_id.clone(),
                script_path: script_path.clone(),
                command: command.clone(),
                cwd: cwd.clone(),
                process_id: process_id.clone(),
                source: *source,
                status: status.clone(),
                command_actions: command_actions.clone(),
                aggregated_output: Some(output),
                exit_code: *exit_code,
                duration_ms: *duration_ms,
            },
            false,
        )
    }

    fn apply_file_change_output_delta(
        &mut self,
        notification: FileChangeOutputDeltaNotification,
    ) -> Option<ProjectionEvent> {
        tracing::debug!("suppressed deprecated file-change output notification");
        let _ = notification;
        Some(ProjectionEvent::Suppressed)
    }

    fn apply_file_change_patch_updated(
        &mut self,
        notification: FileChangePatchUpdatedNotification,
    ) -> Option<ProjectionEvent> {
        let key = TranscriptKey::new(
            &notification.thread_id,
            &notification.turn_id,
            &notification.item_id,
        );
        let index = self.entry_indexes.get(&key).copied()?;
        let ThreadItem::FileChange { id, status, .. } = &self.entries[index].item else {
            return None;
        };
        self.upsert(
            key,
            ThreadItem::FileChange {
                id: id.clone(),
                changes: notification.changes,
                status: status.clone(),
            },
            false,
        )
    }

    fn apply_error(&self, notification: ErrorNotification) -> Vec<ProjectionEvent> {
        if notification.will_retry {
            tracing::debug!("suppressed retryable app-server error");
            return vec![ProjectionEvent::Suppressed];
        }
        vec![ProjectionEvent::Notice(TranscriptNotice {
            origin: EntryOrigin::CodexTranscript,
            thread_id: notification.thread_id,
            turn_id: notification.turn_id,
            text: notification.error.message,
        })]
    }

    fn apply_warning(&self, notification: WarningNotification) -> Vec<ProjectionEvent> {
        vec![ProjectionEvent::Notice(TranscriptNotice {
            origin: EntryOrigin::CodexTranscript,
            thread_id: notification.thread_id.unwrap_or_default(),
            turn_id: String::new(),
            text: notification.message,
        })]
    }

    fn apply_guardian_warning(
        &self,
        notification: GuardianWarningNotification,
    ) -> Vec<ProjectionEvent> {
        vec![ProjectionEvent::Notice(TranscriptNotice {
            origin: EntryOrigin::CodexTranscript,
            thread_id: notification.thread_id,
            turn_id: String::new(),
            text: notification.message,
        })]
    }

    fn apply_config_warning(
        &self,
        notification: ConfigWarningNotification,
    ) -> Vec<ProjectionEvent> {
        let text = match notification.details {
            Some(details) => format!("{}: {details}", notification.summary),
            None => notification.summary,
        };
        vec![ProjectionEvent::Notice(TranscriptNotice {
            origin: EntryOrigin::CodexTranscript,
            thread_id: String::new(),
            turn_id: String::new(),
            text,
        })]
    }

    fn upsert(
        &mut self,
        key: TranscriptKey,
        item: ThreadItem,
        committed: bool,
    ) -> Option<ProjectionEvent> {
        if let Some(index) = self.entry_indexes.get(&key).copied() {
            let entry = &mut self.entries[index];
            if entry.committed && !committed {
                return None;
            }
            if entry.item == item && (!committed || entry.committed) {
                return None;
            }
            self.next_revision += 1;
            entry.item = item;
            entry.committed |= committed;
            entry.revision = self.next_revision;
            return Some(ProjectionEvent::Entry(Box::new(entry.clone())));
        }
        self.next_revision += 1;
        let entry = TranscriptEntry {
            key: key.clone(),
            item,
            origin: EntryOrigin::CodexTranscript,
            revision: self.next_revision,
            committed,
        };
        self.entry_indexes.insert(key, self.entries.len());
        self.entries.push(entry.clone());
        Some(ProjectionEvent::Entry(Box::new(entry)))
    }
}

#[derive(Clone, Copy)]
enum StreamedItem {
    AgentMessage,
    Plan,
}

#[derive(Clone, Copy)]
enum ReasoningPart {
    Summary,
    Content,
}

trait ReasoningDelta {
    fn thread_id(&self) -> &str;
    fn turn_id(&self) -> &str;
    fn item_id(&self) -> &str;
    fn delta(&self) -> &str;
    fn part_index(&self) -> usize;
}

impl ReasoningDelta for ReasoningSummaryTextDeltaNotification {
    fn thread_id(&self) -> &str {
        &self.thread_id
    }
    fn turn_id(&self) -> &str {
        &self.turn_id
    }
    fn item_id(&self) -> &str {
        &self.item_id
    }
    fn delta(&self) -> &str {
        &self.delta
    }
    fn part_index(&self) -> usize {
        self.summary_index as usize
    }
}

impl ReasoningDelta for ReasoningTextDeltaNotification {
    fn thread_id(&self) -> &str {
        &self.thread_id
    }
    fn turn_id(&self) -> &str {
        &self.turn_id
    }
    fn item_id(&self) -> &str {
        &self.item_id
    }
    fn delta(&self) -> &str {
        &self.delta
    }
    fn part_index(&self) -> usize {
        self.content_index as usize
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ToolRequestUserInputParams;
use serde::Deserialize;
use serde::Serialize;

/// Stable identity for one item in one Codex turn.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptKey {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
}

impl TranscriptKey {
    pub fn new(
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        item_id: impl Into<String>,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            item_id: item_id.into(),
        }
    }
}

/// Origin classification used by downstream delivery policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryOrigin {
    /// A user-visible item or warning authored by Codex.
    CodexTranscript,
    /// A serious bridge condition explicitly approved for provider display.
    BridgeNotice,
    /// Diagnostics and operational state that must never reach a provider.
    Internal,
}

/// One ordered semantic transcript entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptEntry {
    pub key: TranscriptKey,
    pub item: ThreadItem,
    pub origin: EntryOrigin,
    pub revision: u64,
    pub committed: bool,
}

impl TranscriptEntry {
    /// Returns the plain text counterpart used by non-styled surfaces.
    ///
    /// The TUI may add styling or richer controls around these same semantic
    /// items. Remote providers use this bounded textual representation and
    /// must not serialize the protocol item as a fallback.
    pub fn plain_text(&self) -> Option<String> {
        match &self.item {
            ThreadItem::AgentMessage { text, .. } | ThreadItem::Plan { text, .. } => {
                non_empty(text.clone())
            }
            ThreadItem::Reasoning {
                summary, content, ..
            } => non_empty(if summary.is_empty() {
                content.join("\n\n")
            } else {
                summary.join("\n\n")
            }),
            ThreadItem::HookPrompt { fragments, .. } => non_empty(
                fragments
                    .iter()
                    .map(|fragment| fragment.text.as_str())
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            ThreadItem::CommandExecution {
                command,
                status,
                aggregated_output,
                exit_code,
                ..
            } => {
                let mut text = format!("$ {command}\nstatus: {status:?}");
                if let Some(exit_code) = exit_code {
                    text.push_str(&format!(" · exit {exit_code}"));
                }
                if let Some(output) = aggregated_output.as_deref()
                    && !output.trim().is_empty()
                {
                    text.push('\n');
                    text.push_str(output.trim_end());
                }
                Some(text)
            }
            ThreadItem::FileChange {
                changes, status, ..
            } => Some(format!(
                "file changes: {status:?} · {} changes",
                changes.len()
            )),
            ThreadItem::McpToolCall {
                server,
                tool,
                status,
                ..
            } => Some(format!("mcp tool: {server}/{tool} · {status:?}")),
            ThreadItem::DynamicToolCall {
                namespace,
                tool,
                status,
                ..
            } => Some(format!(
                "tool: {} · {status:?}",
                namespace
                    .as_ref()
                    .map_or_else(|| tool.clone(), |namespace| format!("{namespace}/{tool}"))
            )),
            ThreadItem::CollabAgentToolCall { tool, status, .. } => {
                Some(format!("agent tool: {tool:?} · {status:?}"))
            }
            ThreadItem::SubAgentActivity {
                kind, agent_path, ..
            } => Some(format!("sub-agent: {kind:?} · {agent_path}")),
            ThreadItem::WebSearch(item) => Some(format!("web search: {}", item.query)),
            ThreadItem::ImageView { path, .. } => Some(format!("image: {path:?}")),
            ThreadItem::ImageGeneration(item) => Some(format!("image generation: {}", item.status)),
            ThreadItem::EnteredReviewMode { review, .. } => {
                Some(format!("review started: {review}"))
            }
            ThreadItem::ExitedReviewMode { review, .. } => {
                Some(format!("review finished: {review}"))
            }
            ThreadItem::ContextCompaction { .. } => Some("context compacted".to_string()),
            ThreadItem::UserMessage { .. } | ThreadItem::Sleep(_) => None,
        }
    }
}

fn non_empty(text: String) -> Option<String> {
    (!text.trim().is_empty()).then_some(text)
}

/// A user-visible notification which is not represented by a `ThreadItem`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptNotice {
    pub key: TranscriptKey,
    pub origin: EntryOrigin,
    pub thread_id: String,
    pub turn_id: String,
    pub text: String,
}

/// Provider-neutral semantic presentation for a `request_user_input` request.
/// Consumers choose how to collect answers and expose reply controls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInputPresentation {
    pub key: TranscriptKey,
    pub request_id: String,
    pub questions: Vec<UserInputQuestionPresentation>,
}

impl UserInputPresentation {
    pub fn from_request(request_id: &RequestId, params: &ToolRequestUserInputParams) -> Self {
        Self {
            key: TranscriptKey::new(&params.thread_id, &params.turn_id, &params.item_id),
            request_id: request_id.to_string(),
            questions: params
                .questions
                .iter()
                .map(UserInputQuestionPresentation::from_protocol)
                .collect(),
        }
    }
}

/// One question in a shared user-input presentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInputQuestionPresentation {
    pub id: String,
    pub header: String,
    pub question: String,
    pub is_other: bool,
    pub is_secret: bool,
    pub options: Option<Vec<UserInputOptionPresentation>>,
}

impl UserInputQuestionPresentation {
    fn from_protocol(question: &codex_app_server_protocol::ToolRequestUserInputQuestion) -> Self {
        Self {
            id: question.id.clone(),
            header: question.header.clone(),
            question: question.question.clone(),
            is_other: question.is_other,
            is_secret: question.is_secret,
            options: question.options.as_ref().map(|options| {
                options
                    .iter()
                    .map(|option| UserInputOptionPresentation {
                        label: option.label.clone(),
                        description: option.description.clone(),
                    })
                    .collect()
            }),
        }
    }
}

/// A selectable option in a shared user-input presentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInputOptionPresentation {
    pub label: String,
    pub description: String,
}

/// The result of classifying an app-server notification.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionEvent {
    Entry(Box<TranscriptEntry>),
    Notice(TranscriptNotice),
    Request(Box<UserInputPresentation>),
    /// The event is deliberately internal-only. It lets callers count and
    /// trace suppressed protocol additions without serializing them.
    Suppressed,
}

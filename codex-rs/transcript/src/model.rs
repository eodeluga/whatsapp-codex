use codex_app_server_protocol::ThreadItem;

/// Stable identity for one item in one Codex turn.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryOrigin {
    /// A user-visible item or warning authored by Codex.
    CodexTranscript,
    /// A serious bridge condition explicitly approved for provider display.
    BridgeNotice,
    /// Diagnostics and operational state that must never reach a provider.
    Internal,
}

/// One ordered semantic transcript entry.
#[derive(Debug, Clone, PartialEq)]
pub struct TranscriptEntry {
    pub key: TranscriptKey,
    pub item: ThreadItem,
    pub origin: EntryOrigin,
    pub revision: u64,
    pub committed: bool,
}

/// A user-visible notification which is not represented by a `ThreadItem`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptNotice {
    pub origin: EntryOrigin,
    pub thread_id: String,
    pub turn_id: String,
    pub text: String,
}

/// The result of classifying an app-server notification.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionEvent {
    Entry(Box<TranscriptEntry>),
    Notice(TranscriptNotice),
    /// The event is deliberately internal-only. It lets callers count and
    /// trace suppressed protocol additions without serializing them.
    Suppressed,
}

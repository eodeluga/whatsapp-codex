use crate::DeliveryIntent;
use crate::ProviderAdapter;
use crate::ProviderError;
use crate::ProviderMessageId;
use crate::ProviderStatus;
use crate::segment_text;
use codex_transcript::EntryOrigin;
use codex_transcript::TranscriptKey;
use serde::Deserialize;
use serde::Serialize;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;

/// Durable state for one provider segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryRecord {
    pub conversation_id: crate::ProviderConversationId,
    #[serde(default)]
    pub generation: u64,
    pub key: TranscriptKey,
    pub origin: EntryOrigin,
    pub segment: usize,
    pub text: String,
    pub revision: u64,
    pub committed: bool,
    pub state: DeliveryState,
    pub provider_message_id: Option<ProviderMessageId>,
    pub attempts: u32,
    #[serde(default)]
    pub coalesced_revisions: u64,
}

/// Delivery state persisted for a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryState {
    Pending,
    Sent,
    Superseded,
}

/// A bounded, serializable delivery journal.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryJournal {
    pub records: Vec<DeliveryRecord>,
}

/// JSON-backed journal storage for a provider-independent delivery queue.
#[derive(Clone, Debug)]
pub struct FileDeliveryStore {
    path: PathBuf,
}

impl FileDeliveryStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl DeliveryStore for FileDeliveryStore {
    async fn load(&mut self) -> Result<DeliveryJournal, String> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| error.to_string()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(DeliveryJournal::default())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    async fn save(&mut self, journal: &DeliveryJournal) -> Result<(), String> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| "delivery journal path has no parent".to_string())?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| error.to_string())?;
        let bytes = serde_json::to_vec_pretty(journal).map_err(|error| error.to_string())?;
        let temporary = self.path.with_extension("json.tmp");
        tokio::fs::write(&temporary, bytes)
            .await
            .map_err(|error| error.to_string())?;
        set_private_permissions(&temporary)
            .await
            .map_err(|error| error.to_string())?;
        tokio::fs::rename(&temporary, &self.path)
            .await
            .map_err(|error| error.to_string())
    }
}

async fn set_private_permissions(path: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        tokio::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600)).await
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

impl DeliveryJournal {
    pub fn apply(&mut self, intent: DeliveryIntent, message_limit: usize) -> usize {
        let segments = segment_text(&intent.text, message_limit);
        let segment_count = segments.len();
        let mut changed = 0;
        for (segment, text) in segments.into_iter().enumerate() {
            let existing = self.records.iter_mut().find(|record| {
                record.key == intent.key
                    && record.segment == segment
                    && record.conversation_id == intent.conversation_id
            });
            match existing {
                Some(record) if intent.revision < record.revision => {}
                Some(record) => {
                    if record.text != text
                        || record.revision != intent.revision
                        || record.committed != intent.committed
                    {
                        record.text = text;
                        record.revision = intent.revision;
                        record.committed = intent.committed;
                        record.generation = intent.generation;
                        if record.state == DeliveryState::Superseded {
                            record.state = DeliveryState::Pending;
                        }
                        if record.provider_message_id.is_some() {
                            record.state = DeliveryState::Pending;
                            record.coalesced_revisions =
                                record.coalesced_revisions.saturating_add(1);
                        }
                        changed += 1;
                    }
                }
                None => {
                    self.records.push(DeliveryRecord {
                        conversation_id: intent.conversation_id.clone(),
                        generation: intent.generation,
                        key: intent.key.clone(),
                        origin: intent.origin,
                        segment,
                        text,
                        revision: intent.revision,
                        committed: intent.committed,
                        state: DeliveryState::Pending,
                        provider_message_id: None,
                        attempts: 0,
                        coalesced_revisions: 0,
                    });
                    changed += 1;
                }
            }
        }
        for record in &mut self.records {
            if record.key == intent.key
                && record.conversation_id == intent.conversation_id
                && record.segment >= segment_count
                && record.revision < intent.revision
                && record.state == DeliveryState::Pending
            {
                record.state = DeliveryState::Superseded;
                changed += 1;
            }
        }
        changed
    }

    fn next_pending_index(&self) -> Option<usize> {
        for (index, record) in self.records.iter().enumerate() {
            if record.state != DeliveryState::Pending {
                continue;
            }
            return Some(index);
        }
        None
    }
}

/// Persistence boundary for the delivery journal.
pub trait DeliveryStore: Send {
    fn load(&mut self) -> impl Future<Output = Result<DeliveryJournal, String>> + Send;
    fn save(
        &mut self,
        journal: &DeliveryJournal,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Commands accepted by the independent delivery worker.
#[derive(Debug)]
pub enum DeliveryWorkerCommand {
    Apply(DeliveryIntent),
    Shutdown,
}

/// Observable worker events contain no message body or credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryWorkerEvent {
    Status(ProviderStatus),
    Enqueued {
        key: TranscriptKey,
        queue_depth: usize,
    },
    Sent {
        key: TranscriptKey,
        segment: usize,
    },
    Edited {
        key: TranscriptKey,
        segment: usize,
        revisions: u64,
    },
    Failed {
        key: TranscriptKey,
        segment: usize,
        error: ProviderError,
    },
    StoreFailed,
}

/// Sender for the bounded delivery worker queue.
#[derive(Clone, Debug)]
pub struct DeliveryWorkerHandle {
    sender: mpsc::Sender<DeliveryWorkerCommand>,
}

impl DeliveryWorkerHandle {
    pub fn try_apply(&self, intent: DeliveryIntent) -> Result<(), DeliveryIntent> {
        self.sender
            .try_send(DeliveryWorkerCommand::Apply(intent))
            .map_err(|error| match error.into_inner() {
                DeliveryWorkerCommand::Apply(intent) => intent,
                DeliveryWorkerCommand::Shutdown => unreachable!("shutdown is never returned"),
            })
    }

    pub async fn apply(&self, intent: DeliveryIntent) -> Result<(), DeliveryIntent> {
        self.sender
            .send(DeliveryWorkerCommand::Apply(intent))
            .await
            .map_err(|error| match error.0 {
                DeliveryWorkerCommand::Apply(intent) => intent,
                DeliveryWorkerCommand::Shutdown => unreachable!("shutdown is never returned"),
            })
    }

    pub async fn shutdown(&self) {
        let _ = self.sender.send(DeliveryWorkerCommand::Shutdown).await;
    }
}

/// Delivery worker that is the only component allowed to call a provider.
pub struct DeliveryWorker<A, S> {
    adapter: A,
    store: S,
    journal: DeliveryJournal,
    events: mpsc::Sender<DeliveryWorkerEvent>,
    retry_delay: Duration,
}

impl<A, S> DeliveryWorker<A, S>
where
    A: ProviderAdapter,
    S: DeliveryStore,
{
    pub async fn new(
        adapter: A,
        mut store: S,
        events: mpsc::Sender<DeliveryWorkerEvent>,
        retry_delay: Duration,
    ) -> Result<Self, String> {
        let journal = store.load().await?;
        Ok(Self {
            adapter,
            store,
            journal,
            events,
            retry_delay,
        })
    }

    pub fn channel(
        capacity: usize,
    ) -> (DeliveryWorkerHandle, mpsc::Receiver<DeliveryWorkerCommand>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (DeliveryWorkerHandle { sender }, receiver)
    }

    pub async fn run(mut self, mut commands: mpsc::Receiver<DeliveryWorkerCommand>) {
        let retry_delay = self.retry_delay.max(Duration::from_millis(1));
        let mut retry = tokio::time::interval(retry_delay);
        let status_interval = Duration::from_secs(5);
        let mut status_check = tokio::time::interval_at(
            tokio::time::Instant::now() + status_interval,
            status_interval,
        );
        loop {
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    match command {
                        DeliveryWorkerCommand::Apply(intent) => {
                            if intent.origin == EntryOrigin::Internal {
                                tracing::debug!(key = ?intent.key, "suppressed internal delivery intent");
                                continue;
                            }
                            let key = intent.key.clone();
                            let capabilities = self.adapter.capabilities();
                            self.journal.apply(intent, capabilities.message_limit);
                            if self.persist().await.is_err() {
                                let _ = self.events.try_send(DeliveryWorkerEvent::StoreFailed);
                                continue;
                            }
                            let queue_depth = self
                                .journal
                                .records
                                .iter()
                                .filter(|record| record.state == DeliveryState::Pending)
                                .count();
                            let _ = self
                                .events
                                .try_send(DeliveryWorkerEvent::Enqueued { key, queue_depth });
                            self.deliver_pending().await;
                        }
                        DeliveryWorkerCommand::Shutdown => break,
                    }
                }
                _ = retry.tick() => self.deliver_pending().await,
                _ = status_check.tick() => {
                    let status = self.adapter.status().await.unwrap_or(ProviderStatus {
                        ready: false,
                        account: None,
                    });
                    let _ = self.events.try_send(DeliveryWorkerEvent::Status(status));
                }
            }
        }
    }

    async fn persist(&mut self) -> Result<(), ()> {
        self.store.save(&self.journal).await.map_err(|_| ())
    }

    async fn deliver_pending(&mut self) {
        let capabilities = self.adapter.capabilities();
        while let Some(index) = self.journal.next_pending_index() {
            let record = self.journal.records[index].clone();
            if capabilities.edit_support
                && let Some(message_id) = record.provider_message_id.clone()
            {
                let started = Instant::now();
                tracing::debug!(
                    key = ?record.key,
                    segment = record.segment,
                    revision = record.revision,
                    "provider edit started"
                );
                match self
                    .adapter
                    .edit_text(
                        record.conversation_id.clone(),
                        message_id,
                        record.text.clone(),
                    )
                    .await
                {
                    Ok(()) => {
                        tracing::debug!(
                            key = ?record.key,
                            segment = record.segment,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "provider edit finished"
                        );
                        self.journal.records[index].state = DeliveryState::Sent;
                        let coalesced_revisions = self.journal.records[index].coalesced_revisions;
                        self.journal.records[index].coalesced_revisions = 0;
                        if self.persist().await.is_err() {
                            let _ = self.events.try_send(DeliveryWorkerEvent::StoreFailed);
                            break;
                        }
                        let _ = self.events.try_send(DeliveryWorkerEvent::Edited {
                            key: record.key,
                            segment: record.segment,
                            revisions: coalesced_revisions,
                        });
                    }
                    Err(error) => {
                        tracing::debug!(
                            key = ?record.key,
                            segment = record.segment,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "provider edit failed"
                        );
                        self.failed(index, record, error).await;
                        break;
                    }
                }
            } else {
                let started = Instant::now();
                tracing::debug!(
                    key = ?record.key,
                    segment = record.segment,
                    revision = record.revision,
                    generation = record.generation,
                    "provider send started"
                );
                match self
                    .adapter
                    .send_text(record.conversation_id.clone(), record.text.clone())
                    .await
                {
                    Ok(message_id) => {
                        tracing::debug!(
                            key = ?record.key,
                            segment = record.segment,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "provider send finished"
                        );
                        let event = DeliveryWorkerEvent::Sent {
                            key: record.key.clone(),
                            segment: record.segment,
                        };
                        self.journal.records[index].provider_message_id = Some(message_id);
                        self.journal.records[index].state = DeliveryState::Sent;
                        if self.persist().await.is_err() {
                            let _ = self.events.try_send(DeliveryWorkerEvent::StoreFailed);
                            break;
                        }
                        let _ = self.events.try_send(event);
                    }
                    Err(error) => {
                        self.failed(index, record, error).await;
                        break;
                    }
                }
            }
        }
    }

    async fn failed(&mut self, index: usize, record: DeliveryRecord, error: ProviderError) {
        self.journal.records[index].attempts =
            self.journal.records[index].attempts.saturating_add(1);
        let _ = self.persist().await;
        let _ = self.events.try_send(DeliveryWorkerEvent::Failed {
            key: record.key,
            segment: record.segment,
            error,
        });
    }
}

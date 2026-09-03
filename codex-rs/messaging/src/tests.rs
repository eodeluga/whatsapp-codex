use super::*;
use codex_transcript::EntryOrigin;
use codex_transcript::TranscriptKey;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[derive(Default)]
struct MemoryStore {
    journal: DeliveryJournal,
}

impl DeliveryStore for MemoryStore {
    async fn load(&mut self) -> Result<DeliveryJournal, String> {
        Ok(self.journal.clone())
    }

    async fn save(&mut self, journal: &DeliveryJournal) -> Result<(), String> {
        self.journal = journal.clone();
        Ok(())
    }
}

#[derive(Clone)]
struct FakeAdapter {
    sent: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    edits: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    edit_support: bool,
}

#[derive(Clone, Default)]
struct RecordingStore {
    journal: Arc<std::sync::Mutex<DeliveryJournal>>,
    events: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl RecordingStore {
    fn with_journal(journal: DeliveryJournal) -> Self {
        Self {
            journal: Arc::new(std::sync::Mutex::new(journal)),
            events: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

impl DeliveryStore for RecordingStore {
    async fn load(&mut self) -> Result<DeliveryJournal, String> {
        Ok(self.journal.lock().unwrap().clone())
    }

    async fn save(&mut self, journal: &DeliveryJournal) -> Result<(), String> {
        self.events.lock().unwrap().push("store");
        *self.journal.lock().unwrap() = journal.clone();
        Ok(())
    }
}

#[derive(Clone)]
struct RecordingAdapter {
    events: Arc<std::sync::Mutex<Vec<&'static str>>>,
    sends: Arc<std::sync::Mutex<Vec<(ProviderDeliveryId, String)>>>,
    acknowledgements: Arc<std::sync::Mutex<Vec<(ProviderDeliveryId, ProviderMessageId)>>>,
    send_failures: Arc<AtomicUsize>,
    acknowledgement_failures: Arc<AtomicUsize>,
    idempotency_conflict: bool,
}

impl RecordingAdapter {
    fn new(
        events: Arc<std::sync::Mutex<Vec<&'static str>>>,
        send_failures: usize,
        acknowledgement_failures: usize,
        idempotency_conflict: bool,
    ) -> Self {
        Self {
            events,
            sends: Arc::new(std::sync::Mutex::new(Vec::new())),
            acknowledgements: Arc::new(std::sync::Mutex::new(Vec::new())),
            send_failures: Arc::new(AtomicUsize::new(send_failures)),
            acknowledgement_failures: Arc::new(AtomicUsize::new(acknowledgement_failures)),
            idempotency_conflict,
        }
    }
}

impl ProviderAdapter for RecordingAdapter {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            message_limit: 4096,
            edit_support: false,
            attachment_support: false,
            supports_markdown: false,
            rich_interaction_support: false,
        }
    }

    async fn status(&self) -> Result<ProviderStatus, ProviderError> {
        Ok(ProviderStatus {
            ready: true,
            account: None,
        })
    }

    async fn send_text(
        &self,
        delivery_id: ProviderDeliveryId,
        _conversation_id: ProviderConversationId,
        text: String,
    ) -> Result<ProviderMessageId, ProviderError> {
        self.events.lock().unwrap().push("send");
        self.sends.lock().unwrap().push((delivery_id, text));
        if self.idempotency_conflict {
            return Err(ProviderError::IdempotencyConflict);
        }
        if self
            .send_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                if remaining == 0 {
                    None
                } else {
                    Some(remaining - 1)
                }
            })
            .is_ok()
        {
            return Err(ProviderError::Unavailable);
        }
        Ok(ProviderMessageId::new(format!(
            "message-{}",
            self.sends.lock().unwrap().len()
        )))
    }

    async fn acknowledge_delivery(
        &self,
        delivery_id: ProviderDeliveryId,
        message_id: ProviderMessageId,
    ) -> Result<(), ProviderError> {
        self.events.lock().unwrap().push("ack");
        self.acknowledgements
            .lock()
            .unwrap()
            .push((delivery_id, message_id));
        if self
            .acknowledgement_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                if remaining == 0 {
                    None
                } else {
                    Some(remaining - 1)
                }
            })
            .is_ok()
        {
            return Err(ProviderError::Unavailable);
        }
        Ok(())
    }

    async fn edit_text(
        &self,
        _conversation_id: ProviderConversationId,
        _message_id: ProviderMessageId,
        _text: String,
    ) -> Result<(), ProviderError> {
        Ok(())
    }
}

fn test_intent(item: &str, text: &str) -> DeliveryIntent {
    DeliveryIntent {
        conversation_id: ProviderConversationId::new("chat"),
        generation: 0,
        key: TranscriptKey::new("thread", "turn", item),
        origin: EntryOrigin::CodexTranscript,
        text: text.to_string(),
        revision: 1,
        committed: true,
    }
}

impl ProviderAdapter for FakeAdapter {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            message_limit: 3,
            edit_support: self.edit_support,
            attachment_support: false,
            supports_markdown: false,
            rich_interaction_support: false,
        }
    }

    async fn status(&self) -> Result<ProviderStatus, ProviderError> {
        Ok(ProviderStatus {
            ready: true,
            account: None,
        })
    }

    async fn send_text(
        &self,
        _delivery_id: ProviderDeliveryId,
        _conversation_id: ProviderConversationId,
        text: String,
    ) -> Result<ProviderMessageId, ProviderError> {
        let mut sent = self.sent.lock().unwrap();
        sent.push(text);
        Ok(ProviderMessageId::new(format!("message-{}", sent.len())))
    }

    async fn acknowledge_delivery(
        &self,
        _delivery_id: ProviderDeliveryId,
        _message_id: ProviderMessageId,
    ) -> Result<(), ProviderError> {
        Ok(())
    }

    async fn edit_text(
        &self,
        _conversation_id: ProviderConversationId,
        _message_id: ProviderMessageId,
        text: String,
    ) -> Result<(), ProviderError> {
        self.edits.lock().unwrap().push(text);
        Ok(())
    }
}

#[test]
fn segmentation_is_lossless_and_unicode_safe() {
    let text = "a🙂b\ncd";
    let parts = segment_text(text, 2);
    assert_eq!(parts, vec!["a🙂", "b\n", "cd"]);
    assert_eq!(parts.concat(), text);
}

#[test]
fn journal_preserves_order_and_coalesces_revisions() {
    let key = TranscriptKey::new("thread", "turn", "item");
    let mut journal = DeliveryJournal::default();
    assert_eq!(
        journal.apply(
            DeliveryIntent {
                conversation_id: ProviderConversationId::new("chat"),
                generation: 0,
                key: key.clone(),
                origin: EntryOrigin::CodexTranscript,
                text: "abcdef".to_string(),
                revision: 1,
                committed: false,
            },
            3,
        ),
        2
    );
    assert_eq!(journal.records[0].text, "abc");
    assert_eq!(journal.records[1].text, "def");
    assert_eq!(
        journal.apply(
            DeliveryIntent {
                conversation_id: ProviderConversationId::new("chat"),
                generation: 0,
                key,
                origin: EntryOrigin::CodexTranscript,
                text: "abcdefgh".to_string(),
                revision: 2,
                committed: true,
            },
            3,
        ),
        3
    );
    assert_eq!(journal.records.len(), 3);
    assert!(journal.records.iter().all(|record| record.committed));
}

#[tokio::test]
async fn worker_sends_segments_before_commit_and_persists_provider_ids() {
    let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let adapter = FakeAdapter {
        sent: std::sync::Arc::clone(&sent),
        edits: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        edit_support: true,
    };
    let (events, mut event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(
        adapter,
        MemoryStore::default(),
        events,
        Duration::from_millis(1),
    )
    .await
    .unwrap();
    let (handle, command_rx) = DeliveryWorker::<FakeAdapter, MemoryStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));
    handle
        .apply(DeliveryIntent {
            conversation_id: ProviderConversationId::new("chat"),
            generation: 0,
            key: TranscriptKey::new("thread", "turn", "item"),
            origin: EntryOrigin::CodexTranscript,
            text: "abcdef".to_string(),
            revision: 1,
            committed: false,
        })
        .await
        .unwrap();
    let received_events = vec![
        event_rx.recv().await.unwrap(),
        event_rx.recv().await.unwrap(),
        event_rx.recv().await.unwrap(),
    ];
    assert_eq!(
        received_events,
        vec![
            DeliveryWorkerEvent::Enqueued {
                key: TranscriptKey::new("thread", "turn", "item"),
                queue_depth: 2,
            },
            DeliveryWorkerEvent::Sent {
                key: TranscriptKey::new("thread", "turn", "item"),
                segment: 0,
                provider_message_id: ProviderMessageId::new("message-1"),
            },
            DeliveryWorkerEvent::Sent {
                key: TranscriptKey::new("thread", "turn", "item"),
                segment: 1,
                provider_message_id: ProviderMessageId::new("message-2"),
            },
        ]
    );
    assert_eq!(*sent.lock().unwrap(), vec!["abc", "def"]);
    handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn worker_sends_uncommitted_revisions_without_edit_support() {
    let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let edits = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let adapter = FakeAdapter {
        sent: std::sync::Arc::clone(&sent),
        edits: Arc::clone(&edits),
        edit_support: false,
    };
    let (events, _event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(
        adapter,
        MemoryStore::default(),
        events,
        Duration::from_millis(1),
    )
    .await
    .unwrap();
    let (handle, command_rx) = DeliveryWorker::<FakeAdapter, MemoryStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));
    let key = TranscriptKey::new("thread", "turn", "item");
    handle
        .apply(DeliveryIntent {
            conversation_id: ProviderConversationId::new("chat"),
            generation: 7,
            key: key.clone(),
            origin: EntryOrigin::CodexTranscript,
            text: "one".to_string(),
            revision: 1,
            committed: false,
        })
        .await
        .unwrap();
    handle
        .apply(DeliveryIntent {
            conversation_id: ProviderConversationId::new("chat"),
            generation: 7,
            key,
            origin: EntryOrigin::CodexTranscript,
            text: "two".to_string(),
            revision: 2,
            committed: false,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if sent.lock().unwrap().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*sent.lock().unwrap(), vec!["one", "two"]);
    assert!(edits.lock().unwrap().is_empty());
    handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn worker_does_not_send_internal_intents() {
    let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let adapter = FakeAdapter {
        sent: Arc::clone(&sent),
        edits: Arc::new(std::sync::Mutex::new(Vec::new())),
        edit_support: true,
    };
    let (events, _event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(
        adapter,
        MemoryStore::default(),
        events,
        Duration::from_millis(1),
    )
    .await
    .unwrap();
    let (handle, command_rx) = DeliveryWorker::<FakeAdapter, MemoryStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));
    handle
        .apply(DeliveryIntent {
            conversation_id: ProviderConversationId::new("chat"),
            generation: 0,
            key: TranscriptKey::new("thread", "turn", "internal"),
            origin: EntryOrigin::Internal,
            text: "diagnostic".to_string(),
            revision: 1,
            committed: true,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(sent.lock().unwrap().is_empty());
    handle.shutdown().await;
    task.await.unwrap();
}

#[test]
fn journal_records_generation_and_coalesced_revision_count() {
    let key = TranscriptKey::new("thread", "turn", "item");
    let mut journal = DeliveryJournal::default();
    journal.apply(
        DeliveryIntent {
            conversation_id: ProviderConversationId::new("chat"),
            generation: 3,
            key: key.clone(),
            origin: EntryOrigin::CodexTranscript,
            text: "old".to_string(),
            revision: 1,
            committed: false,
        },
        10,
    );
    journal.records[0].provider_message_id = Some(ProviderMessageId::new("message"));
    journal.records[0].state = DeliveryState::Sent;
    journal.apply(
        DeliveryIntent {
            conversation_id: ProviderConversationId::new("chat"),
            generation: 4,
            key,
            origin: EntryOrigin::CodexTranscript,
            text: "new".to_string(),
            revision: 2,
            committed: true,
        },
        10,
    );
    assert_eq!(journal.records[0].generation, 4);
    assert_eq!(journal.records[0].coalesced_revisions, 1);
    assert_eq!(journal.records[0].state, DeliveryState::Pending);
}

#[test]
fn authoritative_shorter_revision_supersedes_stale_segments() {
    let key = TranscriptKey::new("thread", "turn", "item");
    let conversation_id = ProviderConversationId::new("chat");
    let mut journal = DeliveryJournal::default();
    journal.apply(
        DeliveryIntent {
            conversation_id: conversation_id.clone(),
            generation: 0,
            key: key.clone(),
            origin: EntryOrigin::CodexTranscript,
            text: "abcdef".to_string(),
            revision: 1,
            committed: false,
        },
        3,
    );

    journal.apply(
        DeliveryIntent {
            conversation_id: conversation_id.clone(),
            generation: 0,
            key: key.clone(),
            origin: EntryOrigin::CodexTranscript,
            text: "a".to_string(),
            revision: 2,
            committed: true,
        },
        3,
    );
    assert_eq!(journal.records[1].state, DeliveryState::Superseded);

    journal.apply(
        DeliveryIntent {
            conversation_id,
            generation: 0,
            key,
            origin: EntryOrigin::CodexTranscript,
            text: "abcdef".to_string(),
            revision: 3,
            committed: true,
        },
        3,
    );
    assert_eq!(journal.records[1].state, DeliveryState::Pending);
}

#[test]
fn retry_deadline_hides_pending_record_until_backoff_expires() {
    let key = TranscriptKey::new("thread", "turn", "item");
    let mut journal = DeliveryJournal::default();
    journal.apply(
        DeliveryIntent {
            conversation_id: ProviderConversationId::new("chat"),
            generation: 0,
            key,
            origin: EntryOrigin::CodexTranscript,
            text: "hello".to_string(),
            revision: 1,
            committed: true,
        },
        100,
    );
    journal.records[0].next_attempt_at_ms = Some(10);
    assert_eq!(journal.next_pending_index(9), None);
    assert_eq!(journal.next_pending_index(10), Some(0));

    journal.records[0].next_attempt_at_ms = Some(10);
    journal.apply(
        DeliveryIntent {
            conversation_id: ProviderConversationId::new("chat"),
            generation: 0,
            key: journal.records[0].key.clone(),
            origin: EntryOrigin::CodexTranscript,
            text: "updated".to_string(),
            revision: 2,
            committed: true,
        },
        100,
    );
    assert_eq!(journal.records[0].next_attempt_at_ms, None);
}

#[test]
fn legacy_delivery_records_load_with_new_metadata_defaults() {
    let record: DeliveryRecord = serde_json::from_value(serde_json::json!({
        "conversationId": "chat",
        "key": {
            "threadId": "thread",
            "turnId": "turn",
            "itemId": "item"
        },
        "origin": "CodexTranscript",
        "segment": 0,
        "text": "hello",
        "revision": 1,
        "committed": true,
        "state": "Sent",
        "providerMessageId": "message",
        "attempts": 0
    }))
    .unwrap();
    assert_eq!(record.generation, 0);
    assert_eq!(record.coalesced_revisions, 0);
    assert_eq!(record.next_attempt_at_ms, None);
}

#[test]
fn new_records_get_distinct_stable_delivery_ids() {
    let mut journal = DeliveryJournal::default();
    journal.apply(test_intent("first", "same text"), 4096);
    journal.apply(test_intent("second", "same text"), 4096);

    let first_id = journal.records[0].delivery_id.clone().unwrap();
    let second_id = journal.records[1].delivery_id.clone().unwrap();
    assert_ne!(first_id, second_id);

    journal.apply(test_intent("first", "updated text"), 4096);
    assert_eq!(journal.records[0].delivery_id, Some(first_id));
}

#[tokio::test]
async fn worker_persists_delivery_id_before_send_and_acknowledges_after_sent() {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = RecordingStore {
        journal: Arc::new(std::sync::Mutex::new(DeliveryJournal::default())),
        events: Arc::clone(&events),
    };
    let adapter = RecordingAdapter::new(Arc::clone(&events), 0, 0, false);
    let sends = Arc::clone(&adapter.sends);
    let acknowledgements = Arc::clone(&adapter.acknowledgements);
    let journal = Arc::clone(&store.journal);
    let (worker_events, mut worker_event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(adapter, store, worker_events, Duration::from_millis(1))
        .await
        .unwrap();
    let (handle, command_rx) = DeliveryWorker::<RecordingAdapter, RecordingStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));

    handle.apply(test_intent("item", "hello")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if acknowledgements.lock().unwrap().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let delivery_id = sends.lock().unwrap()[0].0.clone();
    assert_eq!(
        journal.lock().unwrap().records[0].delivery_id,
        Some(delivery_id.clone())
    );
    assert_eq!(acknowledgements.lock().unwrap()[0].0, delivery_id);
    assert_eq!(
        *events.lock().unwrap(),
        vec!["store", "send", "store", "ack"]
    );
    assert!(matches!(
        worker_event_rx.recv().await,
        Some(DeliveryWorkerEvent::Enqueued { .. })
    ));
    handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn worker_retries_with_the_same_delivery_id() {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = RecordingStore {
        journal: Arc::new(std::sync::Mutex::new(DeliveryJournal::default())),
        events: Arc::clone(&events),
    };
    let adapter = RecordingAdapter::new(Arc::clone(&events), 1, 0, false);
    let sends = Arc::clone(&adapter.sends);
    let (worker_events, _worker_event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(adapter, store, worker_events, Duration::from_millis(1))
        .await
        .unwrap();
    let (handle, command_rx) = DeliveryWorker::<RecordingAdapter, RecordingStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));

    handle.apply(test_intent("item", "hello")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if sends.lock().unwrap().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let same_delivery_id = {
        let sends = sends.lock().unwrap();
        sends[0].0 == sends[1].0
    };
    assert!(same_delivery_id);
    handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn worker_sends_identical_text_for_distinct_transcript_keys() {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = RecordingStore {
        journal: Arc::new(std::sync::Mutex::new(DeliveryJournal::default())),
        events: Arc::clone(&events),
    };
    let adapter = RecordingAdapter::new(Arc::clone(&events), 0, 0, false);
    let sends = Arc::clone(&adapter.sends);
    let (worker_events, _worker_event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(adapter, store, worker_events, Duration::from_millis(1))
        .await
        .unwrap();
    let (handle, command_rx) = DeliveryWorker::<RecordingAdapter, RecordingStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));

    handle
        .apply(test_intent("first", "same text"))
        .await
        .unwrap();
    handle
        .apply(test_intent("second", "same text"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if sends.lock().unwrap().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let distinct_delivery_ids = {
        let sends = sends.lock().unwrap();
        sends[0].0 != sends[1].0
    };
    assert!(distinct_delivery_ids);
    handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn worker_migrates_legacy_pending_record_before_provider_call() {
    let mut journal = DeliveryJournal::default();
    journal.apply(test_intent("item", "hello"), 4096);
    journal.records[0].delivery_id = None;
    let store = RecordingStore::with_journal(journal);
    let events = Arc::clone(&store.events);
    let adapter = RecordingAdapter::new(Arc::clone(&events), 0, 0, false);
    let sends = Arc::clone(&adapter.sends);
    let journal = Arc::clone(&store.journal);
    let (worker_events, _worker_event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(adapter, store, worker_events, Duration::from_millis(1))
        .await
        .unwrap();
    let (handle, command_rx) = DeliveryWorker::<RecordingAdapter, RecordingStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if sends.lock().unwrap().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(journal.lock().unwrap().records[0].delivery_id.is_some());
    assert_eq!(events.lock().unwrap().first().copied(), Some("store"));
    handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn acknowledgement_failure_keeps_sent_record_without_resending() {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = RecordingStore {
        journal: Arc::new(std::sync::Mutex::new(DeliveryJournal::default())),
        events: Arc::clone(&events),
    };
    let adapter = RecordingAdapter::new(Arc::clone(&events), 0, usize::MAX, false);
    let sends = Arc::clone(&adapter.sends);
    let acknowledgements = Arc::clone(&adapter.acknowledgements);
    let journal = Arc::clone(&store.journal);
    let (worker_events, _worker_event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(adapter, store, worker_events, Duration::from_millis(1))
        .await
        .unwrap();
    let (handle, command_rx) = DeliveryWorker::<RecordingAdapter, RecordingStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));

    handle.apply(test_intent("item", "hello")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(sends.lock().unwrap().len(), 1);
    assert!(acknowledgements.lock().unwrap().len() > 1);
    assert_eq!(
        journal.lock().unwrap().records[0].state,
        DeliveryState::Sent
    );
    handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn startup_acknowledges_sent_records_without_sending_again() {
    let mut journal = DeliveryJournal::default();
    journal.apply(test_intent("item", "hello"), 4096);
    journal.records[0].provider_message_id = Some(ProviderMessageId::new("message-1"));
    journal.records[0].state = DeliveryState::Sent;
    let store = RecordingStore::with_journal(journal);
    let adapter = RecordingAdapter::new(Arc::clone(&store.events), 0, 0, false);
    let sends = Arc::clone(&adapter.sends);
    let acknowledgements = Arc::clone(&adapter.acknowledgements);
    let (worker_events, _worker_event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(adapter, store, worker_events, Duration::from_millis(1))
        .await
        .unwrap();
    let (handle, command_rx) = DeliveryWorker::<RecordingAdapter, RecordingStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if acknowledgements.lock().unwrap().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(sends.lock().unwrap().is_empty());
    handle.shutdown().await;
    task.await.unwrap();
}

#[tokio::test]
async fn idempotency_conflict_is_permanent_and_degrades_delivery() {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let store = RecordingStore {
        journal: Arc::new(std::sync::Mutex::new(DeliveryJournal::default())),
        events: Arc::clone(&events),
    };
    let adapter = RecordingAdapter::new(Arc::clone(&events), 0, 0, true);
    let sends = Arc::clone(&adapter.sends);
    let journal = Arc::clone(&store.journal);
    let (worker_events, mut worker_event_rx) = tokio::sync::mpsc::channel(16);
    let worker = DeliveryWorker::new(adapter, store, worker_events, Duration::from_millis(1))
        .await
        .unwrap();
    let (handle, command_rx) = DeliveryWorker::<RecordingAdapter, RecordingStore>::channel(8);
    let task = tokio::spawn(worker.run(command_rx));

    handle.apply(test_intent("item", "hello")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if journal
                .lock()
                .unwrap()
                .records
                .first()
                .is_some_and(|record| record.state == DeliveryState::FailedPermanent)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(sends.lock().unwrap().len(), 1);
    assert!(matches!(
        worker_event_rx.recv().await,
        Some(DeliveryWorkerEvent::Enqueued { .. })
    ));
    handle.shutdown().await;
    task.await.unwrap();
}

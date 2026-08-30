use super::*;
use codex_transcript::EntryOrigin;
use codex_transcript::TranscriptKey;
use pretty_assertions::assert_eq;
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
}

impl ProviderAdapter for FakeAdapter {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            message_limit: 3,
            edit_support: true,
            attachment_support: false,
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
        _conversation_id: ProviderConversationId,
        text: String,
    ) -> Result<ProviderMessageId, ProviderError> {
        let mut sent = self.sent.lock().unwrap();
        sent.push(text);
        Ok(ProviderMessageId::new(format!("message-{}", sent.len())))
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
            key: TranscriptKey::new("thread", "turn", "item"),
            origin: EntryOrigin::CodexTranscript,
            text: "abcdef".to_string(),
            revision: 1,
            committed: false,
        })
        .await
        .unwrap();
    let _ = event_rx.recv().await;
    let _ = event_rx.recv().await;
    let _ = event_rx.recv().await;
    assert_eq!(*sent.lock().unwrap(), vec!["abc", "def"]);
    handle.shutdown().await;
    task.await.unwrap();
}

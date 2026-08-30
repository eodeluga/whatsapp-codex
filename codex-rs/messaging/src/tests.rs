use super::*;
use codex_transcript::EntryOrigin;
use codex_transcript::TranscriptKey;
use pretty_assertions::assert_eq;

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

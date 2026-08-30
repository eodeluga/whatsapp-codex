use super::*;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

#[test]
fn persists_and_prunes_deduplication_records() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.json");
    let mut state = BridgeState::empty();
    state.mark_processed("old".to_string(), 1);
    state.mark_processed("new".to_string(), 100);
    state.prune(100, 1, 1);
    state.save(&path).unwrap();

    let loaded = BridgeState::load(&path).unwrap();
    assert_eq!(
        loaded.processed_events.keys().collect::<Vec<_>>(),
        vec![&"new".to_string()]
    );
}

#[cfg(unix)]
#[test]
fn state_file_is_private() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempdir().unwrap();
    let path = directory.path().join("state.json");
    BridgeState::empty().save(&path).unwrap();

    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn rejects_unknown_state_schema_and_unknown_fields() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.json");
    std::fs::write(
        &path,
        format!(r#"{{"schemaVersion":{}}}"#, STATE_SCHEMA_VERSION + 1),
    )
    .unwrap();
    assert!(matches!(
        BridgeState::load(&path),
        Err(StateError::UnsupportedSchema)
    ));

    std::fs::write(
        &path,
        format!(r#"{{"schemaVersion":{STATE_SCHEMA_VERSION},"unexpected":true}}"#),
    )
    .unwrap();
    assert!(matches!(BridgeState::load(&path), Err(StateError::Parse)));
}

#[test]
fn migrates_schema_two_without_changing_queued_prompt_meaning() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.json");
    std::fs::write(
        &path,
        r#"{
          "schemaVersion": 2,
          "binding": null,
          "activeTurn": null,
          "queuedPrompts": [{
            "idempotencyKey": "event-1",
            "messageId": "message-1",
            "body": "next turn",
            "acceptedAt": 10
          }],
          "outbox": [],
          "processedEvents": {},
          "outboundMessageIds": {}
        }"#,
    )
    .unwrap();

    let state = BridgeState::load(&path).unwrap();
    assert_eq!(state.schema_version, STATE_SCHEMA_VERSION);
    assert_eq!(state.queued_prompts[0].body, "next turn");
    assert!(state.pending_steers.is_empty());
}

#[test]
fn legacy_delivery_fields_are_read_but_not_written() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("state.json");
    let mut state = BridgeState::empty();
    state.legacy_outbox.push(OutboundMessage {
        response_id: "response-1".to_string(),
        chat_id: "chat".to_string(),
        body: "legacy".to_string(),
        attempts: 0,
    });
    state.active_turn = Some(ActiveTurn {
        inbound_message_id: "message-1".to_string(),
        thread_id: "thread-1".to_string(),
        codex_turn_id: "turn-1".to_string(),
        legacy_working_output_message_id: Some("message-1".to_string()),
        attachment_paths: Vec::new(),
    });
    state.save(&path).unwrap();

    let serialized = std::fs::read_to_string(&path).unwrap();
    assert!(!serialized.contains("outbox"));
    assert!(!serialized.contains("workingOutputMessageId"));
}

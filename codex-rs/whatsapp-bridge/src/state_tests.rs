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

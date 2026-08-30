use super::*;
use codex_app_server_protocol::AgentMessageDeltaNotification;
use codex_app_server_protocol::ErrorNotification;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadClosedNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ToolRequestUserInputOption;
use codex_app_server_protocol::ToolRequestUserInputParams;
use codex_app_server_protocol::ToolRequestUserInputQuestion;
use codex_app_server_protocol::TurnError;
use codex_protocol::models::MessagePhase;
use pretty_assertions::assert_eq;

#[test]
fn assistant_items_are_keyed_and_commentary_is_preserved() {
    let mut projector = TranscriptProjector::default();

    let events = projector.apply(ServerNotification::AgentMessageDelta(
        AgentMessageDeltaNotification {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item_id: "commentary".to_owned(),
            delta: "thinking aloud".to_owned(),
        },
    ));
    assert!(matches!(events.as_slice(), [ProjectionEvent::Entry(_)]));

    let events = projector.apply(ServerNotification::AgentMessageDelta(
        AgentMessageDeltaNotification {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item_id: "final".to_owned(),
            delta: "answer".to_owned(),
        },
    ));
    assert!(matches!(events.as_slice(), [ProjectionEvent::Entry(_)]));

    assert_eq!(
        projector
            .entries()
            .iter()
            .map(|entry| entry.key.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["commentary", "final"]
    );
}

#[test]
fn completed_item_replaces_partial_text_and_commits_it() {
    let mut projector = TranscriptProjector::default();
    projector.apply(ServerNotification::AgentMessageDelta(
        AgentMessageDeltaNotification {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item_id: "item-1".to_owned(),
            delta: "partial".to_owned(),
        },
    ));

    let events = projector.apply(ServerNotification::ItemCompleted(
        ItemCompletedNotification {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item: ThreadItem::AgentMessage {
                id: "item-1".to_owned(),
                text: "authoritative".to_owned(),
                phase: Some(MessagePhase::Commentary),
                memory_citation: None,
            },
            completed_at_ms: 1,
        },
    ));

    let [ProjectionEvent::Entry(entry)] = events.as_slice() else {
        panic!("expected one committed entry");
    };
    assert_eq!(
        entry.item,
        ThreadItem::AgentMessage {
            id: "item-1".to_owned(),
            text: "authoritative".to_owned(),
            phase: Some(MessagePhase::Commentary),
            memory_citation: None,
        }
    );
    assert!(entry.committed);

    assert!(
        projector
            .apply(ServerNotification::AgentMessageDelta(
                AgentMessageDeltaNotification {
                    thread_id: "thread-1".to_owned(),
                    turn_id: "turn-1".to_owned(),
                    item_id: "item-1".to_owned(),
                    delta: " stale".to_owned(),
                },
            ))
            .is_empty()
    );
}

#[test]
fn retryable_errors_and_internal_lifecycle_are_suppressed() {
    let mut projector = TranscriptProjector::default();
    let retryable = projector.apply(ServerNotification::Error(ErrorNotification {
        error: TurnError {
            message: "temporary".to_owned(),
            codex_error_info: None,
            additional_details: None,
        },
        will_retry: true,
        thread_id: "thread-1".to_owned(),
        turn_id: "turn-1".to_owned(),
    }));
    assert_eq!(retryable, vec![ProjectionEvent::Suppressed]);

    let lifecycle = projector.apply(ServerNotification::ThreadClosed(ThreadClosedNotification {
        thread_id: "thread-1".to_owned(),
    }));
    assert_eq!(lifecycle, vec![ProjectionEvent::Suppressed]);
}

#[test]
fn user_input_requests_use_a_shared_semantic_presentation() {
    let mut projector = TranscriptProjector::default();
    let event = projector.apply_user_input_request(
        &RequestId::String("request-1".to_owned()),
        &ToolRequestUserInputParams {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item_id: "item-1".to_owned(),
            questions: vec![ToolRequestUserInputQuestion {
                id: "mode".to_owned(),
                header: "Mode".to_owned(),
                question: "Which mode?".to_owned(),
                is_other: true,
                is_secret: false,
                options: Some(vec![ToolRequestUserInputOption {
                    label: "Fast".to_owned(),
                    description: "Use the fast mode".to_owned(),
                }]),
            }],
            auto_resolution_ms: None,
        },
    );

    let ProjectionEvent::Request(presentation) = event else {
        panic!("expected a shared request presentation");
    };
    assert_eq!(presentation.request_id, "request-1");
    assert_eq!(
        presentation.key,
        TranscriptKey::new("thread-1", "turn-1", "item-1")
    );
    assert_eq!(presentation.questions[0].id, "mode");
    assert_eq!(
        presentation.questions[0].options.as_ref().unwrap()[0].label,
        "Fast"
    );
}

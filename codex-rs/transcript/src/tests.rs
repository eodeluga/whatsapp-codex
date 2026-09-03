use super::*;
use codex_app_server_protocol::AgentMessageDeltaNotification;
use codex_app_server_protocol::ErrorNotification;
use codex_app_server_protocol::GuardianWarningNotification;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::PatchApplyStatus;
use codex_app_server_protocol::ReasoningSummaryTextDeltaNotification;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ThreadClosedNotification;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ToolRequestUserInputOption;
use codex_app_server_protocol::ToolRequestUserInputParams;
use codex_app_server_protocol::ToolRequestUserInputQuestion;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnError;
use codex_app_server_protocol::TurnStatus;
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
fn canonical_ids_make_reconciliation_idempotent_and_distinct() {
    let mut projector = TranscriptProjector::default();
    let first = ThreadItem::AgentMessage {
        id: "msg-1".to_owned(),
        text: "same text".to_owned(),
        phase: Some(MessagePhase::FinalAnswer),
        memory_citation: None,
    };
    let events = projector.apply(ServerNotification::ItemCompleted(
        ItemCompletedNotification {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item: first.clone(),
            completed_at_ms: 1,
        },
    ));
    assert!(matches!(events.as_slice(), [ProjectionEvent::Entry(_)]));
    assert!(
        projector
            .reconcile_items("thread-1", "turn-1", std::slice::from_ref(&first))
            .is_empty()
    );

    let revised = ThreadItem::AgentMessage {
        id: "msg-1".to_owned(),
        text: "revised text".to_owned(),
        phase: Some(MessagePhase::FinalAnswer),
        memory_citation: None,
    };
    let events = projector.reconcile_items("thread-1", "turn-1", std::slice::from_ref(&revised));
    assert!(matches!(events.as_slice(), [ProjectionEvent::Entry(_)]));

    let second = ThreadItem::AgentMessage {
        id: "msg-2".to_owned(),
        text: "revised text".to_owned(),
        phase: Some(MessagePhase::FinalAnswer),
        memory_citation: None,
    };
    let events = projector.reconcile_items("thread-1", "turn-1", &[revised, second]);
    assert!(matches!(events.as_slice(), [ProjectionEvent::Entry(_)]));
    assert_eq!(
        projector
            .entries()
            .iter()
            .map(|entry| entry.key.item_id.as_str())
            .collect::<Vec<_>>(),
        vec!["msg-1", "msg-2"]
    );
}

#[test]
fn late_item_start_does_not_erase_streamed_content() {
    let mut projector = TranscriptProjector::default();
    projector.apply(ServerNotification::AgentMessageDelta(
        AgentMessageDeltaNotification {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item_id: "item-1".to_owned(),
            delta: "already streamed".to_owned(),
        },
    ));

    let events = projector.apply(ServerNotification::ItemStarted(ItemStartedNotification {
        thread_id: "thread-1".to_owned(),
        turn_id: "turn-1".to_owned(),
        started_at_ms: 1,
        item: ThreadItem::AgentMessage {
            id: "item-1".to_owned(),
            text: String::new(),
            phase: Some(MessagePhase::Commentary),
            memory_citation: None,
        },
    }));
    let [ProjectionEvent::Entry(entry)] = events.as_slice() else {
        panic!("expected the partial item to be retained");
    };
    assert_eq!(entry.plain_text().as_deref(), Some("already streamed"));
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
fn reasoning_is_allowlisted_but_disabled_by_default() {
    let notification =
        ServerNotification::ReasoningSummaryTextDelta(ReasoningSummaryTextDeltaNotification {
            thread_id: "thread-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            item_id: "reasoning-1".to_owned(),
            delta: "summary".to_owned(),
            summary_index: 0,
        });
    assert_eq!(
        TranscriptProjector::default().apply(notification.clone()),
        vec![ProjectionEvent::Suppressed]
    );

    let mut projector = TranscriptProjector::new(TranscriptProjectionOptions {
        include_reasoning: true,
        include_tool_calls: false,
        include_automatic_approval_reviews: false,
    });
    let events = projector.apply(notification);
    assert!(matches!(events.as_slice(), [ProjectionEvent::Entry(_)]));
    assert_eq!(
        projector.entries()[0].plain_text().as_deref(),
        Some("summary")
    );
}

#[test]
fn tool_calls_are_allowlisted_but_disabled_by_default() {
    let tool_call = ThreadItem::FileChange {
        id: "file-change-1".to_owned(),
        changes: Vec::new(),
        status: PatchApplyStatus::InProgress,
    };
    assert!(
        TranscriptProjector::default()
            .reconcile_items("thread-1", "turn-1", std::slice::from_ref(&tool_call))
            .is_empty()
    );

    let mut projector = TranscriptProjector::new(TranscriptProjectionOptions {
        include_reasoning: false,
        include_tool_calls: true,
        include_automatic_approval_reviews: false,
    });
    let events = projector.reconcile_items("thread-1", "turn-1", &[tool_call]);
    assert!(matches!(events.as_slice(), [ProjectionEvent::Entry(_)]));
    assert_eq!(
        projector.entries()[0].plain_text().as_deref(),
        Some("file changes: InProgress · 0 changes")
    );
}

#[test]
fn automatic_approval_reviews_are_allowlisted_but_disabled_by_default() {
    let notification = ServerNotification::GuardianWarning(GuardianWarningNotification {
        thread_id: "thread-1".to_owned(),
        message: "Automatic approval review approved the requested action.".to_owned(),
    });
    assert_eq!(
        TranscriptProjector::default().apply(notification.clone()),
        vec![ProjectionEvent::Suppressed]
    );

    let mut projector = TranscriptProjector::new(TranscriptProjectionOptions {
        include_reasoning: false,
        include_tool_calls: false,
        include_automatic_approval_reviews: true,
    });
    let events = projector.apply(notification);
    assert!(matches!(events.as_slice(), [ProjectionEvent::Notice(_)]));
    assert_eq!(
        match &events[0] {
            ProjectionEvent::Notice(notice) => notice.text.as_str(),
            _ => unreachable!(),
        },
        "Automatic approval review approved the requested action."
    );
}

#[test]
fn terminal_turn_error_is_not_projected_twice() {
    let mut projector = TranscriptProjector::default();
    let error = TurnError {
        message: "turn failed".to_owned(),
        codex_error_info: None,
        additional_details: None,
    };
    let first = projector.apply(ServerNotification::Error(ErrorNotification {
        error: error.clone(),
        will_retry: false,
        thread_id: "thread-1".to_owned(),
        turn_id: "turn-1".to_owned(),
    }));
    assert!(matches!(first.as_slice(), [ProjectionEvent::Notice(_)]));

    let second = projector.apply(ServerNotification::TurnCompleted(
        TurnCompletedNotification {
            thread_id: "thread-1".to_owned(),
            turn: Turn {
                id: "turn-1".to_owned(),
                items: Vec::new(),
                items_view: Default::default(),
                status: TurnStatus::Failed,
                error: Some(error),
                started_at: None,
                completed_at: None,
                duration_ms: None,
            },
        },
    ));
    assert!(second.is_empty());

    let replay = projector.record_turn_error("thread-1", "turn-1", "turn failed");
    assert!(replay.is_none());
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

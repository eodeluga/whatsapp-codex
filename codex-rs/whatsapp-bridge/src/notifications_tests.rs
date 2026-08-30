use super::*;
use codex_app_server_protocol::ErrorNotification;
use codex_app_server_protocol::ThreadDeletedNotification;
use codex_app_server_protocol::TurnError;
use pretty_assertions::assert_eq;

#[test]
fn lifecycle_notifications_are_suppressed() {
    let notification = ServerNotification::ThreadDeleted(ThreadDeletedNotification {
        thread_id: "thread-1".to_string(),
    });

    assert_eq!(render_notification(&notification), None);
}

#[test]
fn retry_errors_are_suppressed() {
    let notification = ServerNotification::Error(ErrorNotification {
        error: TurnError {
            message: "temporary failure".to_string(),
            codex_error_info: None,
            additional_details: None,
        },
        will_retry: true,
        thread_id: "thread-1".to_string(),
        turn_id: "turn-1".to_string(),
    });

    assert_eq!(render_notification(&notification), None);
}

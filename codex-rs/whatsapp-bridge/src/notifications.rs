//! App-server identity helpers for the WhatsApp transport.

use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;

pub(crate) fn notification_thread_id(notification: &ServerNotification) -> Option<String> {
    serialized_thread_id(notification)
}

pub(crate) fn server_request_thread_id(request: &ServerRequest) -> Option<String> {
    serialized_thread_id(request)
}

fn serialized_thread_id<T: serde::Serialize>(value: &T) -> Option<String> {
    serde_json::to_value(value)
        .ok()?
        .get("params")?
        .get("threadId")?
        .as_str()
        .map(str::to_owned)
}

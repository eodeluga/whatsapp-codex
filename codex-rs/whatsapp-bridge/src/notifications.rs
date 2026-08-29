//! Rendering of user-facing app-server notifications for the WhatsApp transport.

use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;

pub(crate) fn notification_thread_id(notification: &ServerNotification) -> Option<String> {
    serialized_thread_id(notification)
}

pub(crate) fn server_request_thread_id(request: &ServerRequest) -> Option<String> {
    serialized_thread_id(request)
}

pub(crate) fn render_notification(notification: &ServerNotification) -> Option<String> {
    match notification {
        ServerNotification::Error(params) if !params.will_retry => {
            Some(format!("[codex] {}", params.error.message))
        }
        ServerNotification::Warning(params) => Some(format!("[codex] Warning: {}", params.message)),
        ServerNotification::GuardianWarning(params) => {
            Some(format!("[codex] Warning: {}", params.message))
        }
        ServerNotification::DeprecationNotice(params) => Some(params.details.as_ref().map_or_else(
            || format!("[codex] {}", params.summary),
            |details| format!("[codex] {}\n{details}", params.summary),
        )),
        ServerNotification::ConfigWarning(params) => Some(params.details.as_ref().map_or_else(
            || format!("[codex] {}", params.summary),
            |details| format!("[codex] {}\n{details}", params.summary),
        )),
        _ => None,
    }
}

fn serialized_thread_id<T: serde::Serialize>(value: &T) -> Option<String> {
    serde_json::to_value(value)
        .ok()?
        .get("params")?
        .get("threadId")?
        .as_str()
        .map(str::to_owned)
}

#[cfg(test)]
#[path = "notifications_tests.rs"]
mod tests;

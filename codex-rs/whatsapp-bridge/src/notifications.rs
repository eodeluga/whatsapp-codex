//! Rendering of app-server events for the WhatsApp transport.
//!
//! The bridge deliberately keeps this renderer protocol-driven. When the
//! app-server adds a notification, it must still be visible here through the
//! serialized fallback instead of disappearing in an unhandled match arm.

use base64::Engine;
use codex_app_server_protocol::CommandExecOutputStream;
use codex_app_server_protocol::ProcessOutputStream;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;
use serde_json::Value;

const REDACTED: &str = "<redacted>";

pub(crate) fn notification_thread_id(notification: &ServerNotification) -> Option<String> {
    serialized_thread_id(notification)
}

pub(crate) fn server_request_thread_id(request: &ServerRequest) -> Option<String> {
    serialized_thread_id(request)
}

pub(crate) fn render_notification(notification: &ServerNotification) -> String {
    match notification {
        ServerNotification::CommandExecOutputDelta(params) => render_stream_output(
            "command/exec",
            command_exec_stream_name(params.stream),
            &params.delta_base64,
            params.cap_reached,
        ),
        ServerNotification::ProcessOutputDelta(params) => render_stream_output(
            "process",
            process_stream_name(params.stream),
            &params.delta_base64,
            params.cap_reached,
        ),
        ServerNotification::ProcessExited(params) => {
            let mut output = format!("[codex] process exited with code {}.", params.exit_code);
            append_captured_output(
                &mut output,
                "stdout",
                &params.stdout,
                params.stdout_cap_reached,
            );
            append_captured_output(
                &mut output,
                "stderr",
                &params.stderr,
                params.stderr_cap_reached,
            );
            output
        }
        _ => render_serialized("App-server event", notification),
    }
}

pub(crate) fn render_server_request(request: &ServerRequest) -> String {
    render_serialized("App-server request", request)
}

fn render_stream_output(method: &str, stream: &str, encoded: &str, cap_reached: bool) -> String {
    let output = match base64::engine::general_purpose::STANDARD.decode(encoded) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(error) => format!("[invalid base64 output: {error}]"),
    };
    let cap_notice = cap_reached
        .then_some("\n[codex] Output cap reached.")
        .unwrap_or_default();
    format!("[codex] {method} {stream}:\n{output}{cap_notice}")
}

fn append_captured_output(output: &mut String, stream: &str, captured: &str, cap_reached: bool) {
    if !captured.is_empty() {
        output.push_str(&format!("\n[codex] {stream}:\n{captured}"));
    }
    if cap_reached {
        output.push_str(&format!("\n[codex] {stream} output cap reached."));
    }
}

fn command_exec_stream_name(stream: CommandExecOutputStream) -> &'static str {
    match stream {
        CommandExecOutputStream::Stdout => "stdout",
        CommandExecOutputStream::Stderr => "stderr",
    }
}

fn process_stream_name(stream: ProcessOutputStream) -> &'static str {
    match stream {
        ProcessOutputStream::Stdout => "stdout",
        ProcessOutputStream::Stderr => "stderr",
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

fn render_serialized<T: serde::Serialize>(label: &str, value: &T) -> String {
    let serialized = match serde_json::to_value(value) {
        Ok(mut serialized) => {
            redact_value(&mut serialized);
            serde_json::to_string_pretty(&serialized)
                .unwrap_or_else(|error| format!("{{\"serializationError\":\"{error}\"}}"))
        }
        Err(error) => format!("{{\"serializationError\":\"{error}\"}}"),
    };
    format!("[codex] {label}:\n{serialized}")
}

fn redact_value(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                redact_value(value);
            }
        }
        Value::Object(object) => {
            for (key, value) in object {
                let key = key.to_ascii_lowercase();
                if is_sensitive_key(&key) {
                    *value = Value::String(REDACTED.to_string());
                } else {
                    redact_value(value);
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    key == "id"
        || key.ends_with("id")
        || key.contains("token")
        || key.contains("secret")
        || key.contains("password")
        || key.contains("credential")
        || key.contains("api_key")
        || key.contains("apikey")
}

use super::*;
use pretty_assertions::assert_eq;

#[test]
fn verifies_raw_body_and_filters_a_self_chat_message() {
    let body = br#"{"event":"message.received","sessionId":"s","idempotencyKey":"key","data":{"id":"m","from":"447700900000@c.us","to":"447700900000@c.us","body":"!codex hello","type":"chat","isGroup":false}}"#;
    let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
    mac.update(body);
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let webhook = parse_verified_webhook(b"secret", &signature, body).unwrap();
    let inbound = filter_inbound(webhook, "s", "447700900000@c.us", "!codex ", |_| false).unwrap();
    assert_eq!(inbound.idempotency_key, "key");
    assert!(parse_verified_webhook(b"secret", "00", body).is_err());
}

#[test]
fn rejects_a_sent_message_to_another_chat() {
    let webhook = OpenWaWebhook {
        event: "message.sent".to_string(),
        session_id: "s".to_string(),
        idempotency_key: None,
        data: serde_json::to_value(OpenWaMessage {
            id: "m".to_string(),
            from: "447700900000@c.us".to_string(),
            to: "447700900001@c.us".to_string(),
            body: "!codex should not run".to_string(),
            message_type: "chat".to_string(),
            is_group: false,
        })
        .unwrap(),
    };

    assert_eq!(
        filter_inbound(webhook, "s", "447700900000@c.us", "!codex ", |_| false),
        None
    );
}

#[test]
fn rejects_a_message_already_recorded_in_the_outbound_ledger() {
    let webhook = OpenWaWebhook {
        event: "message.sent".to_string(),
        session_id: "s".to_string(),
        idempotency_key: Some("key".to_string()),
        data: serde_json::to_value(OpenWaMessage {
            id: "bridge-message".to_string(),
            from: "447700900000@c.us".to_string(),
            to: "447700900000@c.us".to_string(),
            body: "!codex should not run".to_string(),
            message_type: "chat".to_string(),
            is_group: false,
        })
        .unwrap(),
    };

    assert_eq!(
        filter_inbound(
            webhook,
            "s",
            "447700900000@c.us",
            "!codex ",
            |message_id| message_id == "bridge-message",
        ),
        None
    );
}

#[test]
fn safely_ignores_a_signed_session_status_event() {
    let body = br#"{"event":"session.status","sessionId":"s","idempotencyKey":"status-key","data":{"status":"ready","phone":"447700900000"}}"#;
    let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
    mac.update(body);
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let webhook = parse_verified_webhook(b"secret", &signature, body).unwrap();

    assert_eq!(
        filter_inbound(webhook, "s", "447700900000@c.us", "!codex ", |_| false),
        None
    );
}

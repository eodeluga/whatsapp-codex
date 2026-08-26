use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn verifies_raw_body_and_filters_a_self_chat_message() {
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
    let inbound = filter_inbound(
        webhook,
        "s",
        "447700900000@c.us",
        |_| false,
        |_| async { None },
    )
    .await
    .unwrap();
    assert_eq!(inbound.idempotency_key, "key");
    assert!(parse_verified_webhook(b"secret", "00", body).is_err());
}

#[tokio::test]
async fn rejects_a_sent_message_to_another_chat() {
    let webhook = TransportWebhook {
        event: "message.sent".to_string(),
        session_id: "s".to_string(),
        idempotency_key: None,
        data: serde_json::to_value(TransportMessage {
            id: "m".to_string(),
            from: "447700900000@c.us".to_string(),
            to: "447700900001@c.us".to_string(),
            chat_id: None,
            body: "!codex should not run".to_string(),
            message_type: "chat".to_string(),
            is_group: false,
            from_me: true,
        })
        .unwrap(),
    };

    assert_eq!(
        filter_inbound(
            webhook,
            "s",
            "447700900000@c.us",
            |_| false,
            |_| async { None }
        )
        .await,
        None
    );
}

#[tokio::test]
async fn rejects_a_message_already_recorded_in_the_outbound_ledger() {
    let webhook = TransportWebhook {
        event: "message.sent".to_string(),
        session_id: "s".to_string(),
        idempotency_key: Some("key".to_string()),
        data: serde_json::to_value(TransportMessage {
            id: "bridge-message".to_string(),
            from: "447700900000@c.us".to_string(),
            to: "447700900000@c.us".to_string(),
            chat_id: None,
            body: "!codex should not run".to_string(),
            message_type: "chat".to_string(),
            is_group: false,
            from_me: true,
        })
        .unwrap(),
    };

    assert_eq!(
        filter_inbound(
            webhook,
            "s",
            "447700900000@c.us",
            |message_id| message_id == "bridge-message",
            |_| async { None }
        )
        .await,
        None
    );
}

#[tokio::test]
async fn safely_ignores_a_signed_session_status_event() {
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
        filter_inbound(
            webhook,
            "s",
            "447700900000@c.us",
            |_| false,
            |_| async { None }
        )
        .await,
        None
    );
}

#[tokio::test]
async fn accepts_a_modern_self_chat_message_after_resolving_its_lid() {
    let webhook = TransportWebhook {
        event: "message.sent".to_string(),
        session_id: "s".to_string(),
        idempotency_key: Some("key".to_string()),
        data: serde_json::to_value(TransportMessage {
            id: "m".to_string(),
            from: "447700900000@c.us".to_string(),
            to: "172662718488742@lid".to_string(),
            chat_id: Some("172662718488742@lid".to_string()),
            body: "/status".to_string(),
            message_type: "text".to_string(),
            is_group: false,
            from_me: true,
        })
        .unwrap(),
    };

    assert_eq!(
        filter_inbound(
            webhook,
            "s",
            "447700900000@c.us",
            |_| false,
            |contact_id| async move {
                assert_eq!(contact_id, "172662718488742@lid");
                Some("447700900000".to_string())
            }
        )
        .await,
        Some(InboundMessage {
            idempotency_key: "key".to_string(),
            message_id: "m".to_string(),
            chat_id: "447700900000@c.us".to_string(),
            body: "/status".to_string(),
        })
    );
}

#[tokio::test]
async fn rejects_a_lid_that_resolves_to_another_phone() {
    let webhook = TransportWebhook {
        event: "message.sent".to_string(),
        session_id: "s".to_string(),
        idempotency_key: None,
        data: serde_json::to_value(TransportMessage {
            id: "m".to_string(),
            from: "447700900000@c.us".to_string(),
            to: "172662718488742@lid".to_string(),
            chat_id: None,
            body: "not a self chat".to_string(),
            message_type: "text".to_string(),
            is_group: false,
            from_me: true,
        })
        .unwrap(),
    };

    assert_eq!(
        filter_inbound(
            webhook,
            "s",
            "447700900000@c.us",
            |_| false,
            |_| async { Some("447700900001".to_string()) }
        )
        .await,
        None
    );
}

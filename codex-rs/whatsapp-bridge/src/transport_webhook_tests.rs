use super::*;
use crate::attachment::InboundAttachment;

#[test]
fn image_message_without_text_is_forwarded_with_attachment() {
    let attachment = InboundAttachment::Image {
        mime_type: "image/jpeg".to_string(),
        data_base64: "aW1hZ2U=".to_string(),
    };
    let event = TransportEvent {
        event: "message".to_string(),
        idempotency_key: "event-1".to_string(),
        data: TransportMessage {
            body: String::new(),
            chat_id: "447700900000@c.us".to_string(),
            from_me: true,
            id: "message-1".to_string(),
            is_group: false,
            is_self_chat: true,
            attachment: Some(attachment.clone()),
        },
    };

    assert_eq!(
        filter_inbound(event, "447700900000@c.us", |_| false),
        Some(InboundMessage {
            idempotency_key: "event-1".to_string(),
            message_id: "message-1".to_string(),
            chat_id: "447700900000@c.us".to_string(),
            body: String::new(),
            attachment: Some(attachment),
        })
    );
}

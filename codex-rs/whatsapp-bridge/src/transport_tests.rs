use super::*;
use codex_messaging::ProviderAdapter;
use codex_messaging::ProviderConversationId;
use codex_messaging::ProviderDeliveryId;
use codex_messaging::ProviderMessageId;
use serde_json::json;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_json;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[tokio::test]
async fn http_adapter_matches_gateway_contract() {
    let server = MockServer::start().await;
    let token = "test-token";
    Mock::given(method("GET"))
        .and(path("/v1/status"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"status": "ready", "account": "447700900000"})),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_json(json!({
            "deliveryId": "delivery-1",
            "chatId": "447700900000@c.us",
            "text": "hello"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "message-1"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages/ack"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_json(json!({
            "deliveryId": "delivery-1",
            "messageId": "message-1"
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages/edit"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_json(json!({
            "chatId": "447700900000@c.us",
            "messageId": "message-1",
            "text": "updated"
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let transport = HttpTransportClient::new(server.uri(), token.to_string()).unwrap();
    assert_eq!(transport.capabilities().message_limit, MAX_TEXT_CHARS);
    assert!(transport.capabilities().edit_support);
    assert_eq!(
        ProviderAdapter::status(&transport).await.unwrap(),
        ProviderStatus {
            ready: true,
            account: Some("447700900000".to_string()),
        }
    );
    assert_eq!(
        ProviderAdapter::send_text(
            &transport,
            ProviderDeliveryId::new("delivery-1"),
            ProviderConversationId::new("447700900000@c.us"),
            "hello".to_string(),
        )
        .await
        .unwrap(),
        ProviderMessageId::new("message-1")
    );
    ProviderAdapter::edit_text(
        &transport,
        ProviderConversationId::new("447700900000@c.us"),
        ProviderMessageId::new("message-1"),
        "updated".to_string(),
    )
    .await
    .unwrap();
    ProviderAdapter::acknowledge_delivery(
        &transport,
        ProviderDeliveryId::new("delivery-1"),
        ProviderMessageId::new("message-1"),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn gateway_idempotency_conflict_is_a_permanent_provider_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(409))
        .expect(1)
        .mount(&server)
        .await;

    let transport = HttpTransportClient::new(server.uri(), "test-token".to_string()).unwrap();
    assert_eq!(
        ProviderAdapter::send_text(
            &transport,
            ProviderDeliveryId::new("delivery-1"),
            ProviderConversationId::new("chat"),
            "hello".to_string(),
        )
        .await,
        Err(ProviderError::IdempotencyConflict)
    );
}

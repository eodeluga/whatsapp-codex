use super::*;
use pretty_assertions::assert_eq;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_json;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path;

async fn client(server: &MockServer) -> HttpOpenWaClient {
    HttpOpenWaClient::with_timeout(
        format!("{}/api", server.uri()),
        "personal".to_string(),
        "operator-key".to_string(),
        Duration::from_millis(100),
    )
    .unwrap()
}

#[tokio::test]
async fn provisions_with_a_valid_name_and_uses_the_returned_session_id() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/sessions"))
        .and(header("X-API-Key", "administrator-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/sessions"))
        .and(body_json(serde_json::json!({
            "name": "codex-token-with-symbols"
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "4ba3b15b-7966-41db-9ac7-25b1827acb75",
            "name": "codex-token-with-symbols",
            "status": "created"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(
            "/api/sessions/4ba3b15b-7966-41db-9ac7-25b1827acb75/start",
        ))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/auth/api-keys"))
        .and(body_json(serde_json::json!({
            "name": "WhatsApp Codex bridge",
            "role": "operator",
            "allowedSessions": ["4ba3b15b-7966-41db-9ac7-25b1827acb75"]
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "apiKey": "session-operator-key"
        })))
        .expect(1)
        .mount(&server)
        .await;

    assert_eq!(
        provision_session(
            &format!("{}/api", server.uri()),
            "codex-token_with-symbols",
            "administrator-key",
        )
        .await,
        Ok(ProvisionedSession {
            session_id: "4ba3b15b-7966-41db-9ac7-25b1827acb75".to_string(),
            api_key: "session-operator-key".to_string(),
        })
    );
}

#[tokio::test]
async fn reuses_an_existing_started_session() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/sessions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "id": "4ba3b15b-7966-41db-9ac7-25b1827acb75",
                "name": "codex-existing-session",
                "status": "qr_ready"
            }])),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/auth/api-keys"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "apiKey": "session-operator-key"
        })))
        .expect(1)
        .mount(&server)
        .await;

    assert_eq!(
        provision_session(
            &format!("{}/api", server.uri()),
            "codex-existing-session",
            "administrator-key",
        )
        .await,
        Ok(ProvisionedSession {
            session_id: "4ba3b15b-7966-41db-9ac7-25b1827acb75".to_string(),
            api_key: "session-operator-key".to_string(),
        })
    );
}

#[tokio::test]
async fn sends_text_with_the_api_key_and_reads_message_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/sessions/personal/messages/send-text"))
        .and(header("X-API-Key", "operator-key"))
        .and(body_json(serde_json::json!({
            "chatId": "447700900000@c.us",
            "text": "hello"
        })))
        .respond_with(
            ResponseTemplate::new(201)
                .set_body_json(serde_json::json!({"messageId": "wa-1", "timestamp": 1})),
        )
        .mount(&server)
        .await;

    assert_eq!(
        client(&server)
            .await
            .send_text("447700900000@c.us".to_string(), "hello".to_string())
            .await,
        Ok("wa-1".to_string())
    );
}

#[tokio::test]
async fn resolves_a_lid_to_its_phone_number() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/api/sessions/personal/contacts/172662718488742@lid/phone",
        ))
        .and(header("X-API-Key", "operator-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "contactId": "172662718488742@lid",
            "phone": "447700900000"
        })))
        .expect(1)
        .mount(&server)
        .await;

    assert_eq!(
        client(&server)
            .await
            .resolve_phone("172662718488742@lid".to_string())
            .await,
        Ok(Some("447700900000".to_string()))
    );
}

#[tokio::test]
async fn edits_text_using_the_documented_request_shape() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/sessions/personal/messages/edit"))
        .and(body_json(serde_json::json!({
            "chatId": "447700900000@c.us",
            "messageId": "wa-1",
            "body": "final"
        })))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    assert_eq!(
        client(&server)
            .await
            .edit_text(
                "447700900000@c.us".to_string(),
                "wa-1".to_string(),
                "final".to_string(),
            )
            .await,
        Ok(())
    );
}

#[tokio::test]
async fn updates_an_existing_webhook_instead_of_creating_duplicates() {
    let server = MockServer::start().await;
    let webhook_url = "http://bridge/webhooks/openwa";
    Mock::given(method("GET"))
        .and(path("/api/sessions/personal/webhooks"))
        .and(header("X-API-Key", "operator-key"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "id": "webhook-1",
                "url": webhook_url
            }])),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/sessions/personal/webhooks/webhook-1"))
        .and(body_json(serde_json::json!({
            "url": webhook_url,
            "events": ["message.received", "message.sent", "session.status"],
            "secret": "signing-secret"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "webhook-1",
            "url": webhook_url
        })))
        .expect(1)
        .mount(&server)
        .await;

    assert_eq!(
        client(&server)
            .await
            .register_webhook(webhook_url.to_string(), "signing-secret".to_string())
            .await,
        Ok(())
    );
}

#[tokio::test]
async fn maps_auth_rate_limit_server_and_malformed_responses() {
    for (status, expected) in [
        (401, OpenWaError::Unauthorized),
        (429, OpenWaError::RateLimited),
        (500, OpenWaError::Server),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&server)
            .await;
        let error = client(&server)
            .await
            .send_text("chat".to_string(), "text".to_string())
            .await
            .unwrap_err();
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&expected)
        );
    }

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(201).set_body_string("not-json"))
        .mount(&server)
        .await;
    assert!(matches!(
        client(&server)
            .await
            .send_text("chat".to_string(), "text".to_string())
            .await,
        Err(OpenWaError::InvalidResponse)
    ));
}

#[tokio::test]
async fn times_out_slow_requests() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(201)
                .set_delay(Duration::from_secs(1))
                .set_body_json(serde_json::json!({"messageId": "late"})),
        )
        .mount(&server)
        .await;

    assert!(matches!(
        client(&server)
            .await
            .send_text("chat".to_string(), "text".to_string())
            .await,
        Err(OpenWaError::Transport)
    ));
}

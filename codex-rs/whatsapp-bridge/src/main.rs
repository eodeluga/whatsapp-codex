//! Codex's self-hosted remote-input bridge.

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::routing::post;
use codex_config::load_user_bridge_config;
use codex_config::load_user_whatsapp_config;
use codex_config::load_whatsapp_runtime_config;
use codex_transcript::TranscriptProjectionOptions;
use codex_whatsapp_bridge::CommandCatalog;
use codex_whatsapp_bridge::codex::CodexClient;
use codex_whatsapp_bridge::codex::RemoteCodexClient;
use codex_whatsapp_bridge::coordinator::Coordinator;
use codex_whatsapp_bridge::coordinator::CoordinatorCommand;
use codex_whatsapp_bridge::health::BridgeReadiness;
use codex_whatsapp_bridge::health::BridgeReadinessSnapshot;
use codex_whatsapp_bridge::state::BridgeState;
use codex_whatsapp_bridge::transport::HttpTransportClient;
use codex_whatsapp_bridge::transport::TransportClient;
use codex_whatsapp_bridge::transport_webhook::filter_inbound;
use codex_whatsapp_bridge::transport_webhook::parse_verified_event;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
struct TransportState {
    secret: Arc<Vec<u8>>,
    self_chat_id: String,
    commands: mpsc::Sender<CoordinatorCommand>,
    readiness: Arc<BridgeReadiness>,
    transport: HttpTransportClient,
    bridge_name: String,
}

const MAX_WEBHOOK_BODY_BYTES: usize = 72 * 1024 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt().with_target(false).try_init();
    let Some(config_path) = parse_config_path()? else {
        let response = reqwest::get("http://127.0.0.1:8787/health/ready").await?;
        let status = response.status();
        if !status.is_success() {
            let details = response.text().await.unwrap_or_default();
            anyhow::bail!("bridge is not ready ({status}): {details}");
        }
        return Ok(());
    };
    let Some(config) = load_user_whatsapp_config(&config_path)? else {
        anyhow::bail!("[whatsapp] is not configured");
    };
    let bridge_config = load_user_bridge_config(&config_path)?.unwrap_or_default();
    if !config.enabled {
        return Ok(());
    }
    config.validate()?;
    let codex_home = config_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("WhatsApp config path has no parent directory"))?;
    let (command_catalog, command_catalog_path) = CommandCatalog::load_or_create(codex_home)?;
    let runtime = load_whatsapp_runtime_config(codex_home)?;
    let self_chat_id = config.self_chat_jid()?;
    let configured_phone = config
        .account_phone_number
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("configured WhatsApp phone number is missing"))?
        .trim_start_matches('+')
        .to_string();
    let socket_path = codex_app_server_client::app_server_control_socket_path(codex_home)?;
    let codex = match RemoteCodexClient::connect_unix(socket_path.clone()).await {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(%error, "Codex app-server is unavailable; reconnecting in the background");
            RemoteCodexClient::disconnected_unix(socket_path)
        }
    };
    let mut app_server_connected = codex.is_connected();
    let state_path = runtime.state_path.clone();
    let attachment_dir = env::var_os("CODEX_WHATSAPP_ATTACHMENT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| codex_home.join("whatsapp").join("attachments"));
    let mut state = BridgeState::load(&state_path)?;
    if let Some(binding) = state.binding.clone() {
        if binding.self_chat_id != self_chat_id {
            anyhow::bail!("persisted WhatsApp binding does not match the configured account");
        }
        if let Some(active) = state.active_turn.as_mut()
            && active.thread_id.is_empty()
        {
            active.thread_id = binding.codex_thread_id.clone();
            state.save(&state_path)?;
        }
        if app_server_connected && codex.resume_thread(binding.codex_thread_id).await.is_err() {
            app_server_connected = false;
        }
    }
    let transport = HttpTransportClient::new(
        runtime.transport_base_url.clone(),
        runtime.transport_api_token.clone(),
    )?;
    let transport_healthy = match transport.status().await {
        Ok(status) if status.status.eq_ignore_ascii_case("ready") => {
            if status
                .account
                .as_deref()
                .is_some_and(|account| phone_digits(account) != configured_phone)
            {
                anyhow::bail!("paired WhatsApp account does not match the configured phone number");
            }
            true
        }
        Ok(status) => {
            tracing::info!(
                status = status.status,
                "remote transport is waiting for pairing"
            );
            false
        }
        Err(_) => {
            tracing::warn!("remote transport is unavailable; bridge will retry in the background");
            false
        }
    };
    let readiness = Arc::new(BridgeReadiness::new(BridgeReadinessSnapshot {
        ready: app_server_connected && transport_healthy,
        state_healthy: true,
        app_server_connected,
        transport_healthy,
    }));
    let (commands, command_rx) = mpsc::channel(128);
    let coordinator = tokio::spawn(
        Coordinator::new(
            codex,
            transport.clone(),
            state,
            state_path,
            attachment_dir,
            configured_phone,
            self_chat_id.clone(),
            runtime.output_chunk_chars,
            runtime.edit_interval_ms,
            runtime.max_queued_prompts,
            runtime.dedupe_capacity,
            runtime.dedupe_ttl_hours,
            command_catalog,
            command_catalog_path,
            Arc::clone(&readiness),
            app_server_connected,
            transport_healthy,
        )
        .with_transcript_options(TranscriptProjectionOptions {
            include_reasoning: bridge_config.include_reasoning,
            include_tool_calls: bridge_config.include_tool_calls,
            include_automatic_approval_reviews: bridge_config.include_automatic_approval_reviews,
        })
        .with_approval_notices(bridge_config.include_approval_notices)
        .run(command_rx),
    );
    let app = Router::new()
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(health_ready))
        .route("/pairing", get(pairing_qr))
        .route("/webhooks/transport", post(transport_webhook))
        .layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY_BYTES))
        .with_state(TransportState {
            secret: Arc::new(runtime.webhook_signing_secret.into_bytes()),
            self_chat_id,
            commands: commands.clone(),
            readiness,
            transport,
            bridge_name: runtime.bridge_name,
        });
    let listener = tokio::net::TcpListener::bind(&runtime.bridge_listen).await?;
    let (shutdown_started, shutdown_wait) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        let _ = shutdown_started.send(());
    });
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown_wait.await;
        })
        .await;
    let (complete, complete_rx) = tokio::sync::oneshot::channel();
    let _ = commands.send(CoordinatorCommand::Shutdown(complete)).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(15), complete_rx).await;
    let _ = coordinator.await;
    server_result?;
    Ok(())
}

async fn health_ready(
    State(state): State<TransportState>,
) -> (StatusCode, Json<BridgeReadinessSnapshot>) {
    let snapshot = state.readiness.snapshot();
    let status = if snapshot.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(snapshot))
}

async fn pairing_qr(State(state): State<TransportState>) -> Result<Html<String>, StatusCode> {
    let page = match state.transport.status().await {
        Ok(status) if status.status.eq_ignore_ascii_case("ready") => format!(
            "<!doctype html><meta http-equiv=\"refresh\" content=\"20\"><title>{} connected</title><h1>{} connected</h1><p>The account is paired. This page will continue checking the connection.</p>",
            state.bridge_name, state.bridge_name
        ),
        Ok(status) => match state.transport.pairing_qr().await {
            Ok(Some(qr_code)) => format!(
                "<!doctype html><meta http-equiv=\"refresh\" content=\"20\"><title>Pair {}</title><h1>Pair {}</h1><p>Open the transport's linked devices screen and scan this code.</p><img src=\"{}\" alt=\"{} pairing QR code\">",
                state.bridge_name, state.bridge_name, qr_code, state.bridge_name
            ),
            Ok(None) => format!(
                "<!doctype html><meta http-equiv=\"refresh\" content=\"5\"><title>Preparing {}</title><h1>Preparing {}</h1><p>The gateway is {}. This page will refresh automatically.</p>",
                state.bridge_name, state.bridge_name, status.status
            ),
            Err(_) => pairing_retry_page(&state.bridge_name),
        },
        Err(_) => pairing_retry_page(&state.bridge_name),
    };
    Ok(Html(page))
}

fn pairing_retry_page(bridge_name: &str) -> String {
    format!(
        "<!doctype html><meta http-equiv=\"refresh\" content=\"5\"><title>{bridge_name} unavailable</title><h1>{bridge_name} is starting</h1><p>The gateway is temporarily unavailable. This page will retry automatically; leave it open.</p>"
    )
}

async fn transport_webhook(
    State(state): State<TransportState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let Some(signature) = headers
        .get("X-Codex-Transport-Signature")
        .and_then(|value| value.to_str().ok())
    else {
        return StatusCode::UNAUTHORIZED;
    };
    let Ok(event) = parse_verified_event(&state.secret, signature, &body) else {
        return StatusCode::UNAUTHORIZED;
    };
    tracing::info!(
        event = %event.event,
        message_id = %event.data.id,
        chat_id = %event.data.chat_id,
        from_me = event.data.from_me,
        is_group = event.data.is_group,
        has_body = !event.data.body.is_empty(),
        "received transport webhook"
    );
    let Some(message) = filter_inbound(event, &state.self_chat_id, |_| false) else {
        tracing::info!("ignored transport webhook message");
        return StatusCode::NO_CONTENT;
    };
    let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
    if state
        .commands
        .try_send(CoordinatorCommand::Inbound { message, accepted })
        .is_err()
    {
        tracing::warn!("unable to queue transport webhook message");
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match tokio::time::timeout(std::time::Duration::from_secs(5), accepted_rx).await {
        Ok(Ok(true)) => StatusCode::ACCEPTED,
        Ok(Ok(false) | Err(_)) | Err(_) => {
            tracing::warn!("transport webhook message was not accepted");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
fn phone_digits(value: &str) -> String {
    value.chars().filter(char::is_ascii_digit).collect()
}
fn parse_config_path() -> anyhow::Result<Option<PathBuf>> {
    let mut arguments = env::args_os().skip(1);
    match arguments.next() {
        None => Ok(Some(PathBuf::from("/codex-home/config.toml"))),
        Some(argument) if argument == "--config" => arguments
            .next()
            .map(PathBuf::from)
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("--config requires a path")),
        Some(argument) if argument == "--healthcheck" && arguments.next().is_none() => Ok(None),
        Some(_) => anyhow::bail!("usage: codex-whatsapp-bridge [--config PATH | --healthcheck]"),
    }
}

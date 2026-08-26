//! Codex's self-hosted remote-input bridge.

use axum::Router;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::routing::post;
use codex_config::load_user_whatsapp_config;
use codex_config::load_whatsapp_runtime_config;
use codex_whatsapp_bridge::CommandCatalog;
use codex_whatsapp_bridge::codex::CodexClient;
use codex_whatsapp_bridge::codex::RemoteCodexClient;
use codex_whatsapp_bridge::coordinator::Coordinator;
use codex_whatsapp_bridge::coordinator::CoordinatorCommand;
use codex_whatsapp_bridge::state::BridgeState;
use codex_whatsapp_bridge::transport::HttpTransportClient;
use codex_whatsapp_bridge::transport::TransportClient;
use codex_whatsapp_bridge::transport_webhook::filter_inbound;
use codex_whatsapp_bridge::transport_webhook::parse_verified_event;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

#[derive(Clone)]
struct TransportState {
    secret: Arc<Vec<u8>>,
    self_chat_id: String,
    commands: mpsc::Sender<CoordinatorCommand>,
    ready: Arc<AtomicBool>,
    transport: HttpTransportClient,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt().with_target(false).try_init();
    let Some(config_path) = parse_config_path()? else {
        let response = reqwest::get("http://127.0.0.1:8787/health/ready").await?;
        if !response.status().is_success() {
            anyhow::bail!("bridge is not ready");
        }
        return Ok(());
    };
    let Some(config) = load_user_whatsapp_config(&config_path)? else {
        anyhow::bail!("[whatsapp] is not configured");
    };
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
        Err(_) => RemoteCodexClient::disconnected_unix(socket_path),
    };
    let mut app_server_connected = codex.is_connected();
    let state_path = runtime.state_path.clone();
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
    let ready = Arc::new(AtomicBool::new(app_server_connected && transport_healthy));
    let (commands, command_rx) = mpsc::channel(128);
    let coordinator = tokio::spawn(
        Coordinator::new(
            codex,
            transport.clone(),
            state,
            state_path,
            configured_phone,
            self_chat_id.clone(),
            runtime.output_chunk_chars,
            runtime.edit_interval_ms,
            runtime.max_queued_prompts,
            runtime.dedupe_capacity,
            runtime.dedupe_ttl_hours,
            command_catalog,
            command_catalog_path,
            Arc::clone(&ready),
            app_server_connected,
            transport_healthy,
        )
        .run(command_rx),
    );
    let app = Router::new()
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(health_ready))
        .route("/pairing", get(pairing_qr))
        .route("/webhooks/transport", post(transport_webhook))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(TransportState {
            secret: Arc::new(runtime.webhook_signing_secret.into_bytes()),
            self_chat_id,
            commands: commands.clone(),
            ready,
            transport,
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

async fn health_ready(State(state): State<TransportState>) -> StatusCode {
    if state.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn pairing_qr(State(state): State<TransportState>) -> Result<Html<String>, StatusCode> {
    let status = state
        .transport
        .status()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if status.status.eq_ignore_ascii_case("ready") {
        return Ok(Html("<!doctype html><title>Pairing complete</title><h1>Pairing complete</h1><p>WhatsApp Codex is connected. You may now close this page.</p>".to_string()));
    }
    let qr_code = state
        .transport
        .pairing_qr()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Html(format!(
        "<!doctype html><meta http-equiv=\"refresh\" content=\"20\"><title>Pair WhatsApp Codex</title><h1>Pair WhatsApp Codex</h1><p>In WhatsApp, open Linked devices and scan this code.</p><img src=\"{qr_code}\" alt=\"WhatsApp pairing QR code\">"
    )))
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
    let Some(message) = filter_inbound(event, &state.self_chat_id, |_| false) else {
        return StatusCode::NO_CONTENT;
    };
    let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
    if state
        .commands
        .try_send(CoordinatorCommand::Inbound { message, accepted })
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match tokio::time::timeout(std::time::Duration::from_secs(5), accepted_rx).await {
        Ok(Ok(true)) => StatusCode::ACCEPTED,
        Ok(Ok(false) | Err(_)) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
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

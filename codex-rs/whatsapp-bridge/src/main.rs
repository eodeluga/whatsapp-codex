//! Codex's self-hosted WhatsApp bridge.

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
use codex_config::save_whatsapp_runtime_config;
use codex_whatsapp_bridge::codex::CodexClient;
use codex_whatsapp_bridge::codex::RemoteCodexClient;
use codex_whatsapp_bridge::coordinator::Coordinator;
use codex_whatsapp_bridge::coordinator::CoordinatorCommand;
use codex_whatsapp_bridge::openwa::HttpOpenWaClient;
use codex_whatsapp_bridge::openwa::OpenWaClient;
use codex_whatsapp_bridge::openwa::provision_session;
use codex_whatsapp_bridge::state::BridgeState;
use codex_whatsapp_bridge::webhook::filter_inbound;
use codex_whatsapp_bridge::webhook::parse_verified_webhook;
use std::env;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio::time::Instant;

const OPENWA_ADMINISTRATOR_KEY_PATH: &str = "/run/codex-whatsapp/openwa-administrator-key";
const OPENWA_ADMINISTRATOR_KEY_WAIT: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct WebhookState {
    secret: Arc<Vec<u8>>,
    session_id: String,
    self_chat_id: String,
    commands: mpsc::Sender<CoordinatorCommand>,
    ready: Arc<AtomicBool>,
    openwa: HttpOpenWaClient,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .try_init();
    let Some(config_path) = parse_config_path()? else {
        let response = reqwest::get("http://127.0.0.1:8787/health/ready").await?;
        if !response.status().is_success() {
            anyhow::bail!("bridge is not ready");
        }
        return Ok(());
    };
    let config = load_user_whatsapp_config(&config_path)?;
    let Some(config) = config else {
        anyhow::bail!("[whatsapp] is not configured");
    };
    if !config.enabled {
        return Ok(());
    }
    config.validate()?;
    let codex_home = config_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("WhatsApp config path has no parent directory"))?;
    let mut runtime = load_whatsapp_runtime_config(codex_home)?;
    let self_chat_id = config.self_chat_jid()?;
    let socket_path = codex_app_server_client::app_server_control_socket_path(codex_home)?;
    let codex = match RemoteCodexClient::connect_unix(socket_path.clone()).await {
        Ok(client) => client,
        Err(_) => {
            tracing::warn!("Codex app-server is unavailable; bridge will retry in the background");
            RemoteCodexClient::disconnected_unix(socket_path)
        }
    };
    let mut app_server_connected = codex.is_connected();
    let state_path = runtime.state_path.clone();
    let mut state = BridgeState::load(&state_path)?;
    if let Some(binding) = state.binding.clone() {
        if binding.openwa_session_id != runtime.openwa_session_id
            || binding.self_chat_id != self_chat_id
        {
            anyhow::bail!(
                "persisted WhatsApp binding does not match the configured session or account"
            );
        }
        if let Some(active) = state.active_turn.as_mut()
            && active.thread_id.is_empty()
        {
            active.thread_id = binding.codex_thread_id.clone();
            state.save(&state_path)?;
        }
        if app_server_connected {
            match codex.resume_thread(binding.codex_thread_id.clone()).await {
                Ok(_) => {}
                Err(_) => {
                    tracing::warn!(
                        "stored Codex thread could not be resumed; preserving it while the bridge retries"
                    );
                    app_server_connected = false;
                }
            }
        }
    }
    let mut openwa_client = HttpOpenWaClient::new(
        runtime.openwa_api_base_url.clone(),
        runtime.openwa_session_id.clone(),
        runtime.openwa_api_key.clone(),
    )?;
    let configured_phone = config
        .account_phone_number
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("configured WhatsApp phone number is missing"))?
        .trim_start_matches('+')
        .to_string();
    let webhook_secret = runtime.webhook_signing_secret.clone();
    if matches!(
        openwa_client.session_status().await,
        Err(codex_whatsapp_bridge::openwa::OpenWaError::Unauthorized)
    ) {
        let administrator_key = wait_for_openwa_administrator_key().await?;
        let provisioned = provision_session(
            &runtime.openwa_api_base_url,
            &runtime.openwa_session_id,
            administrator_key.trim(),
        )
        .await?;
        runtime.openwa_session_id = provisioned.session_id;
        runtime.openwa_api_key = provisioned.api_key;
        save_whatsapp_runtime_config(codex_home, &runtime)?;
        openwa_client = HttpOpenWaClient::new(
            runtime.openwa_api_base_url.clone(),
            runtime.openwa_session_id.clone(),
            runtime.openwa_api_key.clone(),
        )?;
    }
    let openwa_healthy = match openwa_client.session_status().await {
        Ok(session) => {
            if session
                .phone
                .as_deref()
                .is_some_and(|phone| phone_digits(phone) != configured_phone)
            {
                anyhow::bail!("paired WhatsApp account does not match the configured phone number");
            }
            if !session.status.eq_ignore_ascii_case("ready") {
                tracing::info!(
                    status = session.status,
                    "OpenWA session is waiting for WhatsApp pairing"
                );
                false
            } else {
                let registered = openwa_client
                    .register_webhook(runtime.webhook_url.clone(), webhook_secret.clone())
                    .await
                    .is_ok();
                if !registered {
                    tracing::warn!("OpenWA webhook registration failed; bridge is not ready");
                }
                registered
            }
        }
        Err(_) => {
            tracing::warn!("OpenWA is unavailable; bridge will retry in the background");
            false
        }
    };
    let ready = Arc::new(AtomicBool::new(app_server_connected && openwa_healthy));
    let (commands, command_rx) = mpsc::channel(128);
    let coordinator = tokio::spawn(
        Coordinator::new(
            codex,
            openwa_client.clone(),
            state,
            state_path,
            runtime.openwa_session_id.clone(),
            configured_phone,
            runtime.webhook_url.clone(),
            webhook_secret.clone(),
            self_chat_id.clone(),
            runtime.output_chunk_chars,
            runtime.edit_interval_ms,
            runtime.max_queued_prompts,
            runtime.dedupe_capacity,
            runtime.dedupe_ttl_hours,
            Arc::clone(&ready),
            app_server_connected,
            openwa_healthy,
        )
        .run(command_rx),
    );
    let app = Router::new()
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(health_ready))
        .route("/pairing", get(pairing_qr))
        .route("/webhooks/openwa", post(openwa_webhook))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(WebhookState {
            secret: Arc::new(webhook_secret.into_bytes()),
            session_id: runtime.openwa_session_id,
            self_chat_id,
            commands: commands.clone(),
            ready,
            openwa: openwa_client,
        });
    let listener = tokio::net::TcpListener::bind(&runtime.bridge_listen).await?;
    tracing::info!(listen = runtime.bridge_listen, "WhatsApp bridge is ready");
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
    let (shutdown_complete, shutdown_complete_rx) = tokio::sync::oneshot::channel();
    let _ = commands
        .send(CoordinatorCommand::Shutdown(shutdown_complete))
        .await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(15), shutdown_complete_rx).await;
    let _ = coordinator.await;
    server_result?;
    Ok(())
}

async fn wait_for_openwa_administrator_key() -> anyhow::Result<String> {
    tracing::info!("waiting for OpenWA administrator key");
    let deadline = Instant::now() + OPENWA_ADMINISTRATOR_KEY_WAIT;
    loop {
        match tokio::fs::read_to_string(OPENWA_ADMINISTRATOR_KEY_PATH).await {
            Ok(key) if !key.trim().is_empty() => return Ok(key),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            anyhow::bail!(
                "OpenWA administrator key was not written to {OPENWA_ADMINISTRATOR_KEY_PATH} within {OPENWA_ADMINISTRATOR_KEY_WAIT:?}"
            );
        }
        tokio::time::sleep(remaining.min(Duration::from_secs(1))).await;
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => Some(signal),
                Err(error) => {
                    tracing::warn!(%error, "failed to install SIGTERM handler");
                    None
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                match terminate.as_mut() {
                    Some(signal) => signal.recv().await,
                    None => std::future::pending().await,
                }
            } => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn health_ready(State(state): State<WebhookState>) -> StatusCode {
    if state.ready.load(Ordering::Acquire) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn pairing_qr(State(state): State<WebhookState>) -> Result<Html<String>, StatusCode> {
    let response = state
        .openwa
        .pairing_qr()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let qr_code = response
        .get("qrCode")
        .and_then(serde_json::Value::as_str)
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let encoded = qr_code
        .strip_prefix("data:image/png;base64,")
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    if encoded.is_empty()
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
    {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Html(format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta http-equiv=\"refresh\" content=\"20\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Pair WhatsApp Codex</title></head><body style=\"font-family:sans-serif;text-align:center;margin:2rem\"><h1>Pair WhatsApp Codex</h1><p>In WhatsApp, open Linked devices and scan this code.</p><img src=\"{qr_code}\" alt=\"WhatsApp pairing QR code\" style=\"width:min(90vw,420px);height:auto\"><p>This page refreshes automatically while pairing.</p></body></html>"
    )))
}

async fn openwa_webhook(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let Some(signature) = headers
        .get("X-OpenWA-Signature")
        .and_then(|value| value.to_str().ok())
    else {
        return StatusCode::UNAUTHORIZED;
    };
    let Ok(webhook) = parse_verified_webhook(&state.secret, signature, &body) else {
        return StatusCode::UNAUTHORIZED;
    };
    let Some(message) = filter_inbound(webhook, &state.session_id, &state.self_chat_id, |_| false)
    else {
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

fn phone_digits(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>()
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
        Some(_) => {
            anyhow::bail!("usage: codex-whatsapp-bridge [--config PATH | --healthcheck]")
        }
    }
}

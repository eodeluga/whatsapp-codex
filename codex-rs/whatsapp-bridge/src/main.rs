//! Codex's self-hosted WhatsApp bridge.

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::routing::get;
use axum::routing::post;
use codex_config::load_user_whatsapp_config;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_whatsapp_bridge::codex::RemoteCodexClient;
use codex_whatsapp_bridge::coordinator::Coordinator;
use codex_whatsapp_bridge::coordinator::CoordinatorCommand;
use codex_whatsapp_bridge::openwa::HttpOpenWaClient;
use codex_whatsapp_bridge::state::BridgeState;
use codex_whatsapp_bridge::webhook::filter_inbound;
use codex_whatsapp_bridge::webhook::parse_verified_webhook;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
struct WebhookState {
    secret: Arc<Vec<u8>>,
    session_id: String,
    self_chat_id: String,
    trigger_prefix: String,
    commands: mpsc::Sender<CoordinatorCommand>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = parse_config_path()?;
    let config = load_user_whatsapp_config(&config_path)?;
    let Some(config) = config else {
        anyhow::bail!("[whatsapp] is not configured");
    };
    if !config.enabled {
        return Ok(());
    }
    config.validate()?;
    let openwa = config.openwa.as_ref().expect("validated OpenWA config");
    let bridge = config.bridge.as_ref().cloned().unwrap_or_default();
    let workspace = config.workspace.clone().expect("validated workspace");
    let self_chat_id = config.self_chat_jid()?;
    let endpoint = bridge.app_server_endpoint();
    let socket_path = endpoint
        .strip_prefix("unix://")
        .ok_or_else(|| anyhow::anyhow!("only unix app-server endpoints are supported"))?;
    let codex =
        RemoteCodexClient::connect_unix(AbsolutePathBuf::from_absolute_path(socket_path)?).await?;
    let state_path = bridge.state_path().to_path_buf();
    let mut state = BridgeState::load(&state_path)?;
    if let Some(binding) = &state.binding
        && codex
            .resume_thread(binding.codex_thread_id.clone())
            .await
            .is_err()
    {
        tracing::warn!(
            "stored Codex thread could not be resumed; a new thread will be created on the next prompt"
        );
        state.binding = None;
        state.active_turn = None;
        state.save(&state_path)?;
    }
    let openwa_client = HttpOpenWaClient::new(
        openwa.api_base_url().to_string(),
        openwa.session_id.clone().expect("validated session ID"),
        openwa.api_key.clone().expect("validated API key"),
    );
    let (commands, command_rx) = mpsc::channel(128);
    tokio::spawn(
        Coordinator::new(
            codex,
            openwa_client,
            state,
            state_path,
            workspace,
            openwa.session_id.clone().expect("validated session ID"),
            self_chat_id.clone(),
            config.trigger_prefix().to_string(),
            bridge.output_chunk_chars(),
            bridge.max_queued_prompts(),
        )
        .run(command_rx),
    );
    let secret = base64::Engine::decode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        openwa
            .webhook_signing_secret
            .as_deref()
            .expect("validated signing secret"),
    )?;
    let app = Router::new()
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(|| async { StatusCode::OK }))
        .route("/webhooks/openwa", post(openwa_webhook))
        .with_state(WebhookState {
            secret: Arc::new(secret),
            session_id: openwa.session_id.clone().expect("validated session ID"),
            self_chat_id,
            trigger_prefix: config.trigger_prefix().to_string(),
            commands,
        });
    let listener = tokio::net::TcpListener::bind(bridge.listen()).await?;
    tracing::info!(listen = bridge.listen(), "WhatsApp bridge is ready");
    axum::serve(listener, app).await?;
    Ok(())
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
    let Some(message) = filter_inbound(
        webhook,
        &state.session_id,
        &state.self_chat_id,
        &state.trigger_prefix,
        |_| false,
    ) else {
        return StatusCode::NO_CONTENT;
    };
    let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
    if state
        .commands
        .send(CoordinatorCommand::Inbound { message, accepted })
        .await
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match accepted_rx.await {
        Ok(true) => StatusCode::ACCEPTED,
        Ok(false) | Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn parse_config_path() -> anyhow::Result<PathBuf> {
    let mut arguments = env::args_os().skip(1);
    match arguments.next() {
        None => Ok(PathBuf::from("/codex-home/config.toml")),
        Some(argument) if argument == "--config" => arguments
            .next()
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("--config requires a path")),
        Some(_) => anyhow::bail!("usage: codex-whatsapp-bridge [--config PATH]"),
    }
}

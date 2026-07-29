# WhatsApp Codex

WhatsApp Codex is a self-hosted coding agent that lets you use one private
WhatsApp self-chat to control a normal Codex app-server thread. OpenWA owns the
WhatsApp session; the bridge handles signed webhooks, durable delivery, queueing,
approvals, and the existing Codex app-server protocol. It does not run a second
agent loop.

## Quickstart

### Prerequisites

- Linux or macOS with a current Rust toolchain.
- Docker Engine with Docker Compose v2.
- A WhatsApp account and a private self-chat.
- A workspace directory that the host Codex app-server can access.

All commands below are run from the repository root unless stated otherwise.

### 1. Build WhatsApp Codex

```shell
cd codex-rs
cargo build --release -p codex-cli -p codex-whatsapp-bridge
cd ..
```

The binaries are created at `codex-rs/target/release/codex` and
`codex-rs/target/release/codex-whatsapp-bridge`.

### 2. Use the normal Codex configuration directory

WhatsApp Codex uses the normal Codex home directory, `~/.codex` by default.
The TUI creates and manages `~/.codex/config.toml`; OpenWA credentials and the
webhook secret are stored there, never in a deployment environment file.

Advanced deployments may override the home directory with `CODEX_HOME` and the
container user with `UID`/`GID`, but neither is part of normal setup.

### 3. Start OpenWA and link WhatsApp

```shell
docker compose \
  -f codex-rs/whatsapp-bridge/deploy/compose.yaml build

docker compose \
  -f codex-rs/whatsapp-bridge/deploy/compose.yaml up -d openwa
```

Read the initial OpenWA administrator key:

```shell
docker compose \
  -f codex-rs/whatsapp-bridge/deploy/compose.yaml exec openwa \
  sh -c 'cat /app/data/.api-key'
```

Use that key temporarily as `OPENWA_ADMIN_KEY` to create and start a session:

```shell
export OPENWA_ADMIN_KEY='replace-with-the-admin-key'

curl -sS -X POST http://127.0.0.1:2785/api/sessions \
  -H "X-API-Key: $OPENWA_ADMIN_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"codex-personal"}'

curl -sS -X POST \
  http://127.0.0.1:2785/api/sessions/SESSION_ID/start \
  -H "X-API-Key: $OPENWA_ADMIN_KEY"

curl -sS \
  http://127.0.0.1:2785/api/sessions/SESSION_ID/qr \
  -H "X-API-Key: $OPENWA_ADMIN_KEY"
```

Scan the QR code in WhatsApp. Then create a session-scoped operator key; the
plaintext key is returned only once:

```shell
curl -sS -X POST http://127.0.0.1:2785/api/auth/api-keys \
  -H "X-API-Key: $OPENWA_ADMIN_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"WhatsApp Codex bridge",
    "role":"operator",
    "allowedSessions":["SESSION_ID"]
  }'
```

Replace `SESSION_ID` in the commands with the ID returned by OpenWA. OpenWA is
pinned to an upstream commit in
[`compose.yaml`](codex-rs/whatsapp-bridge/deploy/compose.yaml), uses the
`whatsapp-web.js` engine, and keeps its session data in a named volume.

### 4. Configure the WhatsApp integration

Start the locally built Codex TUI:

```shell
./codex-rs/target/release/codex
```

Complete the WhatsApp setup step with:

- your own WhatsApp number in canonical E.164 form, such as `+447700900000`;
- the OpenWA session ID;
- the session-scoped operator key;
- the host workspace Codex should use.

The TUI generates the webhook signing secret, masks credentials, shows a
redacted review, and writes the complete table to the base user config. A
“Not now” choice writes `enabled = false` and prevents the step from returning.

For a manual configuration, add this to `$CODEX_HOME/config.toml` and replace
the example values:

```toml
[whatsapp]
onboarding_complete = true
enabled = true
account_phone_number = "+447700900000"
workspace = "/absolute/path/to/workspace"
trigger_prefix = "!codex "

[whatsapp.openwa]
api_base_url = "http://openwa:2785/api"
session_id = "SESSION_ID"
api_key = "session-scoped-operator-key"
webhook_signing_secret = "at-least-32-random-bytes-encoded-as-base64url"
webhook_url = "http://codex-whatsapp-bridge:8787/webhooks/openwa"

[whatsapp.bridge]
app_server_endpoint = "unix:///codex-home/app-server-control/app-server-control.sock"
listen = "0.0.0.0:8787"
state_path = "/codex-home/whatsapp/state.json"
max_queued_prompts = 20
output_chunk_chars = 3500
edit_interval_ms = 1500
dedupe_capacity = 10000
dedupe_ttl_hours = 168
```

Protect this file. It contains the OpenWA API key and webhook secret and should
not be committed or shared. On Unix, WhatsApp Codex restricts it and the bridge
state file to mode `0600`.

### 5. Start Codex app-server

Keep this process running on the host so Codex can access the real workspace,
credentials, and sandbox:

```shell
./codex-rs/target/release/codex app-server --listen unix://
```

The deployment mounts the app-server control-socket directory into the bridge
container. The workspace itself is not mounted into the bridge.

### 6. Start the WhatsApp Codex bridge

```shell
docker compose \
  -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  up -d codex-whatsapp-bridge
```

The bridge checks the OpenWA session state and account number, updates or
creates the signed webhook, resumes the persisted Codex thread, and exposes
health endpoints. OpenWA is published only on loopback; the bridge remains on
the private Compose network.

### 7. Use WhatsApp Codex

Send messages from the configured self-chat using the exact prefix:

```text
!codex status
!codex inspect the current workspace
!codex approve TOKEN
!codex deny TOKEN
!codex answer TOKEN your answer
!codex stop
!codex new
!codex help
```

Prompts received during an active turn are queued in FIFO order. Command and
file-change approvals remain pending until answered. Responses are chunked for
WhatsApp and delivered back to the same self-chat without triggering a loop.

## Recovery and health

```shell
docker compose \
  -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  exec codex-whatsapp-bridge codex-whatsapp-bridge --healthcheck

docker compose \
  -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  logs openwa codex-whatsapp-bridge
```

Bridge state is stored at `$CODEX_HOME/whatsapp/state.json`. It contains the
thread binding, deduplication records, queued prompts, outbound IDs, and
undelivered responses. If app-server disconnects, prompts remain durable and
reconnect uses bounded backoff. If OpenWA disconnects, final responses remain
in the outbox and are retried without restarting the Codex turn.

For a restart check, stop and restart OpenWA, the host app-server, and the
bridge in that order. Confirm that `!codex status` reports the same thread and
that a replayed webhook does not create a second turn.

## Development and documentation

From `codex-rs`, use the repository workflows rather than invoking `cargo test`
directly:

```shell
just fmt
just test -p codex-whatsapp-bridge
just test -p codex-config
just test -p codex-tui
```

Detailed bridge deployment notes are in
[`codex-rs/whatsapp-bridge/README.md`](codex-rs/whatsapp-bridge/README.md).
General repository development guidance is in
[`docs/contributing.md`](docs/contributing.md) and
[`docs/install.md`](docs/install.md).

This repository is licensed under the [Apache-2.0 License](LICENSE).

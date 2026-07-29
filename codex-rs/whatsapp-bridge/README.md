# codex-whatsapp-bridge

This binary connects one private WhatsApp self-chat to a normal Codex
app-server thread. OpenWA owns WhatsApp connectivity; the bridge only filters
signed webhooks, persists delivery state, and speaks the existing app-server
protocol. It does not contain a second agent loop.

## Build and deploy

The supplied Compose file builds OpenWA from the pinned upstream commit
`82b2499dc13a93af922330e2be432174bf0e38a4` and builds the bridge from this
checkout. OpenWA needs internet egress for WhatsApp; the bridge remains on the
private internal network. OpenWA's SSRF guard remains enabled, with only the
Compose hostname `codex-whatsapp-bridge` allowlisted for webhook delivery.

```bash
cd codex-rs
cargo build --locked --release -p codex-whatsapp-bridge
cp whatsapp-bridge/deploy/.env.example whatsapp-bridge/deploy/.env
# Set CODEX_HOME, UID, and GID in .env. Do not put credentials there.
docker compose --env-file whatsapp-bridge/deploy/.env \
  -f whatsapp-bridge/deploy/compose.yaml build
docker compose --env-file whatsapp-bridge/deploy/.env \
  -f whatsapp-bridge/deploy/compose.yaml up -d openwa
```

OpenWA writes its initial admin key to `/app/data/.api-key` and prints it once
in its startup log. Keep it private:

```bash
docker compose --env-file whatsapp-bridge/deploy/.env \
  -f whatsapp-bridge/deploy/compose.yaml exec openwa \
  sh -c 'cat /app/data/.api-key'
```

Use that admin key as `OPENWA_ADMIN_KEY` only in your current shell. Create and
start a session:

```bash
curl -sS -X POST http://127.0.0.1:2785/api/sessions \
  -H "X-API-Key: $OPENWA_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"codex-personal"}'

curl -sS -X POST \
  http://127.0.0.1:2785/api/sessions/SESSION_ID/start \
  -H "X-API-Key: $OPENWA_ADMIN_KEY"

curl -sS http://127.0.0.1:2785/api/sessions/SESSION_ID/qr \
  -H "X-API-Key: $OPENWA_ADMIN_KEY"
```

Scan the returned QR code in WhatsApp, then create a session-scoped operator
key. Its plaintext `apiKey` is returned only once:

```bash
curl -sS -X POST http://127.0.0.1:2785/api/auth/api-keys \
  -H "X-API-Key: $OPENWA_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"Codex bridge",
    "role":"operator",
    "allowedSessions":["SESSION_ID"]
  }'
```

Run the normal Codex TUI. Its WhatsApp onboarding step asks for the linked
account number, host workspace, session ID, and operator key. It generates the
webhook signing secret locally, shows a redacted review, and writes the base
user `$CODEX_HOME/config.toml` through `config/batchWrite`. Choosing “Not now”
persists an explicit disabled opt-out.

The resulting configuration resembles:

```toml
[whatsapp]
onboarding_complete = true
enabled = true
account_phone_number = "+447700900000"
workspace = "/absolute/path/to/workspace"

[whatsapp.openwa]
session_id = "SESSION_ID"
api_key = "owa_k1_session-scoped-operator-key"
webhook_signing_secret = "generated-base64url-secret"
```

Treat `config.toml` as a secret. Do not commit or share it. On Unix, onboarding
restricts it to mode `0600`; bridge state is also written with mode `0600`.

Start app-server on the host, then the bridge:

```bash
codex app-server --listen unix://
docker compose --env-file whatsapp-bridge/deploy/.env \
  -f whatsapp-bridge/deploy/compose.yaml up -d codex-whatsapp-bridge
```

The bridge validates that the OpenWA session is `ready`, that its phone matches
the configured account, and that a resumed Codex thread still uses the
configured workspace. It then registers the signed webhook.

## Usage and recovery

Only text in the configured self-chat with the exact prefix `!codex ` is
accepted. Supported commands are:

```text
!codex <prompt>
!codex new
!codex status
!codex stop
!codex approve <token>
!codex approve-session <token>
!codex deny <token>
!codex answer <token> <answer>
!codex help
```

Prompts received during a turn enter a bounded FIFO queue. Webhook IDs,
outbound IDs, queued prompts, the current thread/turn mapping, and unsent
responses are persisted in `$CODEX_HOME/whatsapp/state.json`. If app-server is
offline, readiness becomes degraded and the bridge reconnects with bounded
backoff. If OpenWA is offline, final output remains in the durable outbox and
is retried without restarting the Codex turn.

Check health and logs with:

```bash
docker compose --env-file whatsapp-bridge/deploy/.env \
  -f whatsapp-bridge/deploy/compose.yaml exec codex-whatsapp-bridge \
  codex-whatsapp-bridge --healthcheck
docker compose --env-file whatsapp-bridge/deploy/.env \
  -f whatsapp-bridge/deploy/compose.yaml logs openwa codex-whatsapp-bridge
```

The bridge container is intentionally not published to the host by default.

For restart acceptance, send `!codex status` and a harmless prompt, stop all
three components, restart OpenWA and host app-server followed by the bridge,
then verify that the same thread ID and context continue. A replayed webhook
must not start a second turn.

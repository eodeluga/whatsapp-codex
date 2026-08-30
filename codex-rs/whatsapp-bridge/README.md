# codex-whatsapp-bridge

`codex-whatsapp-bridge` is another input surface for a standard Codex CLI
session, carried over an allowlisted WhatsApp self-chat. Codex owns normal
threads and conversation history, model and workspace configuration, sandbox
and permission profiles, `approval_policy` and `approvals_reviewer` (including
automatic review), approval decisions and scopes, turn execution, steering,
interruption, output, and completion. The bridge does not create a second
agent or turn-management model.

For the complete source installation and user quickstart, see the repository
[README](../../README.md).

## Runtime design

The supplied Compose deployment runs two services:

- `baileys-gateway` maintains the linked WhatsApp session and delivers signed webhooks;
- `codex-whatsapp-bridge` validates the self-chat, deduplicates events, and
  speaks Codex's typed app-server protocol over a bind-mounted Unix socket.

The host Codex process continues to own threads, working directories, history,
tools, permissions, approvals, and model communication. Neither container
needs the host workspace mounted.

The bridge reads the user-owned `[whatsapp]` table from Codex's normal
`config.toml` and private generated state from
`CODEX_HOME/whatsapp/runtime.json`; durable transcript delivery is kept in a sibling
private journal. Users must not create Baileys transport credentials,
session IDs, API keys, webhook URLs, or signing secrets manually.

On first start, the bridge also creates the user-editable command catalogue at
`CODEX_HOME/whatsapp/commands.json`. `/help` and `/` reload that file, so help
grouping, footer text, approval guidance, and the outbound response prefix can
be changed without rebuilding or restarting the bridge. Catalogue entries are
display metadata and do not alter command parsing, permissions, or execution.

## Start the deployment

Run this from the repository root after the compiled Codex TUI is running:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml up -d --build
```

Then open [http://127.0.0.1:8787/pairing](http://127.0.0.1:8787/pairing).
The page shows an auto-refreshing QR code until Baileys transport is paired, then confirms
that pairing is complete.

The deployment publishes only loopback management endpoints:

- `http://127.0.0.1:8787/pairing` — pairing and completion page;
- `http://127.0.0.1:8787/health/live` — bridge process liveness;
- `http://127.0.0.1:8787/health/ready` — complete integration readiness;
- `http://internal Baileys gateway` — Baileys transport's loopback interface.

Baileys transport and the bridge also share an internal Docker network for webhook and
REST traffic. The host app-server is not exposed on a TCP network interface.
The readiness endpoint returns a JSON component snapshot while using HTTP
`200` for ready and `503` for degraded operation.

## Operate the deployment

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml ps
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml logs -f \
  codex-whatsapp-bridge baileys-gateway
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml restart
```

Container restarts preserve the named `baileys-auth` volume and the bridge state
under `CODEX_HOME/whatsapp/`. Persisted inactive Baileys transport sessions are restarted
automatically. Do not remove the volume unless deliberately resetting the
linked WhatsApp session.

Both services use `restart: unless-stopped`. The bridge has an end-to-end
healthcheck but handles dependency outages through its bounded reconnect loop,
so a temporary app-server or WhatsApp outage does not create a restart storm.

To rebuild only this bridge after a source change:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  build codex-whatsapp-bridge
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  up -d --no-deps --force-recreate codex-whatsapp-bridge
```

The bridge image compiles its own release binary. The repository
`codex-rs/target/` directory is excluded from the Docker context.

## Standard Codex behavior over WhatsApp

Plain self-chat text starts a normal turn when idle. During a steerable turn it
is submitted through `turn/steer`, preserving arrival order. `/stop` interrupts
the active turn, including while an approval is displayed. Approvals are the
same decisions and scopes Codex supplies to the standard TUI, rendered as a
numbered plain-text list with no user-visible approval ID. `Decline` continues
the turn; `Cancel` interrupts it. Automatic review may accept an action without
showing a WhatsApp prompt. Reply with the displayed number to resolve an
approval, or `/stop`; arbitrary text is not interpreted as approval.

The bridge-specific behavior is limited to:

- the private allowlisted WhatsApp self-chat transport;
- `/whatsapp list-threads` and `/whatsapp attach <thread-id>`;
- `/status` for bridge, app-server, and transport health;
- `/stop`, `/help`, and the user-editable WhatsApp help catalogue;
- `/answer <token> <answer>` for sequential `request_user_input` questions;
- webhook deduplication, durable delivery, reconnect handling, and health/pairing
  endpoints.

Normal Codex transcript items are projected and delivered as they stream. Commentary,
plans, reasoning summaries, tool activity, and final answers retain their item order;
provider segmentation is lossless and does not add bridge prefixes or chunk labels.

The bridge does not implement or advertise the TUI-only approval retry flow or
the general TUI slash-command set. `/approve`, `/approve-session`, and `/deny`
are not bridge controls.

Attachments use the normal WhatsApp message flow. Image messages, with an
optional caption, are passed to Codex as native image input. Audio and voice
messages are rejected with a user-facing unsupported message. Documents are
stored in the shared attachment directory and Codex receives an internal context
note with the file path; the note is not echoed to WhatsApp. Video and sticker
messages remain unsupported. Attachment files are capped at 50 MiB for this
experiment, retained while referenced by a turn, and removed after completion or
by stale attachment cleanup. The bridge does not inspect document contents.

Transport-specific thread selection uses:

- `/whatsapp list-threads`;
- `/whatsapp attach <thread-id>`.

Inbound webhook delivery is durably deduplicated. Bridge-authored messages are
excluded from inbound processing, projected delivery is bounded, and app-server
reconnect attempts emit at most one outage notification for an affected
prompt.

## Development validation

Run from `codex-rs`:

```shell
just fmt
just test -p codex-transcript
just test -p codex-whatsapp-bridge
```

The bridge test suite covers webhook signatures and filtering, Baileys transport request
shapes, durable event deduplication, and bounded app-server outage reporting.

# codex-whatsapp-bridge

`codex-whatsapp-bridge` is the local transport between an allowlisted WhatsApp
self-chat and Codex's standard app-server daemon. It forwards normal prompts,
delivers Codex output, and maintains bounded durable delivery state. It does
not own a workspace, agent loop, or separate Codex conversation.

For the complete source installation and user quickstart, see the repository
[README](../../README.md).

## Runtime design

The supplied Compose deployment runs two services:

- `openwa` maintains the linked WhatsApp session and delivers signed webhooks;
- `codex-whatsapp-bridge` validates the self-chat, deduplicates events, and
  speaks Codex's typed app-server protocol over a bind-mounted Unix socket.

The host Codex process continues to own threads, working directories, history,
tools, permissions, approvals, and model communication. Neither container
needs the host workspace mounted.

The bridge reads the user-owned `[whatsapp]` table from Codex's normal
`config.toml` and private generated state from
`CODEX_HOME/whatsapp/runtime.json`. Users must not create OpenWA credentials,
session IDs, API keys, webhook URLs, or signing secrets manually.

On first start, the bridge also creates the user-editable command catalogue at
`CODEX_HOME/whatsapp/commands.json`. `/help` and `/` reload that file, so command
descriptions, help grouping, footer text, and the outbound response prefix can
be changed without rebuilding or restarting the bridge. The default catalogue
contains both commands available through WhatsApp and the complete standard
Codex TUI command reference. Catalogue entries are display metadata and do not
alter command parsing, permissions, or execution.

## Start the deployment

Run this from the repository root after the compiled Codex TUI is running:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml up -d --build
```

Then open [http://127.0.0.1:8787/pairing](http://127.0.0.1:8787/pairing).
The page shows an auto-refreshing QR code until OpenWA is paired, then confirms
that pairing is complete.

The deployment publishes only loopback management endpoints:

- `http://127.0.0.1:8787/pairing` — pairing and completion page;
- `http://127.0.0.1:8787/health/live` — bridge process liveness;
- `http://127.0.0.1:8787/health/ready` — complete integration readiness;
- `http://127.0.0.1:2785` — OpenWA's loopback interface.

OpenWA and the bridge also share an internal Docker network for webhook and
REST traffic. The host app-server is not exposed on a TCP network interface.

## Operate the deployment

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml ps
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml logs -f \
  codex-whatsapp-bridge openwa
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml restart
```

Container restarts preserve the named `openwa-data` volume and the bridge state
under `CODEX_HOME/whatsapp/`. Persisted inactive OpenWA sessions are restarted
automatically. Do not remove the volume unless deliberately resetting the
linked WhatsApp session.

To rebuild only this bridge after a source change:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  build codex-whatsapp-bridge
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  up -d --no-deps --force-recreate codex-whatsapp-bridge
```

The bridge image compiles its own release binary. The repository
`codex-rs/target/` directory is excluded from the Docker context.

## Message behavior

Plain self-chat text starts a normal turn. Normal controls include `/new`,
`/status`, and `/stop`; approvals and requested user input are returned through
the same chat. Transport-specific thread selection uses:

- `/whatsapp list-threads`;
- `/whatsapp attach <thread-id>`.

Inbound webhook delivery is durably deduplicated. Bridge-authored messages are
excluded from inbound processing, queued output is bounded, and app-server
reconnect attempts emit at most one outage notification for an affected
prompt.

## Development validation

Run from `codex-rs`:

```shell
just fmt
just test -p codex-whatsapp-bridge
```

The bridge test suite covers webhook signatures and filtering, OpenWA request
shapes, durable event deduplication, and bounded app-server outage reporting.

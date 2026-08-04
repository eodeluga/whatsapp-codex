# WhatsApp Codex

WhatsApp Codex adds your private WhatsApp self-chat as a first-class input to a
normal local Codex session. Terminal and WhatsApp messages use Codex's normal
threads, history, tools, sandboxing, and approval flow. The gateway does not
create a separate agent, workspace, or conversation model.

This repository currently provides a source-build installation. Codex runs on
the host, while the supplied Docker Compose deployment runs OpenWA and the
small WhatsApp bridge.

## Prerequisites

You need:

- a Rust toolchain compatible with `codex-rs/rust-toolchain.toml`;
- Docker Engine or Docker Desktop with the daemon running;
- Docker Compose v2 (`docker compose version`); and
- a WhatsApp account that can link another device.

Docker is optional when WhatsApp support is disabled. Normal terminal Codex
does not depend on the gateway.

## Quickstart

### 1. Build Codex

From the repository root:

```shell
cd codex-rs
cargo build --locked --release -p codex-cli
```

The host only needs the Codex binary. Docker builds the bridge binary inside
its own image, so building `codex-whatsapp-bridge` with host Cargo is not
required.

### 2. Complete onboarding and keep Codex running

Start the compiled binary:

```shell
./target/release/codex
```

During first-run onboarding, Codex completes its normal sign-in and trust flow,
then asks whether to enable WhatsApp. If enabled, enter only your own E.164
phone number, including its country code, such as `+447700900000`.

Codex checks Docker and Compose, creates private gateway state under the normal
Codex home directory, and starts its local app-server daemon from your user
home directory. Leave this Codex session running while starting the gateway.

### 3. Start the gateway

In another terminal, from the repository root:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml up -d --build
```

The first build can take several minutes. Compose builds and starts:

- OpenWA, which owns the linked WhatsApp session; and
- `codex-whatsapp-bridge`, which connects OpenWA to the host Codex app-server.

OpenWA credentials, its session ID, webhook settings, and signing secrets are
created and stored internally. Do not enter them manually.

### 4. Pair WhatsApp

Open [http://127.0.0.1:8787/pairing](http://127.0.0.1:8787/pairing) in a browser.
In WhatsApp, open **Linked devices**, choose **Link a device**, and scan the QR
code. The page refreshes while pairing and then displays:

> Pairing complete. WhatsApp Codex is connected. You may now close this page.

### 5. Verify the connection

Send `/status` in your WhatsApp self-chat. A ready installation reports the
Codex app-server as connected and OpenWA as healthy. Any other plain text, such
as `Summarise the current project`, starts a normal Codex turn.

The terminal and WhatsApp use the same normal Codex history. WhatsApp also
provides:

- `/help` (or `/`) to display the configured WhatsApp and standard Codex
  slash-command catalogue;
- `/whatsapp list-threads` to list recent resumable Codex threads; and
- `/whatsapp attach <thread-id>` to select one explicitly.

## Configuration

User-owned configuration is stored in the normal Codex configuration file,
usually `~/.codex/config.toml`:

```toml
[whatsapp]
onboarding_complete = true
enabled = true
account_phone_number = "+447700900000"
```

Private runtime data is stored under `~/.codex/whatsapp/`. There is no
WhatsApp-specific workspace, required input prefix, OpenWA API key, webhook
URL, or Docker environment variable to configure.

The gateway creates a user-editable command and display catalogue at
`~/.codex/whatsapp/commands.json` the first time it starts. This file contains
the WhatsApp controls, the standard Codex TUI slash commands, help headings and
footer, and the outbound response prefix. Editing it does not require rebuilding
either binary or container: send `/help` to reload and display the catalogue.
The catalogue controls discovery text only; it cannot enable a command that the
WhatsApp transport does not implement.

## Operations

Check container and endpoint status from the repository root:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml ps
curl --fail http://127.0.0.1:8787/health/live
curl --fail http://127.0.0.1:8787/health/ready
```

`health/live` confirms that the bridge process is running. `health/ready`
returns success only when durable state, OpenWA, its webhook, and the Codex
app-server are all available.

Follow gateway logs with:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml logs -f \
  codex-whatsapp-bridge openwa
```

Restarting the containers preserves OpenWA and bridge state:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml restart
```

The bridge automatically restarts a persisted OpenWA session. You normally do
not need to pair again. If the pairing page presents a new QR code, OpenWA no
longer has an authenticated session and must be linked again. Do not delete the
`openwa-data` volume during routine restart or upgrade work.

After changing only bridge source, rebuild and recreate only that service:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  build codex-whatsapp-bridge
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml \
  up -d --no-deps --force-recreate codex-whatsapp-bridge
```

## Troubleshooting

### The pairing page cannot be reached

Run `docker compose ... ps` using the full Compose path shown above. The bridge
must publish `127.0.0.1:8787->8787/tcp`. If it was interrupted while being
recreated, repeat the bridge-only `up -d --no-deps --force-recreate` command.

### The pairing page returns 503

OpenWA may still be starting or restoring its persisted session. Check the
logs and wait for `qr_ready` or `ready`. The bridge automatically requests a
restart for persisted `disconnected`, `stopped`, or failed sessions.

### WhatsApp reports that the app-server is unavailable

Ensure the compiled Codex TUI is still running. The bridge queues the prompt,
sends one error notification for that outage, and retries silently. Repeated
retries do not generate repeated WhatsApp errors.

### Disk usage is unexpectedly large

Rust release and test builds can produce a large `codex-rs/target/` directory.
The Docker build excludes that directory. Remove host build artifacts only
when you intentionally want a clean rebuild:

```shell
cd codex-rs
cargo clean
```

## Development validation

Run targeted repository workflows from `codex-rs`:

```shell
just fmt
just test -p codex-config
just test -p codex-tui
just test -p codex-whatsapp-bridge
```

The complete workspace test suite is intentionally not part of the routine
workflow because it is resource intensive.

This repository is licensed under the [Apache-2.0 License](LICENSE).

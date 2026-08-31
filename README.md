# WhatsApp Codex

WhatsApp Codex adds your private WhatsApp self-chat as a first-class input to a
normal local Codex session. Terminal and WhatsApp messages use Codex's normal
threads, history, tools, sandboxing, and approval flow. The gateway does not
create a separate agent, workspace, or conversation model.

This repository currently provides a source-build installation. Codex runs on
the host, while the supplied Docker Compose deployment runs Baileys transport and the
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

- Baileys transport, which owns the linked WhatsApp session; and
- `codex-whatsapp-bridge`, which connects Baileys transport to the host Codex app-server.

Baileys credentials and internal transport settings are
created and stored internally. Do not enter them manually.

### 4. Pair WhatsApp

Open [http://127.0.0.1:8787/pairing](http://127.0.0.1:8787/pairing) in a browser.
In WhatsApp, open **Linked devices**, choose **Link a device**, and scan the QR
code. The page refreshes while pairing and then displays:

> Pairing complete. WhatsApp Codex is connected. You may now close this page.

### 5. Verify the connection

Send `/status` in your WhatsApp self-chat. A ready installation reports the
Codex app-server as connected and Baileys transport as healthy. Any other plain text, such
as `Summarise the current project`, starts a normal Codex turn. WhatsApp is
another input surface for that standard Codex CLI session: Codex continues to
own thread history, model and workspace configuration, sandbox and permission
profiles, approval policy and automatic review, approval decisions, turn
execution, output, and completion.

The terminal and WhatsApp use the same normal Codex history. WhatsApp-specific
behavior is limited to the transport and these bridge operations:

- the private allowlisted WhatsApp self-chat transport;
- `/help` (or `/`) to display the user-editable WhatsApp help catalogue;
- `/status` for bridge, app-server, and transport health;
- `/stop` as the WhatsApp text mapping for interrupt;
- `/whatsapp list-threads` and `/whatsapp attach <thread-id>` for thread selection;
- numbered plain-text selection for an active approval overlay; and
- `/answer <token> <answer>` for sequential `request_user_input` questions.

Plain text during an active steerable turn uses `turn/steer`. Transcript items
are mirrored through the shared semantic projector as they stream. Commentary,
plans, and final answers are delivered by default; reasoning summaries and
tool-call activity require the shared `[bridge]` options. WhatsApp only
segments content at its provider limit and does not add normal-output prefixes
or chunk labels. Approval choices
have no user-visible IDs and are exactly the choices supplied by Codex. An
automatic review may accept an action without showing a WhatsApp prompt;
`Decline` lets the turn continue, while `Cancel` interrupts it. Reply with the
displayed approval number, or `/stop`. `/approve`, `/approve-session`, and
`/deny` are not WhatsApp controls, and the bridge does not advertise the
general TUI slash-command set as implemented WhatsApp functionality.

## Configuration

User-owned configuration is stored in the normal Codex configuration file,
usually `~/.codex/config.toml`:

```toml
[whatsapp]
onboarding_complete = true
enabled = true
account_phone_number = "+447700900000"

[bridge]
# All three options default to false when omitted.
include_reasoning = false
include_tool_calls = false
include_approval_notices = false
```

Private runtime data is stored under `~/.codex/whatsapp/`. There is no
WhatsApp-specific workspace, required input prefix, transport API token, webhook
URL, or Docker environment variable to configure.

Message limits and edit behavior are supplied by the WhatsApp adapter and
durable delivery worker; the old runtime chunk/edit tuning fields are accepted
only when reading an existing runtime file and are omitted when it is rewritten.

The top-level `[bridge]` options are shared by every remote provider adapter.
Reasoning output, tool-call activity, and command/file-change approval notices
are allowlisted but off by default. Permission requests remain available and
are not controlled by `include_approval_notices`. When approval notices are
disabled, command and file-change approval requests are rejected rather than
left waiting for an invisible reply.

Each active turn also emits a provider-neutral bridge status at most once per
category: `[codex working...]`, `[codex reasoning...]`, and `[codex tooling]`.
Repeated reasoning steps, tool events, reconnects, and transcript revisions do
not repeat these statuses.

The bridge keeps projected outbound transcript state in a private durable
delivery journal beside its runtime state. It is used to resume pending sends
after a bridge restart; normal transcript content is not stored in the user
editable command catalogue.

The gateway creates a user-editable command and display catalogue at
`~/.codex/whatsapp/commands.json` the first time it starts. This file contains
the implemented WhatsApp controls, approval guidance, help headings and
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
returns success only when durable state, the Baileys transport, and the Codex
app-server are all available. Its JSON response identifies each component, for
example:

```json
{"ready":true,"stateHealthy":true,"appServerConnected":true,"transportHealthy":true}
```

Follow gateway logs with:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml logs -f \
  codex-whatsapp-bridge baileys-gateway
```

Restarting the containers preserves Baileys transport and bridge state:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml restart
```

The bridge automatically reconnects to the persisted Baileys transport. You normally do
not need to pair again. If the pairing page presents a new QR code, Baileys transport no
longer has an authenticated session and must be linked again. Do not delete the
`baileys-auth` volume during routine restart or upgrade work.

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

### The pairing page says the gateway is starting

Baileys transport may still be starting or restoring its persisted session. Check the
logs and leave the page open; it refreshes automatically until pairing or the
restored session is available.

### WhatsApp reports that the app-server is unavailable

Start the compiled Codex TUI once. When WhatsApp is enabled, the TUI
idempotently ensures a detached, pid-managed local app-server and waits for its
protocol readiness before opening the normal chat UI. The app-server survives
TUI exit and container restarts. The bridge queues prompts and reconnects
automatically; repeated retries do not generate repeated WhatsApp errors.

Inspect component readiness and the managed daemon with:

```shell
curl -sS http://127.0.0.1:8787/health/ready
codex app-server daemon version
```

Managed app-server stderr is retained under
`~/.codex/app-server-daemon/app-server.stderr.log` for startup diagnosis.

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
just test -p codex-transcript
just test -p codex-messaging
just test -p codex-tui
just test -p codex-whatsapp-bridge
```

The complete workspace test suite is intentionally not part of the routine
workflow because it is resource intensive.

This repository is licensed under the [Apache-2.0 License](LICENSE).

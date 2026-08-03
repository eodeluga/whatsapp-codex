# WhatsApp Codex

WhatsApp Codex adds a private WhatsApp self-chat as a first-class input to a
normal local Codex session. It uses Codex's existing app-server daemon and
normal thread history; it does not create a separate agent, workspace, or
conversation model.

This branch provides a source-build deployment. The gateway is started with
the supplied Docker Compose file after normal Codex onboarding.

## Intended first-run experience

Run `codex` normally. Its first-run TUI will:

1. complete normal Codex onboarding;
2. ask whether to enable WhatsApp;
3. when selected, ask only for the private account's E.164 phone number and
   check Docker and Docker Compose;
4. create private gateway state below the normal Codex home directory; and
5. switch the TUI to Codex's local app-server daemon before opening the
   ordinary chat.

The normal terminal and WhatsApp then share Codex threads and history. Plain
messages are prompts; standard slash controls use Codex semantics. WhatsApp
also has `/whatsapp list-threads` and `/whatsapp attach <token>` for selecting
an existing normal Codex thread.

## Configuration

User-owned WhatsApp configuration stays in the normal Codex configuration
file, usually `~/.codex/config.toml`:

```toml
[whatsapp]
onboarding_complete = true
enabled = true
account_phone_number = "+447700900000"
```

There is no WhatsApp workspace, message prefix, OpenWA API key, webhook URL,
or Docker environment variable to enter. Gateway credentials and connection
details are private Codex runtime state under `~/.codex/whatsapp/`.

Docker is optional: choosing not to enable WhatsApp leaves normal terminal
Codex available without it.

## Source-build quickstart

Build both binaries from `codex-rs`:

```shell
cargo build --release -p codex-cli -p codex-whatsapp-bridge
```

Run `./target/release/codex` and select WhatsApp during the first-run flow.
Then start the local gateway from the repository root:

```shell
docker compose -f codex-rs/whatsapp-bridge/deploy/compose.yaml up -d --build
```

OpenWA and the bridge create the private OpenWA session and operator key
internally. To retrieve the pairing payload over loopback, open
`http://127.0.0.1:8787/pairing` after the bridge starts, then scan the displayed
QR code. The bridge becomes healthy after WhatsApp is paired and Codex's local
app-server is available. No environment variables or manual TOML settings are
required.

For source validation, run the targeted repository workflows from `codex-rs`:

```shell
just fmt
just test -p codex-config
just test -p codex-tui
just test -p codex-whatsapp-bridge
```

The complete workspace test suite is intentionally not part of the routine
workflow because it is resource intensive.

This repository is licensed under the [Apache-2.0 License](LICENSE).

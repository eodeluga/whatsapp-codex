# WhatsApp Codex

WhatsApp Codex adds a private WhatsApp self-chat as a first-class input to a
normal local Codex session. It uses Codex's existing app-server daemon and
normal thread history; it does not create a separate agent, workspace, or
conversation model.

This branch is under active development. The revised configuration and bridge
transport are present, but automatic gateway packaging, launch, and QR pairing
are not complete. Do not use this checkout for a live WhatsApp workflow yet.

## Intended first-run experience

Run `codex` normally. Its first-run TUI will:

1. complete normal Codex onboarding;
2. ask whether to enable WhatsApp;
3. when selected, ask only for the private account's E.164 phone number and
   check Docker and Docker Compose;
4. create private gateway state below the normal Codex home directory; and
5. start and pair the optional gateway before opening the ordinary Codex chat.

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

## Development status

The source checkout currently requires the remaining gateway lifecycle work
before it can be built and used end-to-end. Once that work is complete, format
and validate from `codex-rs` with the repository workflows:

```shell
just fmt
just test -p codex-config
just test -p codex-tui
just test -p codex-whatsapp-bridge
```

The complete workspace test suite is intentionally not part of the routine
workflow because it is resource intensive.

This repository is licensed under the [Apache-2.0 License](LICENSE).

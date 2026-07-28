# codex-whatsapp-bridge

This binary receives authenticated OpenWA webhooks and delegates Codex turns
to a locally running `codex app-server`. It reads only `[whatsapp]` from the
base user configuration file; it does not run a second agent loop.

Use `codex-whatsapp-bridge --config /codex-home/config.toml` in containers or
the default path for the supplied Compose deployment.

Start `codex app-server --listen unix://` on the host, then use the Compose
file in `deploy/` to run OpenWA and this bridge on their private network. The
bridge mounts only the Codex control socket, the read-only config file, and
its own writable state directory; the host workspace remains accessible only
to Codex.

Configure the user-owned `config.toml` (never a project config) before
starting the bridge:

```toml
[whatsapp]
onboarding_complete = true
enabled = true
account_phone_number = "+447700900000"
workspace = "/absolute/path/to/workspace"

[whatsapp.openwa]
session_id = "openwa-session"
api_key = "session-scoped-operator-key"
webhook_signing_secret = "base64url-encoded-32-byte-secret"
```

Treat this file as a secret: do not commit or share it. On Unix the bridge
writes its state file with `0600` permissions. Incoming messages must be in
the configured self-chat and begin with `!codex `; use `!codex help` for the
available commands.

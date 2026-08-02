# codex-whatsapp-bridge

`codex-whatsapp-bridge` is the local transport between an allowlisted WhatsApp
self-chat and Codex's standard app-server daemon. It forwards normal prompts,
delivers Codex output, and keeps bounded durable delivery state. It does not
own a workspace or a separate Codex conversation.

The bridge reads the small user-owned `[whatsapp]` table from Codex's normal
`config.toml` and private runtime state from `CODEX_HOME/whatsapp/runtime.json`.
Users must not create either OpenWA credentials or webhook settings manually.

Gateway packaging and lifecycle management are not complete on this branch.
The Compose file remains a development-only deployment asset and is not an
installation path for an end user. Do not follow older instructions that ask
for a workspace, `!codex` prefix, OpenWA session ID, API key, or webhook
secret; those settings are no longer part of the product configuration.

For development validation, run the targeted repository workflow from
`codex-rs`:

```shell
just fmt
just test -p codex-whatsapp-bridge
```

FROM rust:1.95.0-bookworm AS builder

WORKDIR /src
COPY . .
RUN cargo build --locked --release -p codex-whatsapp-bridge

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates coreutils util-linux \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/codex-whatsapp-bridge /usr/local/bin/
COPY whatsapp-bridge/deploy/entrypoint.sh /usr/local/bin/codex-whatsapp-bridge-entrypoint
RUN chmod 0755 /usr/local/bin/codex-whatsapp-bridge-entrypoint

ENTRYPOINT ["codex-whatsapp-bridge-entrypoint"]

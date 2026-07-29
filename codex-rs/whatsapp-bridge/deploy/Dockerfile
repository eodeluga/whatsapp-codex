FROM rust:1.95.0-bookworm AS builder

WORKDIR /src
COPY . .
RUN cargo build --locked --release -p codex-whatsapp-bridge

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/codex-whatsapp-bridge /usr/local/bin/

ENTRYPOINT ["codex-whatsapp-bridge"]

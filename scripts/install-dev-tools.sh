#!/usr/bin/env bash

set -Eeuo pipefail

readonly CODEX_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly CODEX_RUST_TOOLCHAIN="${CODEX_RUST_TOOLCHAIN:-1.95.0}"
readonly CODEX_LOCAL_BIN="${CODEX_LOCAL_BIN:-${HOME}/.local/bin}"
readonly CODEX_NVM_DIR="${CODEX_NVM_DIR:-${HOME}/.nvm}"

log() {
  printf '\n==> %s\n' "$*"
}

fail() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

run_as_root() {
  if [[ "${EUID}" -eq 0 ]]; then
    "$@"
  elif command_exists sudo; then
    sudo "$@"
  else
    fail "This script needs root privileges for system packages, but sudo is not installed."
  fi
}

install_system_packages() {
  if ! command_exists apt-get; then
    fail "This installer currently supports Debian and Ubuntu systems with apt-get."
  fi

  log "Installing Linux build dependencies"
  run_as_root apt-get update
  run_as_root apt-get install -y \
    build-essential \
    ca-certificates \
    clang \
    curl \
    git \
    libcap-dev \
    libssl-dev \
    musl-tools \
    pkg-config \
    python3 \
    bubblewrap \
    unzip \
    zip \
    zstd
}

install_rust() {
  if ! command_exists rustup; then
    log "Installing rustup"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --default-toolchain none --profile minimal
  fi

  # rustup may have been installed during this invocation and therefore may
  # not be present in the shell's inherited PATH yet.
  export PATH="${HOME}/.cargo/bin:${PATH}"

  log "Installing Rust ${CODEX_RUST_TOOLCHAIN}"
  rustup toolchain install "${CODEX_RUST_TOOLCHAIN}" --profile minimal
  rustup component add rustfmt clippy --toolchain "${CODEX_RUST_TOOLCHAIN}"

  printf 'Rust toolchain installed: '
  rustup run "${CODEX_RUST_TOOLCHAIN}" rustc --version
}

install_cargo_tool() {
  local binary="$1"
  local package="$2"

  if command_exists "${binary}"; then
    printf '%s already installed: %s\n' "${package}" "$(command -v "${binary}")"
    return
  fi

  log "Installing ${package}"
  cargo install --locked "${package}"
}

install_rust_helpers() {
  export PATH="${HOME}/.cargo/bin:${PATH}"
  install_cargo_tool just just
  install_cargo_tool dotslash dotslash
  install_cargo_tool cargo-nextest cargo-nextest
}

install_node_and_pnpm() {
  export PATH="${CODEX_NVM_DIR}/current/bin:${CODEX_LOCAL_BIN}:${PATH}"

  if ! command_exists node; then
    log "Installing Node.js through nvm"
    mkdir -p "${CODEX_NVM_DIR}"
    curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh \
      | NVM_DIR="${CODEX_NVM_DIR}" bash

    # shellcheck disable=SC1091
    source "${CODEX_NVM_DIR}/nvm.sh"
    nvm install 22
    nvm alias default 22
  fi

  if ! command_exists npm; then
    fail "Node.js is installed but npm is unavailable. Check the Node.js installation."
  fi

  if ! command_exists pnpm || [[ "$(pnpm --version)" != "10.33.0" ]]; then
    log "Installing pnpm 10.33.0"
    npm install --global pnpm@10.33.0
  fi
}

install_bazelisk() {
  if command_exists bazel; then
    printf 'Bazel already installed: %s\n' "$(command -v bazel)"
    return
  fi

  if command_exists bazelisk; then
    ln -sfn "$(command -v bazelisk)" "${CODEX_LOCAL_BIN}/bazel"
    return
  fi

  local asset
  case "$(uname -m)" in
    x86_64) asset="bazelisk-linux-amd64" ;;
    aarch64|arm64) asset="bazelisk-linux-arm64" ;;
    *) fail "Unsupported CPU architecture for Bazelisk: $(uname -m)" ;;
  esac

  log "Installing Bazelisk"
  mkdir -p "${CODEX_LOCAL_BIN}"
  local temporary_bazelisk
  temporary_bazelisk="$(mktemp)"
  trap 'rm -f "${temporary_bazelisk:-}"' RETURN
  curl -fsSL --retry 3 \
    "https://github.com/bazelbuild/bazelisk/releases/latest/download/${asset}" \
    -o "${temporary_bazelisk}"
  install -m 0755 "${temporary_bazelisk}" "${CODEX_LOCAL_BIN}/bazelisk"
  ln -sfn bazelisk "${CODEX_LOCAL_BIN}/bazel"
  trap - RETURN
  rm -f "${temporary_bazelisk}"
}

print_next_steps() {
  cat <<EOF

Tooling installation completed.

Repository: ${CODEX_REPO_ROOT}
Rust toolchain: ${CODEX_RUST_TOOLCHAIN}

If these commands are not already on PATH in a new shell, add:

  export PATH="\${HOME}/.cargo/bin:\${HOME}/.local/bin:\${PATH}"

Build Codex:

  cd "${CODEX_REPO_ROOT}/codex-rs"
  cargo build -p codex-cli

Run the locally built CLI:

  cargo run -p codex-cli --bin codex -- --help

Run the focused app-server client tests:

  cd "${CODEX_REPO_ROOT}"
  just test -p codex-app-server-client
EOF
}

main() {
  install_system_packages
  install_rust
  install_rust_helpers
  install_node_and_pnpm
  install_bazelisk
  print_next_steps
}

main "$@"

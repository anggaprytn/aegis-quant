#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_PATH="${AEGIS_INSTALL_CLI_PATH:-/usr/local/bin/aegis}"
VERSION_OUT="$(mktemp)"
VERSION_ERR="$(mktemp)"
TMP_DIR="$(mktemp -d)"
DOCKER_IMAGE="${AEGIS_INSTALL_CLI_IMAGE:-aegis-quant-cli-install:latest}"
DOCKER_CONTAINER=""

cleanup() {
  rm -f "$VERSION_OUT" "$VERSION_ERR"
  if [ -n "$DOCKER_CONTAINER" ]; then
    docker rm "$DOCKER_CONTAINER" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

cd "$REPO_ROOT"

build_with_cargo() {
  echo "Building release Aegis CLI with host Cargo..."
  cargo build --release -p cli

  if [ ! -x target/release/aegis ]; then
    echo "ERROR: target/release/aegis was not built or is not executable" >&2
    exit 1
  fi

  CLI_BINARY="target/release/aegis"
}

build_with_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: cargo is unavailable and docker is not installed or not in PATH" >&2
    exit 1
  fi

  echo "Host Cargo unavailable or Docker fallback forced; building Aegis CLI with Docker..."
  docker build -t "$DOCKER_IMAGE" .

  DOCKER_CONTAINER="$(docker create "$DOCKER_IMAGE")"
  if ! docker cp "$DOCKER_CONTAINER:/usr/local/bin/aegis" "$TMP_DIR/aegis" >/dev/null 2>&1; then
    echo "ERROR: Docker image does not contain /usr/local/bin/aegis" >&2
    exit 1
  fi

  if [ ! -x "$TMP_DIR/aegis" ]; then
    chmod 0755 "$TMP_DIR/aegis"
  fi

  CLI_BINARY="$TMP_DIR/aegis"
}

CLI_BINARY=""
if [ "${AEGIS_INSTALL_CLI_FORCE_DOCKER:-}" = "1" ]; then
  build_with_docker
elif command -v cargo >/dev/null 2>&1; then
  build_with_cargo
else
  build_with_docker
fi

echo "Installing Aegis CLI to $INSTALL_PATH..."
if [ "$(id -u)" -eq 0 ] || [ "${AEGIS_INSTALL_CLI_NO_SUDO:-}" = "1" ]; then
  install -m 0755 "$CLI_BINARY" "$INSTALL_PATH"
else
  sudo install -m 0755 "$CLI_BINARY" "$INSTALL_PATH"
fi

if [ ! -x "$INSTALL_PATH" ]; then
  echo "ERROR: installed CLI is not executable at $INSTALL_PATH" >&2
  exit 1
fi

echo "Verifying installed Aegis CLI..."
verify_aegis() {
  if [ "${AEGIS_INSTALL_CLI_VERIFY_WITH_DOCKER:-}" = "1" ]; then
    docker run --rm "$DOCKER_IMAGE" aegis "$@"
  else
    "$INSTALL_PATH" "$@"
  fi
}

if verify_aegis --version >"$VERSION_OUT" 2>"$VERSION_ERR"; then
  cat "$VERSION_OUT"
else
  echo "aegis --version is not supported by this build; continuing with help checks."
fi

verify_aegis --help >/dev/null
verify_aegis research --help >/dev/null
verify_aegis research scheduled-jobs --help >/dev/null

echo "Aegis CLI installed and verified at $INSTALL_PATH"

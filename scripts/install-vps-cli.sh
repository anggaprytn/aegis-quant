#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_PATH="/usr/local/bin/aegis"
VERSION_OUT="$(mktemp)"
VERSION_ERR="$(mktemp)"

cleanup() {
  rm -f "$VERSION_OUT" "$VERSION_ERR"
}
trap cleanup EXIT

cd "$REPO_ROOT"

echo "Building release Aegis CLI..."
cargo build --release -p cli

if [ ! -x target/release/aegis ]; then
  echo "ERROR: target/release/aegis was not built or is not executable" >&2
  exit 1
fi

echo "Installing Aegis CLI to $INSTALL_PATH..."
sudo install -m 0755 target/release/aegis "$INSTALL_PATH"

if [ ! -x "$INSTALL_PATH" ]; then
  echo "ERROR: installed CLI is not executable at $INSTALL_PATH" >&2
  exit 1
fi

echo "Verifying installed Aegis CLI..."
if "$INSTALL_PATH" --version >"$VERSION_OUT" 2>"$VERSION_ERR"; then
  cat "$VERSION_OUT"
else
  echo "aegis --version is not supported by this build; continuing with help checks."
fi

"$INSTALL_PATH" research --help >/dev/null
"$INSTALL_PATH" research scheduled-jobs --help >/dev/null

echo "Aegis CLI installed and verified at $INSTALL_PATH"

#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${TEST_DATABASE_URL:-${DATABASE_URL:-}}" ]]; then
  echo "Set TEST_DATABASE_URL or DATABASE_URL before running integration tests." >&2
  exit 1
fi

cargo test -p db --test integration_db -- --ignored
cargo test -p api --test pipeline_persistence -- --ignored

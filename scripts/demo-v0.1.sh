#!/usr/bin/env bash
set -euo pipefail

run_checks=0
run_compose=0

for arg in "$@"; do
  case "$arg" in
    --with-checks)
      run_checks=1
      ;;
    --with-compose)
      run_compose=1
      ;;
    *)
      echo "Unknown flag: $arg" >&2
      echo "Usage: scripts/demo-v0.1.sh [--with-checks] [--with-compose]" >&2
      exit 1
      ;;
  esac
done

echo "Aegis Quant v0.1 demo"
echo

echo "[1/8] Tooling check"
cargo --version
echo "Optional dashboard check: npm --prefix apps/dashboard --version"
echo "Optional compose check: docker compose version"
echo

echo "[2/8] Environment prerequisites"
echo "Required local file: .env copied from .env.example"
echo "Required vars for base demo: DATABASE_URL, AEGIS_JWT_SECRET, AEGIS_BOOTSTRAP_OWNER_EMAIL, AEGIS_BOOTSTRAP_OWNER_PASSWORD"
echo "Optional testnet-only vars: BINANCE_TESTNET_API_KEY, BINANCE_TESTNET_API_SECRET"
echo "Base demo does not require Binance credentials."
echo

if [[ "$run_checks" -eq 1 ]]; then
  echo "[3/8] Running safe verification commands"
  cargo check
  cargo test
else
  echo "[3/8] Safe verification commands"
  echo "Optional: cargo check"
  echo "Optional: cargo test"
fi
echo

if [[ "$run_compose" -eq 1 ]]; then
  echo "[4/8] Starting local core services"
  docker compose -f infra/docker-compose.yml up -d postgres api
else
  echo "[4/8] Local core services"
  echo "Optional: docker compose -f infra/docker-compose.yml up -d postgres api"
  echo "Optional dashboard: docker compose -f infra/docker-compose.yml --profile dashboard up -d dashboard"
  echo "Optional Prometheus: docker compose -f infra/docker-compose.yml --profile prometheus up -d prometheus"
fi
echo

echo "[5/8] Owner bootstrap instructions"
echo "curl -X POST http://127.0.0.1:3000/auth/bootstrap-owner"
echo "cargo run -p cli -- auth login --email \"\$AEGIS_BOOTSTRAP_OWNER_EMAIL\" --password \"\$AEGIS_BOOTSTRAP_OWNER_PASSWORD\""
echo

echo "[6/8] Data and replay examples"
echo "Public market-data backfill example:"
echo "cargo run -p cli -- market backfill --symbol BTCUSDT --timeframe 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z"
echo "Deterministic backtest example:"
echo "cargo run -p cli -- backtest run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m --start 2026-05-01T00:00:00Z --end 2026-05-02T00:00:00Z --initial-capital 1000000 --fee-bps 10 --slippage-bps 5 --holding-candles 3"
echo

echo "[7/8] Shadow and readiness examples"
echo "Shadow mode is optional and no-submit by design."
echo "Optional shadow example:"
echo "cargo run -p cli -- exchange testnet shadow-run --strategy momentum_v1 --symbol BTCUSDT --timeframe 1m"
echo "Readiness example:"
echo "cargo run -p cli -- readiness check --target PAPER_PIPELINE --symbol BTCUSDT --strategy momentum_v1 --timeframe 1m"
echo

echo "[8/8] Reporting example"
echo "cargo run -p cli -- reports operator daily --start 2026-05-24T00:00:00Z --end 2026-05-24T23:59:59Z --symbol BTCUSDT --strategy momentum_v1 --format markdown"
echo
echo "Demo complete. No testnet orders are submitted by this script."

# Security Checklist

## Auth and bootstrap

- Set `AEGIS_AUTH_DISABLED=false` for any demo or shared local environment.
- Use a long random `AEGIS_JWT_SECRET`.
- Bootstrap the owner exactly once and rotate local credentials if they become exposed.
- Keep owner credentials out of recorded demos, screenshots, and terminal history where practical.

## Dashboard and API exposure

- Treat the dashboard as internal-only.
- Bind dashboard, API, and Prometheus to private interfaces or a private network when not developing alone.
- Prefer Tailscale or another private-network layer before exposing any operator surface remotely.

## Metrics exposure

- Set `AEGIS_PROTECT_METRICS=true` when the environment is not strictly local.
- Do not expose `/metrics` publicly.
- Review Prometheus and reverse-proxy boundaries before any shared deployment.

## Testnet secret handling

- Keep `BINANCE_TESTNET_API_KEY` and `BINANCE_TESTNET_API_SECRET` only in local `.env` or another untracked secret store.
- Never commit testnet secrets.
- Never print secrets in scripts, logs, or CI output.
- Use testnet credentials only with Binance Spot Testnet endpoints.

## Exchange boundary

- Do not configure production Binance private endpoints.
- Do not add production exchange keys to `.env`, CI, or compose files.
- Treat public market-data ingest/backfill separately from authenticated exchange execution.
- Keep all authenticated exchange actions testnet-only for v0.1.

## Logging and storage hygiene

- Confirm logs do not contain JWTs, refresh tokens, API keys, or API secrets.
- Confirm generated frontend artifacts, local logs, coverage outputs, and token files remain untracked.
- Keep `.env` untracked and commit only `.env.example`.

## Local environment hygiene

- Review shell history before recording demos.
- Prefer a separate local `.env` for demo credentials and rotate afterward if needed.
- Keep `~/.config/aegis/token.json` local to the operator machine and do not copy it into the repo.
- Remove stale testnet credentials from machines that no longer need them.

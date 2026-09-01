# Security Model

This document describes the security and isolation assumptions implemented in the repository. It complements the [security policy](SECURITY_POLICY.md), which explains how to report a vulnerability.

## Current posture

Aegis Quant is experimental, single-tenant infrastructure for local and VPS-style operation. Live trading and production exchange private trading endpoints are not implemented. Authenticated exchange behavior is isolated to Binance Spot Testnet configuration.

The repository is designed to make dangerous paths explicit and auditable, but it is not a substitute for a production secrets manager, network policy, database hardening, or an operational review.

## Boundary matrix

| Surface | Data source or destination | Intended behavior |
| --- | --- | --- |
| Public market data | Binance public WebSocket and REST endpoints | Ingest and backfill only; no credentials |
| Research and replay | Stored candles and research tables | Deterministic analysis; no execution rows |
| Paper | Paper orders, fills, positions, PnL, and journal tables | Simulated fills only; no exchange calls |
| Shadow | Shadow observation and runner tables | Persist would-submit evidence; never submit |
| Testnet | Binance Spot Testnet and isolated testnet tables | Explicit, authorized testnet actions only |
| Operations | Events, audit logs, readiness, reports, metrics | Inspection and control; no implicit execution |

The intended trade-like sequence is:

~~~text
market event -> signal -> risk decision -> order intent -> execution state
~~~

Strategy and research code do not have direct exchange authority. The LLM analyst crate is a dormant boundary and is not wired into execution.

## Authentication and authorization

- Auth is enabled by default and requires AEGIS_JWT_SECRET.
- The local auth MVP provides OWNER, OPERATOR, and VIEWER roles.
- Passwords are hashed with Argon2id.
- Access tokens are short-lived JWTs.
- Refresh tokens are stored as hashes in PostgreSQL and rotated on refresh.
- The dashboard keeps the access token in browser session storage; refresh tokens use an HTTP-only cookie.
- The CLI persists its local session under the XDG config directory. Treat the token file as sensitive.
- AEGIS_AUTH_DISABLED=true injects a synthetic OWNER actor and is for isolated local development only.
- Metrics are unauthenticated by default for local use; set AEGIS_PROTECT_METRICS=true and apply network controls when metrics are exposed beyond a trusted network.
- CORS origins are explicit and wildcard origins are rejected when credentialed auth is enabled.

## Credentials and secrets

- Use .env.example only as a placeholder template and keep .env untracked.
- Never commit API keys, API secrets, passwords, refresh tokens, access tokens, cookies, or private deployment URLs.
- Binance market-data ingestion and REST backfill do not require credentials.
- BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_API_SECRET are backend-only and must remain empty unless an operator intentionally tests Spot Testnet.
- Testnet private-stream listen keys are masked outside the adapter and hashed when persisted.
- Do not pass secrets as dashboard build arguments or expose them through NEXT_PUBLIC variables.
- Logs, issue reports, screenshots, and research exports must be reviewed for tokens, emails, account identifiers, and private infrastructure details.

Secret management beyond environment variables is not implemented. Production deployment should supply secrets through an external mechanism appropriate to the host.

## Dangerous actions

The following controls are part of the application boundary:

- The kill switch is persistent in PostgreSQL and is checked before paper or testnet actions.
- Paper position close requires typed confirmation in the form CLOSE <SYMBOL> and a fresh local mark price.
- Testnet pipeline submission requires an approved persisted risk decision, an inactive kill switch, owner authorization, and typed confirmation in the form SUBMIT TESTNET <SYMBOL>.
- Direct testnet order actions use TESTNET ORDER confirmation and remain testnet-only.
- Shadow-promotion submission requires a non-expired preview, owner authorization, an inactive kill switch, and PROMOTE TESTNET <SYMBOL> confirmation.
- Testnet repair and cancellation actions use per-order typed confirmations.
- Research plan execution uses an explicit RUN RESEARCH PLAN confirmation and creates research artifacts only.
- Configuration changes and dangerous state transitions write audit or system-event records where the path supports them.

Readiness, analytics, operator reports, research candidate decisions, and shadow observation are decision-support or evidence paths. They do not auto-promote candidates or submit orders.

## Persistence isolation

Research and replay must not create or mutate:

- paper orders, fills, positions, or equity;
- exchange testnet orders or lifecycle events;
- shadow-promotion submission state;
- live execution state.

Paper accounting is separate from replay/backtest state. Testnet order, private-stream, reconciliation, and repair history use isolated tables. Scheduled candidate shadow jobs are constrained to the no-submit path.

## Operational validation

For a deployment that should remain research-only:

- prefer the read-only validator in scripts/validate-vps-readonly.sh;
- use a read-only database role and the ai_read views when they are available;
- verify health, feed freshness, scheduled-job state, and execution safety counts;
- do not run migrations, POST requests, scheduler mutations, repair actions, or execution commands as part of a read-only check;
- back up a non-disposable database before migrations or maintenance;
- investigate non-zero execution counts instead of deleting rows or resetting state.

See [RUNBOOK.md](RUNBOOK.md) and [SECURITY_CHECKLIST.md](SECURITY_CHECKLIST.md) for the operator procedures.

## Known limitations

- There is no production secret-management integration.
- The auth model is local and single-tenant.
- Database roles and privileges require deployment-specific hardening.
- Metrics are public unless explicitly protected.
- Binance public endpoints can be unavailable or rate-limited; provider diagnostics report failures but do not make data complete.
- The exchange adapter is limited to Spot Testnet; no live adapter exists.
- Backtests and research evidence do not establish profitability or production readiness.

# Operator Checklist

## Before running locally

- Copy `.env.example` to `.env` and set a strong local `AEGIS_JWT_SECRET`.
- Confirm `AEGIS_BOOTSTRAP_OWNER_EMAIL` and `AEGIS_BOOTSTRAP_OWNER_PASSWORD` are set to local-only values.
- Confirm no production Binance API keys or production Binance private endpoints are configured.
- Start Postgres and API first.
- Verify `GET /system/health` and `GET /system/db-health` succeed.

## Before paper pipeline

- Confirm kill switch is inactive.
- Confirm feed freshness is healthy from `GET /market/feed-status`.
- Confirm readiness for `PAPER_PIPELINE` is not blocked.
- Confirm risk config is validated and current.
- Confirm strategy config is validated, enabled, and points to the intended symbols/timeframe.
- Confirm paper account state exists and is the expected local/demo state.

## Before shadow runner

- Confirm kill switch is inactive.
- Confirm feed freshness and recent candle availability.
- Confirm readiness for `TESTNET_SHADOW` is acceptable.
- Confirm the shadow runner remains no-submit and operator expectations match that boundary.
- Confirm no production Binance private endpoint overrides are present.

## Before testnet promotion

- Confirm the selected shadow run is `WOULD_SUBMIT` and still relevant.
- Confirm readiness for `TESTNET_PROMOTION` is acceptable.
- Confirm latest reconciliation status does not show unresolved mismatches that would invalidate operator confidence.
- Confirm risk config and strategy config remain validated and unchanged since the shadow run.
- Confirm promotion is still within its TTL window.

## Before testnet submit

- Confirm owner authorization is being used intentionally.
- Confirm kill switch is inactive.
- Confirm readiness for `TESTNET_SUBMIT` is acceptable.
- Confirm Binance Spot Testnet credentials are present and local-only.
- Confirm private stream status is healthy enough for lifecycle visibility.
- Confirm reconciliation mismatches are absent or understood.
- Confirm typed confirmation text is correct for the submit path being used.
- Confirm no production Binance endpoints or keys are configured anywhere in the environment.

## Emergency stop / kill switch

- Activate the persistent kill switch immediately for any uncertain execution state.
- Stop operator-triggered paper or testnet actions until readiness, reconciliation, and feed freshness are understood.
- Capture current readiness output, private-stream status, reconciliation status, and recent events before resuming.
- Resume only with explicit owner/operator intent and the required typed confirmation flow.

## What not to do

- Do not use production Binance API keys.
- Do not point authenticated exchange actions at production Binance endpoints.
- Do not treat shadow mode as order submission.
- Do not bypass readiness, reconciliation, or feed-freshness checks before promotion or submit.
- Do not expose dashboard or metrics publicly without network controls.
- Do not log or paste secrets into shell history, docs, tickets, or demos.

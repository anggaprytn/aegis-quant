# Configuration Reference

The repository uses environment variables rather than a checked-in application configuration file. Copy .env.example to .env for local Compose use, keep .env private, and change the example credentials before using a persistent environment.

## Connection model

Compose uses the service hostname postgres from inside containers and publishes PostgreSQL on the default host port only when the Compose file is changed to do so. The example DATABASE_URL is therefore intended for API and worker containers. The CLI normally talks to the API; direct database research commands and integration tests need a host-reachable URL such as 127.0.0.1.

If you change POSTGRES_PASSWORD, update the password embedded in DATABASE_URL, TEST_DATABASE_URL, and any worker-specific connection override consistently.

## Core application and database

| Variable | Example/default | Purpose |
| --- | --- | --- |
| POSTGRES_DB | aegis_quant | Database created by the Postgres container |
| POSTGRES_USER | aegis | Local database role used by Compose |
| POSTGRES_PASSWORD | aegis-local-only | Local-only example password; replace outside disposable development |
| DATABASE_URL | postgres://aegis:aegis-local-only@postgres:5432/aegis_quant | Required by the API and DB-backed workers |
| TEST_DATABASE_URL | postgres://aegis:aegis-local-only@127.0.0.1:5432/aegis_quant_test | Disposable integration-test database URL |
| DATABASE_MAX_CONNECTIONS | 5 | SQLx pool size |
| APP_NAME | aegis-quant-api | Service name used in logs and events |
| APP_ENV | development | Environment label |
| API_BIND_ADDR | 0.0.0.0:3000 | Address used by the API process; Compose publishes it as host port 3100 |
| RUST_LOG | info,axum=info,tower_http=info | Tracing filter |

## Authentication and browser access

| Variable | Example/default | Purpose |
| --- | --- | --- |
| AEGIS_AUTH_DISABLED | false | Local-only auth bypass; injects a synthetic OWNER actor |
| AEGIS_JWT_SECRET | replace-with-a-long-random-local-dev-secret | Required when auth is enabled; use a long random value |
| AEGIS_ACCESS_TOKEN_TTL_SECONDS | 900 | Access-token lifetime |
| AEGIS_REFRESH_TOKEN_TTL_SECONDS | 86400 | Refresh-token lifetime |
| AEGIS_COOKIE_SECURE | false | Set true when using HTTPS |
| AEGIS_PROTECT_METRICS | false | Requires auth for /metrics when true |
| AEGIS_BOOTSTRAP_OWNER_EMAIL | owner@example.com | One-time owner bootstrap identity |
| AEGIS_BOOTSTRAP_OWNER_PASSWORD | replace-with-a-12-char-min-password | One-time owner bootstrap password; minimum length is enforced |
| AEGIS_CORS_ALLOWED_ORIGINS | local dashboard origins | Comma-separated HTTP/HTTPS origins; wildcard is rejected with credentialed auth |
| AEGIS_API_BASE_URL | http://127.0.0.1:3100 | API URL used by the CLI and local dashboard configuration |
| NEXT_PUBLIC_API_BASE_URL | http://127.0.0.1:3100 | API URL embedded into the Next.js dashboard |

Use a narrow, explicit CORS list. Do not use * with credentialed dashboard authentication. AEGIS_AUTH_DISABLED=true is not a deployment security setting.

## Market data

| Variable | Example/default | Purpose |
| --- | --- | --- |
| MARKET_EXCHANGE | binance | Market-data source label |
| MARKET_SYMBOLS | BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT | Comma-separated symbols for ingest and workers |
| MARKET_STALE_THRESHOLD_SECONDS | 10 | Feed freshness threshold |
| BINANCE_WS_BASE_URL | wss://stream.binance.com:443 | Public Binance WebSocket base |
| BINANCE_REST_BASE_URL | https://api.binance.com | Public Binance REST base |
| BINANCE_REST_FALLBACK_BASE_URLS | data-api and api1-api4 hosts | Comma-separated public REST fallback bases |
| MICROSTRUCTURE_RETENTION_DAYS | 30 | Retention for collected microstructure metrics |
| MICROSTRUCTURE_RUN_RETENTION_DAYS | 90 | Retention for microstructure collector runs |

Market ingest and candle backfill use public endpoints only. These variables do not authorize exchange orders.

## Testnet-only exchange settings

| Variable | Example/default | Purpose |
| --- | --- | --- |
| BINANCE_TESTNET_REST_BASE_URL | https://testnet.binance.vision | Authenticated Spot Testnet REST base |
| BINANCE_TESTNET_WS_BASE_URL | wss://stream.testnet.binance.vision/ws | Spot Testnet private-stream base |
| BINANCE_TESTNET_API_KEY | empty | Optional backend-only testnet key |
| BINANCE_TESTNET_API_SECRET | empty | Optional backend-only testnet secret |
| BINANCE_TESTNET_RECV_WINDOW_MS | 5000 | Signed-request receive window |
| BINANCE_TESTNET_PRIVATE_STREAM_STALE_THRESHOLD_SECONDS | 90 | Private-stream staleness threshold |
| BINANCE_TESTNET_PRIVATE_STREAM_KEEPALIVE_SECONDS | 1800 | Listen-key keepalive interval |
| BINANCE_TESTNET_PRIVATE_STREAM_RECONNECT_DELAY_SECONDS | 5 | Reconnect delay |
| SHADOW_OBSERVATION_ONLY | true | Required safe guard for candidate shadow observation |

Only Spot Testnet values belong in this group. Production private exchange URLs and live-trading credentials are intentionally not supported. Keep these variables out of frontend build arguments and logs.

## Strategy defaults

| Variable | Example/default | Purpose |
| --- | --- | --- |
| STRATEGY_DEFAULT_SYMBOLS | BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT | Symbols used when default strategy configs are initialized |
| STRATEGY_DEFAULT_TIMEFRAME | 1m | Default strategy timeframe |
| STRATEGY_DEFAULT_NOTIONAL | 100000 | Default decimal notional for strategy configs |
| MOMENTUM_LOOKBACK_CANDLES | 3 | Default momentum lookback |
| BREAKOUT_LOOKBACK_CANDLES | 20 | Default breakout lookback |

Strategy configuration is validated, versioned, and audited by the API. These defaults do not bypass risk checks.

## Worker settings

The following optional variables are read by worker binaries and default in code if absent:

| Variable | Default | Worker |
| --- | --- | --- |
| CANDLE_AGGREGATOR_TARGET_INTERVALS | 5m,15m,1h | candle-aggregator |
| CANDLE_AGGREGATOR_INTERVAL_SECONDS | 60 | candle-aggregator |
| CANDLE_AGGREGATOR_BOOTSTRAP_LOOKBACK_HOURS | 24 | candle-aggregator |
| CANDLE_AGGREGATOR_OVERLAP_MINUTES | 120 | candle-aggregator |
| SCHEDULED_RESEARCH_RUNNER_ENABLED | false | scheduled-research-runner |
| SCHEDULED_RESEARCH_RUNNER_INTERVAL_SECONDS | 60 | scheduled-research-runner |
| SCHEDULED_RESEARCH_DISABLED_SLEEP_SECONDS | 300 | scheduled-research-runner |

The scheduled research runner idles without connecting to the database when disabled. Candidate-specific observation jobs are manual and are not part of the safe bootstrap set.

## CLI and validation helpers

These variables are used by the CLI or repository scripts rather than by the API server itself:

| Variable | Purpose |
| --- | --- |
| AEGIS_ACCESS_TOKEN | Optional Bearer token override for CLI and read-only validation |
| AEGIS_DASHBOARD_URL | Dashboard URL used by validation scripts |
| AEGIS_READONLY_DATABASE_URL | Read-only Postgres URL for VPS validation; should use a read-only role |
| AEGIS_VALIDATE_LOG_TAIL_LINES | Log lines inspected by the read-only validator |
| AEGIS_VALIDATE_JOB_LIMIT | Scheduled-job rows inspected by the validator |
| AEGIS_VALIDATE_RUN_LIMIT | Job-run rows inspected by the validator |
| AEGIS_VALIDATE_RUN_JOB_SAMPLE_LIMIT | Number of jobs sampled by the validator |
| AEGIS_RESEARCH_PLAN_ID | Existing plan ID for the optional research-loop smoke run |
| AEGIS_MIGRATIONS_DIR | Optional migration directory override for the CLI |
| AEGIS_MIGRATION_ACTOR | Actor label recorded by the migration runner |
| XDG_CONFIG_HOME | Base directory for the CLI token file |

The CLI stores local auth state under the XDG config directory. Treat that token file as sensitive and do not copy it into the repository.

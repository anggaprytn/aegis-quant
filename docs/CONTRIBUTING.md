# Contributing to Aegis Quant

Thank you for helping improve Aegis Quant. The project values explicit behavior, deterministic tests, useful operational diagnostics, and documentation that tells the truth about the current implementation.

## Before you start

Read the [README](../README.md), [architecture](ARCHITECTURE.md), [security model](SECURITY.md), and [Code of Conduct](CODE_OF_CONDUCT.md). For a new feature, check [ROADMAP.md](ROADMAP.md) and search the existing API, CLI, migrations, and tests before proposing a new boundary.

Please open an issue before large changes when the desired behavior, data model, or safety boundary is not obvious. Small fixes and documentation improvements can go directly into a pull request.

## Local setup

Install Rust, Node.js 20+, npm, Docker Compose, and PostgreSQL or Docker. Then:

~~~bash
cp .env.example .env
npm --prefix apps/dashboard ci
make verify
~~~

The example environment is for local use. Replace the example JWT and owner credentials, keep testnet credentials empty unless you are intentionally testing the isolated testnet path, and never commit .env.

Database-backed integration tests are ignored by default. Set a disposable TEST_DATABASE_URL whose database name contains test, apply migrations through the test harness, and run:

~~~bash
make integration-test
~~~

See [DEVELOPMENT.md](DEVELOPMENT.md) for details.

## Change guidelines

- Keep the control flow explicit: market event -> signal -> risk decision -> order intent -> execution state.
- Do not let strategy or research code submit orders directly.
- Do not use floating-point types for money, prices, balances, notional, or PnL; use the existing decimal domain types.
- Keep research, replay, paper, shadow, and testnet persistence boundaries isolated.
- Make dangerous actions auditable and retain typed confirmations and role checks.
- Add or update tests for changed behavior, especially idempotency, stale data, kill-switch, authorization, and state-transition cases.
- Add a numbered PostgreSQL migration for schema changes. Do not rewrite an applied migration; the migration runner records checksums.
- Prefer the existing workspace dependencies and boring, localized code over new abstractions or dependencies.
- Update the README or the relevant guide when commands, environment variables, routes, workers, or safety semantics change.

## Pull requests

A pull request should explain:

- what changed and why;
- which user-visible commands, routes, schema, or operational behaviors changed;
- how it was tested;
- whether the change affects paper, shadow, testnet, authentication, secrets, or the kill switch;
- any remaining assumptions or follow-up work.

Run the same checks used by CI before opening the pull request:

~~~bash
npm --prefix apps/dashboard ci
make verify
~~~

If integration tests were run, include the command and the fact that the database was disposable. Redact tokens, passwords, API keys, cookies, internal hostnames, and account identifiers from logs and screenshots.

## Review standard

Reviewers should be able to trace a changed action to its persisted state and audit/event record. Claims in documentation must be supported by code or clearly labeled as an assumption or future work.

By contributing, you agree to follow the project [Code of Conduct](CODE_OF_CONDUCT.md).

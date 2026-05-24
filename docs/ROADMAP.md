# Roadmap

## MVP foundation

1. Rust workspace and compile-safe crate boundaries
2. Shared core types
3. Initial Postgres schema
4. Event model and publisher abstraction
5. Health and status API
6. Binance public market ingest and deterministic 1m candles
7. Persistent market feed status and market data read APIs
8. Deterministic candle-only strategy signal generation on stored candles
9. Deterministic paper pipeline: closed candles -> signal -> risk decision -> paper order lifecycle
10. Persistent kill switch and paper-only order lifecycle APIs
11. Deterministic replay/backtest MVP on stored candles and persisted strategy configs
12. Minimal operational dashboard shell for paper-only inspection and control

## Current status

- Completed: persistent kill switch, deterministic risk evaluation, Binance public ingest, deterministic candle building, strategy signal generation, paper-only order lifecycle, replay/backtest MVP, DB-backed integration harness, and minimal dashboard shell
- Completed: Binance public REST historical candle backfill with persisted run tracking, CLI/API entrypoints, and dashboard inspection
- Completed: cockpit inspection APIs for persisted risk decisions, enriched order inspection, and filtered recent events
- Completed: local/operator CLI fallback over the existing HTTP API for status, kill switch control, paper pipeline runs, strategies, orders, events, risk decisions, and backtests
- Current focus: tighten operator visibility, fallback ergonomics, and truthful paper-state inspection before any new product surface area

## Next implementation steps

1. Add paper trading reconciliation and lifecycle extensions
2. Add richer risk rules using data freshness and position state
3. Add kill switch operator workflows and monitoring polish across dashboard and CLI
4. Add strategy scheduling and bounded automation around the existing pipeline
5. Extend replay/backtest with richer sizing, short/flat state, and research workflows
6. Add simple charts only when the operational shell is already stable

## Explicitly deferred

- Live trading
- Real exchange order execution
- Private exchange streams and API keys
- Multi-exchange support
- Complex dashboard UI or heavy charting
- Complex terminal UI
- AuthN/AuthZ
- Production secrets management

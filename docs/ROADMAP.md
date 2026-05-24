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

## Next implementation steps

1. Extend replay/backtest with richer sizing, short/flat state, and research workflows
2. Add richer risk rules using data freshness and position state
3. Add paper trading reconciliation and lifecycle extensions
4. Add kill switch operator workflows and monitoring polish
5. Add strategy scheduling and bounded automation around the existing pipeline

## Explicitly deferred

- Live trading
- Real exchange order execution
- Private exchange streams and API keys
- Multi-exchange support
- Complex dashboard UI
- AuthN/AuthZ
- Production secrets management

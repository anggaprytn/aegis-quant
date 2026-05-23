# Roadmap

## MVP foundation

1. Rust workspace and compile-safe crate boundaries
2. Shared core types
3. Initial Postgres schema
4. Event model and publisher abstraction
5. Health and status API
6. Binance public market ingest and deterministic 1m candles
7. Persistent market feed status and market data read APIs

## Next implementation steps

1. Add strategy signal generation on stored candles
2. Expand replay/backfill primitives for market data
3. Add richer risk rules using data freshness and position state
4. Add paper trading reconciliation and lifecycle extensions
5. Add kill switch operator workflows and monitoring polish

## Explicitly deferred

- Live trading
- Real exchange order execution
- Private exchange streams and API keys
- Multi-exchange support
- Complex dashboard UI
- AuthN/AuthZ
- Production secrets management

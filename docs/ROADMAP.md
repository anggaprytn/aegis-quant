# Roadmap

## MVP foundation

1. Rust workspace and compile-safe crate boundaries
2. Shared core types
3. Initial Postgres schema
4. Event model and publisher abstraction
5. Health and status API

## Next implementation steps

1. Add event persistence into `system_events`
2. Add market ingest skeleton with symbol subscriptions
3. Add candle storage and replay primitives
4. Add deterministic strategy signal generation
5. Add risk engine veto flow
6. Add paper trading order lifecycle and reconciliation
7. Add persistent kill switch

## Explicitly deferred

- Live trading
- Real exchange order execution
- Multi-exchange support
- Complex dashboard UI
- AuthN/AuthZ
- Production secrets management

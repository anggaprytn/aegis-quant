# Architecture Overview

Aegis Quant v0.1 is deterministic execution infrastructure with paper and testnet-only boundaries. No live trading path is enabled.

```txt
                           PUBLIC MARKET DATA ONLY
          Binance public WS trades / public REST klines backfill
                                |
                                v
                    market-ingest / backfill services
                                |
                                v
                    Postgres: ticks, candles, feed status
                                |
      +-------------------------+-------------------------+
      |                         |                         |
      v                         v                         v
strategy dry-run         replay / backtest         read-only analytics
config validation        historical candles        and operator reports
      |                         |                         |
      v                         v                         v
persisted signals        backtest runs/trades      dashboard / CLI / API
      |
      v
persisted risk decisions
      |
      v
paper order intents
      |
      v
paper order lifecycle -> paper fills -> paper positions -> PnL / equity
      |
      v
readiness checks / audit trail / kill switch enforcement
```

## Shadow and testnet boundary

```txt
stored candles + validated strategy + validated risk + fresh local price
                                |
                                v
                    testnet shadow preview / runner
                                |
               persists would-submit state only, no submit
                                |
                                v
               owner-confirmed promotion preview and submit
                                |
                                v
             Binance Spot Testnet adapter and private stream only
                                |
                                v
      isolated testnet orders / lifecycle / reconciliation / repairs
```

## Safety boundaries

- No live execution path exists in v0.1.
- No production Binance private endpoints are used.
- Public market-data endpoints may be used for ingest/backfill only.
- Strategy logic cannot bypass persisted risk decisions.
- Shadow mode does not submit.
- Readiness, analytics, and reports are read-only decision support.

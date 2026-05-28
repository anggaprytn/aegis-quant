FROM rust:1.88-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.toml
COPY crates crates

RUN cargo build --release \
    -p api --bin api \
    -p market-ingest --bin market-ingest \
    -p api --bin testnet-shadow-runner \
    -p api --bin candle-aggregator

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/api /usr/local/bin/api
COPY --from=builder /app/target/release/testnet-shadow-runner /usr/local/bin/testnet-shadow-runner
COPY --from=builder /app/target/release/market-ingest /usr/local/bin/market-ingest
COPY --from=builder /app/target/release/candle-aggregator /usr/local/bin/candle-aggregator

EXPOSE 3000

CMD ["api"]

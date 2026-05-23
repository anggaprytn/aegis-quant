FROM rust:1.88-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.toml
COPY crates crates

RUN cargo build --release -p api

FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/api /usr/local/bin/aegis-quant-api

EXPOSE 3000

CMD ["aegis-quant-api"]

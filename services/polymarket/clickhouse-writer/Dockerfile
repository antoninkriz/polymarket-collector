# Two-stage build for the Polymarket Redis Stream → ClickHouse writer.
#
# Build context is the repo root:
#
#   docker build -f services/polymarket/clickhouse-writer/Dockerfile .

FROM docker.io/library/rust:1.97.1-slim-trixie AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        pkg-config \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY services/polymarket/clickhouse-writer/Cargo.toml ./services/polymarket/clickhouse-writer/Cargo.toml
COPY services/polymarket/clickhouse-writer/Cargo.lock ./services/polymarket/clickhouse-writer/Cargo.lock
COPY services/polymarket/clickhouse-writer/src ./services/polymarket/clickhouse-writer/src

WORKDIR /build/services/polymarket/clickhouse-writer
RUN cargo build --release --locked


FROM docker.io/library/debian:trixie-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/services/polymarket/clickhouse-writer/target/release/polymarket-clickhouse-writer /app/polymarket-clickhouse-writer

ENV RUST_LOG=info

CMD ["/app/polymarket-clickhouse-writer"]

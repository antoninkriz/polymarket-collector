# Two-stage build for the Polymarket Redis pub/sub → ClickHouse writer.
#
# Build context is the repo root:
#
#   docker build -f services/polymarket/polymarket-orderbook-rust-from-pubsub/Dockerfile .

FROM docker.io/library/rust:1.97.1-slim-trixie AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        pkg-config \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY shared/rust/polymarket-orderbook-rust/Cargo.toml ./shared/rust/polymarket-orderbook-rust/Cargo.toml
COPY shared/rust/polymarket-orderbook-rust/src ./shared/rust/polymarket-orderbook-rust/src

COPY services/polymarket/polymarket-orderbook-rust-from-pubsub/Cargo.toml ./services/polymarket/polymarket-orderbook-rust-from-pubsub/Cargo.toml
COPY services/polymarket/polymarket-orderbook-rust-from-pubsub/Cargo.lock ./services/polymarket/polymarket-orderbook-rust-from-pubsub/Cargo.lock
COPY services/polymarket/polymarket-orderbook-rust-from-pubsub/src ./services/polymarket/polymarket-orderbook-rust-from-pubsub/src

WORKDIR /build/services/polymarket/polymarket-orderbook-rust-from-pubsub
RUN cargo build --release --locked \
    && strip target/release/polymarket-orderbook-rust-from-pubsub


FROM docker.io/library/debian:trixie-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/services/polymarket/polymarket-orderbook-rust-from-pubsub/target/release/polymarket-orderbook-rust-from-pubsub /app/polymarket-orderbook-rust-from-pubsub

ENV RUST_LOG=info

CMD ["/app/polymarket-orderbook-rust-from-pubsub"]

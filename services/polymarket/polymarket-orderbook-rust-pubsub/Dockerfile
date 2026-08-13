# Two-stage build for the Polymarket pub/sub publisher.
#
# Build context is the repo root:
#
#   docker build -f services/polymarket/polymarket-orderbook-rust-pubsub/Dockerfile .

FROM rust:1.94-slim-bookworm AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        pkg-config \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY shared/rust/polymarket-orderbook-rust/Cargo.toml ./shared/rust/polymarket-orderbook-rust/Cargo.toml
COPY shared/rust/polymarket-orderbook-rust/src ./shared/rust/polymarket-orderbook-rust/src

COPY services/polymarket/polymarket-orderbook-rust-pubsub/Cargo.toml ./services/polymarket/polymarket-orderbook-rust-pubsub/Cargo.toml
COPY services/polymarket/polymarket-orderbook-rust-pubsub/Cargo.lock ./services/polymarket/polymarket-orderbook-rust-pubsub/Cargo.lock
COPY services/polymarket/polymarket-orderbook-rust-pubsub/src ./services/polymarket/polymarket-orderbook-rust-pubsub/src

WORKDIR /build/services/polymarket/polymarket-orderbook-rust-pubsub
RUN cargo build --release --locked \
    && strip target/release/polymarket-orderbook-rust-pubsub


FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/services/polymarket/polymarket-orderbook-rust-pubsub/target/release/polymarket-orderbook-rust-pubsub /app/polymarket-orderbook-rust-pubsub

ENV RUST_LOG=info

CMD ["/app/polymarket-orderbook-rust-pubsub"]

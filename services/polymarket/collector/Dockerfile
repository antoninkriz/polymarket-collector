# Two-stage build for the Polymarket collector.
#
# Build context is the repo root:
#
#   docker build -f services/polymarket/collector/Dockerfile .

FROM docker.io/library/rust:1.97.1-slim-trixie AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        pkg-config \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY services/polymarket/collector/Cargo.toml ./services/polymarket/collector/Cargo.toml
COPY services/polymarket/collector/Cargo.lock ./services/polymarket/collector/Cargo.lock
COPY services/polymarket/collector/src ./services/polymarket/collector/src

WORKDIR /build/services/polymarket/collector
RUN cargo build --release --locked


FROM docker.io/library/debian:trixie-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/services/polymarket/collector/target/release/polymarket-collector /app/polymarket-collector

ENV RUST_LOG=info

CMD ["/app/polymarket-collector"]

# Two-stage build for the Polymarket ClickHouse to Parquet archive exporter.
#
# Build context is the repository root:
#
#   docker build -f services/polymarket/archive-exporter/Dockerfile .

FROM docker.io/library/rust:1.97.1-slim-trixie AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        cmake \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build/services/polymarket/archive-exporter
COPY services/polymarket/archive-exporter/Cargo.toml ./Cargo.toml
COPY services/polymarket/archive-exporter/Cargo.lock ./Cargo.lock
COPY services/polymarket/archive-exporter/src ./src

RUN cargo build --release --locked


FROM docker.io/library/debian:trixie-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/services/polymarket/archive-exporter/target/release/polymarket-archive-exporter /app/polymarket-archive-exporter

ENV RUST_LOG=info

CMD ["/app/polymarket-archive-exporter"]

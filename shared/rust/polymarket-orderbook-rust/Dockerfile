# Two-stage build: rust:1.94-slim builder, debian:bookworm-slim runtime.
#
# Build context is the repo root (matches the Python service Dockerfile),
# so paths are relative to the repository root.
#
#   docker build -f services/polymarket-orderbook-rust/Dockerfile .

FROM rust:1.94-slim-bookworm AS builder

WORKDIR /build

# System deps for the build (rustls means no openssl-dev needed).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# --- Dependency cache layer ---
# Copy only the manifest first and build a stub binary so cargo populates
# the dependency cache. Subsequent source-only changes reuse this layer.
COPY services/polymarket-orderbook-rust/Cargo.toml ./Cargo.toml
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src target/release/polymarket-orderbook-rust* target/release/deps/polymarket_orderbook_rust*

# --- Real source build ---
COPY services/polymarket-orderbook-rust/src ./src
RUN cargo build --release \
    && strip target/release/polymarket-orderbook-rust


FROM debian:bookworm-slim AS runtime

# rustls uses the system trust store; only ca-certificates is needed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/polymarket-orderbook-rust /app/polymarket-orderbook-rust

ENV RUST_LOG=info

CMD ["/app/polymarket-orderbook-rust"]

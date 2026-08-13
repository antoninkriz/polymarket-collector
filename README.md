# polymarket-orderbook-collector

A collector for Polymarket's public orderbook feed: market discovery,
WebSocket ingestion, replayable ClickHouse storage, and hourly Parquet archival
to S3-compatible object storage. The upstream feed has no exchange sequence or
replay cursor; v3 preserves the collector's actual observations and makes
disconnect boundaries explicit rather than claiming an exchange audit log.

## Pipeline

```
Polymarket Gamma REST ──▶ polymarket-active-markets ──▶ Redis (active_markets cache + market_events stream)
Polymarket WS ──▶ polymarket-orderbook-rust-pubsub ──▶ Redis Stream `polymarket:events:v3`
                  polymarket-orderbook-rust-from-pubsub ──▶ ClickHouse `polymarket_orderbook_v3`
                  r2-archive exporter ──▶ R2 hourly Parquet
```

- `services/polymarket/polymarket-active-markets` — Python; polls the Gamma API,
  maintains the Redis active-markets cache and lifecycle event stream.
- `services/polymarket/polymarket-orderbook-rust-pubsub` — Rust; the only service
  holding Polymarket WS connections. Captures receive time plus one compact
  observed-order sequence and appends all seven event variants to a durable
  Redis Stream. A renewable Redis lease and fenced `XADD` enforce one
  authoritative publisher.
- `services/polymarket/polymarket-orderbook-rust-from-pubsub` — Rust; consumes the
  stream and acknowledges rows only after the compact raw v3 ClickHouse insert.
  Logical ad-hoc reads use `FINAL` to collapse same-sequence transport retries.
- `services/r2-archive/exporter` — projects one typed Parquet per event/hour
  named by `R2_BUCKET`.
- `shared/rust/polymarket-orderbook-rust` — shared library crate (WS pool, REST
  client, ClickHouse sink, event types) used by both Rust binaries.
- `shared/py` — shared Python helpers used by the Python services' Dockerfiles.

## Running

```sh
cp .env.example .env   # fill in secrets
docker compose -f docker-compose.infra.yml up -d       # ClickHouse, Redis, dozzle
docker compose -f docker-compose.polymarket.yml up -d --build
```

Services use `network_mode: host` and expect Redis on `localhost:6380` and
ClickHouse on `localhost:8124`, as provided by the infra compose file.

### Complete local pipeline with local Parquet

The local runner starts Redis, ClickHouse, market discovery, WebSocket
collection, the ClickHouse writer, and the archive exporter:

```sh
./run_local.sh
```

It automatically uses a working Docker Compose installation, or Podman with
either `podman compose` or `podman-compose`. Docker is preferred when both are
available. Override the selection when necessary:

```sh
CONTAINER_RUNTIME=podman ./run_local.sh
CONTAINER_RUNTIME=docker ./run_local.sh
```

It creates `.env` from `.env.example` when needed and writes the archive to
`.data/parquet/YYYY-MM-DD/HH/EVENT_TYPE.parquet`. No R2 credentials are
required. The files are written with the current host user ID, and an existing
completed hour is recognized by its `manifest.json` when the stack restarts.

Useful commands are:

```sh
./run_local.sh logs
./run_local.sh status
./run_local.sh down
```

`down` retains Redis, ClickHouse, and Parquet data under `.data`. The first
Parquet files cannot be produced until the receive-time hour in which
collection began is complete and a record from the following hour has reached
ClickHouse. This can take a little over one hour.

### Live stream without ClickHouse persistence

The WebSocket publisher queries ClickHouse once at startup to recover the
durable sequence-generation floor, even when the ClickHouse writer is not
started. Run Redis and ClickHouse, then omit only the writer and exporter:

```sh
docker compose -f docker-compose.infra.yml up -d \
  obdata-redis \
  obdata-clickhouse
docker compose -f docker-compose.polymarket.yml up -d --build \
  obdata-polymarket-active-markets \
  obdata-polymarket-orderbook-rust-pubsub
```

Then inspect the durable firehose (~10k+ events/sec across all markets):

```sh
docker exec obdata-redis redis-cli XRANGE polymarket:events:v3 - + COUNT 1
```

To persist events in ClickHouse, add
`obdata-polymarket-orderbook-rust-from-pubsub` to the application `up` command.

The v3 data contract and replay rules are documented in
[`docs/POLYMARKET_V3.md`](docs/POLYMARKET_V3.md). The exact per-file Parquet
schemas are documented in
[`docs/PARQUET_EXPORT.md`](docs/PARQUET_EXPORT.md).

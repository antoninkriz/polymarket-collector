# polymarket-orderbook-collector

A Rust collector for Polymarket's public market-data feed. It discovers the
active market universe, captures WebSocket observations in collector order,
stores a compact durable event log, and exports completed UTC hours as typed
Parquet files to Cloudflare R2 or the local filesystem.

Polymarket exposes neither an exchange sequence nor a replay cursor. The
archive therefore preserves exactly what this collector observed and makes
each post-connect orderbook segment reconstructible; it does not claim to be
an exchange audit log.

## Architecture

```text
Polymarket WS lifecycle ─┐
Polymarket Gamma REST ───┼─▶ Rust publisher ─▶ Redis Stream ─▶ Rust writer
Redis restart cache ─────┘                         │              │
                                                  │              ▼
                                                  │         ClickHouse raw v3
                                                  │              │
                                                  └──────────────┴─▶ Rust exporter ─▶ R2/local
```

- `services/polymarket/polymarket-orderbook-rust-pubsub` owns market
  discovery, lifecycle state, WebSocket subscriptions, receive timestamps,
  collector sequencing, and publication to `polymarket:events:v3`. WebSocket
  `new_market` observations subscribe immediately; rate-limited Gamma keyset
  scans reconcile startup state and missed lifecycle messages. A Redis restart
  cache avoids waiting for a complete Gamma scan after an ordinary restart.
- `services/polymarket/polymarket-orderbook-rust-from-pubsub` consumes the
  durable Redis Stream and acknowledges entries only after ClickHouse commits
  them to `polymarket_orderbook_v3`.
- `services/r2-archive/exporter` streams completed receive-time hours from
  ClickHouse into seven event-specific Parquet files and publishes
  `manifest.json` last. It supports local atomic files and bounded multipart R2
  uploads.
- `shared/rust/polymarket-orderbook-rust` contains the shared event, WebSocket,
  Gamma, lifecycle, Redis-cache, and ClickHouse code.

Redis deliberately separates live collection from ClickHouse restarts or
backpressure. ClickHouse supplies compact queryable raw storage and a stable
hourly export boundary. After a validated archive manifest and all seven files
exist, the exporter removes old ClickHouse partitions while retaining roughly
three hours and always preserving the newest partition for sequence recovery.

## Running the stack

```sh
cp .env.example .env
# Fill in ClickHouse and R2 credentials, then:
docker compose -f docker-compose.infra.yml up -d
docker compose -f docker-compose.polymarket.yml up -d --build
```

The application services use host networking. The supplied infrastructure
binds Redis to `localhost:6380` and ClickHouse HTTP to `localhost:8124`.

`EXPORT_BACKEND=r2` requires an existing R2 bucket and the four `R2_*`
settings. Set `CLICKHOUSE_RETENTION_HOURS=0` to disable archive-gated
ClickHouse cleanup; the default is `3`.

## Complete local pipeline

The local runner starts Redis, ClickHouse, collection, persistence, and local
Parquet export:

```sh
./run_local.sh
```

It automatically selects Docker Compose or Podman Compose. Override the
selection when needed:

```sh
CONTAINER_RUNTIME=podman ./run_local.sh
CONTAINER_RUNTIME=docker ./run_local.sh
```

No R2 credentials are required in local mode. Completed hours appear under:

```text
.data/parquet/YYYY-MM-DD/HH/EVENT_TYPE.parquet
```

Useful commands:

```sh
./run_local.sh logs
./run_local.sh status
./run_local.sh down
```

`down` retains Redis, ClickHouse, and Parquet data under `.data`. The first
archive hour is eligible only after that receive-time hour is complete and the
following hour has reached ClickHouse, so the first files can take a little
over one hour to appear.

## Publisher without the ClickHouse writer

The publisher still needs Redis and a readable ClickHouse instance at startup
to recover its durable sequence-generation floor. To inspect the Redis event
stream without starting the writer or exporter:

```sh
docker compose -f docker-compose.infra.yml up -d \
  obdata-redis \
  obdata-clickhouse
docker compose -f docker-compose.polymarket.yml up -d --build \
  obdata-polymarket-orderbook-rust-pubsub

docker exec obdata-redis \
  redis-cli XRANGE polymarket:events:v3 - + COUNT 1
```

Add `obdata-polymarket-orderbook-rust-from-pubsub` when ClickHouse persistence
is wanted.

The collection, ordering, deduplication, and replay contract is documented in
[`docs/POLYMARKET_V3.md`](docs/POLYMARKET_V3.md). Exact Parquet schemas and
encodings are documented in
[`docs/PARQUET_EXPORT.md`](docs/PARQUET_EXPORT.md).

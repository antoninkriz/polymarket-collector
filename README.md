# polymarket-orderbook-collector

A production-oriented Rust pipeline for capturing Polymarket's public CLOB
market-data feed in collector order, isolating live WebSocket ingestion from
storage failures, and exporting reconstructible hourly archives as compact,
typed Parquet files to Cloudflare R2 or the local filesystem.

## The archive at a glance

Every completed UTC receive-time hour is a self-contained directory:

```text
2026-08-13/14/
├── best_bid_ask.parquet
├── book.parquet
├── last_trade_price.parquet
├── market_resolved.parquet
├── new_market.parquet
├── price_change.parquet
├── tick_size_change.parquet
└── manifest.json
```

| File | What one row represents |
|---|---|
| `book.parquet` | A complete aggregated bid/ask snapshot for one outcome asset. |
| `price_change.parquet` | The new absolute size at one `(side, price)` level; zero removes it. |
| `last_trade_price.parquet` | One trade observation with price, size, side, fee, and Polygon transaction hash. |
| `tick_size_change.parquet` | A market's old and new tick size. |
| `best_bid_ask.parquet` | An independently observed best bid, best ask, and spread. |
| `new_market.parquet` | A newly observed binary market, its two assets, outcomes, and metadata. |
| `market_resolved.parquet` | A resolved market and its winning asset/outcome when supplied. |
| `manifest.json` | The completion marker and integrity index for the entire hour. |

Key format rules:

- **Common columns:** `timestamp_received`, `sequence`, source `timestamp`, and
  a 32-byte `market`; token files add a 32-byte `asset_id`.
- **Exact values:** prices and sizes are decimals, and book sides are typed
  lists of `(price, size)` structs.
- **Physical order:** `(market, asset_id, sequence)` for token events and
  `(market, sequence)` for lifecycle events.
- **Replay order:** always `sequence`, even across different files.
- **Completion:** trust an hour only when `manifest.json` exists.
- **Full specification:** [`docs/PARQUET_EXPORT.md`](docs/PARQUET_EXPORT.md).

## Architecture

```text
Polymarket WS lifecycle ─┐
Polymarket Gamma REST ───┼─▶ Rust publisher ─▶ Redis Stream ─▶ Rust writer
Redis restart cache ─────┘                        │              │
                                                  │              ▼
                                                  │         ClickHouse raw v3
                                                  │              │
                                                  └──────────────┴─▶ Rust exporter ─▶ R2/local
```

### Publisher — discovery, collection, and order

[`polymarket-orderbook-rust-pubsub`](services/polymarket/polymarket-orderbook-rust-pubsub)

- Owns market state, authoritative WebSocket subscriptions, receive timestamps,
  and collector sequencing.
- Listens for `new_market` on three lifecycle sockets and subscribes its assets
  immediately—important for short-lived markets.
- Restores known markets from a validated Redis cache after restart.
- Reconciles through one rate-limited Gamma client: new markets every 10
  seconds, resolutions every 30 seconds, and a full scan every 30 minutes.

### Redis Stream — the durable handoff

- Separates live WebSocket collection from ClickHouse restarts and write
  latency.
- Uses a renewable lease and fencing generation so exactly one publisher can
  append to `polymarket:events:v3`.
- Keeps records pending until ClickHouse commits; Redis AOF is enabled.
- Absorbs short storage interruptions. It is not sized as a multi-hour RAM
  backlog.

### Writer and ClickHouse — a compact raw window

[`polymarket-orderbook-rust-from-pubsub`](services/polymarket/polymarket-orderbook-rust-from-pubsub)

- Batches Redis records into `polymarket_orderbook_v3`.
- Stores only receive time, collector sequence, and normalized JSON in hourly
  partitions.
- Collapses delivery retries by `sequence`; it never deduplicates by payload.
- Keeps ClickHouse as a short queryable export window. Validated archived
  partitions expire after three hours by default, while the newest is retained
  for sequence recovery.

### Exporter — typed, immutable hours

The [`Rust exporter`](services/r2-archive/exporter):

- Streams Arrow batches into event-specific Parquet without buffering an hour.
- Writes ZSTD level 9 files and publishes `manifest.json` last.
- Supports atomic local output and bounded multipart uploads to an existing R2
  bucket.
- Safely retries interrupted hours; an incomplete directory has no manifest.

### Correctness model

- `sequence` is the authoritative order. Source and receive timestamps may tie
  and are never used to invent exchange order.
- After a connection or subscription begins, `price_change` rows are withheld
  until a fresh `book` snapshot makes that asset reconstructible again.
- A retry of one collector record keeps its sequence and is idempotent. A
  separate source observation gets a new sequence and is retained even when
  its public fields are identical.
- `transaction_hash` is not a fill ID: one Polygon transaction can settle
  multiple fills, so trade payload fields are never used for deduplication.
- Polymarket exposes no replay cursor or source sequence. The archive faithfully
  records what this collector accepted, but it is not an exchange audit log and
  cannot recreate an event missed during an upstream outage.

The complete collection, restart, timestamp, reconnection, and replay contract
is in [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md).

## Development and operations

### Run the complete pipeline locally

Requires Linux plus Docker Compose or Podman Compose:

```sh
./run_local.sh
```

This creates `.env` when needed, starts the complete stack, builds the Rust
services, and exports to `.data/parquet` without R2 credentials.

Use a specific container runtime when both are installed:

```sh
CONTAINER_RUNTIME=podman ./run_local.sh
CONTAINER_RUNTIME=docker ./run_local.sh
```

| Command | Action |
|---|---|
| `./run_local.sh` | Build and start the entire local pipeline. |
| `./run_local.sh logs` | Follow publisher, writer, and exporter logs. |
| `./run_local.sh status` | Show infrastructure and application containers. |
| `./run_local.sh down` | Stop everything while retaining collected data. |

Redis, ClickHouse, and Parquet data remain under `.data`. Completed files
appear in `.data/parquet/YYYY-MM-DD/HH/`. An hour becomes exportable only after
it is complete and the following receive-time hour has reached ClickHouse, so
the first archive can take a little over one hour to appear.

The supplied ports are loopback-only:

| Service | Address |
|---|---|
| Redis | `localhost:6380` |
| ClickHouse HTTP | `localhost:8124` |
| ClickHouse native | `localhost:9003` |

### Run in production

Production uses an existing R2 bucket. Prepare the environment:

1. Create the R2 bucket.
2. Set a strong `CLICKHOUSE_PASSWORD`.
3. Set `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY`, and `R2_SECRET_KEY`.
4. Restrict access to `.env`.

```sh
cp .env.example .env
chmod 600 .env
$EDITOR .env
```

Start infrastructure and applications with Docker Compose:

```sh
docker compose -f docker-compose.infra.yml up -d
docker compose -f docker-compose.polymarket.yml \
  up -d --build --remove-orphans
```

Production notes:

- Application containers use host networking to reach loopback Redis and
  ClickHouse. Keep those ports private.
- Services use `restart: unless-stopped`.
- `CLICKHOUSE_RETENTION_HOURS=0` retains every raw partition.
- Do not use `run_local.sh` for R2: its local overlay switches the exporter to
  `.data/parquet`.

### Monitor the pipeline

Start with container health and structured logs:

```sh
docker compose -f docker-compose.infra.yml ps
docker compose -f docker-compose.polymarket.yml ps
docker compose -f docker-compose.polymarket.yml logs -f --tail=200
```

The infrastructure Compose file also provides Dozzle on
`http://127.0.0.1:8080` for Docker deployments. Keep it on loopback; an SSH
tunnel is convenient for remote viewing:

```sh
ssh -L 8080:127.0.0.1:8080 collector-host
```

The most useful signals are:

| Area | Healthy pattern | Investigate when |
|---|---|---|
| Publisher | `[QUEUE-STATS]` queues return to zero; `pool_stats` reports current Gamma poll ages. | Queue high-water repeatedly approaches capacity, Gamma ages keep growing, or `assets_down` remains elevated. |
| Reconnects | `[ASSET-DATA-GAP]` is followed quickly by `[ASSET-DATA-RECOVERED]`. | Recovery latency grows, connections stay down, or gaps cluster continuously. |
| Writer | `[POLYMARKET-FROM-PUBSUB-STATS]` shows low queue depth, zero parse failures, and small `forwarded_minus_acked`. | Pending work grows from minute to minute or ClickHouse retries persist. |
| Redis | Stream consumer lag and `used_memory` remain bounded. | The writer is unavailable and the stream grows; current traffic can consume many GiB per hour. |
| Exporter | One `completed receive-time hour` per hour, followed by safe partition cleanup. | Export retries continue, no new manifest appears, or cleanup repeatedly retains a supposedly complete hour. |

Useful Redis checks:

```sh
docker exec obdata-redis redis-cli XINFO GROUPS polymarket:events:v3
docker exec obdata-redis redis-cli INFO memory
```

For an archive-level check, verify that each expected hour has a readable
`manifest.json`, that its seven listed objects exist, and that consumers verify
the recorded SHA-256 digests before trusting the hour.

### System requirements

| | Minimum | Recommended |
|---|---:|---:|
| CPU | 4 modern vCPUs | 8 modern vCPUs |
| RAM | 16 GiB | 32 GiB |
| Local disk with R2 | 50 GiB SSD | 100 GiB NVMe |
| Network | 100 Mbit/s symmetric | 200 Mbit/s symmetric; 1 Gbit/s for comfortable recovery |
| Local archive | Add 22 GB/day retained | Add 0.65 TB per 30 days plus headroom |

> **Reference workload:** measured on 2026-08-14 with approximately 160,000
> binary markets, 320,000 assets, 1,280 WebSocket connections, and 94.5 million
> normalized events per hour (about 26,000/s). Polymarket traffic varies.

| Resource | Measured reference | Explanation |
|---|---|---|
| CPU | Roughly one aggregate core in steady state. | ClickHouse merges, cold subscription, and hourly export are bursty; 4 vCPUs is tight and 8 leaves useful headroom. |
| RAM | About 6–7 GiB total; the publisher used roughly 3–4 GiB. | 16 GiB supports a healthy full-universe pipeline; 32 GiB provides safer burst and backlog capacity. |
| ClickHouse disk | About 3.4–3.8 GiB per raw hour; the rolling window normally uses 15–20 GiB. | SSD latency matters during simultaneous writes, merges, and export. Container images and temporary files also need room. |
| Parquet archive | About 0.8–0.9 GB/hour, or 20–22 GB/day. | Local retention needs roughly 0.65 TB per 30 days before filesystem headroom; R2 avoids that persistent local cost. |
| Incoming network | Roughly 60–75 Mbit/s sustained, with snapshot and reconnect bursts. | 100 Mbit/s is a tight minimum. More bandwidth shortens full-universe and reconnect recovery. |
| Outgoing network | Control traffic stayed below 1 Mbit/s; R2 averages about 2 Mbit/s but uploads hourly in bursts. | Faster outbound completes R2 uploads well before the next hour becomes eligible. |

RAM sizing assumes the ClickHouse writer is healthy. A writer outage leaves
the durable Redis Stream growing at approximately the uncompressed event rate:
the reference hour contained about 27 GiB of normalized JSON before Redis
overhead. Monitor lag and memory closely rather than treating Redis as a
multi-hour backlog store.

## Documentation

- [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md) — collection guarantees,
  timestamps, ordering, reconnect behavior, deduplication, and replay.
- [`docs/PARQUET_EXPORT.md`](docs/PARQUET_EXPORT.md) — every exported column,
  Arrow and Parquet type, nullability, encoding, file order, and manifest field.

## Thanks

This project builds on the work and public datasets that made independent
Polymarket market-data research practical:

- [PMXT archive](https://archive.pmxt.dev/)
- [AG6 Polymarket archive](https://polymarket-archive.ag6.ai/)
- [Original `pmxt-dev/polymarket-orderbook-collector` repository](https://github.com/pmxt-dev/polymarket-orderbook-collector)

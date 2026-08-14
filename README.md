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

Every event row carries `timestamp_received`, `sequence`, Polymarket's source
`timestamp`, and a 32-byte `market` condition ID. Token files also carry a
32-byte `asset_id`. Prices and sizes are exact decimals, orderbook sides are
typed lists of `(price, size)` structs, and nullable columns reflect genuine
source nullability rather than a union of unrelated event schemas.

Files are clustered by `(market, asset_id, sequence)`—or `(market, sequence)`
for lifecycle events—for compact storage and fast selective reads. Global
replay order is always `sequence`. Treat an hour as complete only when its
manifest exists. The full schema, encoding, and integrity contract is in
[`docs/PARQUET_EXPORT.md`](docs/PARQUET_EXPORT.md).

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
owns the live market universe, authoritative WebSocket subscriptions, receive
timestamps, and collector sequencing. It listens for `new_market` on three
lifecycle sockets and subscribes the new assets immediately, which avoids
waiting for a polling cycle on short-lived markets.

A validated Redis restart cache restores known markets quickly. One
rate-limited Gamma client then reconciles the universe with 10-second new-market
polls, 30-second resolution polls, and full keyset scans every 30 minutes.
WebSocket lifecycle messages remain the low-latency primary source.

### Redis Stream — the durable handoff

Redis decouples live collection from ClickHouse restarts and transient write
latency. A renewable lease and fencing generation allow exactly one publisher
to append to `polymarket:events:v3`. Redis AOF is enabled, and stream records
remain pending until the writer confirms their ClickHouse commit.

This boundary protects the WebSocket process from short storage interruptions;
it is not intended to hold hours of peak traffic in RAM. All in-process queues
are bounded, backpressured, and report high-water marks.

### Writer and ClickHouse — a compact raw window

[`polymarket-orderbook-rust-from-pubsub`](services/polymarket/polymarket-orderbook-rust-from-pubsub)
batches stream records into `polymarket_orderbook_v3`. The table stores only
nanosecond receive time, collector sequence, and normalized event JSON in
hourly partitions. `ReplacingMergeTree` keyed by sequence collapses retries of
the same collector record without attempting unsafe payload-based
deduplication.

ClickHouse is a queryable export buffer, not the permanent archive. After an
hour's manifest and all seven objects have been validated, old partitions are
removed according to `CLICKHOUSE_RETENTION_HOURS` (three hours by default).
The newest partition is always retained for restart sequence recovery.

### Exporter — typed, immutable hours

The [`Rust exporter`](services/r2-archive/exporter) streams ClickHouse Arrow
batches into event-specific Parquet files without buffering a full hour in
memory. It writes ZSTD level 9 files, publishes `manifest.json` last, and
supports atomic local output or bounded multipart uploads to an existing R2
bucket. Interrupted hours are safe to retry because an incomplete directory
has no completion manifest.

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

The local runner needs Linux plus Docker Compose or Podman Compose. It creates
`.env` from `.env.example` when needed, starts Redis and ClickHouse, builds all
three Rust services, and overrides the exporter to use local storage:

```sh
./run_local.sh
```

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

The production path uses R2. Create the bucket first, copy the environment
template, set a strong ClickHouse password and all four `R2_*` values, then
protect the credential file:

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

The application containers use host networking so they can reach the
loopback-bound Redis and ClickHouse ports. All long-running services use
`restart: unless-stopped`. Keep those database ports private; remote access
should go through an authenticated tunnel or a firewall-restricted endpoint.

`EXPORT_BACKEND=r2` requires `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY`, and
`R2_SECRET_KEY`. Set `CLICKHOUSE_RETENTION_HOURS=0` only when every raw
ClickHouse partition should be retained. Do not use `run_local.sh` for an R2
deployment: its local Compose overlay intentionally changes the archive
backend to `.data/parquet`.

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

### Capacity planning

Traffic changes with the number and activity of Polymarket markets. The
following reference was measured on 2026-08-14 while collecting the full
universe: approximately 160,000 binary markets, 320,000 assets, 1,280 WebSocket
connections, and 94.5 million normalized events per hour (about 26,000/s).

| Resource | Reference workload | Practical starting point |
|---|---|---|
| CPU | Roughly one aggregate core in steady state; ClickHouse merges, cold subscription, and hourly export are bursty. | 8 modern vCPUs recommended; 4 is a tight minimum. |
| RAM | About 6–7 GiB across publisher, writer, Redis, ClickHouse, and an idle exporter; the publisher accounted for roughly 3–4 GiB. | 16 GiB minimum for a healthy full-universe pipeline; 32 GiB recommended. |
| ClickHouse disk | About 3.4–3.8 GiB per raw receive hour. The default rolling window normally occupies roughly 15–20 GiB including the current partition and headroom. | Fast SSD/NVMe with at least 100 GiB free when exporting to R2. |
| Parquet archive | About 0.8–0.9 GB per hour, or 20–22 GB/day at the reference rate. | For local retention, budget roughly 0.65 TB per 30 days plus filesystem headroom. |
| Incoming network | Roughly 60–75 Mbit/s of sustained upstream data in the measured run, with snapshot and reconnect bursts. | At least 200 Mbit/s reliable inbound; 1 Gbit/s gives comfortable recovery headroom. |
| Outgoing network | Subscription/control traffic was below 1 Mbit/s. R2 data averages about 2 Mbit/s over a day but uploads each hour in a burst. | At least 100 Mbit/s outbound for timely R2 uploads; 200 Mbit/s symmetric is a sensible baseline. |

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

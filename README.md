# Polymarket Collector

This project collects live data from Polymarket across all prediction markets with periodic hourly Parquet exports. Compared to its predecessors, it should correctly handle short-lived markets such as 5-minute BTC up/down markets, preserve the original event order for correct order book reconstruction, and stores different types of market event data separately in an optimized Parquet layout.

## The archive structure TLDR

Every completed UTC receive-time hour is a self-contained directory:

```text
YYYY-MM-DD/HH/
├── best_bid_ask.parquet
├── book.parquet
├── last_trade_price.parquet
├── market_resolved.parquet
├── new_market.parquet
├── price_change.parquet
├── tick_size_change.parquet
└── manifest.json
```

| File                       | What one row represents                                                          |
| -------------------------- | -------------------------------------------------------------------------------- |
| `book.parquet`             | A complete aggregated bid/ask snapshot for one outcome asset.                    |
| `price_change.parquet`     | The new absolute size at one `(side, price)` level; zero removes it.             |
| `last_trade_price.parquet` | One trade observation with price, size, side, fee, and Polygon transaction hash. |
| `tick_size_change.parquet` | A market's old and new tick size.                                                |
| `best_bid_ask.parquet`     | An independently observed best bid, best ask, and spread.                        |
| `new_market.parquet`       | A newly observed binary market, its two assets, outcomes, and metadata.          |
| `market_resolved.parquet`  | A resolved market and its winning asset/outcome when supplied.                   |
| `manifest.json`            | The completion marker and integrity index for the entire hour.                   |

Full format specification can be found in [`docs/PARQUET_EXPORT.md`](docs/PARQUET_EXPORT.md).

Key format rules:

- **Common columns:** `timestamp_received`, `sequence`, source `timestamp`, and `market`; token files add `asset_id`.
- **Exact values:** prices and sizes are decimals, and book sides are typed lists of `(price, size)` structs.
- **Physical order:** `(market, asset_id, sequence)` for token events and `(market, sequence)` for lifecycle events.
- **Replay order:** always `sequence` column as seqnum, even across different files.
- **Completion:** trust an hour only when `manifest.json` exists.



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

### Timestamps

There are two kinds of timestamps in the data.

First, there is the `timestamp` field, which represents Polymarket's internal timestamp. This one has only milliseconds precision and its actual source inside Polymarket's infrastructure is not discosed.

Then, there is the `timestamp_received`, marking the nanosecond timestamp of the arrival of the published message into the Rust code of the collector. Somewhat accurate schema is below.

```text
Polymarket infrastructure
│
├─ Event is published
│  └─ timestamp = Polymarket internal timestamp
│     (millisecond precision; exact source unknown)
│
└─ Network / TLS
   │
   ▼
Collector machine
│
├─ NIC
├─ Kernel / TCP stack
├─ WebSocket processing
│
└─ Complete WebSocket text frame becomes available to Rust
   │
   ├─ timestamp_received = Utc::now()
   │  (nanosecond-resolution timestamp)
   │
   ▼
JSON parsing / event expansion
   │
   ▼
Sequence allocation
   │
   ▼
Redis
   │
   ▼
ClickHouse
   │
   ▼
Parquet
```

### Publisher — discovery, collection, and order

`[polymarket-orderbook-rust-pubsub](services/polymarket/polymarket-orderbook-rust-pubsub)`

- Owns market state, authoritative WebSocket subscriptions, receive timestamps, and collector sequencing.
- Listens for `new_market` on three lifecycle sockets and subscribes its assets immediately—important for short-lived markets.
- Restores known markets from a validated Redis cache after restart.
- Reconciles through one rate-limited Gamma client: new markets every 10 seconds, resolutions every 30 seconds, and a full scan every 30 minutes.



### Redis Stream — the durable handoff

- Separates live WebSocket collection from ClickHouse restarts and write latency.
- Uses a renewable lease and fencing generation so exactly one publisher can append to `polymarket:events:v3`.
- Keeps records pending until ClickHouse commits; Redis AOF is enabled.
- Absorbs short storage interruptions. It is not sized as a multi-hour RAM backlog.



### Writer and ClickHouse — a compact raw window

`[polymarket-orderbook-rust-from-pubsub](services/polymarket/polymarket-orderbook-rust-from-pubsub)`

- Batches Redis records into `polymarket_orderbook_v3`.
- Stores only receive time, collector sequence, and normalized JSON in hourly partitions.
- Collapses delivery retries by `sequence`; it never deduplicates by payload.
- Keeps ClickHouse as a short queryable export window. Validated archived partitions expire after three hours by default, while the newest is retained for sequence recovery.



### Exporter — typed, immutable hours

The `[Rust exporter](services/r2-archive/exporter)`:

- Streams Arrow batches into event-specific Parquet without buffering an hour.
- Writes ZSTD level 9 files and publishes `manifest.json` last.
- Supports atomic local output and bounded multipart uploads to an existing R2 bucket.
- Safely retries interrupted hours; an incomplete directory has no manifest.



### Correctness model

- `sequence` is the authoritative order. Source and receive timestamps may tie and are never used to invent exchange order.
- After a connection or subscription begins, `price_change` rows are withheld until a fresh `book` snapshot makes that asset reconstructible again.
- A retry of one collector record keeps its sequence and is idempotent. A separate source observation gets a new sequence and is retained even when its public fields are identical.
- `transaction_hash` is not a fill ID: one Polygon transaction can settle multiple fills, so trade payload fields are never used for deduplication.
- Polymarket exposes no replay cursor or source sequence. The archive faithfully records what this collector accepted, but it is not an exchange audit log and cannot recreate an event missed during an outage.

The complete collection, restart, timestamp, reconnection, and replay contract is in `[docs/DATA_MODEL.md](docs/DATA_MODEL.md)`.



## Development and operations

### Local development and execution

Requires Linux plus Docker Compose or Podman Compose:

```sh
./run_local.sh
```

This creates `.env` when needed, starts the complete stack, builds the Rust services, and exports to `.data/parquet` without R2 credentials.

Use a specific container runtime when both are installed:

```sh
CONTAINER_RUNTIME=podman ./run_local.sh
CONTAINER_RUNTIME=docker ./run_local.sh
```

| Command                 | Action                                          |
| ----------------------- | ----------------------------------------------- |
| `./run_local.sh`        | Build and start the entire local pipeline.      |
| `./run_local.sh logs`   | Follow publisher, writer, and exporter logs.    |
| `./run_local.sh status` | Show infrastructure and application containers. |
| `./run_local.sh down`   | Stop everything while retaining collected data. |

Redis, ClickHouse, and Parquet data remain under `.data`. Completed files appear in `.data/parquet/YYYY-MM-DD/HH/`. An hour becomes exportable only after it is complete and the following receive-time hour has reached ClickHouse, so the first archive can take a little over one hour to appear.

Redis and ClickHouse are reachable only inside the private `obdata` container network:

| Service           | Internal address         |
| ----------------- | ------------------------ |
| Redis             | `obdata-redis:6379`      |
| ClickHouse HTTP   | `obdata-clickhouse:8123` |
| ClickHouse native | `obdata-clickhouse:9000` |

Use `docker exec` or `podman exec` for local inspection rather than exposing a database port.

### Run in production

Production uses an existing R2 bucket. Prepare the environment:

1. Create the R2 bucket.
2. Set a strong `CLICKHOUSE_PASSWORD`.
3. Set `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY`, and `R2_SECRET_KEY`.
4. Restrict access to `.env`.
5. Confirm the host can provide a high open-file limit.

```sh
cp .env.example .env
chmod 600 .env
nano .env
```

Start infrastructure and applications with Docker Compose:

```sh
docker compose -f docker-compose.infra.yml up -d --remove-orphans
docker compose -f docker-compose.polymarket.yml up -d --build --remove-orphans
```

Start infrastructure first: it creates the private `obdata` bridge that the application Compose project joins.

Production notes:

- Redis, ClickHouse, and all Rust services share the private `obdata` bridge. Redis and ClickHouse publish no host ports.
- Keep `RLIMIT_NOFILE` high. The supplied configuration requests 262,144 open files through `CONTAINER_NOFILE_LIMIT`; 65,536 is a practical minimum for the full market universe. Rootless Podman cannot request more than `ulimit -Hn`, so lower the setting to that host limit when necessary.
- Use a host firewall with default-deny inbound rules and allow only required management traffic—normally SSH or a VPN. Docker manages its own iptables/nftables rules, so verify the effective exposure after deployment instead of relying only on a firewall frontend.
- Services use `restart: unless-stopped`.
- `CLICKHOUSE_RETENTION_HOURS=0` retains every raw partition.
- Do not use `run_local.sh` for R2: its local overlay switches the exporter to `.data/parquet`.



### Monitor the pipeline

Start with container health and structured logs:

```sh
docker compose -f docker-compose.infra.yml ps
docker compose -f docker-compose.polymarket.yml ps
docker compose -f docker-compose.polymarket.yml logs -f --tail=200
```

The most useful signals are:

| Area       | Healthy pattern                                                                                                 | Investigate when                                                                                             |
| ---------- | --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Publisher  | `[QUEUE-STATS]` queues return to zero; `pool_stats` reports current Gamma poll ages.                            | Queue high-water repeatedly approaches capacity, Gamma ages keep growing, or `assets_down` remains elevated. |
| Reconnects | `[ASSET-DATA-GAP]` is followed quickly by `[ASSET-DATA-RECOVERED]`.                                             | Recovery latency grows, connections stay down, or gaps cluster continuously.                                 |
| Writer     | `[POLYMARKET-FROM-PUBSUB-STATS]` shows low queue depth, zero parse failures, and small `forwarded_minus_acked`. | Pending work grows from minute to minute or ClickHouse retries persist.                                      |
| Redis      | Stream consumer lag and `used_memory` remain bounded.                                                           | The writer is unavailable and the stream grows; current traffic can consume many GiB per hour.               |
| Exporter   | One `completed receive-time hour` per hour, followed by safe partition cleanup.                                 | Export retries continue, no new manifest appears, or cleanup repeatedly retains a supposedly complete hour.  |

Useful Redis checks:

```sh
docker exec obdata-redis redis-cli XINFO GROUPS polymarket:events:v3
docker exec obdata-redis redis-cli INFO memory
```

For an archive-level check, verify that each expected hour has a readable `manifest.json`, that its seven listed objects exist, and that consumers verify the recorded SHA-256 digests before trusting the hour.

### System requirements

|                              | Minimum                | Recommended                               |
| ---------------------------- | ---------------------- | ----------------------------------------- |
| CPU                          | 4 modern vCPUs         | 8 modern vCPUs                            |
| RAM                          | 16 GiB                 | 32 GiB                                    |
| Open files (`RLIMIT_NOFILE`) | `32768`                | `262144`                                  |
| Local disk with R2           | 50 GiB SSD             | 100 GiB NVMe                              |
| Network                      | 100 Mbit/s symmetric   | 200+ Mbit/s symmetric; 1 Gbit/s preferred |
| Local archive                | Add 22 GB/day retained | Add 0.65 TB per 30 days plus headroom     |


> **Reference workload:** measured with approximately 160,000 binary markets,
> 320,000 assets, 1,280 WebSocket connections, and 94.5 million normalized
> events per hour (about 26,000/s). Polymarket traffic varies.


| Resource         | Measured reference                                                           | Explanation                                                                                                       |
| ---------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| CPU             | Roughly one aggregate core in steady state.                                   | ClickHouse merges, cold subscription, and hourly export are bursty; 4 vCPUs can be tight, 8 leaves nice headroom. |
| RAM             | About 6–7 GiB total; the publisher used roughly 3–4 GiB.                      | 16 GiB supports a healthy full-universe pipeline; 32 GiB provides safer burst and backlog capacity.               |
| Open files      | ClickHouse loves high `nofile` limit and we need around ~1500 WS connections. | A high limit avoids failures during reconnect waves, merges, and export.                                          |
| ClickHouse disk | About 3.4–3.8 GiB per raw hour; the rolling window normally uses 15–20 GiB.   | Any reasonable SSD should be fine, we need storage for the DB and exports.                                        |
| Parquet archive | About 0.8–0.9 GB/hour, or 20–22 GB/day.                                       | The data take around 0.65 TB per 30 days in local storage or R2.                                                  |
| Network in      | Roughly 60–75 Mbit/s sustained, with snapshot and reconnect bursts.           | 100 Mbit/s is a minimum. More bandwidth shortens full-universe and reconnect recovery.                            |
| Network out     | Control traffic stays below 1 Mbit/s; Burst uploads of ~1 GB of data hourly.  | Faster outbound completes R2 uploads well before the next hour becomes eligible.                                  |

RAM sizing assumes the ClickHouse writer is healthy. A writer outage leaves the durable Redis Stream growing at approximately the uncompressed event rate of about 30 GiB an hour before Redis overhead. Monitor lag and memory closely and consider Redis as a short term back-off rather than treating Redis as a multi-hour backlog store.

## Documentation

- `[docs/DATA_MODEL.md](docs/DATA_MODEL.md)` — collection guarantees, timestamps, ordering, reconnect behavior, deduplication, and replay.
- `[docs/PARQUET_EXPORT.md](docs/PARQUET_EXPORT.md)` — every exported column, Arrow and Parquet type, nullability, encoding, file order, and manifest field.



## Thanks

This project builds on the work and public datasets that made independent Polymarket market-data research practical:

- [PMXT archive](https://archive.pmxt.dev/)
- [AG6 Polymarket archive](https://polymarket-archive.ag6.ai/)
- [Original](https://github.com/pmxt-dev/polymarket-orderbook-collector) `pmxt-dev/polymarket-orderbook-collector` [repository](https://github.com/pmxt-dev/polymarket-orderbook-collector)



## Vibecoded

This project has been extensively ✨vibecoded✨ step by step with *some* human reviews in the process, with all its drawbacks and benefits . Please keep that in mind.

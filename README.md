# polymarket-orderbook-collector

A production-grade collector for the full Polymarket orderbook firehose:
market discovery, websocket ingestion, ClickHouse storage, and hourly Parquet
archival to S3-compatible object storage.

## Pipeline

```
Polymarket Gamma REST ──▶ polymarket-active-markets ──▶ Redis (active_markets cache + market_events stream)
Polymarket WS ──▶ polymarket-orderbook-rust-pubsub ──▶ Redis pub/sub `polymarket:events`
                  polymarket-orderbook-rust-from-pubsub ──▶ ClickHouse `polymarket_orderbook_rust`
                  r2-archive exporter (EXPORTER_PROFILE=polymarket) ──▶ R2 hourly Parquet snapshots
```

- `services/polymarket/polymarket-active-markets` — Python; polls the Gamma API,
  maintains the Redis active-markets cache and lifecycle event stream.
- `services/polymarket/polymarket-orderbook-rust-pubsub` — Rust; the only service
  holding Polymarket WS connections. Publishes all 4 event variants
  (book / price_change / last_trade_price / tick_size_change) to Redis pub/sub.
- `services/polymarket/polymarket-orderbook-rust-from-pubsub` — Rust; consumes the
  pub/sub channel and writes to ClickHouse with full fidelity.
- `services/r2-archive/exporter` — generic exporter, run with
  `EXPORTER_PROFILE=polymarket`, dumps hourly Parquet to the
  bucket named by `R2_BUCKET`.
- `services/polymarket/orderbook-compare` — ad-hoc comparison script (was untracked
  on the prod server; preserved here).
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

### Feed only — zero config, no accounts, no ClickHouse

Each secret is only needed by the service that uses it: R2 credentials by the
archive exporter, `CLICKHOUSE_PASSWORD` by the ClickHouse writer. The live feed
itself needs neither — no `.env` at all:

```sh
docker compose -f docker-compose.infra.yml up -d obdata-redis
docker compose -f docker-compose.polymarket.yml up -d --build \
  obdata-polymarket-active-markets \
  obdata-polymarket-orderbook-rust-pubsub
```

Then subscribe to the firehose (~10k+ events/sec across all markets):

```sh
docker exec obdata-redis redis-cli SUBSCRIBE polymarket:events
```

To also record into ClickHouse, set `CLICKHOUSE_PASSWORD`, start
`obdata-clickhouse` from the infra compose, and add
`obdata-polymarket-orderbook-rust-from-pubsub` to the `up` command.

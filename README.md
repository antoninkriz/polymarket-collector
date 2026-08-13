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
  observed-order sequence and appends all four event variants to a durable
  Redis Stream. A renewable Redis lease and fenced `XADD` enforce one
  authoritative publisher.
- `services/polymarket/polymarket-orderbook-rust-from-pubsub` — Rust; consumes the
  stream and acknowledges rows only after the compact raw v3 ClickHouse insert.
  Logical ad-hoc reads use `FINAL` to collapse same-sequence transport retries.
- `services/r2-archive/exporter` — projects typed hourly Parquet to the bucket
  named by `R2_BUCKET`.
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

Then inspect the durable firehose (~10k+ events/sec across all markets):

```sh
docker exec obdata-redis redis-cli XRANGE polymarket:events:v3 - + COUNT 1
```

To also record into ClickHouse, set `CLICKHOUSE_PASSWORD`, start
`obdata-clickhouse` from the infra compose, and add
`obdata-polymarket-orderbook-rust-from-pubsub` to the `up` command.

The v3 data contract and replay rules are documented in
[`docs/POLYMARKET_V3.md`](docs/POLYMARKET_V3.md).

# Collection and data model

This document defines the collector's correctness contract: what is observed, how events are ordered, how reconnects affect reconstruction, and what the archive can and cannot prove. For the exact Parquet columns, types, encodings, and manifest format, see [`PARQUET_EXPORT.md`](PARQUET_EXPORT.md).

## Guarantees at a glance

| Property | Contract |
|---|---|
| Observation order | `sequence` is the authoritative total order assigned by this collector. |
| Receive time | `timestamp_received` is sampled as soon as the WebSocket library yields a text frame, before parsing or queueing. |
| Orderbook recovery | After a new connection or subscription, deltas are withheld until a fresh `book` snapshot arrives for that asset. |
| Delivery retries | Redis and ClickHouse may retry a record without duplicating it logically because the retry retains its `sequence`. |
| Source duplicates | Separate WebSocket observations are retained even when every public payload field is identical. |
| Market discovery | WebSocket lifecycle events are primary; a Redis restart cache and rate-limited Gamma scans reconcile missed state. |
| Archive completion | An hour is complete only after all seven event files exist and `manifest.json` has been published. |
| Upstream completeness | The archive reproduces what the collector accepted; it is not an exchange audit log. |

## Raw event log

ClickHouse stores a compact, replayable log rather than the wide exported schema:

```sql
CREATE TABLE polymarket_orderbook_v3 (
    timestamp_received DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    sequence           UInt64               CODEC(Delta, ZSTD(1)),
    data               String               CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree()
PARTITION BY toStartOfHour(timestamp_received)
ORDER BY sequence;
```

`data` is one normalized child event encoded as JSON. It contains the source `timestamp`, `market`, `event_type`, and the fields belonging to that event type. Token-scoped events contain `asset_id`; lifecycle events contain the market's complete `assets_ids` list. A multi-entry `price_change` frame becomes consecutive child events in its original array order.

The log deliberately omits payload hashes, raw parent messages, connection identifiers, collector identifiers, transport identifiers, and schema-version columns. None is required to replay the stream observed by this collector. The non-unique Polymarket `hash` field is also removed.

## Ordering across processes and restarts

Polymarket supplies neither an exchange sequence nor a unique public fill ID. Source and receive timestamps may tie, and wall clocks can move. `sequence` is therefore the sole replay key:

- it records the exact order in which normalized events were admitted;
- children of one source frame receive consecutive values in source order;
- an at-least-once retry keeps the same value; and
- a new source observation always receives a new value.

The high 16 bits hold a Redis-issued publisher generation and the low 48 bits hold process-local event order. Before opening WebSockets, the publisher reads the generation from ClickHouse's maximum sequence, compares it with `PUBLISHER_GENERATION_FLOOR`, acquires the Redis publisher lease, and advances the Redis generation above that floor. Redis AOF persistence is confirmed before collection starts.

Archive retention always preserves the newest ClickHouse partition, even when it is older than the retention interval, so ordinary restarts retain a durable sequence watermark. If Redis and ClickHouse are both lost while Parquet survives, find the greatest `max_sequence` in the hourly manifests, shift it right by 48 bits, and set `PUBLISHER_GENERATION_FLOOR` to at least that value before restarting. A lower floor could reuse an already exported sequence.

## Market discovery and subscriptions

Each binary market and its two outcome assets have one authoritative WebSocket route. The publisher enables Polymarket's `custom_feature_enabled` option on every subscription and collects:

- `book`
- `price_change`
- `last_trade_price`
- `tick_size_change`
- `best_bid_ask`
- `new_market`
- `market_resolved`

Three lifecycle listeners keep `new_market` discovery available across an individual socket failure. A WebSocket `new_market` observation is sent to the central lifecycle controller immediately; the controller establishes the authoritative asset subscriptions before admitting the lifecycle event. This path does not wait for a poll and is important for short-lived markets.

The lifecycle controller is the single owner of condition and asset state. It serializes WebSocket and Gamma observations, rejects conflicting asset ownership, suppresses repeated lifecycle state, and prevents a stale `new_market` observation from reactivating a resolved market.

The same Rust publisher owns the reconciliation paths:

- a validated Redis cache restores the last active market set immediately;
- complete Gamma keyset scans run every 30 minutes;
- incremental new-market scans run every 10 seconds; and
- resolved-market scans run every 30 seconds.

On restart, a bounded recent Gamma scan first subscribes active markets missing from the cache and moves recent cached markets ahead of the paced long tail. Incremental reconciliation begins at the cache's `fetched_at` watermark, so markets created while the collector was stopped are not hidden by startup time.

The publisher never replaces a restart-cache snapshot until the current process has completed both a full active-market scan and the initial resolved-market catch-up. This preserves the previous cache and its honest `fetched_at` timestamp if either startup reconciliation is interrupted. After both complete, changed snapshots are saved periodically and during graceful shutdown.

All Gamma work shares a 10 request/second limiter and bounded retry policy. WebSocket lifecycle events remain the low-latency source. Gamma adds missing subscriptions and may synthesize a missing lifecycle row only when a usable source creation or closure timestamp exists.

## Timestamp semantics

Every exported row has two timestamps with different meanings:

| Column | Meaning |
|---|---|
| `timestamp` | Millisecond timestamp supplied by Polymarket. It is source data, not a safe tie-breaker. |
| `timestamp_received` | Nanosecond UTC wall-clock time sampled in userspace when the complete source frame becomes available to the collector. |

All normalized children of one WebSocket frame share its receive timestamp. The timestamp is taken before JSON parsing, fan-out, Redis, or ClickHouse work, but it still includes network-stack, TLS, and scheduler latency; it is not a kernel or hardware packet timestamp.

For a lifecycle event recovered from Gamma, `timestamp_received` is sampled after the complete HTTP body is available and before JSON decoding, while `timestamp` is Gamma's creation or closure time. This records the honest recovery time instead of inventing the receive time of a WebSocket frame that was never observed.

## Disconnects and reconstructible books

The client sends a text `PING` every 10 seconds and requires activity within a separate 5-second deadline. A timeout, peer close, read error, or stream EOF reconnects after deterministic 0–750 ms jitter. Exponential backoff, capped at 60 seconds, is reserved for failed connection attempts and other local session errors.

When a connection or subscription starts, an asset is marked unavailable until its fresh `book` snapshot arrives. `price_change` events for that asset are discarded during this interval, so post-reconnect deltas are never applied to stale pre-reconnect depth. The snapshot replaces the complete reconstructed book and begins a new valid segment. Trades and tick-size observations remain in collector order because they do not mutate depth.

Connection gaps and asset recovery latency are emitted as structured logs. They are not archive rows: the first `book` in the new segment is the replay boundary. No connection epoch is needed in Parquet because stale-segment deltas cannot cross that boundary.

## Delivery, backpressure, and duplication

A Redis lease prevents concurrent publishers, and every stream append is atomically fenced by the lease generation. Redis then separates WebSocket collection from ClickHouse restarts and write latency. The writer acknowledges and removes a stream entry only after ClickHouse commits it.

Only retries of the same collector record are collapsed. They carry the same `sequence`, and `ReplacingMergeTree` keyed by sequence provides idempotency; queries requiring the immediately deduplicated logical result use `FINAL`.

Payload-based deduplication is intentionally forbidden for market-data events. `transaction_hash`, market, asset, timestamp, price, size, side, and fee do not form a unique fill identity: Polygon can settle multiple fills in one transaction, and the feed may legitimately deliver identical-looking observations. A repeated source observation therefore receives a new sequence and remains in the archive. Lifecycle state is the exception: the central controller admits each market transition once.

All internal queues are bounded and backpressured:

| Queue | Capacity |
|---|---:|
| Publisher events | 250,000 records |
| WebSocket and reconciliation lifecycle inputs | 8,192 observations each |
| ClickHouse writer input | 50,000 records |
| ClickHouse acknowledgement channel | 32 batches |

Services sample depths each second and log 60-second high-water marks. Redis is the durable buffer; the in-process queues are deliberately not sized as a second database.

## Archive and replay

The Rust exporter reads completed receive-time hours with `FINAL` and produces seven typed, event-specific Parquet files. It streams Arrow batches into ZSTD level 9 Parquet row groups and publishes `manifest.json` last. Token-scoped files are physically ordered by `(market, asset_id, sequence)` and lifecycle files by `(market, sequence)` for compression and selective reads.

Physical row order is not the global replay order. To replay a complete hour:

1. Read the relevant event files and merge their rows by `sequence`.
2. For each asset, begin at its first `book`; the snapshot replaces all depth.
3. Apply each `price_change` by assigning `size` to `(side, price)`; zero removes the level.
4. Deliver trades and other observations in sequence order without payload-based deduplication.

`best_bid_ask` is an independently observed top-of-book notification, not a depth mutation. Lifecycle rows are market-scoped and do not have an `asset_id`. See [`PARQUET_EXPORT.md`](PARQUET_EXPORT.md) for the complete file contract.

The exporter removes an old ClickHouse partition only after validating the hour's manifest, all seven exact file entries, statistics, digests, and archive objects. Cleanup failures retain the partition for a later attempt. The newest partition is always kept.

## Limits of the public feed

The archive reproduces the observations admitted by this collector. It cannot recover a WebSocket event missed during an upstream disconnect or collector outage because Polymarket exposes no replay cursor or source sequence. Gamma can restore the active subscription and lifecycle state, but it cannot restore an omitted orderbook delta or trade, its original receive timestamp, or its place in matching-engine order. Consumers must not infer exchange-level completeness from a public-feed capture.

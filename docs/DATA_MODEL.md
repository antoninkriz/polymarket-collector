# Collection and data model

This document defines the collector's correctness contract: what is observed, how events are ordered, how reconnects affect reconstruction, and what the archive can and cannot prove. For the exact Parquet columns, types, encodings, and manifest format, see [`PARQUET_EXPORT.md`](PARQUET_EXPORT.md).

## Guarantees at a glance

| Property | Contract |
|---|---|
| Observation order | `sequence` is the authoritative total order assigned by this collector. |
| Receive time | WebSocket rows are stamped when the library yields a complete text frame; Gamma-recovered lifecycle rows are stamped after the complete HTTP response arrives. |
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

The Redis Stream carries the same logical row as exactly three fields: nonnegative nanosecond `timestamp_received`, unsigned `sequence`, and `data` containing one JSON object. The writer validates this exact shape and preserves the `data` text while wrapping batches as ClickHouse `JSONEachRow`; it does not import or deserialize the collector's event types.

## Ordering across processes and restarts

Polymarket supplies neither an exchange sequence nor a unique public fill ID. Source and receive timestamps may tie, and wall clocks can move. `sequence` is therefore the sole replay key:

- it records the exact order in which normalized events were admitted;
- children of one source frame receive consecutive values in source order;
- an at-least-once retry keeps the same value; and
- a new source observation always receives a new value.

The high 16 bits hold a Redis-issued publisher generation and the low 48 bits hold process-local event order. Before opening WebSockets, the collector reads the generation from ClickHouse's maximum sequence, compares it with `PUBLISHER_GENERATION_FLOOR`, acquires the Redis publisher lease, and advances the Redis generation above that floor. Redis AOF persistence is confirmed before collection starts.

Archive retention always preserves the newest ClickHouse partition, even when it is older than the retention interval, so ordinary restarts retain a durable sequence watermark. If Redis and ClickHouse are both lost while Parquet survives, find the greatest `max_sequence` in the hourly manifests, shift it right by 48 bits, and set `PUBLISHER_GENERATION_FLOOR` to at least that value before restarting. A lower floor could reuse an already exported sequence.

## Market discovery and subscriptions

Each binary market and its two outcome assets have one authoritative WebSocket route. The collector enables Polymarket's `custom_feature_enabled` option on every subscription and collects:

- `book`
- `price_change`
- `last_trade_price`
- `tick_size_change`
- `best_bid_ask`
- `new_market`
- `market_resolved`

Three lifecycle listeners keep `new_market` discovery available across an individual socket failure. A WebSocket `new_market` observation is sent to the central lifecycle controller immediately; the controller admits the lifecycle row before issuing the authoritative asset subscription, so the lifecycle sequence precedes any token event from the new route. This path does not wait for a poll and is important for short-lived markets.

The lifecycle controller is the single owner of condition and asset state. It serializes WebSocket and Gamma observations, rejects conflicting asset ownership, suppresses repeated lifecycle state, and prevents a stale `new_market` observation from reactivating a resolved market.

The same collector owns the reconciliation paths:

- a validated Redis cache restores the last active market set immediately;
- complete Gamma keyset scans run every 30 minutes;
- incremental new-market scans run every 10 seconds; and
- resolved-market scans run every 30 seconds.

On restart, a bounded recent Gamma scan first subscribes active markets missing from the cache and moves recent cached markets ahead of the paced long tail. Incremental reconciliation begins at the cache's conservative `fetched_at` watermark, so markets created while the collector was stopped are not hidden by startup time.

The collector never replaces a restart-cache snapshot until the current process has completed a full active-market scan, a successful new-market poll, and a successful resolved-market poll. Its stored `fetched_at` is the earlier of the two successful poll watermarks minus a two-minute overlap; it is never the wall-clock save time. This preserves delayed Gamma visibility and leaves the previous cache untouched if any startup reconciliation is interrupted. After the complete baseline, a changed market revision or coverage watermark is saved at most once per minute and during graceful shutdown.

All Gamma work shares a 10 request/second limiter and bounded retry policy. WebSocket lifecycle events remain the low-latency source. Gamma adds missing subscriptions and may synthesize a missing lifecycle row only when a usable source creation or closure timestamp exists.

## Timestamp semantics

Every exported row has two timestamps with different meanings:

| Column | Meaning |
|---|---|
| `timestamp` | Millisecond timestamp supplied by Polymarket. It is source data, not a safe tie-breaker. |
| `timestamp_received` | Nanosecond UTC wall-clock time sampled in userspace when the complete WebSocket frame or Gamma HTTP response becomes available to the collector. |

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

The collector's internal queues are bounded. Saturation is treated as a fatal data-integrity failure rather than silently dropping an admitted record:

| Queue | Capacity |
|---|---:|
| Collector events | 1,000,000 records |
| WebSocket and reconciliation lifecycle inputs | 8,192 observations each |

The ClickHouse writer has no in-process handoff queues. One actor owns at most one 5,000-record batch, retains its Redis delivery IDs until ClickHouse commits, and acknowledges them directly with `XACKDEL`. While ClickHouse is unavailable it stops reading rather than accumulating a second volatile backlog; unread records remain durable in Redis. `EVENT_CONSUMER_GROUP` and `EVENT_CONSUMER_NAME` must remain stable across restarts; changing either requires explicitly migrating or claiming pending entries and cleaning up the old group when applicable. Services log 60-second high-water marks and counters for their bounded state.

## Archive and replay

The Rust exporter reads completed receive-time hours with `FINAL` and produces seven typed, event-specific Parquet files. It streams Arrow batches into ZSTD level 9 Parquet row groups and publishes `manifest.json` last. Token-scoped files are physically ordered by `(market, asset_id, sequence)` and lifecycle files by `(market, sequence)` for compression and selective reads.

Physical row order is not the global replay order. To replay a complete hour:

1. Read the relevant event files and merge their rows by `sequence`.
2. For each asset, begin at its first `book`; the snapshot replaces all depth.
3. Apply each `price_change` by assigning `size` to `(side, price)`; zero removes the level.
4. Deliver trades and other observations in sequence order without payload-based deduplication.

`best_bid_ask` is an independently observed top-of-book notification, not a depth mutation. Lifecycle rows are market-scoped and do not have an `asset_id`. See [`PARQUET_EXPORT.md`](PARQUET_EXPORT.md) for the complete file contract.

The exporter removes an old ClickHouse partition only after validating the hour's manifest, all seven exact file entries, statistics, digest syntax, and object existence. It does not download every Parquet object and recompute its digest during retention; archive consumers should verify the recorded SHA-256 values when reading an hour. Cleanup failures retain the partition for a later attempt. The newest partition is always kept.

## Limits of the public feed

The archive reproduces the observations admitted by this collector. It cannot recover a WebSocket event missed during an upstream disconnect or collector outage because Polymarket exposes no replay cursor or source sequence. Gamma can restore the active subscription and lifecycle state, but it cannot restore an omitted orderbook delta or trade, its original receive timestamp, or its place in matching-engine order. Consumers must not infer exchange-level completeness from a public-feed capture.

# Polymarket orderbook v3

## What v3 stores

V3 is a compact raw event log. ClickHouse stores only the receive time, the
collector's observed order, and the normalized event:

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

There is no per-row schema version, payload hash, raw parent message,
connection or collector identifier, transport identifier, or session metadata.
Those fields are not needed to reconstruct an observed market stream.

`data` is one normalized child event encoded as JSON. It contains the source
`timestamp`, `market`, `event_type`, and fields owned by that event type. Token
events contain `asset_id`; market lifecycle events contain the full
`assets_ids` list instead of fabricating one row per token. The non-unique
Polymarket `hash` is stripped. A multi-entry `price_change` parent becomes
consecutive child events in its original order.

## Why `sequence` exists

Polymarket supplies neither an exchange sequence nor a unique public fill ID.
Source and receive timestamps can tie, and clocks can move, so neither is a
safe replay key. `sequence` is the one piece of collector metadata required for
correctness:

- it orders every normalized event exactly as the collector admitted it;
- it orders the two outcome assets of a market without a later timestamp merge;
- the same record keeps the same value when Redis or ClickHouse retries it; and
- a genuinely repeated source delivery receives a new value and is retained.

The high 16 bits contain the Redis-issued publisher generation and the low 48
bits contain the process-local event order. This keeps restarts monotonic in one
column. The generation is an internal fencing mechanism, not another exported
provenance field.

At startup the publisher reads the generation in ClickHouse's maximum stored
`sequence`, takes the greater of that value and `PUBLISHER_GENERATION_FLOOR`,
and atomically advances the Redis generation above that floor while acquiring
the publisher lease. It does not open WebSockets until Redis confirms that the
new generation has reached its append-only file. The supplied Redis service
therefore runs with AOF enabled. An ordinary process restart, or loss of only
the Redis generation key, cannot reuse an exported sequence.

If both Redis and ClickHouse are lost while R2 survives, find the greatest
`max_sequence` across the retained hourly manifests, shift it right by 48 bits,
and set `PUBLISHER_GENERATION_FLOOR` to at least that generation before starting
the publisher. This is an explicit disaster-recovery override; starting with a
lower floor could reuse sequence values already present in Parquet.

## Collection rules

Each binary market and both of its outcome assets have exactly one
authoritative WebSocket route. A Redis lease prevents a second publisher from
writing concurrently, and its fencing token is checked atomically with every
stream append.

Every subscription enables Polymarket's `custom_feature_enabled` wire option.
`best_bid_ask` remains token-scoped and is accepted only from that token's
authoritative route. `market_resolved` is accepted on the route that owns one
of the event's `assets_ids`; this is where Polymarket delivers the
market-scoped notification. `new_market` is accepted on three lifecycle
listeners so one disconnected socket does not hide global discovery. A central
controller suppresses repeated `(event_type, market)` lifecycle observations
before assigning a sequence or storing a row. The three listeners remain
connected with an empty token set if their markets are removed, including
`--new-only` operation and reconnects.

WebSocket lifecycle events add and remove subscriptions immediately. The
Gamma pollers remain an idempotent reconciliation path for startup state,
missed notifications, and upstream lifecycle-feed outages.

The client sends Polymarket's text `PING` every ten seconds and arms a separate
five-second response deadline. Any frame received after the ping proves the
socket is alive. A heartbeat timeout, peer close, read error, or stream EOF
reconnects after only a deterministic 0--750 ms jitter; exponential backoff is
reserved for failed connection attempts and other local session errors.

`timestamp_received` is sampled immediately after tungstenite yields a text
frame, before JSON parsing, fan-out, Redis, or ClickHouse work. All children of
one frame share that nanosecond UTC wall-clock value. It is an honest userspace
socket receive time, not a kernel or hardware packet timestamp.

After a connection or subscription starts, the collector discards
`price_change` events for an asset until that asset's fresh `book` snapshot has
arrived. Consequently an exported asset segment never applies post-reconnect
deltas to a stale pre-reconnect book. Trades and tick-size events remain in
their observed order because they do not mutate depth.

## Duplication policy

Public payload fields are never used for deduplication. In particular,
`transaction_hash`, market, asset, timestamp, price, size, side, and fee cannot
identify a fill: Polygon can settle multiple fills in one transaction, and the
public feed can legitimately repeat identical-looking observations.

Only an at-least-once delivery of the same collector record is collapsed. Such
a retry has the same `sequence`; a separate WebSocket observation does not.
`ReplacingMergeTree` keyed by `sequence` provides this idempotency, and readers
that require an immediately logical result use `FINAL`. The subscriber
acknowledges and removes a Redis Stream entry only after ClickHouse commits it.

## Parquet export

The exporter reads the raw table with `FINAL` and writes one ZSTD level 9
Parquet file per event type and UTC receive-time hour:

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

The date is ISO ordered and the hour is always zero-padded from `00` through
`23`. The directory and filename encode the hour and event type, so neither is
repeated as a Parquet column.

The complete per-file data dictionary—including column order, Arrow types,
nullability, identifier encoding, Parquet value encodings, field semantics,
and the manifest contract—is in
[`PARQUET_EXPORT.md`](PARQUET_EXPORT.md).

Every file has these non-null common columns:

| Column | Arrow/Parquet type |
|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` |
| `sequence` | `uint64` |
| `timestamp` | `timestamp[ms, tz=UTC]` |
| `market` | `fixed_size_binary[32]` |

The remaining columns are specific to the file:

| File | Event-owned columns |
|---|---|
| `book.parquet` | `asset_id`, `bids`, `asks` |
| `price_change.parquet` | `asset_id`, `price`, `size`, `side`, `best_bid?`, `best_ask?` |
| `last_trade_price.parquet` | `asset_id`, `price`, `size`, `side`, `fee_rate_bps`, `transaction_hash` |
| `tick_size_change.parquet` | `asset_id`, `old_tick_size`, `new_tick_size` |
| `best_bid_ask.parquet` | `asset_id`, `best_bid?`, `best_ask?`, `spread?` |
| `new_market.parquet` | `id`, `assets_ids`, `outcomes`, `question?`, `slug?` |
| `market_resolved.parquet` | `id`, `assets_ids`, `winning_asset_id?`, `winning_outcome?` |

`asset_id`, `winning_asset_id`, and entries in `assets_ids` are
`fixed_size_binary[32]`; transaction hashes and market condition IDs use the
same width. Prices and tick sizes are Arrow `decimal32(9, 4)` backed by Parquet
`INT32`; sizes are Arrow `decimal64(18, 6)` backed by Parquet `INT64`; and
`fee_rate_bps` is `uint16`. `bids` and `asks` retain the compact JSON array
representation in raw ClickHouse, then export as typed
`list<struct<price: decimal32(9, 4), size: decimal64(18, 6)>>` columns. A `?`
marks genuine source nullability, not an unrelated column made nullable by
combining different event schemas.

Market condition IDs and transaction hashes are decoded from hexadecimal;
every singular or list-valued token ID is decoded as an unsigned 256-bit
integer. These identifier columns therefore use the same 32-byte big-endian
representation as `pmxtdata`'s processed identifier columns.
The export aborts rather than padding malformed hexadecimal IDs or wrapping an
out-of-range decimal token ID. A missing optional price remains null rather
than being changed to zero.

Each event file is ordered by `sequence`. The exporter writes all seven files,
including correctly typed zero-row files, before uploading `manifest.json`.
The manifest lists every file's columns, row count, byte size, digest, and
minimum/maximum sequence, plus the hour-wide row count and sequence range. Its
presence is the sole completion marker; data objects left by an interrupted
attempt are overwritten on retry and are not considered a completed hour.

## Replay

To replay the complete observed stream, open the seven files for an hour and
k-way merge their rows by `sequence`; a sequence belongs to exactly one file.
For one market:

1. Filter or merge the event files for that market in ascending `sequence`.
2. For each asset, begin at its first `book`; the snapshot replaces all depth.
3. Apply each `price_change` by assigning `size` at `(side, price)`; zero size
   removes the level.
4. Deliver `last_trade_price` rows in `sequence` order. Never deduplicate them
   by transaction hash or payload values.

`best_bid_ask` is an independently observed top-of-book notification, not a
depth delta, and is not applied during orderbook reconstruction. Lifecycle rows
are market-scoped and therefore have null `asset_id`.

No connection epoch is required in the file because reconnect deltas are
withheld until a new snapshot makes the stream self-initializing again.

## Explicit limits

V3 reproduces what the collector accepted; it is not an exchange audit log.
Polymarket provides no replay cursor or source sequence, so a disconnect or
collector outage can contain an unknowable gap. The Gamma reconciliation path
can restore the active subscription set, but cannot recreate a missed source
lifecycle row. Unsupported parent event types are logged and omitted. The
userspace receive timestamp includes network-stack, TLS, and scheduling
latency. Consumers must not infer upstream completeness or exchange order that
the public feed does not expose.

The compact layout is incompatible with earlier experimental v3 tables. Deploy
it to an empty table, or archive and recreate the old table explicitly;
`CREATE TABLE IF NOT EXISTS` cannot migrate an existing layout.

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
`timestamp`, `market`, `event_type`, `asset_id`, and fields owned by that event
type. The non-unique Polymarket `hash` is stripped. A multi-entry
`price_change` parent becomes consecutive child events in its original order.

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

## Collection rules

Each binary market and both of its outcome assets have exactly one
authoritative WebSocket route. A Redis lease prevents a second publisher from
writing concurrently, and its fencing token is checked atomically with every
stream append.

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

The exporter reads the raw table with `FINAL`, projects JSON into typed columns,
orders by `(market, sequence)`, and writes ZSTD level 1 Parquet:

| Column | Arrow/Parquet type | Nullable |
|---|---|---:|
| `timestamp_received` | `timestamp[ns, UTC]` | no |
| `sequence` | `uint64` | no |
| `timestamp` | `timestamp[ms, UTC]` | no |
| `market` | `fixed_size_binary[32]` | no |
| `event_type` | `string` | no |
| `asset_id` | `fixed_size_binary[32]` | no |
| `bids`, `asks` | JSON `string` | yes |
| `price`, `best_bid`, `best_ask` | `decimal(9, 4)` | yes |
| `size` | `decimal(18, 6)` | yes |
| `side` | `string` | yes |
| `fee_rate_bps` | `uint16` | yes |
| `transaction_hash` | `fixed_size_binary[32]` | yes |
| `old_tick_size`, `new_tick_size` | `decimal(9, 4)` | yes |

Market condition IDs and transaction hashes are decoded from hexadecimal;
decimal token IDs are decoded as unsigned 256-bit integers. All three therefore
use the same 32-byte big-endian representation as `pmxtdata`'s processed
identifier columns.
The export aborts rather than padding malformed hexadecimal IDs or wrapping an
out-of-range decimal token ID. Event-specific columns are null when they do not
apply.

The sidecar manifest is only an upload-completion marker with row count, byte
size, digest, source table, and sort order. It is not inserted into the data.

## Replay

For one market:

1. Read logical rows (`FINAL`) in ascending `sequence`.
2. For each asset, begin at its first `book`; the snapshot replaces all depth.
3. Apply each `price_change` by assigning `size` at `(side, price)`; zero size
   removes the level.
4. Deliver `last_trade_price` rows in `sequence` order. Never deduplicate them
   by transaction hash or payload values.

No connection epoch is required in the file because reconnect deltas are
withheld until a new snapshot makes the stream self-initializing again.

## Explicit limits

V3 reproduces what the collector accepted; it is not an exchange audit log.
Polymarket provides no replay cursor or source sequence, so a disconnect or
collector outage can contain an unknowable gap. Unsupported parent event types
are logged and omitted. The userspace receive timestamp includes network-stack,
TLS, and scheduling latency. Consumers must not infer upstream completeness or
exchange order that the public feed does not expose.

The compact layout is incompatible with earlier experimental v3 tables. Deploy
it to an empty table, or archive and recreate the old table explicitly;
`CREATE TABLE IF NOT EXISTS` cannot migrate an existing layout.

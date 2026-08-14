# Polymarket v3 Parquet export format

This document is the file-level contract for the event-specific v3 archive.
For collection guarantees, deduplication, restart ordering, and replay rules,
see [`POLYMARKET_V3.md`](POLYMARKET_V3.md).

## Directory layout

Each completed UTC receive-time hour contains seven Parquet files and one JSON
manifest:

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

Dates use `YYYY-MM-DD`; hours are zero-padded from `00` through `23`. The
filename is the event type, so Parquet files do not contain an `event_type`
column. Every file is present even when it has zero rows. Treat the hour as
complete only when `manifest.json` exists.

`sequence` is the authoritative collector observation order. The physical file
order clusters rows by market and asset for compression and selective reads.
To replay across assets, markets, or event files, combine the relevant rows and
order them by `sequence`.

## Schema conventions

Each event section below is self-contained. Its table lists every exported
column, including the four columns shared by all event types.

All binary identifiers are validated before export. `market` and
`transaction_hash` contain the raw 32 bytes decoded from their source `0x`
hexadecimal strings. `asset_id`, `assets_ids`, and `winning_asset_id` contain
unsigned decimal token IDs encoded as exactly 32 big-endian bytes. Render a
hash-like identifier with `"0x" + value.hex()`; recover a token ID with an
unsigned big-endian integer conversion. Malformed or out-of-range identifiers
abort the hour export instead of being truncated or wrapped.

All decimal columns are exact decimal values, not binary floating-point
numbers. `decimal32(9, 4)` is physically stored as Parquet `INT32`, and
`decimal64(18, 6)` is physically stored as Parquet `INT64`.

## `book.parquet`

A row is a complete aggregated depth snapshot for one outcome asset. It
replaces the reconstructed book state for that asset.

| File setting | Value |
|---|---|
| Path | `YYYY-MM-DD/HH/book.parquet` |
| Physical row order | `market`, `asset_id`, `sequence` ascending |
| Compression | ZSTD level 9 on every column |

| Column | Arrow type | Parquet physical and logical type | Nullable | Value encoding | Description |
|---|---|---|---:|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` | `INT64` + `TIMESTAMP(NANOS, UTC)` | no | `PLAIN` | Collector userspace receive time, sampled when the WebSocket library yields the frame. |
| `sequence` | `uint64` | `INT64` + `UINT(64)` | no | `PLAIN` | Globally ordered collector observation and retry identity; authoritative replay order. |
| `timestamp` | `timestamp[ms, tz=UTC]` | `INT64` + `TIMESTAMP(MILLIS, UTC)` | no | `PLAIN` | Millisecond source timestamp supplied by Polymarket; do not use it to break ordering ties. |
| `market` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Market condition ID. |
| `asset_id` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Outcome token whose orderbook is represented. |
| `bids` | `list<struct<price: decimal32(9, 4), size: decimal64(18, 6)>>` | `LIST`; `price` is `INT32` + `DECIMAL(9, 4)`; `size` is `INT64` + `DECIMAL(18, 6)` | no | `PLAIN` for both leaves | Aggregated bid levels in source order. The list, elements, and member values are non-null. |
| `asks` | `list<struct<price: decimal32(9, 4), size: decimal64(18, 6)>>` | `LIST`; `price` is `INT32` + `DECIMAL(9, 4)`; `size` is `INT64` + `DECIMAL(18, 6)` | no | `PLAIN` for both leaves | Aggregated ask levels in source order. The list, elements, and member values are non-null. |

An orderbook level is a typed `(price, size)` tuple. Arrow names its members
`price` and `size`, so readers commonly expose a level as:

```json
{"price":0.4800,"size":30.000000}
```

Empty book sides are empty lists. The upstream book-content `hash` is not
exported because it is neither a unique event identity nor required for
reconstruction.

## `price_change.parquet`

A row assigns the new aggregate size at one `(asset_id, side, price)` level.
Set that level to `size`; remove it when `size == 0`. One upstream message can
contain multiple changes. The collector assigns them consecutive sequence
values in their original array order.

| File setting | Value |
|---|---|
| Path | `YYYY-MM-DD/HH/price_change.parquet` |
| Physical row order | `market`, `asset_id`, `sequence` ascending |
| Compression | ZSTD level 9 on every column |

| Column | Arrow type | Parquet physical and logical type | Nullable | Value encoding | Description |
|---|---|---|---:|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` | `INT64` + `TIMESTAMP(NANOS, UTC)` | no | `PLAIN` | Collector userspace receive time, sampled when the WebSocket library yields the frame. |
| `sequence` | `uint64` | `INT64` + `UINT(64)` | no | `PLAIN` | Globally ordered collector observation and retry identity; authoritative replay order. |
| `timestamp` | `timestamp[ms, tz=UTC]` | `INT64` + `TIMESTAMP(MILLIS, UTC)` | no | `PLAIN` | Millisecond source timestamp supplied by Polymarket; do not use it to break ordering ties. |
| `market` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Market condition ID. |
| `asset_id` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Outcome token being changed. |
| `price` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | no | `PLAIN` | Affected price level. |
| `size` | `decimal64(18, 6)` | `INT64` + `DECIMAL(18, 6)` | no | `PLAIN` | New aggregate size, not a signed delta. Zero removes the level. |
| `side` | `string` | `BYTE_ARRAY` + `STRING` | no | `RLE_DICTIONARY` | `BUY` for the bid side or `SELL` for the ask side. |
| `best_bid` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | yes | `PLAIN` | Source-provided best bid after the change, when present. |
| `best_ask` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | yes | `PLAIN` | Source-provided best ask after the change, when present. |

The upstream per-change `hash` is omitted because it is unsafe for
deduplication. Reconstruction uses `price`, `size`, and `side`; the optional
best prices are accompanying observations, not additional deltas.

## `last_trade_price.parquet`

A row is one trade-execution notification observed on the public market
channel. Preserve every row, including rows with identical payload values or
the same transaction hash.

| File setting | Value |
|---|---|
| Path | `YYYY-MM-DD/HH/last_trade_price.parquet` |
| Physical row order | `market`, `asset_id`, `sequence` ascending |
| Compression | ZSTD level 9 on every column |

| Column | Arrow type | Parquet physical and logical type | Nullable | Value encoding | Description |
|---|---|---|---:|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` | `INT64` + `TIMESTAMP(NANOS, UTC)` | no | `PLAIN` | Collector userspace receive time, sampled when the WebSocket library yields the frame. |
| `sequence` | `uint64` | `INT64` + `UINT(64)` | no | `PLAIN` | Globally ordered collector observation and retry identity; authoritative replay order. |
| `timestamp` | `timestamp[ms, tz=UTC]` | `INT64` + `TIMESTAMP(MILLIS, UTC)` | no | `PLAIN` | Millisecond source timestamp supplied by Polymarket; do not use it to break ordering ties. |
| `market` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Market condition ID. |
| `asset_id` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Outcome token that traded. |
| `price` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | no | `RLE_DICTIONARY` | Execution price. |
| `size` | `decimal64(18, 6)` | `INT64` + `DECIMAL(18, 6)` | no | `PLAIN` | Executed size. |
| `side` | `string` | `BYTE_ARRAY` + `STRING` | no | `RLE_DICTIONARY` | `BUY` or `SELL`, from the taker's perspective. |
| `fee_rate_bps` | `uint16` | `INT32` + `UINT(16)` | no | `RLE_DICTIONARY` | Fee rate in basis points. |
| `transaction_hash` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Polygon transaction containing the fill; metadata, not a unique fill identity. |

One Polygon transaction can settle multiple fills. Never deduplicate on
`transaction_hash`, or on any combination of the public payload columns. Only
the collector-assigned `sequence` identifies a stored observation and
collapses an at-least-once transport retry.

## `tick_size_change.parquet`

A row announces a change to the accepted order-price increment for one outcome
asset. It does not change depth by itself.

| File setting | Value |
|---|---|
| Path | `YYYY-MM-DD/HH/tick_size_change.parquet` |
| Physical row order | `market`, `asset_id`, `sequence` ascending |
| Compression | ZSTD level 9 on every column |

| Column | Arrow type | Parquet physical and logical type | Nullable | Value encoding | Description |
|---|---|---|---:|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` | `INT64` + `TIMESTAMP(NANOS, UTC)` | no | `PLAIN` | Collector userspace receive time, sampled when the WebSocket library yields the frame. |
| `sequence` | `uint64` | `INT64` + `UINT(64)` | no | `PLAIN` | Globally ordered collector observation and retry identity; authoritative replay order. |
| `timestamp` | `timestamp[ms, tz=UTC]` | `INT64` + `TIMESTAMP(MILLIS, UTC)` | no | `PLAIN` | Millisecond source timestamp supplied by Polymarket; do not use it to break ordering ties. |
| `market` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Market condition ID. |
| `asset_id` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Outcome token whose tick size changed. |
| `old_tick_size` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | no | `PLAIN` | Tick size before the event. |
| `new_tick_size` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | no | `PLAIN` | Tick size after the event. |

## `best_bid_ask.parquet`

A row is an independently observed top-of-book notification enabled by
Polymarket's `custom_feature_enabled` subscription option. It is useful for BBO
analysis, but it is not a depth delta and must not be applied to reconstruct the
book.

| File setting | Value |
|---|---|
| Path | `YYYY-MM-DD/HH/best_bid_ask.parquet` |
| Physical row order | `market`, `asset_id`, `sequence` ascending |
| Compression | ZSTD level 9 on every column |

| Column | Arrow type | Parquet physical and logical type | Nullable | Value encoding | Description |
|---|---|---|---:|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` | `INT64` + `TIMESTAMP(NANOS, UTC)` | no | `PLAIN` | Collector userspace receive time, sampled when the WebSocket library yields the frame. |
| `sequence` | `uint64` | `INT64` + `UINT(64)` | no | `PLAIN` | Globally ordered collector observation and retry identity; authoritative replay order. |
| `timestamp` | `timestamp[ms, tz=UTC]` | `INT64` + `TIMESTAMP(MILLIS, UTC)` | no | `PLAIN` | Millisecond source timestamp supplied by Polymarket; do not use it to break ordering ties. |
| `market` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Market condition ID. |
| `asset_id` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Outcome token whose top of book was reported. |
| `best_bid` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | yes | `PLAIN` | Best bid, or null when the source provides no bid. |
| `best_ask` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | yes | `PLAIN` | Best ask, or null when the source provides no ask. |
| `spread` | `decimal32(9, 4)` | `INT32` + `DECIMAL(9, 4)` | yes | `PLAIN` | Source-provided ask-minus-bid spread, or null when unavailable. |

## `new_market.parquet`

A row announces a newly available binary market. This event is market-scoped,
so it has no synthetic `asset_id` column.

| File setting | Value |
|---|---|
| Path | `YYYY-MM-DD/HH/new_market.parquet` |
| Physical row order | `market`, `sequence` ascending |
| Compression | ZSTD level 9 on every column |

| Column | Arrow type | Parquet physical and logical type | Nullable | Value encoding | Description |
|---|---|---|---:|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` | `INT64` + `TIMESTAMP(NANOS, UTC)` | no | `PLAIN` | Collector userspace receive time, sampled when the WebSocket library yields the frame. |
| `sequence` | `uint64` | `INT64` + `UINT(64)` | no | `PLAIN` | Globally ordered collector observation and retry identity; authoritative replay order. |
| `timestamp` | `timestamp[ms, tz=UTC]` | `INT64` + `TIMESTAMP(MILLIS, UTC)` | no | `PLAIN` | Millisecond source timestamp supplied by Polymarket; do not use it to break ordering ties. |
| `market` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Market condition ID. |
| `id` | `string` | `BYTE_ARRAY` + `STRING` | no | `PLAIN` | Polymarket market ID; distinct from the condition ID in `market`. |
| `assets_ids` | `list<fixed_size_binary[32]>` | `LIST`; element is `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` for the element leaf | Outcome token IDs in source order; elements are non-null. |
| `outcomes` | `list<string>` | `LIST`; element is `BYTE_ARRAY` + `STRING` | no | `PLAIN` for the element leaf | Outcome labels in source order; elements are non-null. |
| `question` | `string` | `BYTE_ARRAY` + `STRING` | yes | `PLAIN` | Human-readable market question, when supplied. |
| `slug` | `string` | `BYTE_ARRAY` + `STRING` | yes | `PLAIN` | Human-readable market URL slug, when supplied. |

`assets_ids[i]` corresponds to `outcomes[i]`. The event contains the identity
and outcome-routing fields used by the collector. Descriptive payload fields
such as `description`, `tags`, and `event_message` are not exported.

## `market_resolved.parquet`

A row announces resolution of a binary market. This event is market-scoped, so
it has no synthetic `asset_id` column.

| File setting | Value |
|---|---|
| Path | `YYYY-MM-DD/HH/market_resolved.parquet` |
| Physical row order | `market`, `sequence` ascending |
| Compression | ZSTD level 9 on every column |

| Column | Arrow type | Parquet physical and logical type | Nullable | Value encoding | Description |
|---|---|---|---:|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` | `INT64` + `TIMESTAMP(NANOS, UTC)` | no | `PLAIN` | Collector userspace receive time, sampled when the WebSocket library yields the frame. |
| `sequence` | `uint64` | `INT64` + `UINT(64)` | no | `PLAIN` | Globally ordered collector observation and retry identity; authoritative replay order. |
| `timestamp` | `timestamp[ms, tz=UTC]` | `INT64` + `TIMESTAMP(MILLIS, UTC)` | no | `PLAIN` | Millisecond source timestamp supplied by Polymarket; do not use it to break ordering ties. |
| `market` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` | Market condition ID. |
| `id` | `string` | `BYTE_ARRAY` + `STRING` | no | `PLAIN` | Polymarket market ID. |
| `assets_ids` | `list<fixed_size_binary[32]>` | `LIST`; element is `FIXED_LEN_BYTE_ARRAY(32)` | no | `PLAIN` for the element leaf | All outcome token IDs in source order; elements are non-null. |
| `winning_asset_id` | `fixed_size_binary[32]` | `FIXED_LEN_BYTE_ARRAY(32)` | yes | `PLAIN` | Winning outcome token, or null when the source cannot supply one. |
| `winning_outcome` | `string` | `BYTE_ARRAY` + `STRING` | yes | `PLAIN` | Winning outcome label, or null when the source cannot supply one. |

The winning fields describe the resolution and do not mutate an orderbook.
Parent-event metadata and tags from the upstream lifecycle payload are not
exported.

## `manifest.json`

The manifest is written only after all seven Parquet files. Its presence is
the sole completion marker for the hour.

| Field | JSON type | Nullable | Description |
|---|---|---:|---|
| `hour_utc` | string | no | Exported UTC hour. |
| `row_count` | integer | no | Total rows across all seven files. |
| `min_sequence` | integer | yes | Smallest sequence across the hour, or null for an entirely empty hour. |
| `max_sequence` | integer | yes | Largest sequence across the hour, or null for an entirely empty hour. |
| `files` | object | no | Entries keyed by event type. Each entry contains `file`, `columns`, `order_by`, `row_count`, `byte_size`, `sha256`, `min_sequence`, and `max_sequence`. |
| `source_table` | string | no | ClickHouse table used for the export. |
| `created_at` | string | no | UTC time when the manifest was produced. |

Each `files` entry has its own `order_by` list because lifecycle files do not
have an `asset_id` column. Consumers should verify the listed byte size and
SHA-256 digest when downloading an archive. Objects from an interrupted export
may exist without a manifest; do not treat those objects as a completed hour.

## Archive destinations and local conversion

`EXPORT_BACKEND=r2` uploads objects to the S3-compatible endpoint configured by
the `R2_*` variables. `EXPORT_BACKEND=local` stores the same keys below
`LOCAL_EXPORT_DIR` and requires no R2 credentials. Local files are written to a
temporary sibling and atomically replaced. In both modes, `manifest.json` is
written last.

The repository's `run_local.sh` selects the local backend and maps the
container's `/exports` directory to `.data/parquet` on the host. Set
`EXPORT_ONCE=true` when invoking the exporter directly to backfill every
currently eligible missing hour and exit.

The upstream event definitions are documented by Polymarket's
[real-time market data reference](https://docs.polymarket.com/market-data/realtime-data).
This document describes the normalized representation written by this
repository.

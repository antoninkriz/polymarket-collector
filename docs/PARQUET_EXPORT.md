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

Parquet files use ZSTD level 9. Their physical row order is:

| Files | Row order |
|---|---|
| `book`, `price_change`, `last_trade_price`, `tick_size_change`, `best_bid_ask` | `market`, `asset_id`, `sequence` ascending |
| `new_market`, `market_resolved` | `market`, `sequence` ascending |

This clustering keeps one market and outcome asset together for selective
reads and compression. `sequence` remains the authoritative observation order.
To replay across assets, markets, or event files, combine the relevant rows and
order them by `sequence`; physical file order is not global replay order.

## Parquet storage encodings

The exporter writes Parquet data pages v2 with an explicit per-event policy:

| Columns or leaf values | Event files | Value encoding |
|---|---|---|
| `timestamp_received`, `sequence`, `timestamp` | all | `BYTE_STREAM_SPLIT` |
| `side` | `price_change`, `last_trade_price` | `RLE_DICTIONARY` requested; `PLAIN` fallback allowed |
| `fee_rate_bps`, `price` | `last_trade_price` | `RLE_DICTIONARY` requested; `PLAIN` fallback allowed |
| All other scalar and nested leaf values | their owning event file | `PLAIN` |

Every column is compressed with ZSTD level 9. Dictionary-encoded columns use
an 8 MiB dictionary-page limit. The active dictionary columns are bounded
categories or repeated trade prices and normally remain well below that cap.
Decimal values use `store_decimal_as_integer`, which is why precision-nine
decimals have physical type `INT32` and precision-18 decimals have physical
type `INT64` instead of `FIXED_LEN_BYTE_ARRAY`.

Byte-stream split transposes the bytes of each fixed-width integer before ZSTD
compression. It fits the three 64-bit sequence/time columns because their
high-order bytes repeat even though market clustering introduces backward jumps
between asset runs. It does not alter their Arrow logical types or values.

Dictionary encoding is a writer preference, not part of the logical file
contract. PyArrow may fall back to `PLAIN` within a row group when a dictionary
becomes too large. Parquet metadata can therefore list both `RLE_DICTIONARY`
and `PLAIN` for a dictionary-enabled column because the dictionary page itself
uses `PLAIN` and because value pages may fall back to it. `RLE` is also used for
Parquet definition and repetition levels, including the structure of optional
and nested values; it does not change the logical Arrow types described below.
Readers should rely on those logical types and let their Parquet library decode
the physical encodings. A zero-row file has no value pages, so its metadata may
list only the `RLE` encoding used for definition and repetition levels.

The policy is based on measurements from every physical column in all seven
event formats, including nested snapshot and lifecycle leaves. See
[`PARQUET_ENCODING_BENCHMARK.md`](PARQUET_ENCODING_BENCHMARK.md) for the active
choices, benchmark procedure, and rationale. In particular, Parquet does not
combine dictionary and another explicit value encoding for the same column:
dictionary indexes already use RLE/bit packing, while byte-stream split and
delta encodings operate directly on the values.

## Archive destinations

`EXPORT_BACKEND=r2` uploads objects to the S3-compatible endpoint configured by
the `R2_*` variables. `EXPORT_BACKEND=local` instead stores the same keys below
`LOCAL_EXPORT_DIR`; it requires no R2 credentials. Local files are first
written beside their destination under a temporary name and then atomically
replaced. In both modes `manifest.json` is written last and remains the only
completion marker for an hour.

The repository's `run_local.sh` selects the local backend and maps the
container's `/exports` directory to `.data/parquet` on the host. Set
`EXPORT_ONCE=true` when invoking the exporter directly to backfill every
currently eligible missing hour and exit instead of entering the continuous
export loop.

Completed local files can be sorted and re-encoded in parallel with:

```bash
cd services/r2-archive/exporter
python reencode_local.py ../../../.data/parquet --jobs 4
```

The converter reads and sorts each complete file in memory, so choose the job
count for the available RAM. It prepares and verifies every replacement before
changing any completed hour. It removes the hour's manifest while atomically
replacing its files, then writes a manifest with updated byte sizes, SHA-256
hashes, and row-order metadata last.

## Type and identifier conventions

All seven files begin with the same non-null columns, in this order:

| Column | Arrow type | Meaning |
|---|---|---|
| `timestamp_received` | `timestamp[ns, tz=UTC]` | Collector userspace receive time, sampled as soon as the WebSocket library yields the frame. |
| `sequence` | `uint64` | Globally ordered collector observation and retry identity. This is the authoritative replay order. |
| `timestamp` | `timestamp[ms, tz=UTC]` | Millisecond source timestamp supplied by Polymarket. Do not use it to break ordering ties. |
| `market` | `fixed_size_binary[32]` | Raw 32-byte market condition ID, decoded from the source `0x` hexadecimal string. |

Other types used below are:

| Type | Representation |
|---|---|
| `asset_id`, `winning_asset_id` | Unsigned decimal token ID encoded as exactly 32 big-endian bytes (`fixed_size_binary[32]`). |
| `transaction_hash` | Raw 32 bytes decoded from the source `0x` hexadecimal transaction hash (`fixed_size_binary[32]`). |
| price, best price, spread, or tick size | Arrow `decimal32(9, 4)`, stored as Parquet physical `INT32` with a decimal logical annotation. Read as an exact decimal, never as binary floating point. |
| size | Arrow `decimal64(18, 6)`, stored as Parquet physical `INT64` with a decimal logical annotation. Read as an exact decimal. |
| orderbook levels | `list<struct<price: decimal32(9, 4), size: decimal64(18, 6)>>`; the list, struct, and both values are non-null. |
| string | Arrow UTF-8 string. |
| list | Arrow list whose element type is shown in the file schema. |

Binary identifiers deliberately match the fixed-width representation used by
`pmxtdata`. For example, render `market` or `transaction_hash` as source-style
text with `"0x" + value.hex()`, and recover a decimal token ID with an unsigned
big-endian integer conversion. Malformed or out-of-range identifiers abort the
hour export instead of being truncated, padded ambiguously, or wrapped.

`Nullable` below means a genuine field that the normalized source event may
omit. It never means that the field belongs to some other event type.
List elements are non-null; a non-null list itself may still be empty.

## `book.parquet`

A row is a complete aggregated depth snapshot for one outcome asset. It
replaces the reconstructed book state for that asset.

Columns after the common prefix:

| Column | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `asset_id` | `fixed_size_binary[32]` | no | Outcome token whose orderbook is represented. |
| `bids` | `list<struct<price: decimal32(9, 4), size: decimal64(18, 6)>>` | no | Aggregated bid levels in source order. |
| `asks` | `list<struct<price: decimal32(9, 4), size: decimal64(18, 6)>>` | no | Aggregated ask levels in source order. |

An orderbook level is the Arrow/Parquet equivalent of a typed tuple
`(price, size)`. Arrow names the tuple members `price` and `size`, so readers
typically expose one level like this:

```json
{"price":0.4800,"size":30.000000}
```

The values are exact decimals, not JSON strings or floating-point numbers.
Empty sides are empty lists. The upstream book-content `hash` is not exported
because it is neither a unique event ID nor needed for reconstruction.

## `price_change.parquet`

A row assigns the new aggregate size at one `(asset_id, side, price)` level.
Set that level to `size`; remove it when `size == 0`. One upstream message can
contain multiple changes. The collector assigns them consecutive sequence
values in their original array order. Physical clustering can place changes
for different assets in separate runs.

Columns after the common prefix:

| Column | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `asset_id` | `fixed_size_binary[32]` | no | Outcome token being changed. |
| `price` | `decimal32(9, 4)` | no | Affected price level. |
| `size` | `decimal64(18, 6)` | no | New aggregate size, not a signed size delta. Zero removes the level. |
| `side` | `string` | no | `BUY` for the bid side or `SELL` for the ask side. |
| `best_bid` | `decimal32(9, 4)` | yes | Source-provided best bid after the change, when present. |
| `best_ask` | `decimal32(9, 4)` | yes | Source-provided best ask after the change, when present. |

The upstream per-change `hash` is intentionally omitted. It is not safe for
deduplication. Orderbook reconstruction uses `price`, `size`, and `side`; the
optional best prices are accompanying observations, not additional deltas.

## `last_trade_price.parquet`

A row is one trade-execution notification observed on the public market
channel. Rows within one `(market, asset_id)` group are already in `sequence`
order. Preserve every row, including rows with identical payload values or the
same transaction hash.

Columns after the common prefix:

| Column | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `asset_id` | `fixed_size_binary[32]` | no | Outcome token that traded. |
| `price` | `decimal32(9, 4)` | no | Execution price. |
| `size` | `decimal64(18, 6)` | no | Executed size. |
| `side` | `string` | no | `BUY` or `SELL`, from the taker's perspective. |
| `fee_rate_bps` | `uint16` | no | Fee rate in basis points. |
| `transaction_hash` | `fixed_size_binary[32]` | no | Polygon transaction containing the fill. It is metadata, not a unique fill ID. |

One Polygon transaction can settle multiple fills. Never deduplicate on
`transaction_hash`, or on any combination of the public payload columns. Only
the collector-assigned `sequence` identifies a stored observation and collapses
an at-least-once transport retry.

## `tick_size_change.parquet`

A row announces a change to the accepted order-price increment for one outcome
asset. It does not change depth by itself.

Columns after the common prefix:

| Column | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `asset_id` | `fixed_size_binary[32]` | no | Outcome token whose tick size changed. |
| `old_tick_size` | `decimal32(9, 4)` | no | Tick size before the event. |
| `new_tick_size` | `decimal32(9, 4)` | no | Tick size after the event. |

## `best_bid_ask.parquet`

A row is an independently observed top-of-book notification enabled by
Polymarket's `custom_feature_enabled` subscription option. It is useful for BBO
analysis, but it is not a depth delta and must not be applied to reconstruct the
book.

Columns after the common prefix:

| Column | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `asset_id` | `fixed_size_binary[32]` | no | Outcome token whose top of book was reported. |
| `best_bid` | `decimal32(9, 4)` | yes | Best bid, or null when the source provides no bid. |
| `best_ask` | `decimal32(9, 4)` | yes | Best ask, or null when the source provides no ask. |
| `spread` | `decimal32(9, 4)` | yes | Source-provided ask-minus-bid spread, or null when unavailable. |

## `new_market.parquet`

A row announces a newly available binary market. This is a market-scoped
lifecycle event, so there is no synthetic `asset_id` column.

Columns after the common prefix:

| Column | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `id` | `string` | no | Polymarket market ID. This is distinct from the binary condition ID in `market`. |
| `assets_ids` | `list<fixed_size_binary[32]>` | no | Outcome token IDs in source order. |
| `outcomes` | `list<string>` | no | Outcome labels in source order. |
| `question` | `string` | yes | Human-readable market question, when supplied. |
| `slug` | `string` | yes | Human-readable market URL slug, when supplied. |

`assets_ids[i]` corresponds to `outcomes[i]`. The event contains the identity
and outcome-routing fields used by the collector; descriptive payload fields
such as `description`, `tags`, and `event_message` are not exported.

## `market_resolved.parquet`

A row announces resolution of a binary market. This is also market-scoped and
has no synthetic `asset_id` column.

Columns after the common prefix:

| Column | Arrow type | Nullable | Meaning |
|---|---|---:|---|
| `id` | `string` | no | Polymarket market ID. |
| `assets_ids` | `list<fixed_size_binary[32]>` | no | All outcome token IDs in source order. |
| `winning_asset_id` | `fixed_size_binary[32]` | yes | Winning outcome token, when supplied. |
| `winning_outcome` | `string` | yes | Winning outcome label, when supplied. |

The winning fields describe the resolution and do not mutate an orderbook.
Parent-event metadata and tags from the upstream lifecycle payload are not
exported.

## `manifest.json`

The manifest is written only after all seven Parquet files. Its presence is
the sole completion marker for the hour. It contains:

| Field | Meaning |
|---|---|
| `hour_utc` | Exported UTC hour. |
| `row_count` | Total rows across all seven files. |
| `min_sequence`, `max_sequence` | Hour-wide sequence bounds, or null for an entirely empty hour. |
| `files` | Object keyed by event type with file path, columns, row order, row count, byte size, SHA-256 digest, and per-file sequence bounds. |
| `source_table` | ClickHouse table used for the export. |
| `created_at` | UTC time when the manifest was produced. |

Each entry in `files` has an `order_by` list matching the two physical orders
documented above. There is no hour-wide `order_by` because lifecycle files do
not have an `asset_id` column.

Consumers should verify the listed byte size and SHA-256 when downloading an
archive. Objects from an interrupted export may exist without a manifest; do
not treat those objects as a completed hour.

The upstream event definitions are documented by Polymarket's
[real-time market data reference](https://docs.polymarket.com/market-data/realtime-data).
The archive contract above describes the normalized representation actually
written by this repository.

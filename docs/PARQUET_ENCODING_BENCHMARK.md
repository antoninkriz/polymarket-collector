# Parquet encoding policy and benchmark

The exporter selects physical encodings per event and column. These settings
affect file size and read performance, not the Arrow schemas documented in
[`PARQUET_EXPORT.md`](PARQUET_EXPORT.md).

## Writer settings

All event files use the following settings:

| Setting | Value |
|---|---|
| Compression | ZSTD level 9 |
| Data page version | 2.0 |
| Dictionary page limit | 8 MiB per row group |
| Decimal storage | Integer physical storage (`INT32` or `INT64`) |
| Row order | `sequence` ascending |

The active policy is defined by `PARQUET_DICTIONARY_COLUMNS` and
`PARQUET_DELTA_COLUMNS` in `services/r2-archive/exporter/run.py`. Columns absent
from both maps use `PLAIN` value encoding.

## Active column policy

| Event file | Delta-binary-packed | Dictionary |
|---|---|---|
| `book` | `timestamp_received`, `sequence` | `market`, `asset_id` |
| `price_change` | `sequence` | `market`, `asset_id`, `side`, `best_bid`, `best_ask` |
| `last_trade_price` | `timestamp_received`, `sequence` | `market`, `asset_id`, `price`, `side`, `fee_rate_bps` |
| `tick_size_change` | `timestamp_received`, `sequence` | `market` |
| `best_bid_ask` | `timestamp_received`, `sequence` | `market`, `asset_id`, `best_bid`, `best_ask` |
| `new_market` | `timestamp_received`, `sequence` | `market` |
| `market_resolved` | `sequence` | `market` |

PyArrow treats dictionary encoding as a writer preference. A row group can
fall back to `PLAIN` if its dictionary exceeds the configured limit, so Parquet
metadata can list both encodings for a dictionary-enabled column.

## Selection rationale

`sequence` is a monotonically increasing integer in every event file, which
makes `DELTA_BINARY_PACKED` a natural fit. `timestamp_received` also benefits
from delta encoding except where the event shape works better with plain
integers: one `price_change` frame expands to several rows sharing the same
receive timestamp, and `market_resolved` files are sparse.

The source `timestamp` is always plain. It is not a replay key, is not
guaranteed to be globally monotonic, and frequently repeats across events from
one source message.

`market` is categorical and repeated across all event streams. Token-scoped,
high-volume streams also dictionary-encode `asset_id`. Tick-size changes leave
`asset_id` plain because their hourly files are small enough that a dictionary
page does not amortize its overhead.

Low-cardinality categorical values such as `side` and `fee_rate_bps` use
dictionaries. Trade `price` and accompanying best-price observations also
repeat on a bounded price grid and benefit from dictionary encoding. Continuous
measurements such as `size` and `spread`, high-cardinality transaction hashes,
and lifecycle identity and text fields stay plain.

Snapshot price and size leaves stay plain. ZSTD compresses their compact
integer physical values directly, while a dictionary adds a page and an index
stream. Tick-size values are also plain: their four-byte integer runs compress
well without paying a separate dictionary-page cost in each hourly file.

The 8 MiB dictionary-page limit accommodates the number of distinct market and
asset identifiers found in sequence-ordered price-change row groups. It keeps
the dictionary bounded while avoiding fallback caused by the 32-byte values in
these two columns.

Parquet cannot apply dictionary and `DELTA_BINARY_PACKED` to the same column.
Delta encoding stores the integer values directly; dictionary encoding stores
the values in a dictionary page and RLE/bit-packs their indexes.

## Reproducing the benchmark

The benchmark utility reads decoded Arrow values and rewrites every physical
leaf with each encoding supported by its Parquet physical type. It reports
compressed column-chunk bytes using the same ZSTD level, data-page version,
decimal storage, and dictionary-page limit as the exporter.

From the exporter directory, sample three evenly spaced row groups from every
completed event file:

```bash
cd services/r2-archive/exporter
python benchmark_encodings.py ../../../.data/parquet --row-groups-per-file 3
```

Use `--event price_change` to restrict the run to one event type. Set
`--row-groups-per-file 0` to benchmark every row group; this is substantially
more expensive for high-volume files. Cardinality is calculated independently
inside each sampled row group because Parquet dictionaries are row-group local.

The utility tests `PLAIN` and `RLE_DICTIONARY` for every physical type. It also
tests delta-binary-packed and byte-stream-split for integer columns,
delta-byte-array and delta-length-byte-array for byte arrays, and the supported
fixed-width byte-array alternatives. Unsupported PyArrow combinations are
omitted from the report.

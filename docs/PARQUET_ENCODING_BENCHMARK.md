# Parquet encoding policy and benchmark

The exporter clusters rows by market and asset, then selects physical encodings
per event and column. These settings affect file size and read performance, not
the Arrow schemas documented in
[`PARQUET_EXPORT.md`](PARQUET_EXPORT.md).

## Writer settings

All event files use the following settings:

| Setting | Value |
|---|---|
| Compression | ZSTD level 9 |
| Data page version | 2.0 |
| Dictionary page limit | 8 MiB |
| Decimal storage | Integer physical storage (`INT32` or `INT64`) |
| Token-event row order | `market`, `asset_id`, `sequence` ascending |
| Lifecycle-event row order | `market`, `sequence` ascending |

The active policy is defined by `PARQUET_DICTIONARY_COLUMNS`,
`PARQUET_COLUMN_ENCODINGS`, and `PARQUET_SORT_COLUMNS` in
`services/r2-archive/exporter/run.py`. Columns absent from both encoding maps
use `PLAIN` value encoding.

## Active column policy

| Event file | Byte-stream split | Dictionary |
|---|---|---|
| `book` | `timestamp_received`, `sequence`, `timestamp` | none |
| `price_change` | `timestamp_received`, `sequence`, `timestamp` | `side` |
| `last_trade_price` | `timestamp_received`, `sequence`, `timestamp` | `price`, `side`, `fee_rate_bps` |
| `tick_size_change` | `timestamp_received`, `sequence`, `timestamp` | none |
| `best_bid_ask` | `timestamp_received`, `sequence`, `timestamp` | none |
| `new_market` | `timestamp_received`, `sequence`, `timestamp` | none |
| `market_resolved` | `timestamp_received`, `sequence`, `timestamp` | none |

Every other scalar or nested leaf is plain. PyArrow treats dictionary encoding
as a writer preference and can fall back to plain values if a dictionary
reaches the configured limit. Empty row groups have no value pages and
therefore do not advertise a value encoding in Parquet metadata.

## Selection rationale

Market clustering changes the shape of the three common 64-bit columns.
`sequence` is increasing within an asset run but jumps between runs; source and
receive timestamps have the same piecewise-ordered shape. Byte-stream split is
robust to those jumps and exposes their shared high-order bytes to ZSTD. Delta
binary packing assumes a smoother numeric series and performs poorly at the
run boundaries.

The fixed-width `market` and `asset_id` identifiers are plain. Sorting turns
them into long identical runs, which ZSTD compresses directly. A dictionary
would store the same 32-byte identifiers again and add an index stream. The
same reasoning applies to the unique lifecycle identifiers and transaction
hashes, which do not have useful dictionary cardinality.

Price-change `price`, `best_bid`, and `best_ask`, plus all BBO prices and
spreads, are plain integers. Within one asset they form repeated or slowly
changing runs but are not monotonic; plain values let ZSTD exploit those runs.
Delta binary packing expands this pattern, while dictionaries add an index page
without enough benefit to justify a different representation.

`side` and `fee_rate_bps` are genuine low-cardinality categories and use
dictionaries. Trade prices also repeat heavily on a bounded tick grid, so the
trade stream dictionary-encodes `price`. Continuous sizes remain plain even
when a sample gives dictionary encoding a tiny advantage.

Snapshot level prices and sizes remain plain. ZSTD compresses their compact
integer physical values directly, while a dictionary adds a page and an index
stream. Tick-size values are also plain: their four-byte integer runs compress
well without paying a separate dictionary-page cost in each hourly file.

The 8 MiB dictionary-page limit is a hard cap. The active dictionaries contain
only small categories or trade prices and normally remain far below it.

Parquet cannot apply dictionary and another explicit value encoding to the same
column. Dictionary pages store values separately and RLE/bit-pack their indexes;
byte-stream split and delta encodings operate directly on the values.

## Reproducing the benchmark

The benchmark utility reads decoded Arrow values and rewrites every physical
leaf with each encoding supported by its Parquet physical type. It reports
compressed column-chunk bytes using the same ZSTD level, data-page version,
decimal storage, and dictionary-page limit as the exporter.

From the exporter directory, sample three evenly spaced row groups from every
completed file:

```bash
cd services/r2-archive/exporter
python benchmark_encodings.py ../../../.data/parquet --row-groups-per-file 3
```

Use `--event price_change` to restrict the run to one event type. Set
`--row-groups-per-file 0` to benchmark every row group; this is substantially
more expensive for high-volume files. Cardinality is calculated independently
inside each sampled row group because Parquet dictionaries are row-group local.

To test the clustered layout against files in another row order, add
`--sort-by-export-order`. The utility then loads and sorts each complete file
before selecting row groups, so this mode requires enough memory for the
largest input file.

The utility tests `PLAIN` and `RLE_DICTIONARY` for every physical type. It also
tests delta-binary-packed and byte-stream-split for integer columns,
delta-byte-array and delta-length-byte-array for byte arrays, and the supported
fixed-width byte-array alternatives. Unsupported PyArrow combinations are
omitted from the report.

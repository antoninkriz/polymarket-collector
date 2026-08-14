# Parquet encoding benchmark

This report records the measurements used to choose physical encodings for the
event-specific v3 exports. Encodings are storage details only; they do not
change the Arrow schemas documented in
[`PARQUET_EXPORT.md`](PARQUET_EXPORT.md).

## Method

The benchmark covered all seven event files and every physical Parquet column,
including the leaves below nested snapshot and lifecycle lists. Its input was
11 completed hourly exports from `2026-08-13/21` through `2026-08-14/07`.
Large files contributed one evenly selected row group per hour; small files
contributed their complete single row group. A second run sampled the first,
middle, and last row groups of `2026-08-14/07` to check that conclusions were
not artifacts of the cross-hour sample.

Each decoded Arrow column was rewritten with every applicable PyArrow
encoding. The numbers below are compressed column-chunk bytes using data pages
v2 and ZSTD level 9. They exclude schema and footer overhead. Run the benchmark
again with:

```bash
cd services/r2-archive/exporter
python benchmark_encodings.py ../../../.data/parquet --row-groups-per-file 1
python benchmark_encodings.py \
  ../../../.data/parquet/2026-08-14/07 --row-groups-per-file 3
```

`RG distinct` is the sum of distinct values within each sampled row group,
divided by the number of non-null values. This is more relevant than
file-global cardinality because Parquet builds dictionaries per row group.

Parquet and PyArrow do not provide a "delta with dictionary" combination for a
column. `DELTA_BINARY_PACKED` directly encodes integer values. A dictionary
page instead stores values separately and data pages encode dictionary indexes
with RLE/bit packing. Asking PyArrow for both on one column fails with:

```text
ValueError: To use 'column_encoding' set 'use_dictionary' to False
```

Consequently the valid `timestamp_received` alternatives are delta,
dictionary, plain, and byte-stream split. There is no fourth delta-plus-dict
data-page encoding to benchmark.

## Common columns

### `timestamp_received`

Receive timestamps are ordered and almost unique in most event files, so delta
usually wins. `price_change` is different: one WebSocket frame expands into
several consecutive rows with the same receive timestamp. Plain integers plus
ZSTD exploit those repetitions much better than delta or dictionary encoding.

| Event file | RG distinct | Plain | Dictionary | Delta | Byte-stream split | Selected |
|---|---:|---:|---:|---:|---:|---|
| `best_bid_ask` | 99.99% | 30.09 MiB | 32.81 MiB | 27.82 MiB | 30.93 MiB | delta |
| `book` | 31.80% | 4.63 MiB | 7.04 MiB | 4.50 MiB | 4.81 MiB | delta |
| `last_trade_price` | 100.00% | 2.37 MiB | 3.49 MiB | 2.12 MiB | 2.35 MiB | delta |
| `market_resolved` | 100.00% | 464 B | 620 B | 496 B | 464 B | plain |
| `new_market` | 100.00% | about 0.03 MiB | about 0.03 MiB | about 0.02 MiB | about 0.03 MiB | delta |
| `price_change` | 49.98% | 15.14 MiB | 20.03 MiB | 21.52 MiB | 23.33 MiB | plain |
| `tick_size_change` | 100.00% | about 0.06 MiB | about 0.09 MiB | about 0.06 MiB | about 0.07 MiB | delta |

The selected encoding is per event file because the two row-generation shapes
are intentionally different. `market_resolved` stays plain: it had only seven
rows spread across seven hourly files, so a delta header is pure overhead.

### `sequence`, `timestamp`, and `market`

| Column | Result | Selected reasoning |
|---|---|---|
| `sequence` | Delta was 48% to 93% smaller than plain in every nontrivial event stream. | `DELTA_BINARY_PACKED`; it is a globally increasing integer and the user-visible replay key. |
| `timestamp` | Plain won for `best_bid_ask`, `book`, `price_change`, and `tick_size_change`; delta narrowly won for trades and lifecycle creation. | `PLAIN` in every file. The source timestamp is not a replay key and is neither guaranteed unique nor globally monotonic; a uniform plain representation is robust and was explicitly requested. |
| `market` | Dictionary saved 25% to 32% in high-volume BBO, price-change, and trade files, was nearly tied for books, and added only tiny overhead to sparse lifecycle/tick files. | `RLE_DICTIONARY` in every file. It is a repeated categorical identifier and was explicitly requested. |

## Event-owned columns

The tables use `P`, `D`, `DBP`, and `BSS` for plain, dictionary,
delta-binary-packed, and byte-stream-split. Sizes aggregate the cross-hour
sample. `-` means the encoding does not apply to that physical type.

### `book.parquet`

| Column leaf | RG distinct | P | D | DBP | BSS | Selected |
|---|---:|---:|---:|---:|---:|---|
| `asset_id` | 40.31% | 87.56 MiB | 86.91 MiB | - | 111.03 MiB | dictionary |
| `bids.list.element.price` | 0.02% | 28.41 MiB | 28.50 MiB | 47.32 MiB | 32.14 MiB | plain |
| `bids.list.element.size` | 3.52% | 97.40 MiB | 99.18 MiB | 256.94 MiB | 120.89 MiB | plain |
| `asks.list.element.price` | 0.02% | 28.42 MiB | 28.51 MiB | 51.53 MiB | 32.28 MiB | plain |
| `asks.list.element.size` | 3.52% | 97.40 MiB | 99.22 MiB | 258.51 MiB | 120.95 MiB | plain |

Although snapshot prices have low cardinality, ZSTD sees repeated four-byte
integers directly and avoids a dictionary page and index stream. Sizes are
continuous numeric measurements, so plain is also the stable semantic choice.

### `price_change.parquet`

| Column | RG distinct | P | D | DBP | BSS | Selected |
|---|---:|---:|---:|---:|---:|---|
| `asset_id` | 8.70% | 109.14 MiB | 100.06 MiB | - | 345.49 MiB | dictionary |
| `price` | 0.10% | 11.62 MiB | 11.92 MiB | 19.89 MiB | 13.77 MiB | plain |
| `size` | 6.95% | 17.12 MiB | 17.97 MiB | 28.36 MiB | 28.39 MiB | plain |
| `side` | effectively 0% | 1.45 MiB | 0.61 MiB | - | - | dictionary |
| `best_bid` | 0.09% | 12.62 MiB | 11.78 MiB | 19.86 MiB | 15.67 MiB | dictionary |
| `best_ask` | 0.09% | 12.74 MiB | 11.79 MiB | 19.88 MiB | 15.92 MiB | dictionary |

`price` and `size` are assignment values in a dense update stream; plain wins
and avoids treating continuous measurements as categories. BBO values come
from a small price grid and repeatedly accompany changes, so dictionary is
both meaningful and smaller.

### `last_trade_price.parquet`

| Column | RG distinct | P | D | DBP | BSS | Selected |
|---|---:|---:|---:|---:|---:|---|
| `asset_id` | 6.84% | 2.71 MiB | 2.05 MiB | - | 16.54 MiB | dictionary |
| `price` | 0.93% | 0.80 MiB | 0.63 MiB | 1.11 MiB | 0.93 MiB | dictionary |
| `size` | 25.87% | 1.53 MiB | 1.51 MiB | 2.35 MiB | 1.79 MiB | plain |
| `side` | effectively 0% | 0.13 MiB | 0.07 MiB | - | - | dictionary |
| `fee_rate_bps` | effectively 0% | 2,385 B | 2,043 B | 2,454 B | 2,385 B | dictionary |
| `transaction_hash` | 100.00% | 18.69 MiB | 19.29 MiB | - | 18.69 MiB | plain |

The 1.3% dictionary saving for `size` is too small to justify dictionary
encoding a continuous, moderately high-cardinality quantity. Transaction
hashes are unique in the sample, exactly as expected; dictionary encoding only
duplicates them in a dictionary page.

### `tick_size_change.parquet`

All 21,708 observed rows contained `old_tick_size = 0.0100` and
`new_tick_size = 0.0010`. Even in this best-cardinality case, the sum across
the 11 independently encoded hourly files was:

| Column | P | D | DBP | BSS | Selected |
|---|---:|---:|---:|---:|---:|
| `asset_id` | about 0.34 MiB | about 0.36 MiB | - | about 0.64 MiB | plain |
| `old_tick_size` | 844 B | 935 B | 851 B | 866 B | plain |
| `new_tick_size` | 844 B | 935 B | 840 B | 866 B | plain |

The dictionary result is not a ZSTD anomaly. Each hourly file pays for a new
dictionary page, whereas ZSTD directly reduces a run of identical four-byte
integers. A controlled 1,968-row check containing all five possible values
also favored plain: 95--96 B for plain versus 116--153 B for dictionary,
depending on whether values were blocked or interleaved. Plain therefore
makes sense for this tiny bounded integer column even though its logical domain
is categorical.

### `best_bid_ask.parquet`

| Column | RG distinct | P | D | DBP | BSS | Selected |
|---|---:|---:|---:|---:|---:|---|
| `asset_id` | 7.37% | 104.55 MiB | 96.06 MiB | - | 349.20 MiB | dictionary |
| `best_bid` | 0.10% | 14.92 MiB | 12.39 MiB | 20.62 MiB | 16.29 MiB | dictionary |
| `best_ask` | 0.10% | 14.72 MiB | 12.42 MiB | 20.62 MiB | 16.71 MiB | dictionary |
| `spread` | 0.10% | 10.81 MiB | 12.02 MiB | 18.96 MiB | 13.16 MiB | plain |

The absolute BBO prices repeat well across assets and benefit from dictionary
encoding. Spread is a derived numeric measurement whose plain integer stream
compresses better.

### Lifecycle files

`new_market` had 6,015 rows and `market_resolved` had seven rows across the 11
hours. Every event-owned identity, list leaf, question, slug, and outcome field
was smallest as plain data. In `new_market`, per-row-group cardinality was 100%
for `id`, `assets_ids`, and `slug`, 97.14% for `question`, and 6.23% for outcome
labels. Even the repeated outcome strings did not amortize a dictionary page
in these small hourly files. All event-owned lifecycle columns therefore use
plain encoding.

## Selected policy

The production policy follows the column's meaning and observed distribution,
not merely the smallest cell in each row:

- Delta: `sequence` everywhere; `timestamp_received` except in
  `price_change` and `market_resolved`.
- Dictionary: `market`, `side`, and `fee_rate_bps`; repeated `asset_id` in
  book, price-change, trade, and BBO files; trade `price`; and accompanying
  `best_bid`/`best_ask` values.
- Plain: source `timestamp`; receive time in exploded price changes and sparse
  resolutions; continuous sizes; unique hashes and lifecycle identities;
  snapshot leaves; spreads; and tick-change event-owned columns.

No column selected byte-stream split or a byte-array delta encoding. Those
encodings were consistently worse for the observed data and do not better
match the columns' semantics.

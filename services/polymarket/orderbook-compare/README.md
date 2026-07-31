# Polymarket orderbook comparison

Compares an external Polymarket dataset (parquet) against our ClickHouse store
(`polymarket_orderbook_rust`) to quantify collection gaps and content fidelity.

Currently implemented: **`price_change`** events (`compare_price_changes.py`).

## What it does

Both sides are normalized to one per-update tuple
`(asset_id, ts_ms, side, price, size)` (+ `best_bid`/`best_ask`), then compared:

- **Coverage** — exact-tuple multiset diff: matched / only-in-external / only-in-CH.
- **Fidelity** — updates matched on `(asset_id, ts_ms, side, price)` whose
  `size`/`best_bid`/`best_ask` disagree.
- **Time alignment** — distinct event-millisecond coverage per source.

Join key is the exchange timestamp in **epoch milliseconds** (timezone-safe). The book
`hash` is *not* usable — prod runs `EXCLUDE_HASH=true`.

## Run

```bash
export CLICKHOUSE_PASSWORD=...            # + CLICKHOUSE_HOST/PORT/USER/DATABASE/TABLE if non-default
source .venv/bin/activate
python services/polymarket/orderbook-compare/compare_price_changes.py \
    --parquet-glob '/path/to/price_changes_arrays/*.parquet' \
    --minutes 5 --anchor start --sample 10
# widen: --start 2026-07-02T08:00:00Z --end 2026-07-02T09:00:00Z
# dump full diffs: --out-dir ./diffs
```

The comparison runs on a bounded window (default 5 min) because the full hour is
~90M exploded updates per side. ClickHouse is filtered on `timestamp_received`
(the partition/ORDER key) with a slack margin, then refined on the exact event ms.

## Known caveat

The fidelity join cross-products when the same `(asset_id, ts_ms, side, price)` key
repeats within one WS message (~66k such keys in a 5-min slice). Fidelity mismatch
counts are therefore an **upper bound**; the coverage multiset diff is exact.

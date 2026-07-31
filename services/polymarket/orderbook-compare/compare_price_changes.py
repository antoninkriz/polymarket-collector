"""Compare external Polymarket `price_change` data (parquet) against our ClickHouse store.

The external dataset stores one row per WS message with parallel arrays (one entry
per price-change in that message). Our ClickHouse table `polymarket_orderbook_rust`
stores the raw event JSON in a `data` string, already *exploded* to one row per entry.

This script normalizes both sides to the same per-update tuple

    (asset_id, ts_ms, side, price, size)   [+ best_bid / best_ask]

and reports three kinds of difference:

  1. Coverage   — updates present on one side but missing on the other.
  2. Fidelity   — updates matched on (asset_id, ts_ms, side, price) whose size /
                  best_bid / best_ask disagree.
  3. Time align — distinct event-millisecond coverage per source.

Join key notes
--------------
* `ts_ms` is the *exchange* timestamp in epoch milliseconds. External `ts` and our
  `timestamp` both carry Polymarket's own timestamp, so the epoch value is directly
  comparable and timezone-safe.
* The book `hash` is stripped on our side (prod runs EXCLUDE_HASH=true), so it cannot
  be used as a join key. `price_change` never carried a usable hash on our side anyway.

Performance
-----------
The full hour of external data is ~90M exploded updates. Pulling the matching volume
out of ClickHouse over HTTP is heavy, so the comparison runs on a bounded time window
(default: a 5-minute slice, clearly reported). Widen it with --minutes / --start / --end.

Our table is ORDER BY (market, timestamp_received) and partitioned by
toStartOfHour(timestamp_received), so we MUST filter on `timestamp_received` for the
partition prune, then refine on the exact event ms. We add a slack margin because
timestamp_received (batch-flush time) lags the event time by up to ~1s.
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone

import duckdb

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_PARQUET_GLOB = os.environ.get(
    "EXTERNAL_PARQUET_GLOB", "./price_changes_arrays/*.parquet"
)
DECIMAL = "DECIMAL(18,6)"          # canonical numeric scale for price / size
RECV_SLACK_SEC = 30                # timestamp_received margin around the event window
EVENT_TYPE = "price_change"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parquet-glob", default=DEFAULT_PARQUET_GLOB,
                   help="glob for external price_changes_arrays parquet files")
    p.add_argument("--minutes", type=float, default=5.0,
                   help="window length in minutes when --start/--end are not given")
    p.add_argument("--anchor", default="start", choices=["start", "middle", "end"],
                   help="where in the external range to place the default window")
    p.add_argument("--start", default=None, help="window start (ISO-UTC or epoch ms)")
    p.add_argument("--end", default=None, help="window end (ISO-UTC or epoch ms)")
    p.add_argument("--sample", type=int, default=10, help="sample rows per diff category")
    p.add_argument("--out-dir", default=None, help="optional dir to dump full diff CSVs")

    p.add_argument("--ch-host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    p.add_argument("--ch-port", default=os.environ.get("CLICKHOUSE_PORT", "8124"))
    p.add_argument("--ch-user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    p.add_argument("--ch-password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    p.add_argument("--ch-database", default=os.environ.get("CLICKHOUSE_DATABASE", "default"))
    p.add_argument("--ch-table", default=os.environ.get("CLICKHOUSE_TABLE", "polymarket_orderbook_rust"))
    return p.parse_args()


def to_epoch_ms(value: str) -> int:
    """Accept an epoch-ms integer string or an ISO-8601 UTC datetime."""
    value = value.strip()
    if value.isdigit():
        return int(value)
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def external_range(con: duckdb.DuckDBPyConnection, glob: str) -> tuple[int, int]:
    row = con.execute(
        f"SELECT min(epoch_ms(ts)), max(epoch_ms(ts)) FROM read_parquet('{glob}')"
    ).fetchone()
    if row[0] is None:
        sys.exit(f"No rows found in parquet glob: {glob}")
    return int(row[0]), int(row[1])


def resolve_window(args: argparse.Namespace, ext_min: int, ext_max: int) -> tuple[int, int]:
    if args.start and args.end:
        return to_epoch_ms(args.start), to_epoch_ms(args.end)

    span = int(args.minutes * 60_000)
    if args.anchor == "start":
        start = ext_min
    elif args.anchor == "end":
        start = max(ext_min, ext_max - span)
    else:  # middle
        start = max(ext_min, (ext_min + ext_max) // 2 - span // 2)
    return start, min(start + span, ext_max)


def load_external(con: duckdb.DuckDBPyConnection, glob: str, start_ms: int, end_ms: int) -> None:
    """Explode parallel arrays into one row per update, restricted to the window."""
    con.execute(
        f"""
        CREATE OR REPLACE TABLE ext AS
        WITH exploded AS (
            SELECT
                epoch_ms(ts)                        AS ts_ms,
                unnest(asset_ids)                   AS asset_id,
                unnest(sides)                       AS side,
                TRY_CAST(unnest(prices)    AS {DECIMAL}) AS price,
                TRY_CAST(unnest(sizes)     AS {DECIMAL}) AS size,
                TRY_CAST(unnest(best_bids) AS {DECIMAL}) AS best_bid,
                TRY_CAST(unnest(best_asks) AS {DECIMAL}) AS best_ask
            FROM read_parquet('{glob}')
            WHERE epoch_ms(ts) >= {start_ms} AND epoch_ms(ts) < {end_ms}
        )
        SELECT * FROM exploded
        """
    )


def load_clickhouse(con: duckdb.DuckDBPyConnection, args: argparse.Namespace,
                    start_ms: int, end_ms: int, tsv_path: str) -> None:
    """Stream compact TSV out of ClickHouse into a DuckDB table `ch`."""
    recv_lo = ms_to_utc(start_ms - RECV_SLACK_SEC * 1000)
    recv_hi = ms_to_utc(end_ms + RECV_SLACK_SEC * 1000)
    query = f"""
        SELECT
            JSONExtractString(data, 'asset_id')  AS asset_id,
            toUnixTimestamp64Milli(timestamp)    AS ts_ms,
            JSONExtractString(data, 'side')       AS side,
            JSONExtractString(data, 'price')      AS price,
            JSONExtractString(data, 'size')       AS size,
            JSONExtractString(data, 'best_bid')   AS best_bid,
            JSONExtractString(data, 'best_ask')   AS best_ask
        FROM {args.ch_database}.{args.ch_table}
        WHERE timestamp_received >= '{recv_lo}' AND timestamp_received < '{recv_hi}'
          AND event_type = '{EVENT_TYPE}'
          AND toUnixTimestamp64Milli(timestamp) >= {start_ms}
          AND toUnixTimestamp64Milli(timestamp) <  {end_ms}
        FORMAT TSV
    """
    url = f"http://{args.ch_host}:{args.ch_port}/"
    print(f"  querying ClickHouse (recv window {recv_lo} .. {recv_hi}) ...", flush=True)
    with open(tsv_path, "wb") as fh:
        proc = subprocess.run(
            ["curl", "-sS", "--fail", url,
             "--user", f"{args.ch_user}:{args.ch_password}",
             "--data-binary", query],
            stdout=fh, stderr=subprocess.PIPE,
        )
    if proc.returncode != 0:
        sys.exit(f"ClickHouse query failed: {proc.stderr.decode()[:500]}")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE ch AS
        SELECT
            asset_id,
            CAST(ts_ms AS BIGINT)          AS ts_ms,
            side,
            TRY_CAST(price    AS {DECIMAL}) AS price,
            TRY_CAST(size     AS {DECIMAL}) AS size,
            TRY_CAST(best_bid AS {DECIMAL}) AS best_bid,
            TRY_CAST(best_ask AS {DECIMAL}) AS best_ask
        FROM read_csv('{tsv_path}', delim='\t', header=false, nullstr='',
                      columns={{
                        'asset_id':'VARCHAR','ts_ms':'BIGINT','side':'VARCHAR',
                        'price':'VARCHAR','size':'VARCHAR',
                        'best_bid':'VARCHAR','best_ask':'VARCHAR'}})
        """
    )


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def scalar(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def run_comparison(con: duckdb.DuckDBPyConnection, args: argparse.Namespace,
                   start_ms: int, end_ms: int) -> None:
    ext_total = scalar(con, "SELECT count(*) FROM ext")
    ch_total = scalar(con, "SELECT count(*) FROM ch")

    # Full-tuple multiset comparison ----------------------------------------
    # Group each side to (tuple -> count), then FULL JOIN to get matched vs unique.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE ext_g AS
          SELECT asset_id, ts_ms, side, price, size, count(*) AS c
          FROM ext GROUP BY ALL;
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE ch_g AS
          SELECT asset_id, ts_ms, side, price, size, count(*) AS c
          FROM ch GROUP BY ALL;
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE tuple_cmp AS
        SELECT
            coalesce(e.asset_id, c.asset_id) AS asset_id,
            coalesce(e.ts_ms, c.ts_ms)       AS ts_ms,
            coalesce(e.side, c.side)         AS side,
            coalesce(e.price, c.price)       AS price,
            coalesce(e.size, c.size)         AS size,
            coalesce(e.c, 0)                 AS ext_c,
            coalesce(c.c, 0)                 AS ch_c
        FROM ext_g e
        FULL OUTER JOIN ch_g c USING (asset_id, ts_ms, side, price, size)
        """
    )
    matched = scalar(con, "SELECT coalesce(sum(least(ext_c, ch_c)),0) FROM tuple_cmp") or 0
    only_ext = scalar(con, "SELECT coalesce(sum(greatest(ext_c - ch_c,0)),0) FROM tuple_cmp") or 0
    only_ch = scalar(con, "SELECT coalesce(sum(greatest(ch_c - ext_c,0)),0) FROM tuple_cmp") or 0

    # Fidelity: matched on (asset_id, ts_ms, side, price), disagree on size/best -
    con.execute(
        """
        CREATE OR REPLACE TABLE fidelity AS
        SELECT e.asset_id, e.ts_ms, e.side, e.price,
               e.size AS ext_size, c.size AS ch_size,
               e.best_bid AS ext_bb, c.best_bid AS ch_bb,
               e.best_ask AS ext_ba, c.best_ask AS ch_ba
        FROM ext e
        JOIN ch c USING (asset_id, ts_ms, side, price)
        """
    )
    size_diff = scalar(con, "SELECT count(*) FROM fidelity WHERE ext_size IS DISTINCT FROM ch_size")
    bb_diff = scalar(con, "SELECT count(*) FROM fidelity WHERE ext_bb IS DISTINCT FROM ch_bb")
    ba_diff = scalar(con, "SELECT count(*) FROM fidelity WHERE ext_ba IS DISTINCT FROM ch_ba")

    # Time alignment --------------------------------------------------------
    ext_ms = scalar(con, "SELECT count(DISTINCT ts_ms) FROM ext")
    ch_ms = scalar(con, "SELECT count(DISTINCT ts_ms) FROM ch")
    both_ms = scalar(con, "SELECT count(*) FROM (SELECT DISTINCT ts_ms FROM ext INTERSECT SELECT DISTINCT ts_ms FROM ch)")

    # ---- Report -----------------------------------------------------------
    def pct(n, d):
        return f"{100.0 * n / d:6.2f}%" if d else "   n/a"

    print("\n" + "=" * 74)
    print("PRICE_CHANGE COMPARISON  —  external parquet  vs  ClickHouse")
    print("=" * 74)
    print(f"window (UTC) : {ms_to_utc(start_ms)}  ..  {ms_to_utc(end_ms)}")
    print(f"window (ms)  : {start_ms} .. {end_ms}  ({(end_ms-start_ms)/60000:.2f} min)")
    print("-" * 74)
    print(f"{'updates (exploded)':32} external={ext_total:>12,}   clickhouse={ch_total:>12,}")
    print("-" * 74)
    print("COVERAGE  (exact tuple: asset_id, ts_ms, side, price, size)")
    print(f"  matched both sides        : {matched:>12,}   ({pct(matched, ext_total)} of external)")
    print(f"  only in external (we lost): {only_ext:>12,}   ({pct(only_ext, ext_total)} of external)")
    print(f"  only in clickhouse        : {only_ch:>12,}   ({pct(only_ch, ch_total)} of clickhouse)")
    print("-" * 74)
    print("FIDELITY  (matched on asset_id, ts_ms, side, price — do values agree?)")
    print(f"  size mismatches      : {size_diff:>12,}")
    print(f"  best_bid mismatches  : {bb_diff:>12,}")
    print(f"  best_ask mismatches  : {ba_diff:>12,}")
    print("-" * 74)
    print("TIME ALIGNMENT  (distinct event milliseconds)")
    print(f"  external ms : {ext_ms:>10,}   clickhouse ms : {ch_ms:>10,}   in both : {both_ms:>10,}")
    print("=" * 74)

    _print_samples(con, args.sample)

    if args.out_dir:
        _dump_csvs(con, args.out_dir)


def _print_samples(con: duckdb.DuckDBPyConnection, n: int) -> None:
    print(f"\n--- sample: updates ONLY in external (missing from ClickHouse), up to {n} ---")
    print(con.execute(
        "SELECT asset_id, ts_ms, side, price, size FROM tuple_cmp "
        "WHERE ext_c > ch_c ORDER BY ts_ms LIMIT ?", [n]).fetchdf().to_string())

    print(f"\n--- sample: updates ONLY in ClickHouse (missing from external), up to {n} ---")
    print(con.execute(
        "SELECT asset_id, ts_ms, side, price, size FROM tuple_cmp "
        "WHERE ch_c > ext_c ORDER BY ts_ms LIMIT ?", [n]).fetchdf().to_string())

    print(f"\n--- sample: SIZE mismatches (same asset/ts/side/price), up to {n} ---")
    print(con.execute(
        "SELECT asset_id, ts_ms, side, price, ext_size, ch_size FROM fidelity "
        "WHERE ext_size IS DISTINCT FROM ch_size ORDER BY ts_ms LIMIT ?", [n]).fetchdf().to_string())

    print(f"\n--- sample: best_bid/best_ask mismatches, up to {n} ---")
    print(con.execute(
        "SELECT asset_id, ts_ms, side, price, ext_bb, ch_bb, ext_ba, ch_ba FROM fidelity "
        "WHERE ext_bb IS DISTINCT FROM ch_bb OR ext_ba IS DISTINCT FROM ch_ba "
        "ORDER BY ts_ms LIMIT ?", [n]).fetchdf().to_string())


def _dump_csvs(con: duckdb.DuckDBPyConnection, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    con.execute(f"COPY (SELECT asset_id, ts_ms, side, price, size FROM tuple_cmp WHERE ext_c > ch_c) "
                f"TO '{out_dir}/only_in_external.csv' (HEADER)")
    con.execute(f"COPY (SELECT asset_id, ts_ms, side, price, size FROM tuple_cmp WHERE ch_c > ext_c) "
                f"TO '{out_dir}/only_in_clickhouse.csv' (HEADER)")
    con.execute(f"COPY (SELECT * FROM fidelity WHERE ext_size IS DISTINCT FROM ch_size "
                f"OR ext_bb IS DISTINCT FROM ch_bb OR ext_ba IS DISTINCT FROM ch_ba) "
                f"TO '{out_dir}/value_mismatches.csv' (HEADER)")
    print(f"\nFull diff CSVs written to {out_dir}/")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")

    ext_min, ext_max = external_range(con, args.parquet_glob)
    print(f"external price_change range (UTC): {ms_to_utc(ext_min)} .. {ms_to_utc(ext_max)}")

    start_ms, end_ms = resolve_window(args, ext_min, ext_max)
    if start_ms >= end_ms:
        sys.exit("Empty window after resolution — check --start/--end/--minutes.")

    print("loading external (exploding arrays) ...", flush=True)
    load_external(con, args.parquet_glob, start_ms, end_ms)

    tsv_path = os.path.join(args.out_dir or ".", "_ch_price_changes.tsv") if args.out_dir \
        else os.path.join(os.path.dirname(os.path.abspath(__file__)), "_ch_price_changes.tsv")
    load_clickhouse(con, args, start_ms, end_ms, tsv_path)

    run_comparison(con, args, start_ms, end_ms)


if __name__ == "__main__":
    main()

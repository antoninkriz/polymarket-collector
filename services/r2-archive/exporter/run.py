"""Hourly orderbook → Cloudflare R2 exporter.

Reads one hour of orderbook rows from ClickHouse, re-encodes to Parquet
with DELTA_BINARY_PACKED on integer timestamp columns and ZSTD(9)
dictionary encoding elsewhere, and uploads to R2. ClickHouse's own
FORMAT Parquet writer never emits DELTA, so we fetch FORMAT ArrowStream
and re-encode client-side (pass 6 in docs/data-dump-optimizations.md).

Profiles, selected via ``EXPORTER_PROFILE`` env (default ``polymarket_v3``):

* ``polymarket_v3`` — exports the replayable typed
  ``polymarket_orderbook_v3`` table. ``FINAL`` removes only collector-owned
  transport retries, and rows are ordered by collector session and receive
  sequence without regrouping the two market assets.

* ``polymarket`` — legacy v2 compatibility profile. Rewrites the raw-JSON
  ``polymarket_orderbook_rust``
  source table into Schema D in the same SELECT: event-specific fields
  as ``Nullable(...)`` only on owning event types, ``bids`` / ``asks``
  as a single ``Nullable(String)`` holding the raw JSON depth (``NULL``
  outside ``book`` events).

* ``kalshi`` — pass-through ``SELECT *`` from the already-typed
  ``kalshi_orderbook`` table (which natively uses ``Nullable`` columns
  for non-owning event types — no JSON transform needed).

* ``limitless`` — pass-through ``SELECT *`` from the already-typed
  ``limitless_orderbook_rust`` table.

* ``opinion`` — pass-through ``SELECT *`` from the already-typed
  ``opinion_orderbook`` table.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

log = logging.getLogger(__name__)

# ClickHouse
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "polymarket_orderbook_v3")
CLICKHOUSE_HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"

# Cloudflare R2
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "")

# Export
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9
EXPORT_DELAY_MINUTES = int(os.environ.get("EXPORT_DELAY_MINUTES", "5"))
EXPORT_LAG_HOURS = int(os.environ.get("EXPORT_LAG_HOURS", "1"))
LOOP_CHECK_INTERVAL_SECONDS = int(os.environ.get("LOOP_CHECK_INTERVAL_SECONDS", "60"))
QUERY_MAX_RETRIES = 10
QUERY_RETRY_DELAY_SECONDS = 10

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ---------- Profile ----------


@dataclass(frozen=True)
class Profile:
    """Per-exchange exporter configuration."""

    name: str
    default_filename_prefix: str
    delta_encoded_columns: tuple[str, ...]
    default_select_order_by: tuple[str, ...]
    select_template: str


# Event-type ownership for the polymarket schema-D transform. Event
# types populate only the columns they own; everything else is NULL.
# Mirror of the table in docs/data-dump-optimizations.md.
BOOK_EVENTS = ("book",)
TRADE_LIKE_EVENTS = ("price_change", "last_trade_price")
PRICE_CHANGE_EVENTS = ("price_change",)
LAST_TRADE_EVENTS = ("last_trade_price",)
TICK_SIZE_EVENTS = ("tick_size_change",)


def _in_list(events: tuple[str, ...]) -> str:
    """Render a tuple of event types as a SQL ``IN ('a','b')`` fragment."""
    quoted = ",".join(f"'{e}'" for e in events)
    return f"IN ({quoted})"


POLYMARKET_SELECT_TEMPLATE = f"""
SELECT
    timestamp_received,
    timestamp,
    toFixedString(market, 66)                                      AS market,
    event_type,
    JSONExtractString(data, 'asset_id')                            AS asset_id,

    if(event_type {_in_list(BOOK_EVENTS)},
       JSONExtractString(data, 'bids'), NULL)                      AS bids,
    if(event_type {_in_list(BOOK_EVENTS)},
       JSONExtractString(data, 'asks'), NULL)                      AS asks,

    if(event_type {_in_list(TRADE_LIKE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'price'), 4),
       NULL)                                                       AS price,
    if(event_type {_in_list(TRADE_LIKE_EVENTS)},
       toDecimal64OrZero(JSONExtractString(data, 'size'), 6),
       NULL)                                                       AS size,
    if(event_type {_in_list(TRADE_LIKE_EVENTS)},
       JSONExtractString(data, 'side'), NULL)                      AS side,

    if(event_type {_in_list(PRICE_CHANGE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'best_bid'), 4),
       NULL)                                                       AS best_bid,
    if(event_type {_in_list(PRICE_CHANGE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'best_ask'), 4),
       NULL)                                                       AS best_ask,

    if(event_type {_in_list(LAST_TRADE_EVENTS)},
       toUInt16OrZero(JSONExtractString(data, 'fee_rate_bps')),
       NULL)                                                       AS fee_rate_bps,
    if(event_type {_in_list(LAST_TRADE_EVENTS)},
       JSONExtractString(data, 'transaction_hash'), NULL)          AS transaction_hash,

    if(event_type {_in_list(TICK_SIZE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'old_tick_size'), 4),
       NULL)                                                       AS old_tick_size,
    if(event_type {_in_list(TICK_SIZE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'new_tick_size'), 4),
       NULL)                                                       AS new_tick_size
FROM {{source_table}}
WHERE timestamp_received >= toDateTime64('{{target}}', 3)
  AND timestamp_received <  toDateTime64('{{target}}', 3) + INTERVAL 1 HOUR
ORDER BY {{order_by}}
FORMAT ArrowStream
"""

PASSTHROUGH_SELECT_TEMPLATE = """
SELECT * FROM {source_table}
WHERE timestamp_received >= toDateTime64('{target}', 3)
  AND timestamp_received <  toDateTime64('{target}', 3) + INTERVAL 1 HOUR
ORDER BY {order_by}
FORMAT ArrowStream
"""

POLYMARKET_V3_SELECT_TEMPLATE = """
SELECT * FROM {source_table} FINAL
WHERE timestamp_received >= toDateTime64('{target}', 9, 'UTC')
  AND timestamp_received <  toDateTime64('{target}', 9, 'UTC') + INTERVAL 1 HOUR
ORDER BY {order_by}
FORMAT ArrowStream
"""

PROFILES: dict[str, Profile] = {
    "polymarket_v3": Profile(
        name="polymarket_v3",
        default_filename_prefix="polymarket_orderbook_v3_",
        delta_encoded_columns=(
            "timestamp",
            "timestamp_received",
            "collector_session_started_at",
            "connection_id",
            "connection_epoch",
            "frame_sequence",
            "receive_sequence",
            "message_index",
            "message_count",
            "row_index",
            "row_count",
            "fee_rate_bps",
        ),
        default_select_order_by=(
            "collector_session_started_at",
            "collector_session_id",
            "receive_sequence",
            "message_index",
            "row_index",
        ),
        select_template=POLYMARKET_V3_SELECT_TEMPLATE,
    ),
    "polymarket": Profile(
        name="polymarket",
        default_filename_prefix="polymarket_orderbook_",
        delta_encoded_columns=("timestamp", "timestamp_received", "fee_rate_bps"),
        default_select_order_by=("market", "asset_id", "timestamp_received"),
        select_template=POLYMARKET_SELECT_TEMPLATE,
    ),
    "kalshi": Profile(
        name="kalshi",
        default_filename_prefix="kalshi_orderbook_",
        delta_encoded_columns=("timestamp", "timestamp_received"),
        default_select_order_by=("market_ticker", "timestamp_received"),
        select_template=PASSTHROUGH_SELECT_TEMPLATE,
    ),
    "limitless": Profile(
        name="limitless",
        default_filename_prefix="limitless_orderbook_",
        delta_encoded_columns=(
            "timestamp",
            "timestamp_received",
            "fee_rate_bps",
            "receive_sequence",
            "row_index",
        ),
        default_select_order_by=(
            "market",
            "asset_id",
            "timestamp_received",
            "receive_sequence",
            "row_index",
        ),
        select_template=PASSTHROUGH_SELECT_TEMPLATE,
    ),
    "opinion": Profile(
        name="opinion",
        default_filename_prefix="opinion_orderbook_",
        delta_encoded_columns=(
            "timestamp",
            "timestamp_received",
            "receive_sequence",
            "row_index",
        ),
        default_select_order_by=(
            "market",
            "asset_id",
            "timestamp_received",
            "receive_sequence",
            "row_index",
        ),
        select_template=PASSTHROUGH_SELECT_TEMPLATE,
    ),
}

EXPORTER_PROFILE = os.environ.get("EXPORTER_PROFILE", "polymarket_v3")
if EXPORTER_PROFILE not in PROFILES:
    raise SystemExit(
        f"Unknown EXPORTER_PROFILE={EXPORTER_PROFILE!r}; "
        f"expected one of {sorted(PROFILES)}"
    )
PROFILE = PROFILES[EXPORTER_PROFILE]

FILENAME_PREFIX = os.environ.get("FILENAME_PREFIX", PROFILE.default_filename_prefix)


def _env_column_list(name: str, default: tuple[str, ...]) -> list[str]:
    """Read a JSON array env var containing ClickHouse column identifiers."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return list(default)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"{name} must be a JSON array of column names") from e

    if not isinstance(parsed, list) or not parsed:
        raise ValueError(f"{name} must be a non-empty JSON array of column names")

    columns: list[str] = []
    for value in parsed:
        if not isinstance(value, str):
            raise ValueError(f"{name} must contain only string column names")
        column = value.strip()
        if not IDENTIFIER_RE.fullmatch(column):
            raise ValueError(f"{name} contains invalid column identifier: {value!r}")
        columns.append(column)

    return columns
SELECT_ORDER_BY = _env_column_list("SELECT_ORDER_BY", PROFILE.default_select_order_by)


# ---------- ClickHouse ----------


def _ch_query(query: str, timeout: int = 60) -> requests.Response:
    """POST a query to ClickHouse and return the response."""
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
    resp = requests.post(CLICKHOUSE_HTTP_URL, data=query.encode(), auth=auth, timeout=timeout)
    resp.raise_for_status()
    return resp


def query_earliest_hour() -> datetime | None:
    """Return the earliest hour with data, or None if the table is empty.

    The table is partitioned by ``toStartOfHour(timestamp_received)``, so
    ``min(timestamp_received)`` is O(1) via partition pruning.
    """
    for attempt in range(1, QUERY_MAX_RETRIES + 1):
        try:
            resp = _ch_query(
                f"SELECT toStartOfHour(min(timestamp_received)) FROM {CLICKHOUSE_TABLE} "
                "FORMAT TabSeparated"
            )
            text = resp.text.strip()
            if not text or text.startswith("1970"):
                return None
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception as e:
            if attempt == QUERY_MAX_RETRIES:
                raise
            log.warning(
                "ClickHouse query failed (attempt %d/%d): %s — retrying in %ds",
                attempt, QUERY_MAX_RETRIES, e, QUERY_RETRY_DELAY_SECONDS,
            )
            time.sleep(QUERY_RETRY_DELAY_SECONDS)
    return None


def query_latest_received_hour() -> datetime | None:
    """Return the hour containing the latest committed receive timestamp."""
    for attempt in range(1, QUERY_MAX_RETRIES + 1):
        try:
            resp = _ch_query(
                f"SELECT toStartOfHour(max(timestamp_received)) FROM {CLICKHOUSE_TABLE} "
                "FORMAT TabSeparated"
            )
            text = resp.text.strip()
            if not text or text.startswith("1970"):
                return None
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception as e:
            if attempt == QUERY_MAX_RETRIES:
                raise
            log.warning(
                "ClickHouse watermark query failed (attempt %d/%d): %s — retrying in %ds",
                attempt, QUERY_MAX_RETRIES, e, QUERY_RETRY_DELAY_SECONDS,
            )
            time.sleep(QUERY_RETRY_DELAY_SECONDS)
    return None


def fetch_hour_parquet(hour: datetime) -> bytes | None:
    """Fetch one hour of rows via the active profile's SELECT template,
    encode as Parquet with DELTA on integer timestamps + ZSTD(9) dict
    elsewhere, and return the bytes (or ``None`` for an empty hour).
    """
    target = hour.strftime("%Y-%m-%d %H:00:00")
    select_order_by = ", ".join(SELECT_ORDER_BY)
    query = PROFILE.select_template.format(
        source_table=CLICKHOUSE_TABLE,
        target=target,
        order_by=select_order_by,
    )
    arrow_bytes = _ch_query(query, timeout=600).content

    with pa.ipc.open_stream(pa.BufferReader(arrow_bytes)) as reader:
        table = reader.read_all()

    if table.num_rows == 0:
        return None

    delta_cols = [c for c in PROFILE.delta_encoded_columns if c in table.column_names]
    dict_cols = [c for c in table.column_names if c not in delta_cols]

    out = BytesIO()
    pq.write_table(
        table,
        out,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        use_dictionary=dict_cols,
        column_encoding={c: "DELTA_BINARY_PACKED" for c in delta_cols},
        data_page_version="2.0",
    )
    return out.getvalue()


# ---------- R2 ----------


class R2Client:
    """Thin S3-compatible client for Cloudflare R2."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not yet exist."""
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                self._client.create_bucket(Bucket=self._bucket)
                log.info("Created bucket %s", self._bucket)
            else:
                raise

    def list_keys(self) -> set[str]:
        """Return the set of exported parquet object keys."""
        keys: set[str] = set()
        kwargs: dict = {"Bucket": self._bucket, "Prefix": FILENAME_PREFIX}
        while True:
            resp = self._client.list_objects_v2(**kwargs)
            keys.update(obj["Key"] for obj in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                return keys
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]

    def upload(self, key: str, data: bytes) -> None:
        """Upload an in-memory blob to the bucket."""
        self._client.upload_fileobj(BytesIO(data), self._bucket, key)


# ---------- Export orchestration ----------


def hour_to_filename(hour: datetime) -> str:
    """Convert a datetime to the standard snapshot filename."""
    return f"{FILENAME_PREFIX}{hour.strftime('%Y-%m-%dT%H')}.parquet"


def latest_exportable_hour() -> datetime | None:
    """Return the latest hour proven complete by wall time and data watermark.

    Redis Stream consumption and ClickHouse insertion are ordered. Seeing a
    committed record in hour H therefore proves all earlier stream records
    were inserted. We still apply ``EXPORT_LAG_HOURS`` against wall time, then
    take the more conservative of the two bounds.
    """
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    wall_bound = now - timedelta(hours=EXPORT_LAG_HOURS)
    latest_received_hour = query_latest_received_hour()
    if latest_received_hour is None:
        return None
    watermark_bound = latest_received_hour - timedelta(hours=1)
    return min(wall_bound, watermark_bound)


def export_hour(client: R2Client, hour: datetime) -> bool:
    """Fetch one hour from ClickHouse and upload it to R2.

    Returns True if an object was uploaded, False if the hour has no rows
    (the caller should keep polling until data appears).
    """
    filename = hour_to_filename(hour)
    log.info("Exporting %s", filename)
    data = fetch_hour_parquet(hour)
    if data is None:
        log.info("Skipping %s: 0 rows, will retry next tick", filename)
        return False
    client.upload(filename, data)
    log.info("Uploaded %s (%.2f MB)", filename, len(data) / (1024 * 1024))
    return True


def backfill(client: R2Client) -> None:
    """Export every missing hour from ClickHouse to R2."""
    earliest = query_earliest_hour()
    if earliest is None:
        log.warning("No data in ClickHouse yet")
        return

    latest = latest_exportable_hour()
    if latest is None or latest < earliest:
        log.info("No complete ClickHouse hour is exportable yet")
        return
    existing = client.list_keys()

    missing: list[datetime] = []
    current = earliest
    while current <= latest:
        if hour_to_filename(current) not in existing:
            missing.append(current)
        current += timedelta(hours=1)

    log.info(
        "Backfill: %d missing hours (%s to %s), %d already exported",
        len(missing), earliest.isoformat(), latest.isoformat(), len(existing),
    )

    for i, hour in enumerate(missing, 1):
        try:
            export_hour(client, hour)
            log.info("Backfill progress: %d/%d", i, len(missing))
        except Exception as e:
            log.error("Failed to export %s: %s", hour.isoformat(), e)


def run_loop(client: R2Client) -> None:
    """Steady-state loop: export each new hour shortly after it completes.

    Advances only on successful upload — empty hours are re-polled on the
    next tick so gaps never produce zero-row objects in R2.
    """
    log.info("Entering steady-state loop (check every %ds)", LOOP_CHECK_INTERVAL_SECONDS)
    latest = latest_exportable_hour()
    next_hour = (latest + timedelta(hours=1)) if latest is not None else None

    while True:
        time.sleep(LOOP_CHECK_INTERVAL_SECONDS)
        now = datetime.now(timezone.utc)
        latest = latest_exportable_hour()
        if latest is None:
            continue
        if next_hour is None:
            earliest = query_earliest_hour()
            if earliest is None:
                continue
            next_hour = earliest
        if now.minute < EXPORT_DELAY_MINUTES:
            continue
        while next_hour <= latest:
            try:
                if not export_hour(client, next_hour):
                    break
                next_hour += timedelta(hours=1)
            except Exception as e:
                log.error("Failed to export %s: %s", next_hour.isoformat(), e)
                break


def main() -> None:
    """Start the R2 snapshot exporter."""
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    missing = [v for v in ("R2_ENDPOINT", "R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_BUCKET")
               if not globals()[v]]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    client = R2Client(R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET)
    client.ensure_bucket()
    log.info(
        "Connected to R2 at %s, bucket=%s, profile=%s, source_table=%s, filename_prefix=%s, order_by=%s",
        R2_ENDPOINT, R2_BUCKET, PROFILE.name, CLICKHOUSE_TABLE, FILENAME_PREFIX, SELECT_ORDER_BY,
    )

    backfill(client)
    run_loop(client)


if __name__ == "__main__":
    main()

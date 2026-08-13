"""Export compact Polymarket v3 ClickHouse rows to hourly Parquet in R2.

The query applies ``FINAL`` to collapse collector-owned retries, projects the
normalized JSON into typed columns, and orders rows by ``(market, sequence)``.
PyArrow writes ZSTD level 1 with delta encoding on integer columns.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
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

# Export format
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 1
FILENAME_PREFIX = os.environ.get("FILENAME_PREFIX", "polymarket_orderbook_v3_")
DELTA_ENCODED_COLUMNS = (
    "timestamp",
    "timestamp_received",
    "sequence",
    "fee_rate_bps",
)
SELECT_ORDER_BY = ("market", "sequence")
EXPORT_DELAY_MINUTES = int(os.environ.get("EXPORT_DELAY_MINUTES", "5"))
EXPORT_LAG_HOURS = int(os.environ.get("EXPORT_LAG_HOURS", "1"))
LOOP_CHECK_INTERVAL_SECONDS = int(os.environ.get("LOOP_CHECK_INTERVAL_SECONDS", "60"))
QUERY_MAX_RETRIES = 10
QUERY_RETRY_DELAY_SECONDS = 10

SELECT_TEMPLATE = """
WITH
    JSONExtractString(data, 'market') AS market_text,
    JSONExtractString(data, 'event_type') AS event_type,
    JSONExtractString(data, 'asset_id') AS asset_id_text,
    JSONExtractString(data, 'transaction_hash') AS transaction_hash_text
SELECT
    timestamp_received,
    sequence,
    fromUnixTimestamp64Milli(
        toInt64OrZero(JSONExtractString(data, 'timestamp')),
        'UTC'
    ) AS timestamp,
    toFixedString(
        unhex(substring(market_text, 3)),
        32
    ) AS market,
    event_type,
    toFixedString(
        unhex(leftPad(hex(toUInt256OrZero(asset_id_text)), 64, '0')),
        32
    ) AS asset_id,

    if(event_type = 'book', JSONExtractRaw(data, 'bids'), NULL) AS bids,
    if(event_type = 'book', JSONExtractRaw(data, 'asks'), NULL) AS asks,

    if(event_type IN ('price_change', 'last_trade_price'),
       toDecimal32OrZero(JSONExtractString(data, 'price'), 4), NULL) AS price,
    if(event_type IN ('price_change', 'last_trade_price'),
       toDecimal64OrZero(JSONExtractString(data, 'size'), 6), NULL) AS size,
    if(event_type IN ('price_change', 'last_trade_price'),
       JSONExtractString(data, 'side'), NULL) AS side,

    if(event_type = 'price_change',
       toDecimal32OrZero(JSONExtractString(data, 'best_bid'), 4), NULL) AS best_bid,
    if(event_type = 'price_change',
       toDecimal32OrZero(JSONExtractString(data, 'best_ask'), 4), NULL) AS best_ask,

    if(event_type = 'last_trade_price',
       toUInt16OrZero(JSONExtractString(data, 'fee_rate_bps')), NULL) AS fee_rate_bps,
    if(event_type = 'last_trade_price',
       toFixedString(
           unhex(substring(transaction_hash_text, 3)),
           32
       ), NULL) AS transaction_hash,

    if(event_type = 'tick_size_change',
       toDecimal32OrZero(JSONExtractString(data, 'old_tick_size'), 4), NULL)
       AS old_tick_size,
    if(event_type = 'tick_size_change',
       toDecimal32OrZero(JSONExtractString(data, 'new_tick_size'), 4), NULL)
       AS new_tick_size
FROM {source_table} FINAL
WHERE timestamp_received >= toDateTime64('{target}', 9, 'UTC')
  AND timestamp_received <  toDateTime64('{target}', 9, 'UTC') + INTERVAL 1 HOUR
  AND throwIf(
      NOT match(market_text, '^0[xX][0-9a-fA-F]{{64}}$'),
      'invalid Polymarket condition ID'
  ) = 0
  AND throwIf(
      NOT match(asset_id_text, '^(0|[1-9][0-9]{{0,77}})$')
          OR toString(toUInt256OrZero(asset_id_text)) != asset_id_text,
      'invalid Polymarket asset ID'
  ) = 0
  AND throwIf(
      event_type = 'last_trade_price'
          AND NOT match(transaction_hash_text, '^0[xX][0-9a-fA-F]{{64}}$'),
      'invalid Polymarket transaction hash'
  ) = 0
ORDER BY {order_by}
FORMAT ArrowStream
"""


# ClickHouse


def _ch_query(query: str, timeout: int = 60) -> requests.Response:
    """POST a query to ClickHouse and return the response."""
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
    resp = requests.post(
        CLICKHOUSE_HTTP_URL, data=query.encode(), auth=auth, timeout=timeout
    )
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
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except Exception as e:
            if attempt == QUERY_MAX_RETRIES:
                raise
            log.warning(
                "ClickHouse query failed (attempt %d/%d): %s — retrying in %ds",
                attempt,
                QUERY_MAX_RETRIES,
                e,
                QUERY_RETRY_DELAY_SECONDS,
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
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except Exception as e:
            if attempt == QUERY_MAX_RETRIES:
                raise
            log.warning(
                "ClickHouse watermark query failed (attempt %d/%d): %s — retrying in %ds",
                attempt,
                QUERY_MAX_RETRIES,
                e,
                QUERY_RETRY_DELAY_SECONDS,
            )
            time.sleep(QUERY_RETRY_DELAY_SECONDS)
    return None


def fetch_hour_parquet(hour: datetime) -> bytes | None:
    """Return one logical receive-time hour as typed Parquet bytes."""
    target = hour.strftime("%Y-%m-%d %H:00:00")
    select_order_by = ", ".join(SELECT_ORDER_BY)
    query = SELECT_TEMPLATE.format(
        source_table=CLICKHOUSE_TABLE,
        target=target,
        order_by=select_order_by,
    )
    arrow_bytes = _ch_query(query, timeout=600).content

    with pa.ipc.open_stream(pa.BufferReader(arrow_bytes)) as reader:
        table = reader.read_all()

    if table.num_rows == 0:
        return None

    delta_cols = [c for c in DELTA_ENCODED_COLUMNS if c in table.column_names]
    dict_cols = [c for c in table.column_names if c not in delta_cols]

    out = BytesIO()
    pq.write_table(
        table,
        out,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        use_dictionary=dict_cols,  # pyright: ignore[reportArgumentType]
        column_encoding={c: "DELTA_BINARY_PACKED" for c in delta_cols},
        data_page_version="2.0",
    )
    return out.getvalue()


# R2


class R2Client:
    """Thin S3-compatible client for Cloudflare R2."""

    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, bucket: str
    ) -> None:
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
        """Return the set of exported object keys."""
        keys: set[str] = set()
        kwargs: dict[str, str] = {"Bucket": self._bucket, "Prefix": FILENAME_PREFIX}
        while True:
            resp = self._client.list_objects_v2(**kwargs)
            keys.update(obj["Key"] for obj in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                return keys
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]

    def upload(self, key: str, data: bytes) -> None:
        """Upload an in-memory blob to the bucket."""
        self._client.upload_fileobj(BytesIO(data), self._bucket, key)

    def exists(self, key: str) -> bool:
        """Return whether an object exists in the bucket."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise


# Export orchestration


def hour_to_filename(hour: datetime) -> str:
    """Convert a datetime to the standard snapshot filename."""
    return f"{FILENAME_PREFIX}{hour.strftime('%Y-%m-%dT%H')}.parquet"


def hour_to_completion_key(hour: datetime) -> str:
    """Return the object whose presence means an hour was fully published."""
    return f"{hour_to_filename(hour)}.manifest.json"


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
    manifest = {
        "file": filename,
        "hour_utc": hour.isoformat(),
        "row_count": pq.read_metadata(BytesIO(data)).num_rows,
        "byte_size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "source_table": CLICKHOUSE_TABLE,
        "order_by": SELECT_ORDER_BY,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_data = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    client.upload(hour_to_completion_key(hour), manifest_data)
    log.info("Uploaded %s (%.2f MB)", filename, len(data) / (1024 * 1024))
    return True


def backfill(client: R2Client) -> datetime | None:
    """Export missing complete hours and return the next hour to attempt."""
    earliest = query_earliest_hour()
    if earliest is None:
        log.warning("No data in ClickHouse yet")
        return None

    latest = latest_exportable_hour()
    if latest is None or latest < earliest:
        log.info("No complete ClickHouse hour is exportable yet")
        return earliest
    existing = client.list_keys()

    missing: list[datetime] = []
    current = earliest
    while current <= latest:
        if hour_to_completion_key(current) not in existing:
            missing.append(current)
        current += timedelta(hours=1)

    log.info(
        "Backfill: %d missing hours (%s to %s), %d already exported",
        len(missing),
        earliest.isoformat(),
        latest.isoformat(),
        len(existing),
    )

    for i, hour in enumerate(missing, 1):
        try:
            if not export_hour(client, hour):
                return hour
            log.info("Backfill progress: %d/%d", i, len(missing))
        except Exception as e:
            log.error("Failed to export %s: %s", hour.isoformat(), e)
            return hour
    return latest + timedelta(hours=1)


def run_loop(client: R2Client, next_hour: datetime | None) -> None:
    """Steady-state loop: export each new hour shortly after it completes.

    Advances only on successful upload — empty hours are re-polled on the
    next tick so gaps never produce zero-row objects in R2.
    """
    log.info(
        "Entering steady-state loop (check every %ds)", LOOP_CHECK_INTERVAL_SECONDS
    )
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
                completion_key = hour_to_completion_key(next_hour)
                if client.exists(completion_key):
                    next_hour += timedelta(hours=1)
                    continue
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

    required = ("R2_ENDPOINT", "R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_BUCKET")
    missing = [name for name in required if not globals()[name]]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    client = R2Client(R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET)
    client.ensure_bucket()
    log.info(
        "Connected to R2 at %s, bucket=%s, source_table=%s, filename_prefix=%s, order_by=%s",
        R2_ENDPOINT,
        R2_BUCKET,
        CLICKHOUSE_TABLE,
        FILENAME_PREFIX,
        SELECT_ORDER_BY,
    )

    next_hour = backfill(client)
    run_loop(client, next_hour)


if __name__ == "__main__":
    main()

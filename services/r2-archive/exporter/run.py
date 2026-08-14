"""Export compact Polymarket v3 rows to hourly, event-specific Parquet files.

Each query applies ``FINAL`` to collapse collector-owned retries, projects one
event type into its own schema, and orders rows by the shared collector
sequence. PyArrow writes ZSTD level 9 with encodings selected for each event's
observed column distributions.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Protocol

import boto3
import pyarrow as pa
import pyarrow.compute as pc
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
EXPORT_BACKEND = os.environ.get("EXPORT_BACKEND", "r2").strip().lower()
LOCAL_EXPORT_DIR = os.environ.get("LOCAL_EXPORT_DIR", "/exports")
EXPORT_ONCE = os.environ.get("EXPORT_ONCE", "false").strip().lower() in {
    "1",
    "true",
    "yes",
}

# Export format
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9
PARQUET_DICTIONARY_PAGE_SIZE_LIMIT = 8 * 1024 * 1024
PARQUET_DICTIONARY_COLUMNS: dict[str, tuple[str, ...]] = {
    "book": (),
    "price_change": ("side",),
    "last_trade_price": ("price", "side", "fee_rate_bps"),
    "tick_size_change": (),
    "best_bid_ask": (),
    "new_market": (),
    "market_resolved": (),
}
PARQUET_COLUMN_ENCODINGS: dict[str, dict[str, str]] = {
    "book": {
        "timestamp_received": "BYTE_STREAM_SPLIT",
        "sequence": "BYTE_STREAM_SPLIT",
        "timestamp": "BYTE_STREAM_SPLIT",
    },
    "price_change": {
        "timestamp_received": "BYTE_STREAM_SPLIT",
        "sequence": "BYTE_STREAM_SPLIT",
        "timestamp": "BYTE_STREAM_SPLIT",
    },
    "last_trade_price": {
        "timestamp_received": "BYTE_STREAM_SPLIT",
        "sequence": "BYTE_STREAM_SPLIT",
        "timestamp": "BYTE_STREAM_SPLIT",
    },
    "tick_size_change": {
        "timestamp_received": "BYTE_STREAM_SPLIT",
        "sequence": "BYTE_STREAM_SPLIT",
        "timestamp": "BYTE_STREAM_SPLIT",
    },
    "best_bid_ask": {
        "timestamp_received": "BYTE_STREAM_SPLIT",
        "sequence": "BYTE_STREAM_SPLIT",
        "timestamp": "BYTE_STREAM_SPLIT",
    },
    "new_market": {
        "timestamp_received": "BYTE_STREAM_SPLIT",
        "sequence": "BYTE_STREAM_SPLIT",
        "timestamp": "BYTE_STREAM_SPLIT",
    },
    "market_resolved": {
        "timestamp_received": "BYTE_STREAM_SPLIT",
        "sequence": "BYTE_STREAM_SPLIT",
        "timestamp": "BYTE_STREAM_SPLIT",
    },
}
ORDER_LEVEL_TYPE = pa.struct(
    [
        pa.field("price", pa.decimal32(9, 4), nullable=False),
        pa.field("size", pa.decimal64(18, 6), nullable=False),
    ]
)
ORDER_LEVELS_TYPE = pa.list_(pa.field("item", ORDER_LEVEL_TYPE, nullable=False))
PARQUET_COLUMN_TYPES: dict[str, pa.DataType] = {
    "bids": ORDER_LEVELS_TYPE,
    "asks": ORDER_LEVELS_TYPE,
    "price": pa.decimal32(9, 4),
    "best_bid": pa.decimal32(9, 4),
    "best_ask": pa.decimal32(9, 4),
    "spread": pa.decimal32(9, 4),
    "old_tick_size": pa.decimal32(9, 4),
    "new_tick_size": pa.decimal32(9, 4),
    "size": pa.decimal64(18, 6),
}
PARQUET_SORT_COLUMNS: dict[str, tuple[str, ...]] = {
    "book": ("market", "asset_id", "sequence"),
    "price_change": ("market", "asset_id", "sequence"),
    "last_trade_price": ("market", "asset_id", "sequence"),
    "tick_size_change": ("market", "asset_id", "sequence"),
    "best_bid_ask": ("market", "asset_id", "sequence"),
    "new_market": ("market", "sequence"),
    "market_resolved": ("market", "sequence"),
}
EXPORT_DELAY_MINUTES = int(os.environ.get("EXPORT_DELAY_MINUTES", "5"))
EXPORT_LAG_HOURS = int(os.environ.get("EXPORT_LAG_HOURS", "1"))
LOOP_CHECK_INTERVAL_SECONDS = int(os.environ.get("LOOP_CHECK_INTERVAL_SECONDS", "60"))
QUERY_MAX_RETRIES = 10
QUERY_RETRY_DELAY_SECONDS = 10


@dataclass(frozen=True)
class EventProjection:
    """Event-owned SQL columns and validation for one Parquet schema."""

    aliases: tuple[str, ...]
    columns: tuple[str, ...]
    validations: tuple[str, ...]


ASSET_ID_ALIAS = "JSONExtractString(data, 'asset_id') AS asset_id_text"
ASSET_ID_COLUMN = """toFixedString(
    unhex(leftPad(hex(toUInt256OrZero(asset_id_text)), 64, '0')),
    32
) AS asset_id"""
ASSET_ID_VALIDATION = """throwIf(
    NOT match(asset_id_text, '^(0|[1-9][0-9]{0,77})$')
        OR toString(toUInt256OrZero(asset_id_text)) != asset_id_text,
    'invalid Polymarket asset ID'
) = 0"""
ASSETS_IDS_ALIAS = "JSONExtract(data, 'assets_ids', 'Array(String)') AS assets_ids_text"
ASSETS_IDS_COLUMN = """arrayMap(
    value -> toFixedString(
        unhex(leftPad(hex(toUInt256OrZero(value)), 64, '0')),
        32
    ),
    assets_ids_text
) AS assets_ids"""
ASSETS_IDS_VALIDATION = """throwIf(
    arrayExists(
        value -> NOT match(value, '^(0|[1-9][0-9]{0,77})$')
            OR toString(toUInt256OrZero(value)) != value,
        assets_ids_text
    ),
    'invalid Polymarket lifecycle asset ID'
) = 0"""

EVENT_PROJECTIONS: dict[str, EventProjection] = {
    "book": EventProjection(
        aliases=(ASSET_ID_ALIAS,),
        columns=(
            ASSET_ID_COLUMN,
            "JSONExtract(data, 'bids', 'Array(Tuple(price Decimal32(4), size Decimal64(6)))') AS bids",
            "JSONExtract(data, 'asks', 'Array(Tuple(price Decimal32(4), size Decimal64(6)))') AS asks",
        ),
        validations=(ASSET_ID_VALIDATION,),
    ),
    "price_change": EventProjection(
        aliases=(ASSET_ID_ALIAS,),
        columns=(
            ASSET_ID_COLUMN,
            "toDecimal32OrZero(JSONExtractString(data, 'price'), 4) AS price",
            "toDecimal64OrZero(JSONExtractString(data, 'size'), 6) AS size",
            "JSONExtractString(data, 'side') AS side",
            "toDecimal32OrNull(JSONExtractString(data, 'best_bid'), 4) AS best_bid",
            "toDecimal32OrNull(JSONExtractString(data, 'best_ask'), 4) AS best_ask",
        ),
        validations=(ASSET_ID_VALIDATION,),
    ),
    "last_trade_price": EventProjection(
        aliases=(
            ASSET_ID_ALIAS,
            "JSONExtractString(data, 'transaction_hash') AS transaction_hash_text",
        ),
        columns=(
            ASSET_ID_COLUMN,
            "toDecimal32OrZero(JSONExtractString(data, 'price'), 4) AS price",
            "toDecimal64OrZero(JSONExtractString(data, 'size'), 6) AS size",
            "JSONExtractString(data, 'side') AS side",
            "toUInt16OrZero(JSONExtractString(data, 'fee_rate_bps')) AS fee_rate_bps",
            "toFixedString(unhex(substring(transaction_hash_text, 3)), 32) AS transaction_hash",
        ),
        validations=(
            ASSET_ID_VALIDATION,
            """throwIf(
    NOT match(transaction_hash_text, '^0[xX][0-9a-fA-F]{64}$'),
    'invalid Polymarket transaction hash'
) = 0""",
        ),
    ),
    "tick_size_change": EventProjection(
        aliases=(ASSET_ID_ALIAS,),
        columns=(
            ASSET_ID_COLUMN,
            "toDecimal32OrZero(JSONExtractString(data, 'old_tick_size'), 4) AS old_tick_size",
            "toDecimal32OrZero(JSONExtractString(data, 'new_tick_size'), 4) AS new_tick_size",
        ),
        validations=(ASSET_ID_VALIDATION,),
    ),
    "best_bid_ask": EventProjection(
        aliases=(ASSET_ID_ALIAS,),
        columns=(
            ASSET_ID_COLUMN,
            "toDecimal32OrNull(JSONExtractString(data, 'best_bid'), 4) AS best_bid",
            "toDecimal32OrNull(JSONExtractString(data, 'best_ask'), 4) AS best_ask",
            "toDecimal32OrNull(JSONExtractString(data, 'spread'), 4) AS spread",
        ),
        validations=(ASSET_ID_VALIDATION,),
    ),
    "new_market": EventProjection(
        aliases=(ASSETS_IDS_ALIAS,),
        columns=(
            "JSONExtractString(data, 'id') AS id",
            ASSETS_IDS_COLUMN,
            "JSONExtract(data, 'outcomes', 'Array(String)') AS outcomes",
            "if(JSONHas(data, 'question'), JSONExtractString(data, 'question'), NULL) AS question",
            "if(JSONHas(data, 'slug'), JSONExtractString(data, 'slug'), NULL) AS slug",
        ),
        validations=(ASSETS_IDS_VALIDATION,),
    ),
    "market_resolved": EventProjection(
        aliases=(
            ASSETS_IDS_ALIAS,
            "JSONExtractString(data, 'winning_asset_id') AS winning_asset_id_text",
        ),
        columns=(
            "JSONExtractString(data, 'id') AS id",
            ASSETS_IDS_COLUMN,
            """if(
    JSONHas(data, 'winning_asset_id'),
    toFixedString(
        unhex(leftPad(hex(toUInt256OrZero(winning_asset_id_text)), 64, '0')),
        32
    ),
    NULL
) AS winning_asset_id""",
            """if(
    JSONHas(data, 'winning_outcome'),
    JSONExtractString(data, 'winning_outcome'),
    NULL
) AS winning_outcome""",
        ),
        validations=(
            ASSETS_IDS_VALIDATION,
            """throwIf(
    JSONHas(data, 'winning_asset_id')
        AND (
            NOT match(winning_asset_id_text, '^(0|[1-9][0-9]{0,77})$')
            OR toString(toUInt256OrZero(winning_asset_id_text))
                != winning_asset_id_text
        ),
    'invalid Polymarket winning asset ID'
) = 0""",
        ),
    ),
}


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


def build_event_query(hour: datetime, event_type: str) -> str:
    """Build the typed ClickHouse projection for one event type and hour."""
    projection = EVENT_PROJECTIONS[event_type]
    target = hour.astimezone(timezone.utc).strftime("%Y-%m-%d %H:00:00")
    aliases = "".join(f",\n    {alias}" for alias in projection.aliases)
    columns = ",\n    ".join(projection.columns)
    validations = "".join(
        f"\n  AND {validation}" for validation in projection.validations
    )
    order_by = ", ".join(PARQUET_SORT_COLUMNS[event_type])
    return f"""
WITH
    JSONExtractString(data, 'market') AS market_text{aliases}
SELECT
    timestamp_received,
    sequence,
    fromUnixTimestamp64Milli(
        toInt64OrZero(JSONExtractString(data, 'timestamp')),
        'UTC'
    ) AS timestamp,
    toFixedString(unhex(substring(market_text, 3)), 32) AS market,
    {columns}
FROM {CLICKHOUSE_TABLE} FINAL
WHERE timestamp_received >= toDateTime64('{target}', 9, 'UTC')
  AND timestamp_received <  toDateTime64('{target}', 9, 'UTC') + INTERVAL 1 HOUR
  AND JSONExtractString(data, 'event_type') = '{event_type}'
  AND throwIf(
      NOT match(market_text, '^0[xX][0-9a-fA-F]{{64}}$'),
      'invalid Polymarket condition ID'
  ) = 0{validations}
ORDER BY {order_by}
SETTINGS do_not_merge_across_partitions_select_final = 1
FORMAT ArrowStream
"""


def fetch_event_table(hour: datetime, event_type: str) -> pa.Table:
    """Return one event type for one receive-time hour as a typed Arrow table."""
    query = build_event_query(hour, event_type)
    arrow_bytes = _ch_query(query, timeout=600).content

    with pa.ipc.open_stream(pa.BufferReader(arrow_bytes)) as reader:
        return reader.read_all()


def table_to_parquet(table: pa.Table, event_type: str) -> bytes:
    """Encode a typed event table using its measured Parquet policy."""
    if event_type not in EVENT_PROJECTIONS:
        raise ValueError(f"unsupported event type: {event_type}")

    fields = [
        pa.field(
            field.name,
            PARQUET_COLUMN_TYPES.get(field.name, field.type),
            nullable=field.nullable,
            metadata=field.metadata,
        )
        for field in table.schema
    ]
    target_schema = pa.schema(fields, metadata=table.schema.metadata)
    if not table.schema.equals(target_schema):
        # ClickHouse's Arrow stream exposes Decimal32/Decimal64 as decimal128,
        # including decimals nested in snapshot levels. Narrow them losslessly
        # before Parquet serialization so readers see the intended widths and
        # Parquet can use physical INT32/INT64.
        table = table.cast(target_schema, safe=True)

    column_encodings = {
        column: encoding
        for column, encoding in PARQUET_COLUMN_ENCODINGS[event_type].items()
        if column in table.column_names
    }
    dict_cols = [
        column
        for column in PARQUET_DICTIONARY_COLUMNS[event_type]
        if column in table.column_names
    ]

    out = BytesIO()
    pq.write_table(
        table,
        out,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        use_dictionary=dict_cols,  # pyright: ignore[reportArgumentType]
        column_encoding=column_encodings,
        data_page_version="2.0",
        dictionary_pagesize_limit=PARQUET_DICTIONARY_PAGE_SIZE_LIMIT,
        store_decimal_as_integer=True,
    )
    return out.getvalue()


# Archive destinations


class ArchiveWriter(Protocol):
    """Destination that accepts completed archive objects."""

    def upload(self, key: str, data: bytes) -> None:
        """Store one object under its archive key."""
        ...


class ArchiveClient(ArchiveWriter, Protocol):
    """Queryable destination for completed archive objects."""

    def list_keys(self) -> set[str]:
        """Return the stored object keys."""
        ...

    def exists(self, key: str) -> bool:
        """Return whether an object exists."""
        ...


class LocalArchive:
    """Atomic local-filesystem archive destination."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()

    def ensure_directory(self) -> None:
        """Create the archive root when it does not exist."""
        self._root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        target = (self._root / key).resolve()
        if target == self._root or not target.is_relative_to(self._root):
            raise ValueError(f"archive key escapes local export directory: {key!r}")
        return target

    def list_keys(self) -> set[str]:
        """Return all file keys relative to the archive root."""
        if not self._root.exists():
            return set()
        return {
            path.relative_to(self._root).as_posix()
            for path in self._root.rglob("*")
            if path.is_file()
        }

    def upload(self, key: str, data: bytes) -> None:
        """Atomically replace one local archive object."""
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=target.parent,
                prefix=f".{target.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary.write(data)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            os.replace(temporary_path, target)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def exists(self, key: str) -> bool:
        """Return whether a local archive object exists."""
        return self._path(key).is_file()


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
        kwargs: dict[str, str] = {"Bucket": self._bucket, "Prefix": ""}
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


def hour_to_prefix(hour: datetime) -> str:
    """Return the sortable UTC object prefix for one receive-time hour."""
    return hour.astimezone(timezone.utc).strftime("%Y-%m-%d/%H")


def event_to_key(hour: datetime, event_type: str) -> str:
    """Return the Parquet object key for one event type and UTC hour."""
    if event_type not in EVENT_PROJECTIONS:
        raise ValueError(f"unsupported event type: {event_type}")
    return f"{hour_to_prefix(hour)}/{event_type}.parquet"


def hour_to_completion_key(hour: datetime) -> str:
    """Return the object whose presence means an hour was fully published."""
    return f"{hour_to_prefix(hour)}/manifest.json"


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


def export_hour(client: ArchiveWriter, hour: datetime) -> None:
    """Upload every event-specific file, then atomically complete the hour."""
    prefix = hour_to_prefix(hour)
    log.info("Exporting %s", prefix)
    files: dict[str, dict[str, object]] = {}
    total_rows = 0
    hour_min_sequence: int | None = None
    hour_max_sequence: int | None = None

    for event_type in EVENT_PROJECTIONS:
        table = fetch_event_table(hour, event_type)
        data = table_to_parquet(table, event_type)
        key = event_to_key(hour, event_type)
        row_count = table.num_rows
        sequence_bounds = pc.min_max(  # pyright: ignore[reportAttributeAccessIssue]
            table.column("sequence")
        ).as_py()
        min_sequence = int(sequence_bounds["min"]) if row_count else None
        max_sequence = int(sequence_bounds["max"]) if row_count else None
        client.upload(key, data)
        files[event_type] = {
            "file": key,
            "row_count": row_count,
            "byte_size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "min_sequence": min_sequence,
            "max_sequence": max_sequence,
            "columns": table.column_names,
            "order_by": PARQUET_SORT_COLUMNS[event_type],
        }
        total_rows += row_count
        if min_sequence is not None:
            assert max_sequence is not None
            hour_min_sequence = (
                min_sequence
                if hour_min_sequence is None
                else min(hour_min_sequence, min_sequence)
            )
            hour_max_sequence = (
                max_sequence
                if hour_max_sequence is None
                else max(hour_max_sequence, max_sequence)
            )
        log.info(
            "Uploaded %s: rows=%d size=%.2f MB",
            key,
            row_count,
            len(data) / (1024 * 1024),
        )

    manifest = {
        "hour_utc": hour.astimezone(timezone.utc).isoformat(),
        "row_count": total_rows,
        "min_sequence": hour_min_sequence,
        "max_sequence": hour_max_sequence,
        "files": files,
        "source_table": CLICKHOUSE_TABLE,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_data = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    client.upload(hour_to_completion_key(hour), manifest_data)
    log.info(
        "Completed %s: rows=%d sequence=%s..%s",
        prefix,
        total_rows,
        hour_min_sequence,
        hour_max_sequence,
    )


def backfill(client: ArchiveClient) -> datetime | None:
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
        sum(key.endswith("/manifest.json") for key in existing),
    )

    for i, hour in enumerate(missing, 1):
        try:
            export_hour(client, hour)
            log.info("Backfill progress: %d/%d", i, len(missing))
        except Exception as e:
            log.error("Failed to export %s: %s", hour.isoformat(), e)
            return hour
    return latest + timedelta(hours=1)


def run_loop(client: ArchiveClient, next_hour: datetime | None) -> None:
    """Steady-state loop: export each new hour shortly after it completes.

    The manifest is uploaded only after all seven typed Parquet files. Empty
    event types and entirely empty hours are valid completed exports.
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
                export_hour(client, next_hour)
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

    client: ArchiveClient
    if EXPORT_BACKEND == "local":
        local_client = LocalArchive(LOCAL_EXPORT_DIR)
        local_client.ensure_directory()
        client = local_client
        log.info(
            "Using local archive directory %s, source_table=%s, sort_orders=%s",
            LOCAL_EXPORT_DIR,
            CLICKHOUSE_TABLE,
            PARQUET_SORT_COLUMNS,
        )
    elif EXPORT_BACKEND == "r2":
        required = ("R2_ENDPOINT", "R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_BUCKET")
        missing = [name for name in required if not globals()[name]]
        if missing:
            log.error("Missing required environment variables: %s", ", ".join(missing))
            sys.exit(1)

        r2_client = R2Client(R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET)
        r2_client.ensure_bucket()
        client = r2_client
        log.info(
            "Connected to R2 at %s, bucket=%s, source_table=%s, sort_orders=%s",
            R2_ENDPOINT,
            R2_BUCKET,
            CLICKHOUSE_TABLE,
            PARQUET_SORT_COLUMNS,
        )
    else:
        log.error("Unsupported EXPORT_BACKEND: %s", EXPORT_BACKEND)
        sys.exit(1)

    next_hour = backfill(client)
    if EXPORT_ONCE:
        log.info("One-shot export complete")
        return
    run_loop(client, next_hour)


if __name__ == "__main__":
    main()

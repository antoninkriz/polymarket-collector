"""Unit tests for event-specific R2 export orchestration."""

from __future__ import annotations

import json
import unittest
from decimal import Decimal
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import run as exporter


class MemoryR2:
    """Minimal upload recorder used by exporter tests."""

    def __init__(self) -> None:
        self.uploads: dict[str, bytes] = {}
        self.upload_order: list[str] = []

    def upload(self, key: str, data: bytes) -> None:
        self.uploads[key] = data
        self.upload_order.append(key)


class ExportPathsTest(unittest.TestCase):
    def test_hour_and_event_keys_are_sortable(self) -> None:
        hour = datetime(2026, 8, 13, 4, tzinfo=timezone.utc)

        self.assertEqual(exporter.hour_to_prefix(hour), "2026-08-13/04")
        self.assertEqual(
            exporter.event_to_key(hour, "price_change"),
            "2026-08-13/04/price_change.parquet",
        )
        self.assertEqual(
            exporter.hour_to_completion_key(hour),
            "2026-08-13/04/manifest.json",
        )

    def test_unknown_event_type_is_rejected(self) -> None:
        hour = datetime(2026, 8, 13, 4, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            exporter.event_to_key(hour, "unknown")


class ParquetSchemaTest(unittest.TestCase):
    def test_decimals_are_narrow_and_integer_backed(self) -> None:
        decimal32_columns = (
            "price",
            "best_bid",
            "best_ask",
            "spread",
            "old_tick_size",
            "new_tick_size",
        )
        fields = [
            pa.field(
                name,
                pa.decimal128(9, 4),
                nullable=name in {"best_bid", "best_ask", "spread"},
            )
            for name in decimal32_columns
        ]
        fields.append(pa.field("size", pa.decimal128(18, 6), nullable=False))
        source_schema = pa.schema(fields)
        arrays = [
            pa.array(
                [None if field.nullable else Decimal("0.1234")],
                type=field.type,
            )
            for field in fields[:-1]
        ]
        arrays.append(pa.array([Decimal("123.456789")], type=fields[-1].type))
        source = pa.Table.from_arrays(arrays, schema=source_schema)

        data = exporter.table_to_parquet(source)
        parquet_file = pq.ParquetFile(BytesIO(data))
        arrow_schema = parquet_file.schema_arrow
        for name in decimal32_columns:
            self.assertEqual(arrow_schema.field(name).type, pa.decimal32(9, 4))
        self.assertEqual(arrow_schema.field("size").type, pa.decimal64(18, 6))

        physical_types = {
            parquet_file.metadata.schema.column(
                i
            ).name: parquet_file.metadata.schema.column(i).physical_type
            for i in range(parquet_file.metadata.num_columns)
        }
        for name in decimal32_columns:
            self.assertEqual(physical_types[name], "INT32")
        self.assertEqual(physical_types["size"], "INT64")

        round_trip = parquet_file.read()
        self.assertEqual(round_trip.schema, arrow_schema)
        self.assertEqual(round_trip.column("price")[0].as_py(), Decimal("0.1234"))
        self.assertEqual(
            round_trip.column("size")[0].as_py(),
            Decimal("123.456789"),
        )


class ExportHourTest(unittest.TestCase):
    def test_manifest_is_uploaded_last_and_records_empty_types(self) -> None:
        hour = datetime(2026, 8, 13, 14, tzinfo=timezone.utc)
        client = MemoryR2()

        def fetch_event_table(_hour: datetime, event_type: str) -> pa.Table:
            sequences = [10] if event_type == "book" else []
            return pa.table({"sequence": pa.array(sequences, type=pa.uint64())})

        with patch.object(
            exporter,
            "fetch_event_table",
            side_effect=fetch_event_table,
        ):
            exporter.export_hour(client, hour)  # pyright: ignore[reportArgumentType]

        manifest_key = "2026-08-13/14/manifest.json"
        self.assertEqual(client.upload_order[-1], manifest_key)
        self.assertEqual(len(client.uploads), len(exporter.EVENT_PROJECTIONS) + 1)

        manifest = json.loads(client.uploads[manifest_key])
        self.assertEqual(manifest["row_count"], 1)
        self.assertEqual(manifest["min_sequence"], 10)
        self.assertEqual(manifest["max_sequence"], 10)
        self.assertEqual(set(manifest["files"]), set(exporter.EVENT_PROJECTIONS))
        self.assertEqual(manifest["files"]["book"]["row_count"], 1)
        self.assertEqual(
            manifest["files"]["price_change"]["row_count"],
            0,
        )
        for event_type, metadata in manifest["files"].items():
            self.assertEqual(
                metadata["file"],
                f"2026-08-13/14/{event_type}.parquet",
            )
            self.assertIn(metadata["file"], client.uploads)


if __name__ == "__main__":
    unittest.main()

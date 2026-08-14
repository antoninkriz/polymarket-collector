"""Tests for atomic local Parquet re-encoding."""

from __future__ import annotations

import hashlib
import json
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

import reencode_local as reencoder
import run as exporter


class ReencodeLocalTest(unittest.TestCase):
    def test_prepares_and_commits_completed_hour(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            hour = root / "2026-08-14" / "08"
            hour.mkdir(parents=True)
            parquet_path = hour / "price_change.parquet"
            table = self._price_changes()
            parquet_path.write_bytes(exporter.table_to_parquet(table, "price_change"))
            old_bytes = parquet_path.read_bytes()
            manifest = {
                "created_at": "2026-08-14T09:10:19+00:00",
                "files": {
                    "price_change": {
                        "byte_size": len(old_bytes),
                        "columns": table.column_names,
                        "file": "2026-08-14/08/price_change.parquet",
                        "max_sequence": 4,
                        "min_sequence": 1,
                        "row_count": table.num_rows,
                        "sha256": hashlib.sha256(old_bytes).hexdigest(),
                    }
                },
                "hour_utc": datetime(2026, 8, 14, 8, tzinfo=timezone.utc).isoformat(),
                "row_count": table.num_rows,
            }
            manifest_path = hour / "manifest.json"
            manifest_path.write_text(json.dumps(manifest))

            plan = reencoder._discover(root)[0]
            prepared = reencoder._prepare_file(plan.tasks[0])

            self.assertEqual(parquet_path.read_bytes(), old_bytes)
            self.assertTrue(prepared.temporary.exists())
            reencoder._commit_hour(plan, (prepared,))

            rewritten = pq.read_table(parquet_path)
            self.assertTrue(rewritten.equals(table))
            updated = json.loads(manifest_path.read_text())
            self.assertEqual(
                updated["files"]["price_change"]["byte_size"],
                parquet_path.stat().st_size,
            )
            self.assertEqual(
                updated["files"]["price_change"]["sha256"],
                hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
            )
            self.assertFalse(prepared.temporary.exists())
            self.assertFalse(reencoder._backup_path(parquet_path).exists())
            self.assertFalse(reencoder._backup_path(manifest_path).exists())

    def test_ignores_incomplete_hour_without_manifest(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            hour = root / "2026-08-14" / "08"
            hour.mkdir(parents=True)
            (hour / "price_change.parquet").touch()

            self.assertEqual(reencoder._discover(root), ())

    @staticmethod
    def _price_changes() -> pa.Table:
        return pa.table(
            {
                "timestamp_received": pa.array(
                    [1_000_000_001, 1_000_000_002, 1_000_000_003, 1_000_000_004],
                    type=pa.timestamp("ns", tz="UTC"),
                ),
                "sequence": pa.array([1, 2, 3, 4], type=pa.uint64()),
                "timestamp": pa.array(
                    [1_000, 1_001, 1_002, 1_003],
                    type=pa.timestamp("ms", tz="UTC"),
                ),
                "market": pa.array([b"m" * 32] * 4, type=pa.binary(32)),
                "asset_id": pa.array([b"a" * 32] * 4, type=pa.binary(32)),
                "price": pa.array(
                    [Decimal("0.5000")] * 4,
                    type=pa.decimal32(9, 4),
                ),
                "size": pa.array(
                    [Decimal("2.000000")] * 4,
                    type=pa.decimal64(18, 6),
                ),
                "side": pa.array(["BUY", "SELL", "BUY", "SELL"]),
                "best_bid": pa.array(
                    [Decimal("0.4900")] * 4,
                    type=pa.decimal32(9, 4),
                ),
                "best_ask": pa.array(
                    [Decimal("0.5100")] * 4,
                    type=pa.decimal32(9, 4),
                ),
            }
        )


if __name__ == "__main__":
    unittest.main()

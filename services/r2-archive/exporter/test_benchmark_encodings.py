"""Tests for the Parquet encoding benchmark utility."""

from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

import benchmark_encodings as benchmark


class EncodingBenchmarkTest(unittest.TestCase):
    def test_evenly_spaced_row_group_selection(self) -> None:
        self.assertEqual(benchmark._selected_row_groups(10, 3), (0, 4, 9))
        self.assertEqual(benchmark._selected_row_groups(3, 0), (0, 1, 2))
        self.assertEqual(benchmark._selected_row_groups(0, 3), ())

    def test_benchmarks_scalar_and_nested_physical_leaves(self) -> None:
        level_type = pa.struct(
            [
                pa.field("price", pa.decimal32(9, 4), nullable=False),
                pa.field("size", pa.decimal64(18, 6), nullable=False),
            ]
        )
        levels_type = pa.list_(pa.field("item", level_type, nullable=False))
        table = pa.table(
            {
                "sequence": pa.array(range(100), type=pa.uint64()),
                "side": pa.array(["BUY", "SELL"] * 50),
                "bids": pa.array(
                    [
                        [
                            {
                                "price": Decimal("0.5000"),
                                "size": Decimal("2.000000"),
                            }
                        ]
                    ]
                    * 100,
                    type=levels_type,
                ),
            }
        )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "book.parquet"
            pq.write_table(
                table,
                path,
                row_group_size=50,
                store_decimal_as_integer=True,
            )
            results = benchmark.benchmark_event([path], row_group_limit=0)

        self.assertEqual(results["sequence"].row_groups, 2)
        self.assertEqual(results["sequence"].value_count, 100)
        self.assertIn("DELTA_BINARY_PACKED", results["sequence"].compressed_sizes)
        self.assertIn(benchmark.DICTIONARY, results["side"].compressed_sizes)
        self.assertEqual(results["bids.list.element.price"].value_count, 100)
        self.assertEqual(results["bids.list.element.size"].value_count, 100)


if __name__ == "__main__":
    unittest.main()

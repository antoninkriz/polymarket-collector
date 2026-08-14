"""Benchmark candidate Parquet value encodings on exported event files.

The benchmark reads decoded Arrow values and rewrites one physical leaf at a
time. Reported byte counts are compressed column-chunk sizes, so Parquet file
footer overhead does not distort comparisons for small event files.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

COMPRESSION = "zstd"
COMPRESSION_LEVEL = 9
DICTIONARY_PAGE_SIZE_LIMIT = 8 * 1024 * 1024
DICTIONARY = "RLE_DICTIONARY"


@dataclass
class ColumnResult:
    """Aggregated measurements for one physical Parquet column."""

    physical_type: str
    row_groups: int = 0
    value_count: int = 0
    null_count: int = 0
    distinct_per_row_group: int = 0
    compressed_sizes: dict[str, int] = field(default_factory=dict)

    def add_values(self, values: pa.Array) -> None:
        """Add value and row-group-local cardinality statistics."""
        self.row_groups += 1
        self.value_count += len(values)
        self.null_count += values.null_count
        distinct = pc.count_distinct(values).as_py()  # pyright: ignore[reportAttributeAccessIssue]
        self.distinct_per_row_group += int(distinct or 0)

    def add_size(self, encoding: str, compressed_size: int) -> None:
        """Add a compressed column-chunk size for an encoding."""
        self.compressed_sizes[encoding] = (
            self.compressed_sizes.get(encoding, 0) + compressed_size
        )


def _candidate_encodings(physical_type: str) -> tuple[str, ...]:
    """Return encodings supported by the physical value family."""
    if physical_type in {"INT32", "INT64"}:
        return ("PLAIN", DICTIONARY, "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT")
    if physical_type == "BYTE_ARRAY":
        return ("PLAIN", DICTIONARY, "DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY")
    if physical_type == "FIXED_LEN_BYTE_ARRAY":
        return ("PLAIN", DICTIONARY, "DELTA_BYTE_ARRAY", "BYTE_STREAM_SPLIT")
    return ("PLAIN", DICTIONARY)


def _selected_row_groups(count: int, limit: int) -> tuple[int, ...]:
    """Select evenly spaced row groups, or all row groups when limit is zero."""
    if count == 0:
        return ()
    if limit == 0 or count <= limit:
        return tuple(range(count))
    if limit == 1:
        return (count // 2,)
    return tuple(
        sorted({round(index * (count - 1) / (limit - 1)) for index in range(limit)})
    )


def _leaf_values(column: pa.ChunkedArray, physical_path: str) -> pa.Array:
    """Extract the primitive values represented by a Parquet physical path."""
    values = column.combine_chunks()
    components = physical_path.split(".")[1:]
    while components:
        if pa.types.is_list(values.type) or pa.types.is_large_list(values.type):
            if components[:2] != ["list", "element"]:
                raise ValueError(f"unexpected list path: {physical_path}")
            values = pc.list_flatten(values)  # pyright: ignore[reportAttributeAccessIssue]
            components = components[2:]
            continue
        if pa.types.is_struct(values.type):
            field_name = components.pop(0)
            values = values.field(field_name)
            continue
        raise ValueError(f"path does not resolve to a primitive leaf: {physical_path}")
    return values


def _compressed_size(
    table: pa.Table,
    physical_path: str,
    encoding: str,
) -> int:
    """Encode a table and return one physical leaf's compressed chunk size."""
    options: dict[str, object] = {"use_dictionary": False}
    if encoding == DICTIONARY:
        options["use_dictionary"] = [physical_path]
    else:
        options["column_encoding"] = {physical_path: encoding}

    output = BytesIO()
    pq.write_table(
        table,
        output,
        compression=COMPRESSION,
        compression_level=COMPRESSION_LEVEL,
        data_page_version="2.0",
        dictionary_pagesize_limit=DICTIONARY_PAGE_SIZE_LIMIT,
        store_decimal_as_integer=True,
        **options,  # pyright: ignore[reportArgumentType]
    )
    parquet_file = pq.ParquetFile(output)
    metadata = parquet_file.metadata
    for index in range(metadata.num_columns):
        column = metadata.row_group(0).column(index)
        if column.path_in_schema == physical_path:
            return column.total_compressed_size
    raise ValueError(f"encoded output has no column {physical_path!r}")


def benchmark_event(
    paths: Iterable[Path],
    row_group_limit: int,
) -> dict[str, ColumnResult]:
    """Benchmark all physical columns in files for one event type."""
    results: dict[str, ColumnResult] = {}
    unsupported: set[tuple[str, str]] = set()

    for path in paths:
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        leaves_by_top_level: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for index in range(metadata.num_columns):
            column = metadata.schema.column(index)
            leaves_by_top_level[column.path.split(".", 1)[0]].append(
                (column.path, column.physical_type)
            )

        selected = _selected_row_groups(metadata.num_row_groups, row_group_limit)
        for row_group in selected:
            for top_level, leaves in leaves_by_top_level.items():
                table = parquet_file.read_row_group(row_group, columns=[top_level])
                if table.num_rows == 0:
                    continue
                for physical_path, physical_type in leaves:
                    result = results.setdefault(
                        physical_path,
                        ColumnResult(physical_type=physical_type),
                    )
                    result.add_values(_leaf_values(table.column(0), physical_path))
                    for encoding in _candidate_encodings(physical_type):
                        if (physical_path, encoding) in unsupported:
                            continue
                        try:
                            size = _compressed_size(table, physical_path, encoding)
                        except (pa.ArrowException, ValueError):
                            unsupported.add((physical_path, encoding))
                            result.compressed_sizes.pop(encoding, None)
                            continue
                        result.add_size(encoding, size)
    return results


def _format_size(size: int | None) -> str:
    """Format a byte count compactly without hiding small differences."""
    if size is None:
        return "-"
    if size < 10_000:
        return f"{size:,} B"
    return f"{size / (1024 * 1024):.2f} MiB"


def _print_event(
    event_type: str,
    paths: list[Path],
    results: dict[str, ColumnResult],
) -> None:
    """Print one event type's benchmark as a Markdown table."""
    encodings = tuple(
        dict.fromkeys(
            encoding
            for result in results.values()
            for encoding in result.compressed_sizes
        )
    )
    print(f"\n## `{event_type}`")
    print()
    print(f"Files: {len(paths)}; sampled files: `{paths[0]}` through `{paths[-1]}`.")
    print()
    headers = [
        "Column leaf",
        "Physical",
        "Values",
        "Nulls",
        "RG distinct",
        *encodings,
    ]
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join("---" for _ in headers) + "|")
    for path, result in results.items():
        non_null = result.value_count - result.null_count
        cardinality = result.distinct_per_row_group / non_null if non_null else 0.0
        sizes = [
            _format_size(result.compressed_sizes.get(encoding))
            for encoding in encodings
        ]
        values = [
            f"`{path}`",
            result.physical_type,
            f"{result.value_count:,}",
            f"{result.null_count:,}",
            f"{cardinality:.2%}",
            *sizes,
        ]
        print("| " + " | ".join(values) + " |")


def _find_files(inputs: Iterable[Path], events: set[str]) -> dict[str, list[Path]]:
    """Resolve input paths and group Parquet files by event type."""
    grouped: dict[str, set[Path]] = defaultdict(set)
    for input_path in inputs:
        paths = [input_path] if input_path.is_file() else input_path.rglob("*.parquet")
        for path in paths:
            event_type = path.stem
            if not events or event_type in events:
                grouped[event_type].add(path)
    return {event_type: sorted(paths) for event_type, paths in sorted(grouped.items())}


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        help="Parquet file or directory to search recursively",
    )
    parser.add_argument(
        "--event",
        action="append",
        default=[],
        help="event filename stem to include; may be repeated",
    )
    parser.add_argument(
        "--row-groups-per-file",
        type=int,
        default=3,
        help="evenly spaced row groups to sample from each file; 0 means all",
    )
    args = parser.parse_args()
    if args.row_groups_per_file < 0:
        parser.error("--row-groups-per-file must be non-negative")
    return args


def main() -> None:
    """Run the benchmark and print Markdown to standard output."""
    args = parse_args()
    grouped = _find_files(args.inputs, set(args.event))
    if not grouped:
        raise SystemExit("no matching Parquet files")

    print("# Parquet encoding benchmark")
    print()
    print(
        "Compressed column-chunk bytes using ZSTD level 9. "
        "RG distinct is the summed per-row-group cardinality divided by "
        "non-null values."
    )
    for event_type, paths in grouped.items():
        results = benchmark_event(paths, args.row_groups_per_file)
        _print_event(event_type, paths, results)


if __name__ == "__main__":
    main()

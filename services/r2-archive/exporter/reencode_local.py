"""Re-encode completed local Parquet exports with the current writer policy."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

import run as exporter

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReencodeTask:
    """One immutable completed Parquet file to prepare."""

    path: Path
    event_type: str
    expected_size: int
    expected_rows: int


@dataclass(frozen=True)
class PreparedFile:
    """Verified replacement waiting beside its source file."""

    source: Path
    temporary: Path
    event_type: str
    old_size: int
    new_size: int
    sha256: str


@dataclass(frozen=True)
class HourPlan:
    """Completed hour and the files named by its manifest."""

    manifest_path: Path
    manifest: dict[str, Any]
    tasks: tuple[ReencodeTask, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _discover(root: Path) -> tuple[HourPlan, ...]:
    root = root.resolve()
    plans: list[HourPlan] = []
    for manifest_path in sorted(root.glob("*/*/manifest.json")):
        manifest = json.loads(manifest_path.read_text())
        tasks: list[ReencodeTask] = []
        for event_type, record in sorted(manifest["files"].items()):
            if event_type not in exporter.EVENT_PROJECTIONS:
                raise ValueError(f"unsupported event type {event_type!r}")
            source = (root / record["file"]).resolve()
            if not source.is_relative_to(root):
                raise ValueError(f"manifest path escapes export root: {source}")
            if source.parent != manifest_path.parent.resolve():
                raise ValueError(f"manifest file is outside its hour: {source}")
            tasks.append(
                ReencodeTask(
                    path=source,
                    event_type=event_type,
                    expected_size=int(record["byte_size"]),
                    expected_rows=int(record["row_count"]),
                )
            )
        plans.append(
            HourPlan(
                manifest_path=manifest_path.resolve(),
                manifest=manifest,
                tasks=tuple(tasks),
            )
        )
    return tuple(plans)


def _physical_types(parquet_file: pq.ParquetFile) -> tuple[str, ...]:
    metadata = parquet_file.metadata
    return tuple(
        metadata.schema.column(index).physical_type
        for index in range(metadata.num_columns)
    )


def _prepare_file(task: ReencodeTask) -> PreparedFile:
    current_size = task.path.stat().st_size
    if current_size != task.expected_size:
        raise ValueError(
            f"{task.path} is {current_size} bytes, manifest says {task.expected_size}"
        )

    source = pq.ParquetFile(task.path)
    source_metadata = source.metadata
    if source_metadata.num_rows != task.expected_rows:
        raise ValueError(
            f"{task.path} has {source_metadata.num_rows} rows, manifest says "
            f"{task.expected_rows}"
        )

    temporary_file = tempfile.NamedTemporaryFile(
        prefix=f".{task.path.name}.reencode-",
        suffix=".tmp",
        dir=task.path.parent,
        delete=False,
    )
    temporary = Path(temporary_file.name)
    temporary_file.close()

    dictionary_columns = [
        column
        for column in exporter.PARQUET_DICTIONARY_COLUMNS[task.event_type]
        if column in source.schema_arrow.names
    ]
    column_encodings = {
        column: encoding
        for column, encoding in exporter.PARQUET_COLUMN_ENCODINGS[
            task.event_type
        ].items()
        if column in source.schema_arrow.names
    }

    try:
        with pq.ParquetWriter(
            temporary,
            source.schema_arrow,
            compression=exporter.PARQUET_COMPRESSION,
            compression_level=exporter.PARQUET_COMPRESSION_LEVEL,
            use_dictionary=dictionary_columns,  # pyright: ignore[reportArgumentType]
            column_encoding=column_encodings,
            data_page_version="2.0",
            dictionary_pagesize_limit=(exporter.PARQUET_DICTIONARY_PAGE_SIZE_LIMIT),
            store_decimal_as_integer=True,
        ) as writer:
            for row_group in range(source_metadata.num_row_groups):
                table = source.read_row_group(row_group)
                writer.write_table(table, row_group_size=max(1, table.num_rows))

        os.chmod(temporary, task.path.stat().st_mode & 0o777)
        with temporary.open("rb") as output:
            os.fsync(output.fileno())

        replacement = pq.ParquetFile(temporary)
        replacement_metadata = replacement.metadata
        if not replacement.schema_arrow.equals(
            source.schema_arrow,
            check_metadata=True,
        ):
            raise ValueError(f"schema changed while re-encoding {task.path}")
        if _physical_types(replacement) != _physical_types(source):
            raise ValueError(f"physical types changed while re-encoding {task.path}")
        if replacement_metadata.num_rows != source_metadata.num_rows:
            raise ValueError(f"row count changed while re-encoding {task.path}")
        source_groups = tuple(
            source_metadata.row_group(index).num_rows
            for index in range(source_metadata.num_row_groups)
        )
        replacement_groups = tuple(
            replacement_metadata.row_group(index).num_rows
            for index in range(replacement_metadata.num_row_groups)
        )
        if replacement_groups != source_groups:
            raise ValueError(f"row groups changed while re-encoding {task.path}")

        return PreparedFile(
            source=task.path,
            temporary=temporary,
            event_type=task.event_type,
            old_size=current_size,
            new_size=temporary.stat().st_size,
            sha256=_sha256(temporary),
        )
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _backup_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.reencode-old")


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    payload = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    temporary_file = tempfile.NamedTemporaryFile(
        prefix=".manifest.json.reencode-",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    )
    temporary = Path(temporary_file.name)
    try:
        temporary_file.write(payload)
        temporary_file.flush()
        os.fsync(temporary_file.fileno())
        temporary_file.close()
        os.replace(temporary, path)
    except BaseException:
        temporary_file.close()
        temporary.unlink(missing_ok=True)
        raise


def _commit_hour(plan: HourPlan, prepared: tuple[PreparedFile, ...]) -> None:
    by_event = {item.event_type: item for item in prepared}
    expected_events = {task.event_type for task in plan.tasks}
    if set(by_event) != expected_events:
        raise ValueError(f"prepared files do not match {plan.manifest_path}")

    manifest_backup = _backup_path(plan.manifest_path)
    backups = {item.source: _backup_path(item.source) for item in prepared}
    conflicts = [path for path in (manifest_backup, *backups.values()) if path.exists()]
    if conflicts:
        raise FileExistsError(f"stale re-encode backups exist: {conflicts}")

    moved: list[PreparedFile] = []
    os.replace(plan.manifest_path, manifest_backup)
    try:
        for item in sorted(prepared, key=lambda value: value.source.name):
            os.replace(item.source, backups[item.source])
            moved.append(item)
            os.replace(item.temporary, item.source)

        manifest = dict(plan.manifest)
        manifest["files"] = {
            event_type: dict(record)
            for event_type, record in plan.manifest["files"].items()
        }
        for event_type, item in by_event.items():
            manifest["files"][event_type]["byte_size"] = item.new_size
            manifest["files"][event_type]["sha256"] = item.sha256
        manifest["created_at"] = datetime.now(timezone.utc).isoformat()
        _write_manifest(plan.manifest_path, manifest)
    except BaseException:
        plan.manifest_path.unlink(missing_ok=True)
        for item in reversed(moved):
            item.source.unlink(missing_ok=True)
            os.replace(backups[item.source], item.source)
        os.replace(manifest_backup, plan.manifest_path)
        raise
    else:
        for backup in backups.values():
            backup.unlink()
        manifest_backup.unlink()


def reencode(root: Path, jobs: int) -> None:
    """Re-encode every file referenced by a completed local manifest."""
    plans = _discover(root)
    tasks = tuple(task for plan in plans for task in plan.tasks)
    if not tasks:
        log.info("No completed Parquet hours found below %s", root)
        return

    log.info(
        "Preparing %d files from %d completed hours with %d workers",
        len(tasks),
        len(plans),
        jobs,
    )
    prepared: list[PreparedFile] = []
    errors: list[BaseException] = []
    with ProcessPoolExecutor(max_workers=jobs) as executor:
        futures = {executor.submit(_prepare_file, task): task for task in tasks}
        for future in as_completed(futures):
            task = futures[future]
            try:
                item = future.result()
            except BaseException as error:
                log.error("Failed to prepare %s: %s", task.path, error)
                errors.append(error)
                continue
            prepared.append(item)
            log.info(
                "Prepared %s: %.2f MiB -> %.2f MiB",
                item.source,
                item.old_size / (1024 * 1024),
                item.new_size / (1024 * 1024),
            )

    if errors:
        for item in prepared:
            item.temporary.unlink(missing_ok=True)
        raise RuntimeError(f"failed to prepare {len(errors)} files") from errors[0]

    prepared_by_source = {item.source: item for item in prepared}
    try:
        for plan in plans:
            hour_files = tuple(prepared_by_source[task.path] for task in plan.tasks)
            _commit_hour(plan, hour_files)
            log.info("Committed %s", plan.manifest_path.parent)
    finally:
        for item in prepared:
            item.temporary.unlink(missing_ok=True)

    old_bytes = sum(item.old_size for item in prepared)
    new_bytes = sum(item.new_size for item in prepared)
    log.info(
        "Re-encoded %d files: %.2f GiB -> %.2f GiB, saved %.2f GiB (%.1f%%)",
        len(prepared),
        old_bytes / (1024**3),
        new_bytes / (1024**3),
        (old_bytes - new_bytes) / (1024**3),
        100 * (old_bytes - new_bytes) / old_bytes,
    )


def main() -> None:
    """Parse command-line arguments and run the local conversion."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="local Parquet export root")
    parser.add_argument(
        "--jobs",
        type=int,
        default=4,
        help="number of files to encode concurrently (default: 4)",
    )
    arguments = parser.parse_args()
    if arguments.jobs < 1:
        parser.error("--jobs must be at least 1")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(processName)s: %(message)s",
    )
    reencode(arguments.root, arguments.jobs)


if __name__ == "__main__":
    main()

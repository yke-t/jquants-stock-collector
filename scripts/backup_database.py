"""Create and verify a SQLite backup without modifying the source database."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = PROJECT_ROOT / "stock_data.db"
FREE_SPACE_MARGIN_BYTES = 64 * 1024 * 1024


def read_only_connection(path: Path) -> sqlite3.Connection:
    """Open an existing SQLite database without permitting writes."""
    uri = path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30.0)
    connection.execute("PRAGMA query_only = ON")
    return connection


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def schema_sha256(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        """SELECT type, name, tbl_name, COALESCE(sql, '')
           FROM sqlite_schema
           WHERE name NOT LIKE 'sqlite_%'
           ORDER BY type, name, tbl_name"""
    ).fetchall()
    serialized = json.dumps(
        rows,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def table_row_counts(connection: sqlite3.Connection) -> dict[str, int]:
    names = [
        str(row[0])
        for row in connection.execute(
            """SELECT name
               FROM sqlite_schema
               WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
               ORDER BY name"""
        )
    ]
    return {
        name: int(
            connection.execute(
                f"SELECT COUNT(*) FROM {quote_identifier(name)}"
            ).fetchone()[0]
        )
        for name in names
    }


def summarize_connection(
    connection: sqlite3.Connection,
    path: Path,
) -> dict[str, Any]:
    quick_check_rows = [
        str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()
    ]
    quick_check = "ok" if quick_check_rows == ["ok"] else quick_check_rows
    if quick_check != "ok":
        raise ValueError(f"SQLite quick_check failed for {path}: {quick_check_rows}")
    return {
        "path": str(path.resolve()),
        "size_bytes": path.stat().st_size,
        "quick_check": quick_check,
        "schema_sha256": schema_sha256(connection),
        "table_row_counts": table_row_counts(connection),
        "application_id": int(connection.execute("PRAGMA application_id").fetchone()[0]),
        "user_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
    }


def summarize_database(path: Path) -> dict[str, Any]:
    with closing(read_only_connection(path)) as connection:
        return summarize_connection(connection, path)


def assert_equivalent(
    reference: dict[str, Any],
    candidate: dict[str, Any],
    candidate_label: str,
) -> None:
    comparisons = {
        "quick_check": candidate["quick_check"] == "ok",
        "schema_sha256": (
            candidate["schema_sha256"] == reference["schema_sha256"]
        ),
        "table_row_counts": (
            candidate["table_row_counts"] == reference["table_row_counts"]
        ),
        "application_id": (
            candidate["application_id"] == reference["application_id"]
        ),
        "user_version": candidate["user_version"] == reference["user_version"],
    }
    failed = [name for name, passed in comparisons.items() if not passed]
    if failed:
        raise ValueError(
            f"{candidate_label} differs from the source snapshot: {', '.join(failed)}"
        )


def copy_database(
    source: sqlite3.Connection,
    destination_path: Path,
    label: str,
) -> None:
    next_report = 0

    def progress(_status: int, remaining: int, total: int) -> None:
        nonlocal next_report
        if total <= 0:
            return
        percent = int(((total - remaining) * 100) / total)
        if percent >= next_report or remaining == 0:
            print(f"[INFO] {label}: {percent}%", file=sys.stderr, flush=True)
            next_report = min(100, percent + 10)

    with closing(sqlite3.connect(destination_path)) as destination:
        source.backup(destination, pages=4096, progress=progress, sleep=0.05)


def remove_incomplete_database(path: Path) -> None:
    for candidate in (
        path,
        Path(f"{path}-journal"),
        Path(f"{path}-wal"),
        Path(f"{path}-shm"),
    ):
        if candidate.exists():
            candidate.unlink()


def storage_preflight(
    source_path: Path,
    backup_path: Path,
    restore_drill: bool,
) -> dict[str, int]:
    existing_parent = backup_path.parent
    while not existing_parent.exists() and existing_parent != existing_parent.parent:
        existing_parent = existing_parent.parent
    source_size = source_path.stat().st_size
    copies_required = 2 if restore_drill else 1
    required_free = source_size * copies_required + FREE_SPACE_MARGIN_BYTES
    free = shutil.disk_usage(existing_parent).free
    if free < required_free:
        raise OSError(
            "Insufficient free space for backup"
            f"{' and restore drill' if restore_drill else ''}: "
            f"required={required_free}, available={free}, volume={existing_parent}"
        )
    return {
        "available_bytes_before": free,
        "required_bytes": required_free,
        "temporary_copies_required": copies_required,
    }


def validate_result_path(
    source_path: Path,
    backup_path: Path,
    result_path: Path | None,
) -> None:
    if result_path is None:
        return
    resolved = result_path.resolve()
    if resolved in (source_path.resolve(), backup_path.resolve()):
        raise ValueError("Result path must differ from source and backup paths")
    if resolved.exists():
        raise FileExistsError(f"Result path already exists: {resolved}")


def create_verified_backup(
    source_path: Path,
    backup_path: Path,
    restore_drill: bool,
) -> dict[str, Any]:
    source_path = source_path.resolve()
    backup_path = backup_path.resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"Source database not found: {source_path}")
    if source_path == backup_path:
        raise ValueError("Backup path must differ from the source database")
    if backup_path.exists():
        raise FileExistsError(f"Backup path already exists: {backup_path}")

    storage = storage_preflight(source_path, backup_path, restore_drill)
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with closing(read_only_connection(source_path)) as source:
            source.execute("BEGIN")
            source_summary = summarize_connection(source, source_path)
            copy_database(source, backup_path, "backup")
            source.rollback()
    except Exception:
        remove_incomplete_database(backup_path)
        raise

    try:
        backup_summary = summarize_database(backup_path)
        assert_equivalent(source_summary, backup_summary, "Backup")
    except Exception:
        remove_incomplete_database(backup_path)
        raise

    restore_summary: dict[str, Any] | None = None
    restore_removed: bool | None = None
    if restore_drill:
        restored_path: Path | None = None
        with tempfile.TemporaryDirectory(
            prefix=f".{backup_path.stem}-restore-",
            dir=backup_path.parent,
        ) as temp_dir:
            restored_path = Path(temp_dir) / "restored.db"
            with closing(read_only_connection(backup_path)) as backup:
                copy_database(backup, restored_path, "restore drill")
            restore_summary = summarize_database(restored_path)
            assert_equivalent(backup_summary, restore_summary, "Restored database")
        restore_removed = restored_path is not None and not restored_path.exists()

    return {
        "schema_version": "1.0",
        "created_at": datetime.now().astimezone().isoformat(),
        "source": source_summary,
        "backup": backup_summary,
        "backup_verified": True,
        "storage_preflight": storage,
        "restore_drill": {
            "performed": restore_drill,
            "verified": bool(restore_drill),
            "temporary_database_removed": restore_removed,
            "summary": restore_summary,
        },
        "source_database_modified": False,
        "existing_backup_overwritten": False,
    }


def write_result(path: Path, payload: dict[str, Any]) -> None:
    path = path.resolve()
    if path.exists():
        raise FileExistsError(f"Result path already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a non-overwriting SQLite backup and verify its integrity."
        )
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
        help="Source SQLite database opened read-only (default: stock_data.db)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New backup database path; an existing file is never overwritten",
    )
    parser.add_argument(
        "--result",
        type=Path,
        help="Optional new JSON path for durable verification evidence",
    )
    parser.add_argument(
        "--restore-drill",
        action="store_true",
        help="Restore the backup to a temporary database and verify it",
    )
    return parser.parse_args()


def render_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def main() -> int:
    args = parse_args()
    try:
        validate_result_path(args.source, args.output, args.result)
        result = create_verified_backup(
            args.source,
            args.output,
            restore_drill=args.restore_drill,
        )
        if args.result:
            write_result(args.result, result)
    except Exception as error:
        print(
            render_json(
                {
                    "status": "failed",
                    "error": f"{type(error).__name__}: {error}",
                }
            )
        )
        return 1

    print(render_json({"status": "passed", **result}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Plan or apply tightly scoped retention for verified database backups."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANAGED_DATABASE_RE = re.compile(
    r"^stock_data-(?P<stamp>\d{8}-\d{6})\.db$"
)
MANAGED_RESULT_RE = re.compile(
    r"^stock_data-(?P<stamp>\d{8}-\d{6})\.verification\.json$"
)
DEFAULT_RETAIN_COUNT = 8
DEFAULT_MINIMUM_COUNT = 1
DEFAULT_MAX_TOTAL_BYTES = 20 * 1024**3


def ensure_safe_directory(directory: Path) -> Path:
    resolved = directory.resolve()
    if not resolved.is_dir():
        raise NotADirectoryError(f"Backup directory not found: {resolved}")
    if resolved == Path(resolved.anchor):
        raise ValueError("Refusing to manage a filesystem root")
    if resolved == PROJECT_ROOT or resolved.is_relative_to(PROJECT_ROOT):
        raise ValueError("Backup retention directory must be outside the repository")
    return resolved


def paired_result_path(database_path: Path) -> Path:
    return database_path.with_name(
        f"{database_path.stem}.verification.json"
    )


def load_verified_pair(database_path: Path) -> dict[str, Any]:
    result_path = paired_result_path(database_path)
    if database_path.is_symlink() or result_path.is_symlink():
        raise ValueError("symbolic links are not managed")
    if not result_path.is_file():
        raise FileNotFoundError(f"verification JSON is missing: {result_path.name}")
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"verification JSON is unreadable: {error}") from error

    backup = payload.get("backup")
    restore = payload.get("restore_drill")
    required = {
        "backup_verified": payload.get("backup_verified") is True,
        "source_database_modified": (
            payload.get("source_database_modified") is False
        ),
        "existing_backup_overwritten": (
            payload.get("existing_backup_overwritten") is False
        ),
        "backup_summary": isinstance(backup, dict),
        "restore_summary": isinstance(restore, dict),
    }
    if isinstance(backup, dict):
        try:
            recorded_path_matches = (
                Path(str(backup.get("path"))).resolve()
                == database_path.resolve()
            )
        except (OSError, TypeError, ValueError):
            recorded_path_matches = False
        required.update(
            {
                "recorded_path": recorded_path_matches,
                "recorded_size": (
                    backup.get("size_bytes") == database_path.stat().st_size
                ),
                "quick_check": backup.get("quick_check") == "ok",
                "schema_sha256": bool(backup.get("schema_sha256")),
                "table_row_counts": isinstance(
                    backup.get("table_row_counts"), dict
                ),
            }
        )
    if isinstance(restore, dict):
        required.update(
            {
                "restore_performed": restore.get("performed") is True,
                "restore_verified": restore.get("verified") is True,
                "restore_removed": (
                    restore.get("temporary_database_removed") is True
                ),
            }
        )
    failed = [name for name, passed in required.items() if not passed]
    if failed:
        raise ValueError(
            "verification record is not eligible for automatic retention: "
            + ", ".join(failed)
        )

    match = MANAGED_DATABASE_RE.fullmatch(database_path.name)
    if match is None:
        raise ValueError("database name is outside the managed naming convention")
    stamp = datetime.strptime(match.group("stamp"), "%Y%m%d-%H%M%S")
    return {
        "stamp": stamp,
        "database_path": database_path,
        "result_path": result_path,
        "size_bytes": database_path.stat().st_size + result_path.stat().st_size,
    }


def directory_file_bytes(directory: Path) -> int:
    return sum(
        path.stat().st_size
        for path in directory.iterdir()
        if path.is_file() and not path.is_symlink()
    )


def discover_backups(directory: Path) -> dict[str, Any]:
    directory = ensure_safe_directory(directory)
    managed: list[dict[str, Any]] = []
    protected: list[dict[str, Any]] = []
    recognized_databases: set[Path] = set()
    recognized_results: set[Path] = set()

    for database_path in sorted(directory.iterdir(), key=lambda path: path.name):
        if not database_path.is_file() or database_path.is_symlink():
            continue
        if MANAGED_DATABASE_RE.fullmatch(database_path.name) is None:
            continue
        recognized_databases.add(database_path)
        result_path = paired_result_path(database_path)
        recognized_results.add(result_path)
        try:
            managed.append(load_verified_pair(database_path))
        except Exception as error:
            protected.append(
                {
                    "path": str(database_path),
                    "reason": f"{type(error).__name__}: {error}",
                }
            )

    managed_database_paths = {item["database_path"] for item in managed}
    for path in sorted(directory.iterdir(), key=lambda candidate: candidate.name):
        if (
            path in managed_database_paths
            or path in recognized_databases
            or path in recognized_results
        ):
            continue
        if path.is_file() and MANAGED_RESULT_RE.fullmatch(path.name):
            protected.append(
                {
                    "path": str(path),
                    "reason": "verification JSON has no eligible managed database",
                }
            )
        else:
            protected.append(
                {
                    "path": str(path),
                    "reason": "outside the managed verified-pair convention",
                }
            )

    managed.sort(key=lambda item: (item["stamp"], item["database_path"].name))
    return {
        "directory": directory,
        "managed": managed,
        "protected": protected,
        "directory_total_bytes": directory_file_bytes(directory),
    }


def plan_retention(
    directory: Path,
    retain_count: int = DEFAULT_RETAIN_COUNT,
    minimum_count: int = DEFAULT_MINIMUM_COUNT,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
) -> dict[str, Any]:
    if minimum_count < 1:
        raise ValueError("minimum_count must be at least 1")
    if retain_count < minimum_count:
        raise ValueError("retain_count must be at least minimum_count")
    if max_total_bytes < 1:
        raise ValueError("max_total_bytes must be greater than zero")

    inventory = discover_backups(directory)
    managed = inventory["managed"]
    prune = list(managed[: max(0, len(managed) - retain_count)])
    kept = list(managed[len(prune) :])
    projected_total = inventory["directory_total_bytes"] - sum(
        item["size_bytes"] for item in prune
    )

    while projected_total > max_total_bytes and len(kept) > minimum_count:
        candidate = kept.pop(0)
        prune.append(candidate)
        projected_total -= candidate["size_bytes"]

    return {
        **inventory,
        "retain_count": retain_count,
        "minimum_count": minimum_count,
        "max_total_bytes": max_total_bytes,
        "keep": kept,
        "prune": prune,
        "projected_total_bytes": projected_total,
        "limit_satisfied": projected_total <= max_total_bytes,
    }


def apply_retention(plan: dict[str, Any]) -> list[str]:
    deleted: list[str] = []
    for planned in plan["prune"]:
        database_path = planned["database_path"]
        result_path = planned["result_path"]
        current = load_verified_pair(database_path)
        if (
            current["size_bytes"] != planned["size_bytes"]
            or current["stamp"] != planned["stamp"]
        ):
            raise ValueError(
                f"Backup pair changed after planning: {database_path.name}"
            )
        database_path.unlink()
        deleted.append(str(database_path))
        result_path.unlink()
        deleted.append(str(result_path))
    return deleted


def serialize_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "stamp": item["stamp"].isoformat(),
        "database_path": str(item["database_path"]),
        "result_path": str(item["result_path"]),
        "size_bytes": item["size_bytes"],
    }


def render_plan(plan: dict[str, Any], applied: bool, deleted: list[str]) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "status": "passed" if plan["limit_satisfied"] else "limit_exceeded",
        "applied": applied,
        "directory": str(plan["directory"]),
        "policy": {
            "retain_count": plan["retain_count"],
            "minimum_count": plan["minimum_count"],
            "max_total_bytes": plan["max_total_bytes"],
        },
        "directory_total_bytes_before": plan["directory_total_bytes"],
        "projected_total_bytes": plan["projected_total_bytes"],
        "limit_satisfied": plan["limit_satisfied"],
        "managed": [serialize_item(item) for item in plan["managed"]],
        "keep": [serialize_item(item) for item in plan["keep"]],
        "prune": [serialize_item(item) for item in plan["prune"]],
        "protected": plan["protected"],
        "deleted": deleted,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan or apply retention to verified scheduled backups only."
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--retain-count", type=int, default=DEFAULT_RETAIN_COUNT)
    parser.add_argument("--minimum-count", type=int, default=DEFAULT_MINIMUM_COUNT)
    parser.add_argument(
        "--max-total-bytes",
        type=int,
        default=DEFAULT_MAX_TOTAL_BYTES,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete only verified pairs selected by the displayed policy",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        plan = plan_retention(
            args.directory,
            retain_count=args.retain_count,
            minimum_count=args.minimum_count,
            max_total_bytes=args.max_total_bytes,
        )
        deleted = apply_retention(plan) if args.apply else []
        result = render_plan(plan, args.apply, deleted)
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": f"{type(error).__name__}: {error}",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if plan["limit_satisfied"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Move misattributed price rows between security codes with strict guards."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from refresh_listed_info import validate_backup_evidence
from src.settings import DATABASE_PATH


CODE_PATTERN = re.compile(r"^[0-9A-Z]{4,5}$")


def validate_transition(source_code: str, target_code: str) -> tuple[str, str]:
    source = source_code.strip().upper()
    target = target_code.strip().upper()
    if not CODE_PATTERN.fullmatch(source):
        raise ValueError(f"Invalid source code: {source_code!r}")
    if not CODE_PATTERN.fullmatch(target):
        raise ValueError(f"Invalid target code: {target_code!r}")
    if source == target:
        raise ValueError("Source and target codes must differ")
    return source, target


def connect_read_only(database: Path) -> sqlite3.Connection:
    resolved = database.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Database not found: {resolved}")
    connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def rows_fingerprint(rows: list[sqlite3.Row]) -> str:
    payload = [dict(row) for row in rows]
    serialized = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def inspect_transition(
    connection: sqlite3.Connection,
    source_code: str,
    target_code: str,
    cutoff_date: date,
) -> dict[str, Any]:
    source, target = validate_transition(source_code, target_code)
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(prices)")
    }
    if not {"date", "code"}.issubset(columns):
        raise ValueError("prices table lacks date or code")

    source_rows = connection.execute(
        "SELECT * FROM prices WHERE code = ? AND date > ? ORDER BY date",
        (source, cutoff_date.isoformat()),
    ).fetchall()
    conflicts = connection.execute(
        """
        SELECT source.date
        FROM prices source
        JOIN prices target ON target.date = source.date AND target.code = ?
        WHERE source.code = ? AND source.date > ?
        ORDER BY source.date
        """,
        (target, source, cutoff_date.isoformat()),
    ).fetchall()
    target_rows = int(
        connection.execute(
            "SELECT COUNT(*) FROM prices WHERE code = ? AND date > ?",
            (target, cutoff_date.isoformat()),
        ).fetchone()[0]
    )
    return {
        "source_code": source,
        "target_code": target,
        "cutoff_date": cutoff_date.isoformat(),
        "source_rows": len(source_rows),
        "source_min_date": source_rows[0]["date"] if source_rows else None,
        "source_max_date": source_rows[-1]["date"] if source_rows else None,
        "source_rows_sha256": rows_fingerprint(source_rows),
        "target_existing_rows_after_cutoff": target_rows,
        "conflict_count": len(conflicts),
        "conflict_dates": [str(row[0]) for row in conflicts],
    }


def repair_transition(
    database: Path,
    source_code: str,
    target_code: str,
    cutoff_date: date,
    *,
    apply: bool,
    backup_verification: Path | None,
    expected_source_rows: int | None,
    expected_source_sha256: str | None,
) -> dict[str, Any]:
    database = database.resolve()
    with closing(connect_read_only(database)) as connection:
        inspection = inspect_transition(
            connection,
            source_code,
            target_code,
            cutoff_date,
        )
        total_rows_before = int(
            connection.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        )

    backup = None
    verification = None
    if apply:
        if expected_source_rows is None or expected_source_sha256 is None:
            raise ValueError(
                "--expected-source-rows and --expected-source-sha256 are required "
                "with --apply"
            )
        if inspection["source_rows"] != expected_source_rows:
            raise ValueError("Source row count differs from the approved dry-run")
        if inspection["source_rows_sha256"] != expected_source_sha256.lower():
            raise ValueError("Source row fingerprint differs from the approved dry-run")
        if inspection["source_rows"] <= 0:
            raise ValueError("No source rows match the requested transition")
        if inspection["conflict_count"]:
            raise ValueError("Target code already has rows on source dates")
        if backup_verification is None:
            raise ValueError("--backup-verification is required with --apply")
        backup = validate_backup_evidence(database, backup_verification.resolve())

        source = inspection["source_code"]
        target = inspection["target_code"]
        with closing(sqlite3.connect(database)) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                cursor = connection.execute(
                    "UPDATE prices SET code = ? WHERE code = ? AND date > ?",
                    (target, source, cutoff_date.isoformat()),
                )
                if cursor.rowcount != expected_source_rows:
                    raise ValueError(
                        f"Moved {cursor.rowcount} rows; expected {expected_source_rows}"
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

        with closing(connect_read_only(database)) as connection:
            post = inspect_transition(
                connection,
                source,
                target,
                cutoff_date,
            )
            total_rows_after = int(
                connection.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            )
            quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
        expected_target_rows = (
            inspection["target_existing_rows_after_cutoff"] + expected_source_rows
        )
        if (
            post["source_rows"] != 0
            or post["target_existing_rows_after_cutoff"] != expected_target_rows
            or total_rows_after != total_rows_before
            or quick_check != ["ok"]
        ):
            raise ValueError("Post-repair verification failed")
        verification = {
            "quick_check": "ok",
            "moved_rows": expected_source_rows,
            "price_rows_before": total_rows_before,
            "price_rows_after": total_rows_after,
            "target_rows_after_cutoff": post["target_existing_rows_after_cutoff"],
        }

    return {
        "schema_version": "1.0",
        "status": "passed",
        "applied": apply,
        "database": str(database),
        "inspection": inspection,
        "backup_verification": backup,
        "database_verification": verification,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE_PATH)
    parser.add_argument("--source-code", required=True)
    parser.add_argument("--target-code", required=True)
    parser.add_argument("--cutoff-date", required=True, type=date.fromisoformat)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-verification", type=Path)
    parser.add_argument("--expected-source-rows", type=int)
    parser.add_argument("--expected-source-sha256")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = repair_transition(
            args.database,
            args.source_code,
            args.target_code,
            args.cutoff_date,
            apply=args.apply,
            backup_verification=args.backup_verification,
            expected_source_rows=args.expected_source_rows,
            expected_source_sha256=args.expected_source_sha256,
        )
    except Exception as error:
        print(
            json.dumps(
                {"status": "failed", "error": f"{type(error).__name__}: {error}"},
                ensure_ascii=True,
                indent=2,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

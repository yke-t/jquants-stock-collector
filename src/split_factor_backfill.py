"""Repair targeted J-Quants price rows and backfill explicit split factors."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.client import JQuantsClient
from src.settings import DATABASE_PATH


@dataclass(frozen=True)
class SplitFactorEvent:
    code: str
    date: str
    source_date: str
    factor: float
    close: Optional[float]
    adjusted_close: Optional[float]


@dataclass(frozen=True)
class PriceRepairRow:
    code: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: Optional[float]
    adjustmentfactor: Optional[float]
    adjustmentopen: Optional[float]
    adjustmenthigh: Optional[float]
    adjustmentlow: Optional[float]
    adjustmentclose: Optional[float]
    adjustmentvolume: Optional[float]


def to_float(value: Any) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def response_rows(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("data", "daily_quotes", "prices"):
        rows = response.get(key)
        if isinstance(rows, list):
            return rows
    return []


def normalize_date(value: Any) -> str:
    text = str(value or "").replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"Invalid date: {value}")
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def parse_event_spec(spec: str) -> tuple[str, str, str]:
    try:
        code, date = spec.split("=", 1)
    except ValueError as exc:
        raise ValueError(f"Expected CODE=YYYY-MM-DD, received: {spec}") from exc
    code = code.strip()
    if not code:
        raise ValueError(f"Missing code in event spec: {spec}")
    date_parts = date.strip().split("@", 1)
    effective_date = normalize_date(date_parts[0])
    source_date = normalize_date(date_parts[1]) if len(date_parts) == 2 else effective_date
    return code, effective_date, source_date


def parse_repair_spec(spec: str) -> tuple[str, str, str]:
    try:
        code, date_range = spec.split("=", 1)
        start_date, end_date = date_range.split(":", 1)
    except ValueError as exc:
        raise ValueError(f"Expected CODE=START_DATE:END_DATE, received: {spec}") from exc
    code = code.strip()
    if not code:
        raise ValueError(f"Missing code in repair spec: {spec}")
    start_date = normalize_date(start_date.strip())
    end_date = normalize_date(end_date.strip())
    if start_date > end_date:
        raise ValueError(f"Repair start date is after end date: {spec}")
    return code, start_date, end_date


def extract_split_factor(
    response: Dict[str, Any],
    expected_code: str,
    source_date: str,
    effective_date: Optional[str] = None,
) -> SplitFactorEvent:
    source_date = normalize_date(source_date)
    effective_date = normalize_date(effective_date or source_date)
    matching = []
    for row in response_rows(response):
        code = str(row.get("Code") or row.get("code") or "")
        raw_date = row.get("Date") or row.get("date")
        try:
            date = normalize_date(raw_date)
        except ValueError:
            continue
        if code == str(expected_code) and date == source_date:
            matching.append(row)

    if len(matching) != 1:
        raise ValueError(
            f"Expected one J-Quants row for {expected_code} on {source_date}, "
            f"found {len(matching)}"
        )

    row = matching[0]
    close = to_float(row.get("C") or row.get("close"))
    adjusted_close = to_float(row.get("AdjC") or row.get("adjustmentclose"))
    api_factor = to_float(row.get("AdjFactor") or row.get("adjustmentfactor"))
    ratio_factor = (
        adjusted_close / close
        if close is not None and close > 0 and adjusted_close is not None
        else None
    )
    non_unit_api_factor = (
        api_factor
        if api_factor is not None and api_factor > 0 and abs(api_factor - 1.0) > 1e-12
        else None
    )
    non_unit_ratio_factor = (
        ratio_factor
        if ratio_factor is not None and ratio_factor > 0 and abs(ratio_factor - 1.0) > 1e-12
        else None
    )
    if (
        non_unit_api_factor is not None
        and non_unit_ratio_factor is not None
        and abs(non_unit_api_factor - non_unit_ratio_factor) > 1e-9
    ):
        raise ValueError(
            f"J-Quants factor mismatch for {expected_code} on {source_date}: "
            f"AdjFactor={non_unit_api_factor}, AdjC/C={non_unit_ratio_factor}"
        )
    factor = non_unit_api_factor or non_unit_ratio_factor
    if factor is None:
        raise ValueError(
            f"J-Quants returned no non-unit adjustment factor for "
            f"{expected_code} on {source_date}: "
            f"AdjFactor={api_factor}, AdjC/C={ratio_factor}"
        )
    return SplitFactorEvent(
        code=str(expected_code),
        date=effective_date,
        source_date=source_date,
        factor=factor,
        close=close,
        adjusted_close=adjusted_close,
    )


def fetch_split_factors(
    client: JQuantsClient,
    event_specs: Iterable[tuple[str, str, str]],
    repair_rows: Iterable[PriceRepairRow] = (),
) -> List[SplitFactorEvent]:
    repair_rows_by_key = {
        (row.code, row.date): row for row in repair_rows
    }
    events = []
    for code, effective_date, source_date in event_specs:
        repair = repair_rows_by_key.get((code, source_date))
        if repair is not None:
            response = {"data": [{
                "Code": repair.code,
                "Date": repair.date,
                "C": repair.close,
                "AdjC": repair.adjustmentclose,
                "AdjFactor": repair.adjustmentfactor,
            }]}
        else:
            response = client.get_daily_quotes(code=code, date=source_date)
        events.append(
            extract_split_factor(response, code, source_date, effective_date)
        )
    return events


def extract_price_repair_rows(
    response: Dict[str, Any],
    expected_code: str,
    expected_dates: Iterable[str],
) -> List[PriceRepairRow]:
    expected_dates = {normalize_date(date) for date in expected_dates}
    rows_by_date: Dict[str, Dict[str, Any]] = {}
    for row in response_rows(response):
        code = str(row.get("Code") or row.get("code") or "")
        if code != str(expected_code):
            continue
        try:
            date = normalize_date(row.get("Date") or row.get("date"))
        except ValueError:
            continue
        if date in expected_dates:
            if date in rows_by_date:
                raise ValueError(f"Duplicate J-Quants row for {expected_code} on {date}")
            rows_by_date[date] = row

    if set(rows_by_date) != expected_dates:
        missing = sorted(expected_dates - set(rows_by_date))
        extra = sorted(set(rows_by_date) - expected_dates)
        raise ValueError(
            f"J-Quants repair dates differ for {expected_code}: "
            f"missing={missing}, extra={extra}"
        )

    repairs = []
    for date in sorted(rows_by_date):
        row = rows_by_date[date]
        required = {
            "open": to_float(row.get("O") or row.get("open")),
            "high": to_float(row.get("H") or row.get("high")),
            "low": to_float(row.get("L") or row.get("low")),
            "close": to_float(row.get("C") or row.get("close")),
            "volume": to_float(row.get("Vo") or row.get("volume")),
        }
        missing_required = [name for name, value in required.items() if value is None]
        if missing_required:
            raise ValueError(
                f"Missing J-Quants fields for {expected_code} on {date}: "
                f"{','.join(missing_required)}"
            )
        repairs.append(PriceRepairRow(
            code=str(expected_code),
            date=date,
            open=required["open"],
            high=required["high"],
            low=required["low"],
            close=required["close"],
            volume=required["volume"],
            turnover=to_float(row.get("Va") or row.get("turnover")),
            adjustmentfactor=to_float(
                row.get("AdjFactor") or row.get("adjustmentfactor")
            ),
            adjustmentopen=to_float(row.get("AdjO") or row.get("adjustmentopen")),
            adjustmenthigh=to_float(row.get("AdjH") or row.get("adjustmenthigh")),
            adjustmentlow=to_float(row.get("AdjL") or row.get("adjustmentlow")),
            adjustmentclose=to_float(row.get("AdjC") or row.get("adjustmentclose")),
            adjustmentvolume=to_float(
                row.get("AdjVo") or row.get("adjustmentvolume")
            ),
        ))
    return repairs


def fetch_price_repairs(
    client: JQuantsClient,
    db_path: Path,
    repair_specs: Iterable[tuple[str, str, str]],
) -> List[PriceRepairRow]:
    repairs: List[PriceRepairRow] = []
    uri = db_path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        for code, start_date, end_date in repair_specs:
            expected_dates = [
                row[0]
                for row in connection.execute(
                    """SELECT date FROM prices
                       WHERE code = ? AND date BETWEEN ? AND ?
                       ORDER BY date""",
                    (code, start_date, end_date),
                ).fetchall()
            ]
            if not expected_dates:
                raise ValueError(
                    f"No DB price rows for {code} between {start_date} and {end_date}"
                )
            response = client.get_daily_quotes(
                code=code,
                from_date=start_date,
                to_date=end_date,
            )
            repairs.extend(
                extract_price_repair_rows(response, code, expected_dates)
            )
    return repairs


def verify_backup(db_path: Path, backup_path: Path) -> None:
    db_path = db_path.resolve()
    backup_path = backup_path.resolve()
    if backup_path == db_path:
        raise ValueError("Backup path must differ from the active database")
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup not found: {backup_path}")
    if backup_path.stat().st_size != db_path.stat().st_size:
        raise ValueError("Backup size does not match the active database")

    uri = backup_path.as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        quick_check = connection.execute("PRAGMA quick_check").fetchone()[0]
    if quick_check != "ok":
        raise ValueError(f"Backup SQLite quick_check failed: {quick_check}")


def verify_target_rows_match_backup(
    db_path: Path,
    backup_path: Path,
    events: Iterable[SplitFactorEvent],
    repairs: Iterable[PriceRepairRow],
) -> None:
    event_keys = [(event.code, event.date) for event in events]
    repair_keys = [(repair.code, repair.date) for repair in repairs]
    if len(event_keys) != len(set(event_keys)):
        raise ValueError("Duplicate split-factor target")
    if len(repair_keys) != len(set(repair_keys)):
        raise ValueError("Duplicate price-repair target")

    target_keys = sorted(set(event_keys + repair_keys))
    active_uri = db_path.resolve().as_uri() + "?mode=ro"
    backup_uri = backup_path.resolve().as_uri() + "?mode=ro"
    with (
        closing(sqlite3.connect(active_uri, uri=True)) as active,
        closing(sqlite3.connect(backup_uri, uri=True)) as backup,
    ):
        for code, date in target_keys:
            active_row = active.execute(
                "SELECT * FROM prices WHERE code = ? AND date = ?",
                (code, date),
            ).fetchone()
            backup_row = backup.execute(
                "SELECT * FROM prices WHERE code = ? AND date = ?",
                (code, date),
            ).fetchone()
            if active_row is None or backup_row is None:
                raise ValueError(f"Target row missing for {code} on {date}")
            if active_row != backup_row:
                raise ValueError(
                    f"Target row does not match verified backup for {code} on {date}"
                )


def load_existing_rows(
    db_path: Path,
    events: Iterable[SplitFactorEvent],
) -> Dict[tuple[str, str], tuple[Optional[float], Optional[float]]]:
    rows: Dict[tuple[str, str], tuple[Optional[float], Optional[float]]] = {}
    uri = db_path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        for event in events:
            stored = connection.execute(
                """SELECT close, adjustmentfactor
                   FROM prices WHERE code = ? AND date = ?""",
                (event.code, event.date),
            ).fetchone()
            if stored is None:
                raise ValueError(f"Price row not found for {event.code} on {event.date}")
            rows[(event.code, event.date)] = (stored[0], stored[1])
    return rows


def apply_split_factors(
    db_path: Path,
    backup_path: Path,
    events: Iterable[SplitFactorEvent],
) -> int:
    events = list(events)
    return apply_backfill(db_path, backup_path, events, [])


def apply_backfill(
    db_path: Path,
    backup_path: Path,
    events: Iterable[SplitFactorEvent],
    repairs: Iterable[PriceRepairRow],
) -> int:
    events = list(events)
    repairs = list(repairs)
    verify_backup(db_path, backup_path)
    verify_target_rows_match_backup(db_path, backup_path, events, repairs)
    load_existing_rows(db_path, events)

    connection = sqlite3.connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        updated = 0
        for repair in repairs:
            cursor = connection.execute(
                """UPDATE prices SET
                       open = ?, high = ?, low = ?, close = ?, volume = ?,
                       turnover = ?, adjustmentfactor = ?, adjustmentopen = ?,
                       adjustmenthigh = ?, adjustmentlow = ?, adjustmentclose = ?,
                       adjustmentvolume = ?
                   WHERE code = ? AND date = ?""",
                (
                    repair.open,
                    repair.high,
                    repair.low,
                    repair.close,
                    repair.volume,
                    repair.turnover,
                    repair.adjustmentfactor,
                    repair.adjustmentopen,
                    repair.adjustmenthigh,
                    repair.adjustmentlow,
                    repair.adjustmentclose,
                    repair.adjustmentvolume,
                    repair.code,
                    repair.date,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    f"Expected one repaired row for {repair.code} on {repair.date}, "
                    f"updated {cursor.rowcount}"
                )
            updated += cursor.rowcount
        for event in events:
            cursor = connection.execute(
                """UPDATE prices SET adjustmentfactor = ?
                   WHERE code = ? AND date = ?""",
                (event.factor, event.code, event.date),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    f"Expected one updated row for {event.code} on {event.date}, "
                    f"updated {cursor.rowcount}"
                )
            updated += cursor.rowcount
        connection.commit()
        return updated
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill explicit J-Quants adjustment factors for known split dates"
    )
    parser.add_argument(
        "--event",
        action="append",
        metavar="CODE=EFFECTIVE_DATE[@SOURCE_DATE]",
        help="Known split event and optional J-Quants factor evidence date",
    )
    parser.add_argument(
        "--inspect",
        action="append",
        metavar="CODE=YYYY-MM-DD",
        help="Print raw J-Quants adjustment fields without changing the DB",
    )
    parser.add_argument(
        "--repair",
        action="append",
        metavar="CODE=START_DATE:END_DATE",
        help="Replace an exact DB date window with J-Quants OHLCV fields",
    )
    parser.add_argument("--db", type=Path, default=DATABASE_PATH)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply only the explicitly requested price repairs and split factors",
    )
    parser.add_argument("--backup", type=Path, help="Required verified backup for --apply")
    args = parser.parse_args()

    if not args.event and not args.inspect and not args.repair:
        parser.error("Specify at least one --event, --repair, or --inspect")

    client = JQuantsClient()
    for inspect_spec in args.inspect or []:
        code, date, _ = parse_event_spec(inspect_spec)
        response = client.get_daily_quotes(code=code, date=date)
        rows = response_rows(response)
        if len(rows) != 1:
            raise ValueError(f"Expected one J-Quants row for {code} on {date}")
        row = rows[0]
        close = to_float(row.get("C") or row.get("close"))
        adjusted_close = to_float(row.get("AdjC") or row.get("adjustmentclose"))
        ratio = adjusted_close / close if close and adjusted_close is not None else None
        print(
            f"[QUOTE] code={code} date={date} C={close} AdjC={adjusted_close} "
            f"AdjFactor={row.get('AdjFactor')} AdjC/C={ratio}"
        )

    repair_specs = [parse_repair_spec(spec) for spec in args.repair or []]
    repairs = fetch_price_repairs(client, args.db, repair_specs) if repair_specs else []
    for repair in repairs:
        print(
            f"[REPAIR] code={repair.code} date={repair.date} "
            f"jquants_close={repair.close} jquants_factor={repair.adjustmentfactor}"
        )

    if not args.event and not repairs:
        print("[DRY-RUN] No database rows were changed")
        return 0

    specs = [parse_event_spec(spec) for spec in args.event or []]
    events = fetch_split_factors(client, specs, repairs)
    existing = load_existing_rows(args.db, events)
    for event in events:
        stored_close, stored_factor = existing[(event.code, event.date)]
        print(
            f"[FACTOR] code={event.code} date={event.date} "
            f"source_date={event.source_date} "
            f"api_factor={event.factor:g} api_close={event.close} "
            f"api_adjusted_close={event.adjusted_close} "
            f"db_close={stored_close} db_factor={stored_factor}"
        )

    if not args.apply:
        print("[DRY-RUN] No database rows were changed")
        return 0
    if args.backup is None:
        parser.error("--backup is required with --apply")

    updated = apply_backfill(args.db, args.backup, events, repairs)
    print(f"[DONE] Updated {updated} targeted price row(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# -*- coding: utf-8 -*-
import shutil
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.split_factor_backfill import (
    PriceRepairRow,
    SplitFactorEvent,
    apply_backfill,
    extract_price_repair_rows,
    apply_split_factors,
    extract_split_factor,
    fetch_split_factors,
    parse_event_spec,
)
from src.client import JQuantsClient


class SplitFactorBackfillTest(unittest.TestCase):
    def test_daily_quotes_supports_bounded_date_range(self):
        client = object.__new__(JQuantsClient)
        client.get = Mock(return_value={"data": []})

        client.get_daily_quotes(
            code="20030", from_date="2026-03-23", to_date="2026-03-27"
        )

        client.get.assert_called_once_with(
            "/equities/bars/daily",
            params={
                "code": "20030",
                "from": "2026-03-23",
                "to": "2026-03-27",
            },
        )

    def test_daily_quotes_rejects_date_and_range_combination(self):
        client = object.__new__(JQuantsClient)
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            client.get_daily_quotes(
                date="2026-03-27", from_date="2026-03-23"
            )

    def test_extracts_non_unit_jquants_factor(self):
        event = extract_split_factor(
            {"data": [{
                "Date": "20260323",
                "Code": "20030",
                "C": 1633.0,
                "AdjC": 1633.0,
                "AdjFactor": 0.25,
            }]},
            "20030",
            "2026-03-23",
        )

        self.assertEqual(event.factor, 0.25)
        self.assertEqual(event.close, 1633.0)

    def test_derives_factor_from_adjusted_close_ratio(self):
        event = extract_split_factor(
            {"data": [{
                "Date": "20260331",
                "Code": "20030",
                "C": 7200.0,
                "AdjC": 1800.0,
                "AdjFactor": 1.0,
            }]},
            "20030",
            "2026-03-31",
            "2026-04-01",
        )

        self.assertEqual(event.date, "2026-04-01")
        self.assertEqual(event.source_date, "2026-03-31")
        self.assertEqual(event.factor, 0.25)

    def test_rejects_unit_factor(self):
        with self.assertRaisesRegex(ValueError, "no non-unit adjustment factor"):
            extract_split_factor(
                {"data": [{
                    "Date": "20260323",
                    "Code": "20030",
                    "AdjFactor": 1.0,
                }]},
                "20030",
                "2026-03-23",
            )

    def test_parses_event_spec(self):
        self.assertEqual(
            parse_event_spec("20030=20260323"),
            ("20030", "2026-03-23", "2026-03-23"),
        )

    def test_extracts_exact_price_repair_rows(self):
        repairs = extract_price_repair_rows(
            {"data": [{
                "Date": "20260323",
                "Code": "20030",
                "O": 7120.0,
                "H": 7200.0,
                "L": 7050.0,
                "C": 7060.0,
                "Vo": 30000,
                "Va": 211800000,
                "AdjFactor": 1.0,
                "AdjC": 1765.0,
            }]},
            "20030",
            ["2026-03-23"],
        )

        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0].close, 7060.0)
        self.assertEqual(repairs[0].adjustmentclose, 1765.0)
        self.assertEqual(
            parse_event_spec("20030=2026-04-01@2026-03-31"),
            ("20030", "2026-04-01", "2026-03-31"),
        )

    def test_reuses_repair_evidence_for_factor_without_api_call(self):
        class FailingClient:
            def get_daily_quotes(self, **kwargs):
                raise AssertionError(f"Unexpected API call: {kwargs}")

        repairs = [PriceRepairRow(
            code="20030",
            date="2026-03-27",
            open=7500.0,
            high=7550.0,
            low=7480.0,
            close=7510.0,
            volume=30000.0,
            turnover=225300000.0,
            adjustmentfactor=1.0,
            adjustmentopen=1875.0,
            adjustmenthigh=1887.5,
            adjustmentlow=1870.0,
            adjustmentclose=1877.5,
            adjustmentvolume=120000.0,
        )]

        events = fetch_split_factors(
            FailingClient(),
            [("20030", "2026-03-30", "2026-03-27")],
            repairs,
        )

        self.assertEqual(events[0].factor, 0.25)
        self.assertEqual(events[0].date, "2026-03-30")

    def test_apply_updates_only_target_adjustment_factor(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "prices.db"
            backup_path = Path(temp_dir) / "prices.backup.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("""
                    CREATE TABLE prices (
                        date TEXT,
                        code TEXT,
                        close REAL,
                        adjustmentfactor REAL,
                        PRIMARY KEY (date, code)
                    )
                """)
                connection.executemany(
                    "INSERT INTO prices VALUES (?, ?, ?, ?)",
                    [
                        ("2026-03-23", "20030", 1633.0, None),
                        ("2026-03-24", "20030", 1675.0, None),
                    ],
                )
                connection.commit()
            shutil.copy2(db_path, backup_path)

            updated = apply_split_factors(
                db_path,
                backup_path,
                [SplitFactorEvent(
                    "20030",
                    "2026-03-23",
                    "2026-03-19",
                    0.25,
                    7160.0,
                    1790.0,
                )],
            )

            with closing(sqlite3.connect(db_path)) as connection:
                rows = connection.execute(
                    "SELECT date, close, adjustmentfactor FROM prices ORDER BY date"
                ).fetchall()

        self.assertEqual(updated, 1)
        self.assertEqual(rows[0], ("2026-03-23", 1633.0, 0.25))
        self.assertEqual(rows[1], ("2026-03-24", 1675.0, None))

    def test_apply_backfill_repairs_price_and_factor_in_one_transaction(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "prices.db"
            backup_path = Path(temp_dir) / "prices.backup.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("""
                    CREATE TABLE prices (
                        date TEXT,
                        code TEXT,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        turnover REAL,
                        adjustmentfactor REAL,
                        adjustmentopen REAL,
                        adjustmenthigh REAL,
                        adjustmentlow REAL,
                        adjustmentclose REAL,
                        adjustmentvolume REAL,
                        PRIMARY KEY (date, code)
                    )
                """)
                connection.executemany(
                    "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("2026-03-27", "20030", 1800, 1900, 1750, 1877.5,
                         120000, None, None, None, None, None, None, None),
                        ("2026-03-30", "20030", 1880, 1900, 1850, 1885,
                         100000, None, None, None, None, None, None, None),
                        ("2026-03-31", "20030", 1885, 1910, 1870, 1900,
                         90000, None, None, None, None, None, None, None),
                    ],
                )
                connection.commit()
            shutil.copy2(db_path, backup_path)

            repair = PriceRepairRow(
                "20030", "2026-03-27", 7500, 7550, 7480, 7510, 30000,
                225300000, 1.0, 1875, 1887.5, 1870, 1877.5, 120000,
            )
            event = SplitFactorEvent(
                "20030", "2026-03-30", "2026-03-27", 0.25, 7510, 1877.5,
            )
            updated = apply_backfill(
                db_path, backup_path, [event], [repair]
            )

            with closing(sqlite3.connect(db_path)) as connection:
                rows = connection.execute(
                    """SELECT date, close, adjustmentfactor
                       FROM prices ORDER BY date"""
                ).fetchall()

        self.assertEqual(updated, 2)
        self.assertEqual(rows[0], ("2026-03-27", 7510.0, 1.0))
        self.assertEqual(rows[1], ("2026-03-30", 1885.0, 0.25))
        self.assertEqual(rows[2], ("2026-03-31", 1900.0, None))

    def test_apply_rejects_target_that_changed_after_backup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "prices.db"
            backup_path = Path(temp_dir) / "prices.backup.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("""
                    CREATE TABLE prices (
                        date TEXT,
                        code TEXT,
                        close REAL,
                        adjustmentfactor REAL,
                        PRIMARY KEY (date, code)
                    )
                """)
                connection.execute(
                    "INSERT INTO prices VALUES (?, ?, ?, ?)",
                    ("2026-03-30", "20030", 1885.0, None),
                )
                connection.commit()
            shutil.copy2(db_path, backup_path)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "UPDATE prices SET adjustmentfactor = 1.0"
                )
                connection.commit()

            with self.assertRaisesRegex(
                ValueError, "does not match verified backup"
            ):
                apply_split_factors(
                    db_path,
                    backup_path,
                    [SplitFactorEvent(
                        "20030", "2026-03-30", "2026-03-27",
                        0.25, 7510.0, 1877.5,
                    )],
                )


if __name__ == "__main__":
    unittest.main()

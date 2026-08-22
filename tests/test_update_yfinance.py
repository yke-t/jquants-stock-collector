# -*- coding: utf-8 -*-
import gc
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.update_yfinance import fetch_single_stock, update_database


class UpdateYfinanceTest(unittest.TestCase):
    @patch("src.update_yfinance.yf.Ticker")
    def test_split_ratio_is_stored_as_per_share_adjustment_factor(self, ticker):
        ticker.return_value.history.return_value = pd.DataFrame(
            [{
                "Open": 1800.0,
                "High": 1850.0,
                "Low": 1790.0,
                "Close": 1820.0,
                "Volume": 100000,
                "Stock Splits": 4.0,
            }],
            index=pd.to_datetime(["2026-03-23"]),
        )

        rows = fetch_single_stock("2003.T", "20030", "2026-03-23", "2026-03-24")

        self.assertEqual(rows[0]["adjustmentfactor"], 0.25)
        ticker.return_value.history.assert_called_once_with(
            start="2026-03-23",
            end="2026-03-24",
            auto_adjust=False,
            actions=True,
        )

    def test_upsert_preserves_existing_jquants_adjusted_columns(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "prices.db"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute("""
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
                conn.execute(
                    """INSERT INTO prices
                       (date, code, close, adjustmentfactor, adjustmentclose)
                       VALUES (?, ?, ?, ?, ?)""",
                    ("2026-03-19", "20030", 7160.0, 1.0, 1790.0),
                )
                conn.commit()

            incoming = pd.DataFrame([{
                "date": "2026-03-19",
                "code": "20030",
                "open": 7100.0,
                "high": 7200.0,
                "low": 7000.0,
                "close": 7160.0,
                "volume": 100000,
                "adjustmentfactor": None,
            }])

            self.assertEqual(update_database(incoming, db_path), 1)
            with closing(sqlite3.connect(db_path)) as conn:
                stored = conn.execute(
                    """SELECT adjustmentfactor, adjustmentclose
                       FROM prices WHERE date = ? AND code = ?""",
                    ("2026-03-19", "20030"),
                ).fetchone()
            del incoming
            gc.collect()

        self.assertEqual(stored, (1.0, 1790.0))


if __name__ == "__main__":
    unittest.main()

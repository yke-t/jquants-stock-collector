"""
Monthly rebalance backtest for the long-term dividend strategy.

This is an intentionally simple first pass:
- Select candidates at each month-end using financial data disclosed on or
  before that date.
- Buy on the next trading day after each signal date and hold equal-weight
  positions until the next rebalance execution date.
- Add a rough monthly dividend accrual from the selected dividend yield.
"""

import argparse
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.dividend_scan import (
    MAX_SHARE_PRICE,
    VERDICT_PRIORITY,
    add_price_indicators,
    annotate_share_basis,
    classify_candidate,
)
from src.settings import DATABASE_PATH, REPORTS_DIR as PROJECT_REPORTS_DIR


DB_PATH = DATABASE_PATH
REPORTS_DIR = PROJECT_REPORTS_DIR
INDICATOR_LOOKBACK_DAYS = 420


def load_backtest_data(
    db_path: Path,
    start_date: str,
    end_date: Optional[str] = None,
    indicator_lookback_days: int = INDICATOR_LOOKBACK_DAYS,
) -> Dict[str, pd.DataFrame]:
    price_start = (
        pd.to_datetime(start_date) - pd.Timedelta(days=indicator_lookback_days)
    ).strftime("%Y-%m-%d")
    with closing(sqlite3.connect(db_path)) as conn:
        price_query = """
        SELECT p.date, p.code, p.close, p.volume, p.adjustmentfactor,
               f.coname AS name, f.s33nm, f.mktnm
        FROM prices p
        LEFT JOIN fundamentals f ON p.code = f.code
        WHERE p.date >= ?
        """
        params: List[str] = [price_start]
        if end_date:
            price_query += " AND p.date <= ?"
            params.append(end_date)
        price_query += " ORDER BY p.code, p.date"
        prices = pd.read_sql(price_query, conn, params=params, parse_dates=["date"])

        financials = pd.read_sql(
            "SELECT * FROM dividend_financials ORDER BY code, disclosure_date",
            conn,
            parse_dates=["disclosure_date"],
        )

    if not prices.empty:
        prices["code"] = prices["code"].astype(str)
    if not financials.empty:
        financials["code"] = financials["code"].astype(str)
    return {"prices": prices, "financials": financials}


def month_end_dates(prices: pd.DataFrame) -> List[pd.Timestamp]:
    if prices.empty:
        return []
    dates = prices[["date"]].drop_duplicates().sort_values("date")
    dates["month"] = dates["date"].dt.to_period("M")
    return dates.groupby("month")["date"].max().tolist()


def next_trading_date_after(dates: List[pd.Timestamp], signal_date: pd.Timestamp) -> Optional[pd.Timestamp]:
    for date in dates:
        if date > signal_date:
            return date
    return None


def latest_financials_asof(financials: pd.DataFrame, asof_date: pd.Timestamp) -> pd.DataFrame:
    if financials.empty:
        return financials
    eligible = financials[financials["disclosure_date"] <= asof_date].copy()
    if eligible.empty:
        return eligible
    return (
        eligible.sort_values(["code", "disclosure_date", "period"])
        .drop_duplicates("code", keep="last")
    )


def candidates_asof(prices: pd.DataFrame, financials: pd.DataFrame, asof_date: pd.Timestamp) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame()

    if {"ma75", "ma200", "avg_volume_20"}.issubset(prices.columns):
        priced = prices
    else:
        priced = add_price_indicators(prices[prices["date"] <= asof_date].copy())

    latest = priced[priced["date"] == asof_date].copy()
    latest = latest[
        latest["close"].notna()
        & (latest["close"] <= MAX_SHARE_PRICE)
    ].copy()
    fins = latest_financials_asof(financials, asof_date)
    if fins.empty:
        return pd.DataFrame()

    merged = latest.merge(fins, on="code", how="inner", suffixes=("", "_fin"))
    if merged.empty:
        return merged

    merged = annotate_share_basis(priced, merged, asof_date)
    classified = merged.apply(lambda row: classify_candidate(row), axis=1, result_type="expand")
    result = pd.concat([merged.reset_index(drop=True), classified.reset_index(drop=True)], axis=1)
    result["verdict_priority"] = result["verdict"].map(VERDICT_PRIORITY).fillna(9)
    return result.sort_values(["verdict_priority", "score", "dividend_yield"], ascending=[True, False, False])


def period_return(
    prices: pd.DataFrame,
    selected: pd.DataFrame,
    entry_date: pd.Timestamp,
    exit_date: pd.Timestamp,
    include_dividend: bool = True,
) -> Dict[str, float]:
    if selected.empty:
        return {"return": 0.0, "price_return": 0.0, "dividend_return": 0.0, "holdings": 0}

    price_pairs = prices[
        (prices["code"].isin(selected["code"]))
        & (prices["date"].isin([entry_date, exit_date]))
    ][["date", "code", "close"]]
    pivot = price_pairs.pivot(index="code", columns="date", values="close").dropna()
    if pivot.empty:
        return {"return": 0.0, "price_return": 0.0, "dividend_return": 0.0, "holdings": 0}

    pivot.columns = ["entry", "exit"]
    returns = (pivot["exit"] / pivot["entry"]) - 1.0
    selected_indexed = selected.set_index("code")
    selected_yields = selected_indexed.reindex(pivot.index)["dividend_yield"].fillna(0.0) / 100.0
    dividend_return = (selected_yields / 12.0).mean() if include_dividend else 0.0
    price_return = returns.mean()
    total_return = price_return + dividend_return

    return {
        "return": float(total_return),
        "price_return": float(price_return),
        "dividend_return": float(dividend_return),
        "holdings": int(len(pivot)),
    }


def run_monthly_backtest(
    db_path: Path = DB_PATH,
    start_date: str = "2023-01-01",
    end_date: Optional[str] = None,
    top_n: int = 20,
    include_dividend: bool = True,
) -> pd.DataFrame:
    data = load_backtest_data(db_path, start_date, end_date)
    prices = data["prices"]
    financials = data["financials"]
    if prices.empty or financials.empty:
        return pd.DataFrame()

    prices = add_price_indicators(prices)
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date) if end_date else None
    all_trading_dates = sorted(prices["date"].drop_duplicates().tolist())
    dates = [date for date in month_end_dates(prices) if date >= start_ts]
    if end_ts is not None:
        dates = [date for date in dates if date <= end_ts]
    rows = []
    for signal_date, next_signal_date in zip(dates[:-1], dates[1:]):
        entry_date = next_trading_date_after(all_trading_dates, signal_date)
        exit_date = next_trading_date_after(all_trading_dates, next_signal_date)
        if entry_date is None or exit_date is None:
            continue

        candidates = candidates_asof(prices, financials, signal_date)
        selected = candidates[candidates["verdict"] == "BUY_ZONE"].head(top_n)
        if selected.empty:
            selected = candidates[candidates["verdict"] == "WATCH"].head(top_n)

        perf = period_return(prices, selected, entry_date, exit_date, include_dividend=include_dividend)
        rows.append({
            "signal_date": signal_date.strftime("%Y-%m-%d"),
            "next_signal_date": next_signal_date.strftime("%Y-%m-%d"),
            "entry_date": entry_date.strftime("%Y-%m-%d"),
            "exit_date": exit_date.strftime("%Y-%m-%d"),
            "holdings": perf["holdings"],
            "return": perf["return"],
            "price_return": perf["price_return"],
            "dividend_return": perf["dividend_return"],
        })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result["equity"] = (1.0 + result["return"]).cumprod()
    result["rolling_max"] = result["equity"].cummax()
    result["drawdown"] = result["equity"] / result["rolling_max"] - 1.0
    return result


def summarize_backtest(results: pd.DataFrame) -> Dict[str, float]:
    if results.empty:
        return {}
    months = len(results)
    total_return = results["equity"].iloc[-1] - 1.0
    annual_return = (results["equity"].iloc[-1] ** (12 / months) - 1.0) if months > 0 else 0.0
    max_drawdown = results["drawdown"].min()
    win_rate = (results["return"] > 0).mean()
    avg_holdings = results["holdings"].mean()
    return {
        "months": months,
        "total_return": float(total_return),
        "annual_return": float(annual_return),
        "max_drawdown": float(max_drawdown),
        "win_rate": float(win_rate),
        "avg_holdings": float(avg_holdings),
    }


def save_backtest_report(results: pd.DataFrame, output_dir: Path = REPORTS_DIR) -> Optional[Path]:
    if results.empty:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "dividend_backtest_monthly.csv"
    results.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Run monthly dividend strategy backtest")
    parser.add_argument("--db", default=str(DB_PATH), help="Database file path")
    parser.add_argument("--start", default="2023-01-01", help="Start date")
    parser.add_argument("--end", default=None, help="End date")
    parser.add_argument("--top-n", type=int, default=20, help="Number of holdings")
    parser.add_argument("--no-dividend", action="store_true", help="Exclude rough dividend accrual")
    parser.add_argument("--no-save", action="store_true", help="Do not save CSV report")
    args = parser.parse_args()

    results = run_monthly_backtest(
        db_path=Path(args.db),
        start_date=args.start,
        end_date=args.end,
        top_n=args.top_n,
        include_dividend=not args.no_dividend,
    )
    if results.empty:
        print("[INFO] No backtest results. Check dividend_financials coverage.")
        return

    summary = summarize_backtest(results)
    print("Dividend Monthly Backtest")
    print(f"Months:        {summary['months']}")
    print(f"Total Return:  {summary['total_return']:.2%}")
    print(f"Annual Return: {summary['annual_return']:.2%}")
    print(f"Max Drawdown:  {summary['max_drawdown']:.2%}")
    print(f"Win Rate:      {summary['win_rate']:.2%}")
    print(f"Avg Holdings:  {summary['avg_holdings']:.1f}")

    if not args.no_save:
        path = save_backtest_report(results)
        if path:
            print(f"[REPORT] Saved {path}")


if __name__ == "__main__":
    main()

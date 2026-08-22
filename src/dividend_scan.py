"""
Long-term dividend candidate scanner.

The scanner is separate from the short-term dip strategy. It builds a watchlist
from dividend yield, payout sustainability, balance-sheet strength, liquidity,
and price location.
"""

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import StockDatabase
from src.settings import DATABASE_PATH, REPORTS_DIR as PROJECT_REPORTS_DIR


DB_PATH = DATABASE_PATH
REPORTS_DIR = PROJECT_REPORTS_DIR

MIN_DIVIDEND_YIELD = 0.035
HIGH_YIELD_RISK = 0.070
MAX_PAYOUT_RATIO = 0.70
EXTREME_PAYOUT_RATIO = 1.00
MIN_EQUITY_RATIO = 0.30
MIN_AVG_VOLUME_20 = 10_000
MAX_SHARE_PRICE = 10_000
PRICE_TO_MA75_MAX = 1.05
DEEP_DOWNTREND_TO_MA200 = 0.90
SHARE_BASIS_GAP_LOW = 0.70
SHARE_BASIS_GAP_HIGH = 1.40

VERDICT_PRIORITY = {
    "BUY_ZONE": 0,
    "WATCH": 1,
    "AVOID": 2,
    "DATA_WARNING": 3,
    "DATA_MISSING": 4,
}


@dataclass
class DividendThresholds:
    min_dividend_yield: float = MIN_DIVIDEND_YIELD
    high_yield_risk: float = HIGH_YIELD_RISK
    max_payout_ratio: float = MAX_PAYOUT_RATIO
    extreme_payout_ratio: float = EXTREME_PAYOUT_RATIO
    min_equity_ratio: float = MIN_EQUITY_RATIO
    min_avg_volume_20: int = MIN_AVG_VOLUME_20
    max_share_price: float = MAX_SHARE_PRICE
    price_to_ma75_max: float = PRICE_TO_MA75_MAX
    deep_downtrend_to_ma200: float = DEEP_DOWNTREND_TO_MA200


def has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def load_recent_prices(conn: sqlite3.Connection, lookback_days: int = 260) -> pd.DataFrame:
    query = """
    WITH recent_dates AS (
        SELECT date
        FROM (SELECT DISTINCT date FROM prices ORDER BY date DESC LIMIT ?)
    )
    SELECT
        p.date,
        p.code,
        p.close,
        p.volume,
        p.adjustmentfactor,
        f.coname AS name,
        f.s17nm,
        f.s33nm,
        f.scalecat,
        f.mktnm
    FROM prices p
    LEFT JOIN fundamentals f ON p.code = f.code
    WHERE p.date IN (SELECT date FROM recent_dates)
    ORDER BY p.code, p.date
    """
    df = pd.read_sql(query, conn, params=(lookback_days,), parse_dates=["date"])
    if not df.empty:
        df["code"] = df["code"].astype(str)
    return df


def load_latest_financials(conn: sqlite3.Connection) -> pd.DataFrame:
    if not has_table(conn, "dividend_financials"):
        return pd.DataFrame()

    query = """
    SELECT df.*
    FROM dividend_financials df
    JOIN (
        SELECT code, MAX(disclosure_date) AS disclosure_date
        FROM dividend_financials
        GROUP BY code
    ) latest
      ON df.code = latest.code
     AND df.disclosure_date = latest.disclosure_date
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return df

    df["code"] = df["code"].astype(str)
    df = df.sort_values(["code", "disclosure_date", "period"]).drop_duplicates("code", keep="last")
    return df


def add_price_indicators(prices: pd.DataFrame) -> pd.DataFrame:
    prices = prices.copy()
    prices["ma75"] = prices.groupby("code")["close"].transform(lambda s: s.rolling(75).mean())
    prices["ma200"] = prices.groupby("code")["close"].transform(lambda s: s.rolling(200).mean())
    prices["avg_volume_20"] = prices.groupby("code")["volume"].transform(lambda s: s.rolling(20).mean())
    return prices


def annotate_share_basis(
    prices: pd.DataFrame,
    candidates: pd.DataFrame,
    asof_date: pd.Timestamp,
) -> pd.DataFrame:
    """Attach point-in-time per-share normalization metadata.

    J-Quants adjustment factors are event factors. A financial per-share value
    disclosed before an event is multiplied by the product of factors through
    the valuation date. Large price discontinuities without an explicit factor
    are never guessed over; the affected row is marked unverified instead.
    """
    result = candidates.copy()
    if result.empty:
        return result

    result["share_basis_factor"] = 1.0
    result["share_basis_status"] = "VERIFIED"
    result["share_basis_reason"] = "no post-disclosure adjustment event"

    if prices.empty or "disclosure_date" not in result.columns:
        result["share_basis_status"] = "UNVERIFIED"
        result["share_basis_reason"] = "share-basis history unavailable"
        return result

    priced = prices.copy()
    priced["date"] = pd.to_datetime(priced["date"], errors="coerce")
    priced["code"] = priced["code"].astype(str)
    candidate_codes = set(result["code"].astype(str))
    priced = priced[priced["code"].isin(candidate_codes)].copy()
    priced["close"] = pd.to_numeric(priced["close"], errors="coerce")
    if "adjustmentfactor" not in priced.columns:
        priced["adjustmentfactor"] = pd.NA
    priced["adjustmentfactor"] = pd.to_numeric(
        priced["adjustmentfactor"], errors="coerce"
    )
    priced = priced[priced["date"] <= pd.Timestamp(asof_date)].sort_values(
        ["code", "date"]
    )
    priced["previous_close"] = priced.groupby("code")["close"].shift(1)
    priced["close_ratio"] = priced["close"] / priced["previous_close"]
    prices_by_code = {
        code: group for code, group in priced.groupby("code", sort=False)
    }

    for index, candidate in result.iterrows():
        disclosure_date = pd.to_datetime(
            candidate.get("disclosure_date"), errors="coerce"
        )
        if pd.isna(disclosure_date):
            result.at[index, "share_basis_reason"] = "no financial row to normalize"
            continue

        code_prices = prices_by_code.get(str(candidate.get("code")))
        if code_prices is None:
            result.at[index, "share_basis_status"] = "UNVERIFIED"
            result.at[index, "share_basis_reason"] = "price history unavailable"
            continue
        window = code_prices[code_prices["date"] > disclosure_date].copy()
        if window.empty:
            continue

        explicit_actions = window[
            window["adjustmentfactor"].notna()
            & (window["adjustmentfactor"] > 0)
            & ((window["adjustmentfactor"] - 1.0).abs() > 1e-12)
        ]
        factor = float(explicit_actions["adjustmentfactor"].prod())
        result.at[index, "share_basis_factor"] = factor

        discontinuities = window[
            (window["close_ratio"] <= SHARE_BASIS_GAP_LOW)
            | (window["close_ratio"] >= SHARE_BASIS_GAP_HIGH)
        ]
        unverified = discontinuities[
            discontinuities["adjustmentfactor"].isna()
            | ((discontinuities["adjustmentfactor"] - 1.0).abs() <= 1e-12)
        ]
        if not unverified.empty:
            dates = ",".join(unverified["date"].dt.strftime("%Y-%m-%d").tolist())
            result.at[index, "share_basis_status"] = "UNVERIFIED"
            result.at[index, "share_basis_reason"] = (
                f"price discontinuity without adjustment factor: {dates}"
            )
        elif not explicit_actions.empty:
            result.at[index, "share_basis_reason"] = (
                f"normalized with {len(explicit_actions)} explicit adjustment factor(s)"
            )

    return result


def coalesce_numeric(row: pd.Series, columns: List[str]) -> Optional[float]:
    for column in columns:
        value = row.get(column)
        if pd.notna(value):
            return float(value)
    return None


def build_candidate_frame(prices: pd.DataFrame, financials: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame()

    priced = add_price_indicators(prices)
    latest_date = priced["date"].max()
    latest_prices = priced[priced["date"] == latest_date].copy()
    latest_prices = latest_prices[
        latest_prices["close"].notna()
        & (latest_prices["close"] <= MAX_SHARE_PRICE)
    ].copy()

    if financials.empty:
        return latest_prices

    merged = latest_prices.merge(financials, on="code", how="left", suffixes=("", "_fin"))
    return annotate_share_basis(priced, merged, latest_date)


def classify_candidate(row: pd.Series, thresholds: DividendThresholds = DividendThresholds()) -> Dict[str, object]:
    close = row.get("close")
    dividend = coalesce_numeric(row, ["forecast_dividend_per_share", "dividend_per_share"])
    eps = coalesce_numeric(row, ["forecast_eps", "eps"])
    share_basis_status = row.get("share_basis_status", "VERIFIED")
    share_basis_factor = row.get("share_basis_factor", 1.0)
    profit = row.get("profit")
    equity = row.get("equity")
    total_assets = row.get("total_assets")
    avg_volume_20 = row.get("avg_volume_20")
    ma75 = row.get("ma75")
    ma200 = row.get("ma200")

    if share_basis_status != "VERIFIED" or pd.isna(share_basis_factor) or float(share_basis_factor) <= 0:
        return {
            "verdict": "DATA_WARNING",
            "reason": str(row.get("share_basis_reason", "share basis unverified")),
            "score": 0.0,
            "dividend_yield": None,
            "payout_ratio": None,
            "equity_ratio": None,
        }

    if dividend is not None:
        dividend *= float(share_basis_factor)
    if eps is not None:
        eps *= float(share_basis_factor)

    missing = []
    for name, value in [
        ("close", close),
        ("dividend", dividend),
        ("eps", eps),
        ("profit", profit),
        ("equity", equity),
        ("total_assets", total_assets),
    ]:
        if pd.isna(value):
            missing.append(name)

    if missing:
        return {
            "verdict": "DATA_MISSING",
            "reason": "missing " + ",".join(missing),
            "score": 0.0,
            "dividend_yield": None,
            "payout_ratio": None,
            "equity_ratio": None,
        }

    dividend_yield = float(dividend) / float(close) if float(close) > 0 else None
    payout_ratio = float(dividend) / float(eps) if float(eps) > 0 else None
    equity_ratio = float(equity) / float(total_assets) if float(total_assets) > 0 else None

    reasons = []
    avoid = False
    watch = False

    if dividend is None or dividend <= 0:
        reasons.append("no dividend")
        avoid = True
    if eps is None or eps <= 0:
        reasons.append("eps<=0")
        avoid = True
    if pd.notna(profit) and float(profit) <= 0:
        reasons.append("profit<=0")
        avoid = True
    if payout_ratio is None:
        reasons.append("payout unavailable")
        avoid = True
    elif payout_ratio > thresholds.extreme_payout_ratio:
        reasons.append("payout>100%")
        avoid = True
    elif payout_ratio > thresholds.max_payout_ratio:
        reasons.append("payout high")
        watch = True

    if equity_ratio is None:
        reasons.append("equity ratio unavailable")
        watch = True
    elif equity_ratio < thresholds.min_equity_ratio:
        reasons.append("equity ratio low")
        watch = True

    if dividend_yield is None or dividend_yield < thresholds.min_dividend_yield:
        reasons.append("yield below target")
        watch = True
    elif dividend_yield > thresholds.high_yield_risk:
        reasons.append("yield too high")
        watch = True

    if pd.notna(avg_volume_20) and avg_volume_20 < thresholds.min_avg_volume_20:
        reasons.append("low liquidity")
        watch = True

    if pd.notna(ma75) and float(ma75) > 0:
        if float(close) > float(ma75) * thresholds.price_to_ma75_max:
            reasons.append("price above MA75")
            watch = True

    if pd.notna(ma200) and float(ma200) > 0:
        if float(close) < float(ma200) * thresholds.deep_downtrend_to_ma200:
            reasons.append("deep downtrend")
            watch = True

    if avoid:
        verdict = "AVOID"
    elif watch:
        verdict = "WATCH"
    else:
        verdict = "BUY_ZONE"
        reasons.append("quality dividend candidate")

    score = 0.0
    if dividend_yield is not None:
        score += dividend_yield * 100
    if payout_ratio is not None:
        score += max(0.0, (thresholds.max_payout_ratio - payout_ratio) * 10)
    if equity_ratio is not None:
        score += equity_ratio * 5
    if pd.notna(avg_volume_20):
        score += min(float(avg_volume_20) / 1_000_000, 2.0)
    if verdict == "AVOID":
        score -= 10
    elif verdict == "DATA_MISSING":
        score -= 20

    return {
        "verdict": verdict,
        "reason": "; ".join(reasons),
        "score": round(score, 3),
        "dividend_yield": round(dividend_yield * 100, 2) if dividend_yield is not None else None,
        "payout_ratio": round(payout_ratio * 100, 1) if payout_ratio is not None else None,
        "equity_ratio": round(equity_ratio * 100, 1) if equity_ratio is not None else None,
    }


def scan_dividend_candidates(db_path: Path = DB_PATH, limit: int = 50) -> pd.DataFrame:
    StockDatabase(str(db_path))

    with sqlite3.connect(db_path) as conn:
        prices = load_recent_prices(conn)
        financials = load_latest_financials(conn)

    candidates = build_candidate_frame(prices, financials)
    if candidates.empty:
        return candidates

    classified = candidates.apply(lambda row: classify_candidate(row), axis=1, result_type="expand")
    result = pd.concat([candidates.reset_index(drop=True), classified.reset_index(drop=True)], axis=1)
    result["verdict_priority"] = result["verdict"].map(VERDICT_PRIORITY).fillna(9)
    result = result.sort_values(["verdict_priority", "score", "dividend_yield"], ascending=[True, False, False])

    output_columns = [
        "date",
        "code",
        "name",
        "close",
        "dividend_yield",
        "payout_ratio",
        "equity_ratio",
        "avg_volume_20",
        "ma75",
        "ma200",
        "verdict",
        "score",
        "reason",
        "share_basis_status",
        "share_basis_factor",
        "share_basis_reason",
        "disclosure_date",
        "period",
        "s17nm",
        "s33nm",
        "scalecat",
        "mktnm",
    ]
    existing_columns = [col for col in output_columns if col in result.columns]
    return result[existing_columns].head(limit)


def apply_dividend_news_risk(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    try:
        from src.news_analyzer import batch_analyze_dividend_risk
    except ImportError:
        return df

    df = df.copy()
    target_df = df[df["verdict"].isin(["BUY_ZONE", "WATCH"])].copy()
    if target_df.empty:
        df["news_risk"] = ""
        df["news_hit"] = ""
        return df

    risk_results = batch_analyze_dividend_risk(target_df.to_dict("records"))
    risk_by_code = {str(item.get("code", "")): item for item in risk_results}

    news_risks = []
    news_hits = []
    updated_reasons = []
    updated_verdicts = []
    for _, row in df.iterrows():
        risk = risk_by_code.get(str(row.get("code", "")), {})
        news_risk = risk.get("risk", "")
        news_hit = risk.get("news_hit", "")
        reason = str(row.get("reason", ""))
        verdict = str(row.get("verdict", ""))

        if news_risk == "HIGH":
            risk_reason = risk.get("reason", "DividendRisk")
            reason = f"{risk_reason}; {reason}" if reason else risk_reason
            if verdict == "BUY_ZONE":
                verdict = "WATCH"

        news_risks.append(news_risk)
        news_hits.append(news_hit)
        updated_reasons.append(reason)
        updated_verdicts.append(verdict)

    df["news_risk"] = news_risks
    df["news_hit"] = news_hits
    df["reason"] = updated_reasons
    df["verdict"] = updated_verdicts
    df["verdict_priority"] = df["verdict"].map(VERDICT_PRIORITY).fillna(9)
    return df.sort_values(["verdict_priority", "score", "dividend_yield"], ascending=[True, False, False])


def save_report(df: pd.DataFrame, output_dir: Path = REPORTS_DIR) -> Optional[Path]:
    if df.empty:
        return None
    output_dir.mkdir(parents=True, exist_ok=True)
    latest_date = pd.to_datetime(df["date"].max()).strftime("%Y-%m-%d")
    path = output_dir / f"dividend_candidates_{latest_date}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Scan long-term dividend candidates")
    parser.add_argument("--db", default=str(DB_PATH), help="Database file path")
    parser.add_argument("--limit", type=int, default=50, help="Maximum rows to print and save")
    parser.add_argument("--no-save", action="store_true", help="Do not save CSV report")
    parser.add_argument("--with-news", action="store_true", help="Apply dividend risk news checks")
    parser.add_argument("--notify", action="store_true", help="Update Google Sheets dividend tab")
    args = parser.parse_args()

    df = scan_dividend_candidates(Path(args.db), limit=args.limit)
    if df.empty:
        print("[INFO] No dividend candidates. Check prices and dividend_financials data.")
        return 1
    if args.with_news:
        df = apply_dividend_news_risk(df)

    print(df.to_string(index=False))
    if not args.no_save:
        path = save_report(df)
        if path:
            print(f"[REPORT] Saved {path}")
    if args.notify:
        try:
            from src.notifier import update_dividend_candidate_sheet
            success = update_dividend_candidate_sheet(df.to_dict("records"))
            print(f"[NOTIFIER] Dividend sheet update: {'OK' if success else 'FAILED'}")
            if not success:
                return 1
        except Exception as e:
            print(f"[NOTIFIER] Dividend sheet update failed: {e}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

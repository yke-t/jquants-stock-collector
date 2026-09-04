"""Read-only, share-basis-aware analysis of stored trading signals.

The script reconstructs RSI from price history, evaluates each signal from the
next trading day's open through the twentieth future close, and excludes price
windows whose share basis cannot be verified. It never writes to SQLite or to
an external service.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = PROJECT_ROOT / "stock_data.db"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "reports" / "signal_analysis"
DEFAULT_START_DATE = "2026-01-26"
DEFAULT_EVAL_DAYS = 20
RSI_PERIOD = 14
GAP_LOW = 0.70
GAP_HIGH = 1.40


@dataclass(frozen=True)
class AnalysisConfig:
    database: Path = DEFAULT_DATABASE
    output_dir: Path = DEFAULT_OUTPUT_DIR
    start_date: str = DEFAULT_START_DATE
    eval_days: int = DEFAULT_EVAL_DAYS


def connect_read_only(database: Path) -> sqlite3.Connection:
    """Open an existing SQLite database with writes disabled."""
    resolved = database.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Database not found: {resolved}")
    return sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)


def calculate_rsi(series: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """Match the simple rolling RSI calculation used by src.scan."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    average_gain = gain.rolling(window=period).mean()
    average_loss = loss.rolling(window=period).mean()
    relative_strength = average_gain / average_loss
    return 100 - (100 / (1 + relative_strength))


def add_share_basis_features(prices: pd.DataFrame) -> pd.DataFrame:
    """Normalize OHLC to one basis using explicit event factors only."""
    priced = prices.copy()
    if priced.empty:
        return priced

    priced["date"] = pd.to_datetime(priced["date"], errors="coerce")
    priced["code"] = priced["code"].astype(str)
    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "adjustmentfactor",
    ]
    for column in numeric_columns:
        priced[column] = pd.to_numeric(priced[column], errors="coerce")
    priced = priced.sort_values(["code", "date"]).reset_index(drop=True)

    priced["event_factor"] = priced["adjustmentfactor"].where(
        priced["adjustmentfactor"].notna()
        & (priced["adjustmentfactor"] > 0)
        & ((priced["adjustmentfactor"] - 1.0).abs() > 1e-12),
        1.0,
    )
    reverse_product = priced.groupby("code", group_keys=False)["event_factor"].apply(
        lambda values: values.iloc[::-1].cumprod().iloc[::-1]
    )
    priced["basis_scale"] = reverse_product.to_numpy() / priced["event_factor"]
    for column in ["open", "high", "low", "close"]:
        priced[f"basis_{column}"] = priced[column] * priced["basis_scale"]

    priced["previous_close"] = priced.groupby("code")["close"].shift(1)
    priced["close_ratio"] = priced["close"] / priced["previous_close"]
    priced["unverified_gap"] = (
        (priced["close_ratio"] <= GAP_LOW) | (priced["close_ratio"] >= GAP_HIGH)
    ) & (priced["event_factor"] == 1.0)
    priced["raw_rsi"] = priced.groupby("code")["close"].transform(calculate_rsi)
    priced["basis_rsi"] = priced.groupby("code")["basis_close"].transform(calculate_rsi)
    return priced


def load_inputs(config: AnalysisConfig) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Load the bounded signal population and matching prices."""
    with connect_read_only(config.database) as connection:
        signals = pd.read_sql_query(
            """
            SELECT signal_date, code, name, signal_price, ma25_rate,
                   stop_loss, take_profit, verdict, reason, news_hit
            FROM signals
            WHERE signal_date >= ?
            ORDER BY signal_date, code
            """,
            connection,
            params=[config.start_date],
        )
        if signals.empty:
            return signals, pd.DataFrame(), ""

        earliest = pd.to_datetime(signals["signal_date"].min()) - timedelta(days=180)
        prices = pd.read_sql_query(
            """
            SELECT p.date, p.code, p.open, p.high, p.low, p.close,
                   p.adjustmentfactor
            FROM prices p
            JOIN (
                SELECT DISTINCT code
                FROM signals
                WHERE signal_date >= ?
            ) selected ON selected.code = p.code
            WHERE p.date >= ?
            ORDER BY p.code, p.date
            """,
            connection,
            params=[config.start_date, earliest.strftime("%Y-%m-%d")],
        )
        price_as_of = connection.execute("SELECT MAX(date) FROM prices").fetchone()[0]

    signals["signal_date"] = pd.to_datetime(signals["signal_date"], errors="coerce")
    signals["code"] = signals["code"].astype(str)
    return signals, prices, str(price_as_of or "")


def _window_has_unverified_gap(window: pd.DataFrame) -> bool:
    return bool(window.get("unverified_gap", pd.Series(dtype=bool)).fillna(False).any())


def evaluate_signals(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    eval_days: int = DEFAULT_EVAL_DAYS,
) -> pd.DataFrame:
    """Evaluate stored signals without changing their historical verdicts."""
    if signals.empty:
        return signals.copy()

    priced = add_share_basis_features(prices)
    prices_by_code = {
        code: group.reset_index(drop=True)
        for code, group in priced.groupby("code", sort=False)
    }
    results: list[dict[str, Any]] = []

    for signal in signals.to_dict("records"):
        code = str(signal["code"])
        signal_date = pd.Timestamp(signal["signal_date"])
        code_prices = prices_by_code.get(code)
        result = dict(signal)
        result.update(
            {
                "eval_complete": False,
                "eval_observations": 0,
                "eval_end_date": pd.NaT,
                "return_pct": np.nan,
                "max_gain_pct": np.nan,
                "max_loss_pct": np.nan,
                "stop_hit": np.nan,
                "take_hit": np.nan,
                "signal_price_sane": False,
                "share_basis_status": "UNVERIFIED",
                "share_basis_reason": "price history unavailable",
                "explicit_events": 0,
                "raw_rsi": np.nan,
                "rsi": np.nan,
                "rsi_basis_status": "UNVERIFIED",
            }
        )
        if code_prices is None or code_prices.empty:
            results.append(result)
            continue

        signal_rows = code_prices[code_prices["date"] <= signal_date]
        if signal_rows.empty:
            result["share_basis_reason"] = "no price on or before signal date"
            results.append(result)
            continue
        signal_row = signal_rows.iloc[-1]
        result["raw_rsi"] = signal_row["raw_rsi"]
        result["rsi"] = signal_row["basis_rsi"]
        rsi_window = signal_rows.tail(RSI_PERIOD + 1)
        if len(rsi_window) >= RSI_PERIOD and not _window_has_unverified_gap(rsi_window):
            result["rsi_basis_status"] = "VERIFIED"

        future = code_prices[code_prices["date"] > signal_date].head(eval_days)
        result["eval_observations"] = len(future)
        if future.empty:
            result["share_basis_reason"] = "no future price observations"
            results.append(result)
            continue

        next_open = float(future.iloc[0]["basis_open"])
        signal_scale = float(signal_row["basis_scale"])
        signal_price = pd.to_numeric(signal.get("signal_price"), errors="coerce")
        scaled_signal_price = signal_price * signal_scale
        ratio = scaled_signal_price / next_open if next_open > 0 else np.nan
        result["signal_price_sane"] = bool(pd.notna(ratio) and 0.2 <= ratio <= 5.0)
        result["signal_price_ratio"] = ratio

        result["explicit_events"] = int((future["event_factor"] != 1.0).sum())
        if _window_has_unverified_gap(future):
            bad_dates = future.loc[future["unverified_gap"], "date"].dt.strftime(
                "%Y-%m-%d"
            )
            result["share_basis_reason"] = (
                "price discontinuity without adjustment factor: " + ",".join(bad_dates)
            )
            results.append(result)
            continue

        result["share_basis_status"] = "VERIFIED"
        result["share_basis_reason"] = (
            "normalized with explicit adjustment factor"
            if result["explicit_events"]
            else "no adjustment event in evaluation window"
        )
        if len(future) < eval_days:
            results.append(result)
            continue

        eval_close = float(future.iloc[-1]["basis_close"])
        result["eval_complete"] = True
        result["eval_end_date"] = future.iloc[-1]["date"]
        result["return_pct"] = (eval_close / next_open - 1.0) * 100
        result["max_gain_pct"] = (float(future["basis_high"].max()) / next_open - 1.0) * 100
        result["max_loss_pct"] = (float(future["basis_low"].min()) / next_open - 1.0) * 100

        stop_loss = pd.to_numeric(signal.get("stop_loss"), errors="coerce")
        take_profit = pd.to_numeric(signal.get("take_profit"), errors="coerce")
        result["stop_hit"] = bool(
            pd.notna(stop_loss)
            and stop_loss > 0
            and (future["basis_low"] <= stop_loss * signal_scale).any()
        )
        result["take_hit"] = bool(
            pd.notna(take_profit)
            and take_profit > 0
            and (future["basis_high"] >= take_profit * signal_scale).any()
        )
        results.append(result)

    evaluated = pd.DataFrame(results)
    evaluated["mature"] = evaluated["eval_observations"] >= eval_days
    evaluated["analysis_eligible"] = (
        evaluated["eval_complete"].fillna(False)
        & evaluated["signal_price_sane"].fillna(False)
        & (evaluated["share_basis_status"] == "VERIFIED")
    )
    evaluated["rsi_eligible"] = (
        evaluated["analysis_eligible"]
        & (evaluated["rsi_basis_status"] == "VERIFIED")
        & evaluated["rsi"].notna()
    )
    evaluated["epoch"] = np.select(
        [
            evaluated["signal_date"] < pd.Timestamp("2026-06-05"),
            evaluated["signal_date"] < pd.Timestamp("2026-08-22"),
        ],
        ["pre_rebound_guard", "rebound_guard_before_news_fix"],
        default="current_news_filter",
    )
    return evaluated


def select_non_overlapping_episodes(evaluated: pd.DataFrame) -> pd.DataFrame:
    """Keep the first signal per code until its evaluation window finishes."""
    if evaluated.empty:
        return evaluated.copy()

    kept_indexes: list[int] = []
    for _, group in evaluated.sort_values(["code", "signal_date"]).groupby("code"):
        blocked_through = pd.NaT
        for index, row in group.iterrows():
            if pd.notna(blocked_through) and row["signal_date"] <= blocked_through:
                continue
            kept_indexes.append(index)
            if pd.notna(row.get("eval_end_date")):
                blocked_through = row["eval_end_date"]
            else:
                blocked_through = row["signal_date"] + timedelta(days=28)
    return evaluated.loc[kept_indexes].sort_values(["signal_date", "code"]).copy()


def summarize_groups(frame: pd.DataFrame, group_columns: Iterable[str]) -> pd.DataFrame:
    """Return decision-oriented return and risk metrics."""
    columns = list(group_columns)
    if frame.empty:
        return pd.DataFrame()
    return (
        frame.groupby(columns, observed=True, dropna=False)
        .agg(
            observations=("return_pct", "size"),
            codes=("code", "nunique"),
            mean_return_pct=("return_pct", "mean"),
            median_return_pct=("return_pct", "median"),
            win_rate=("return_pct", lambda values: (values > 0).mean()),
            q25_return_pct=("return_pct", lambda values: values.quantile(0.25)),
            q75_return_pct=("return_pct", lambda values: values.quantile(0.75)),
            stop_hit_rate=("stop_hit", "mean"),
            take_hit_rate=("take_hit", "mean"),
        )
        .reset_index()
    )


def summarize_population(
    frame: pd.DataFrame, group_columns: Iterable[str]
) -> pd.DataFrame:
    """Describe stored coverage before performance exclusions."""
    columns = list(group_columns)
    if frame.empty:
        return pd.DataFrame()
    return (
        frame.groupby(columns, observed=True, dropna=False)
        .agg(
            signals=("code", "size"),
            signal_dates=("signal_date", "nunique"),
            codes=("code", "nunique"),
            mature=("mature", "sum"),
            eligible=("analysis_eligible", "sum"),
        )
        .reset_index()
    )


def add_rsi_bucket(frame: pd.DataFrame) -> pd.DataFrame:
    bucketed = frame.copy()
    bucketed["rsi_bucket"] = pd.cut(
        bucketed["rsi"],
        bins=[-np.inf, 30, 40, 50, 60, np.inf],
        labels=["<=30", "30-40", "40-50", "50-60", ">60"],
        right=True,
    )
    return bucketed


def summarize_rsi_quintiles(frame: pd.DataFrame) -> pd.DataFrame:
    """Summarize equal-count RSI groups for a stable five-category visual."""
    if frame.empty:
        return pd.DataFrame()

    quantile_count = min(5, len(frame))
    labels = [f"Q{index}" for index in range(1, quantile_count + 1)]
    labels[0] += " (lowest)"
    if quantile_count > 1:
        labels[-1] += " (highest)"
    bucketed = frame.copy()
    bucketed["rsi_quintile"] = pd.qcut(
        bucketed["rsi"].rank(method="first"),
        q=quantile_count,
        labels=labels,
    )
    return (
        bucketed.groupby("rsi_quintile", observed=True)
        .agg(
            observations=("return_pct", "size"),
            codes=("code", "nunique"),
            rsi_min=("rsi", "min"),
            rsi_max=("rsi", "max"),
            rsi_median=("rsi", "median"),
            mean_return_pct=("return_pct", "mean"),
            median_return_pct=("return_pct", "median"),
            win_rate=("return_pct", lambda values: (values > 0).mean()),
            stop_hit_rate=("stop_hit", "mean"),
        )
        .reset_index()
    )


def correlation_summary(frame: pd.DataFrame) -> dict[str, Any]:
    if len(frame) < 3:
        return {"observations": len(frame), "pearson": None, "spearman": None}
    return {
        "observations": len(frame),
        "codes": int(frame["code"].nunique()),
        "pearson": float(frame["rsi"].corr(frame["return_pct"])),
        "spearman": float(
            frame["rsi"].rank(method="average").corr(
                frame["return_pct"].rank(method="average")
            )
        ),
    }


def clustered_bootstrap_difference(
    frame: pd.DataFrame,
    left: str = "ENTRY",
    right: str = "WATCH",
    iterations: int = 5000,
    seed: int = 20260904,
) -> dict[str, Any]:
    """Estimate ENTRY-WATCH differences while resampling whole stock codes."""
    eligible = frame[frame["verdict"].isin([left, right])].copy()
    codes = eligible["code"].drop_duplicates().to_numpy()
    if len(codes) < 2 or eligible["verdict"].nunique() < 2:
        return {"iterations": 0, "reason": "both verdict groups are required"}

    by_code = {code: eligible[eligible["code"] == code] for code in codes}
    random = np.random.default_rng(seed)
    mean_differences: list[float] = []
    win_differences: list[float] = []
    for _ in range(iterations):
        sampled_codes = random.choice(codes, size=len(codes), replace=True)
        sample = pd.concat([by_code[code] for code in sampled_codes], ignore_index=True)
        left_rows = sample[sample["verdict"] == left]
        right_rows = sample[sample["verdict"] == right]
        if left_rows.empty or right_rows.empty:
            continue
        mean_differences.append(
            float(left_rows["return_pct"].mean() - right_rows["return_pct"].mean())
        )
        win_differences.append(
            float((left_rows["return_pct"] > 0).mean() - (right_rows["return_pct"] > 0).mean())
        )

    def interval(values: list[float]) -> list[float] | None:
        if not values:
            return None
        return [float(value) for value in np.quantile(values, [0.025, 0.5, 0.975])]

    return {
        "iterations": len(mean_differences),
        "mean_return_difference_pct_points_ci95": interval(mean_differences),
        "win_rate_difference_ci95": interval(win_differences),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (pd.Timestamp, date)):
        return value.isoformat()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [_json_safe(record) for record in frame.to_dict("records")]


def build_outputs(
    evaluated: pd.DataFrame,
    price_as_of: str,
    config: AnalysisConfig,
) -> dict[str, Any]:
    eligible = evaluated[evaluated["analysis_eligible"]].copy()
    episodes = select_non_overlapping_episodes(eligible)
    actionable = episodes[
        episodes["verdict"].isin(["ENTRY", "WATCH"]) & episodes["rsi_eligible"]
    ].copy()
    rsi_bucket_summary = summarize_groups(add_rsi_bucket(actionable), ["rsi_bucket"])
    rsi_quintile_summary = summarize_rsi_quintiles(actionable)
    verdict_summary = summarize_groups(episodes, ["verdict"])
    epoch_summary = summarize_groups(episodes, ["epoch", "verdict"])
    population_summary = summarize_population(evaluated, ["epoch", "verdict"])
    pre_guard_actionable = actionable[actionable["epoch"] == "pre_rebound_guard"]
    correlations_by_epoch = {
        epoch: correlation_summary(group)
        for epoch, group in actionable.groupby("epoch", observed=True)
    }
    rsi_differences = evaluated.loc[
        evaluated["rsi_eligible"] & evaluated["raw_rsi"].notna(),
        ["raw_rsi", "rsi"],
    ]

    summary = {
        "generated_at": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(),
        "source": {
            "database": config.database.name,
            "tables": ["signals", "prices"],
            "price_as_of": price_as_of,
            "start_date": config.start_date,
        },
        "definitions": {
            "entry": "next trading day's share-basis-normalized open",
            "outcome": f"close after {config.eval_days} future trading observations",
            "win": "return_pct > 0",
            "primary_grain": "first non-overlapping evaluation episode per code",
            "rsi": f"{RSI_PERIOD}-observation simple rolling RSI on explicit-factor-normalized closes",
        },
        "quality": {
            "signals": len(evaluated),
            "signal_dates": int(evaluated["signal_date"].nunique()),
            "codes": int(evaluated["code"].nunique()),
            "complete": int(evaluated["eval_complete"].sum()),
            "mature": int(evaluated["mature"].sum()),
            "analysis_eligible": int(evaluated["analysis_eligible"].sum()),
            "non_overlapping_episodes": len(episodes),
            "unverified_share_basis": int(
                (evaluated["share_basis_status"] != "VERIFIED").sum()
            ),
            "mature_unverified_share_basis": int(
                (
                    evaluated["mature"]
                    & (evaluated["share_basis_status"] != "VERIFIED")
                ).sum()
            ),
            "invalid_signal_price": int((~evaluated["signal_price_sane"]).sum()),
            "rsi_eligible": int(evaluated["rsi_eligible"].sum()),
            "current_policy_complete": int(
                (
                    evaluated["analysis_eligible"]
                    & (evaluated["epoch"] == "current_news_filter")
                ).sum()
            ),
            "current_policy_signals": int(
                (evaluated["epoch"] == "current_news_filter").sum()
            ),
            "rsi_materially_changed_by_adjustment": int(
                ((rsi_differences["raw_rsi"] - rsi_differences["rsi"]).abs() >= 5).sum()
            ),
        },
        "correlation_actionable_episodes": correlation_summary(actionable),
        "correlation_by_epoch": correlations_by_epoch,
        "entry_minus_watch_bootstrap_all_epochs": clustered_bootstrap_difference(actionable),
        "entry_minus_watch_bootstrap_pre_guard": clustered_bootstrap_difference(
            pre_guard_actionable
        ),
        "verdict_summary": frame_records(verdict_summary),
        "epoch_verdict_summary": frame_records(epoch_summary),
        "population_summary": frame_records(population_summary),
        "rsi_bucket_summary": frame_records(rsi_bucket_summary),
        "rsi_quintile_summary": frame_records(rsi_quintile_summary),
    }
    return {
        "summary": _json_safe(summary),
        "evaluated": evaluated,
        "episodes": episodes,
        "verdict_summary": verdict_summary,
        "epoch_summary": epoch_summary,
        "population_summary": population_summary,
        "rsi_bucket_summary": rsi_bucket_summary,
        "rsi_quintile_summary": rsi_quintile_summary,
    }


def write_outputs(outputs: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "analysis_summary.json").write_text(
        json.dumps(outputs["summary"], ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    outputs["evaluated"].to_csv(output_dir / "signal_outcomes.csv", index=False)
    outputs["episodes"].to_csv(output_dir / "non_overlapping_episodes.csv", index=False)
    outputs["verdict_summary"].to_csv(output_dir / "verdict_summary.csv", index=False)
    outputs["epoch_summary"].to_csv(output_dir / "epoch_verdict_summary.csv", index=False)
    outputs["population_summary"].to_csv(output_dir / "population_summary.csv", index=False)
    outputs["rsi_bucket_summary"].to_csv(output_dir / "rsi_bucket_summary.csv", index=False)
    outputs["rsi_quintile_summary"].to_csv(
        output_dir / "rsi_quintile_summary.csv", index=False
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--eval-days", type=int, default=DEFAULT_EVAL_DAYS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.eval_days <= 0:
        raise ValueError("--eval-days must be positive")
    pd.Timestamp(args.start_date)
    config = AnalysisConfig(
        database=args.database,
        output_dir=args.output_dir,
        start_date=args.start_date,
        eval_days=args.eval_days,
    )
    signals, prices, price_as_of = load_inputs(config)
    if signals.empty:
        raise RuntimeError("No signals found for the requested period")
    evaluated = evaluate_signals(signals, prices, config.eval_days)
    outputs = build_outputs(evaluated, price_as_of, config)
    write_outputs(outputs, config.output_dir)
    print(json.dumps(outputs["summary"], ensure_ascii=True, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

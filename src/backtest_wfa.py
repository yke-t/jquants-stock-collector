"""Share-basis-aware, next-session portfolio walk-forward backtest.

Signals are calculated after a session closes and may only execute at the next
available session's open. SQLite is opened read-only. Price histories with an
unexplained large discontinuity are excluded at the code level instead of
guessing a corporate-action factor.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

from src.price_basis import normalize_price_history
from src.settings import DATABASE_PATH, REPORTS_DIR


DEFAULT_START_DATE = "2016-01-01"
DEFAULT_LOOKBACK_DAYS = 400
TARGET_CAGR = 0.15
TARGET_MAX_DRAWDOWN = -0.20
DEFAULT_SCALE_CATEGORIES = (
    "TOPIX Small 1",
    "TOPIX Small 2",
    "TOPIX Mid400",
)


@dataclass(frozen=True)
class StrategyParams:
    dip_threshold: float = 0.97
    stop_loss: float = 0.05
    trailing_stop: float = 0.10
    market_threshold: float = 0.40

    def validate(self) -> None:
        if not 0 < self.dip_threshold <= 1.10:
            raise ValueError("dip_threshold must be in (0, 1.10]")
        if not 0 < self.stop_loss < 1:
            raise ValueError("stop_loss must be in (0, 1)")
        if not 0 < self.trailing_stop < 1:
            raise ValueError("trailing_stop must be in (0, 1)")
        if not 0 <= self.market_threshold <= 1:
            raise ValueError("market_threshold must be in [0, 1]")


@dataclass(frozen=True)
class ExecutionConfig:
    initial_capital: float = 3_000_000.0
    max_positions: int = 20
    lot_size: int = 100
    commission_bps: float = 10.0
    slippage_bps: float = 5.0

    def validate(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if self.max_positions <= 0:
            raise ValueError("max_positions must be positive")
        if self.lot_size <= 0:
            raise ValueError("lot_size must be positive")
        if self.commission_bps < 0 or self.slippage_bps < 0:
            raise ValueError("execution costs cannot be negative")


@dataclass
class Position:
    code: str
    qty: int
    signal_date: pd.Timestamp
    entry_date: pd.Timestamp
    entry_fill: float
    entry_fee: float
    high_water: float
    last_close: float

    @property
    def entry_cost(self) -> float:
        return self.qty * self.entry_fill + self.entry_fee


DEFAULT_PARAM_GRID = (
    StrategyParams(0.98, 0.10, 0.10, 0.40),
    StrategyParams(0.95, 0.10, 0.15, 0.40),
    StrategyParams(1.00, 0.08, 0.10, 0.40),
    StrategyParams(0.97, 0.12, 0.12, 0.40),
)


def connect_read_only(database: Path) -> sqlite3.Connection:
    resolved = database.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Database not found: {resolved}")
    return sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)


def load_price_history(
    database: Path = DATABASE_PATH,
    *,
    start_date: str = DEFAULT_START_DATE,
    end_date: str | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    scale_categories: Sequence[str] | None = DEFAULT_SCALE_CATEGORIES,
) -> pd.DataFrame:
    """Load price history without mutating SQLite or interpolating rows."""
    if lookback_days < 0:
        raise ValueError("lookback_days cannot be negative")
    start = pd.Timestamp(start_date)
    if end_date and pd.Timestamp(end_date) < start:
        raise ValueError("end_date must be on or after start_date")
    history_start = (start - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    clauses = ["p.date >= ?"]
    params: list[Any] = [history_start]
    join_clause = ""
    category_params: list[Any] = []
    if end_date:
        clauses.append("p.date <= ?")
        params.append(pd.Timestamp(end_date).strftime("%Y-%m-%d"))
    if scale_categories:
        placeholders = ",".join("?" for _ in scale_categories)
        join_clause = (
            "JOIN (SELECT DISTINCT code FROM fundamentals "
            f"WHERE scalecat IN ({placeholders})) f ON f.code = p.code"
        )
        category_params.extend(scale_categories)

    query = f"""
        SELECT p.date, p.code, p.open, p.high, p.low, p.close,
               p.volume, p.adjustmentfactor
        FROM prices p
        {join_clause}
        WHERE {' AND '.join(clauses)}
        ORDER BY p.code, p.date
    """
    with closing(connect_read_only(database)) as connection:
        prices = pd.read_sql_query(
            query,
            connection,
            params=category_params + params,
            parse_dates=["date"],
        )
    if not prices.empty:
        prices["code"] = prices["code"].astype(str)
    return prices


def prepare_price_history(
    prices: pd.DataFrame,
    *,
    ma_short: int = 25,
    ma_long: int = 75,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Normalize prices and exclude codes whose share basis is not auditable."""
    if ma_short <= 0 or ma_long <= ma_short:
        raise ValueError("moving-average windows must satisfy 0 < short < long")
    normalized = normalize_price_history(prices)
    if normalized.empty:
        return normalized, {
            "input_rows": 0,
            "input_codes": 0,
            "eligible_rows": 0,
            "eligible_codes": 0,
            "unverified_codes": 0,
            "invalid_ohlc_codes": 0,
            "excluded_codes": 0,
            "explicit_adjustment_events": 0,
        }

    basis_columns = [f"basis_{column}" for column in ("open", "high", "low", "close")]
    finite_positive = normalized[basis_columns].apply(
        lambda column: np.isfinite(column) & (column > 0)
    ).all(axis=1)
    coherent = (
        normalized["basis_high"]
        >= normalized[["basis_open", "basis_close", "basis_low"]].max(axis=1)
    ) & (
        normalized["basis_low"]
        <= normalized[["basis_open", "basis_close", "basis_high"]].min(axis=1)
    )
    invalid_ohlc_codes = set(
        normalized.loc[~(finite_positive & coherent), "code"].astype(str)
    )
    unverified_codes = set(
        normalized.loc[normalized["unverified_gap"], "code"].astype(str)
    )
    excluded_codes = invalid_ohlc_codes | unverified_codes
    eligible = normalized[~normalized["code"].isin(excluded_codes)].copy()
    eligible["ma_short"] = eligible.groupby("code")["basis_close"].transform(
        lambda values: values.rolling(ma_short, min_periods=ma_short).mean()
    )
    eligible["ma_long"] = eligible.groupby("code")["basis_close"].transform(
        lambda values: values.rolling(ma_long, min_periods=ma_long).mean()
    )
    eligible["indicator_ready"] = eligible[["ma_short", "ma_long"]].notna().all(
        axis=1
    )
    eligible["gc_trend"] = eligible["ma_short"] > eligible["ma_long"]
    eligible["is_bullish"] = eligible["basis_close"] > eligible["ma_long"]
    eligible["priority_score"] = eligible["basis_close"] / eligible["ma_short"]
    quality = {
        "input_rows": int(len(normalized)),
        "input_codes": int(normalized["code"].nunique()),
        "eligible_rows": int(len(eligible)),
        "eligible_codes": int(eligible["code"].nunique()),
        "unverified_codes": int(len(unverified_codes)),
        "invalid_ohlc_codes": int(len(invalid_ohlc_codes)),
        "excluded_codes": int(len(excluded_codes)),
        "explicit_adjustment_events": int((normalized["event_factor"] != 1.0).sum()),
    }
    return eligible, quality


def _round_lot_quantity(
    available_cash: float,
    target_value: float,
    fill_price: float,
    commission_rate: float,
    lot_size: int,
) -> int:
    budget = min(available_cash, target_value)
    per_share_cost = fill_price * (1.0 + commission_rate)
    lots = math.floor(budget / (per_share_cost * lot_size))
    return max(0, lots * lot_size)


class PortfolioSimulator:
    """Deterministic long-only simulator using close-to-next-open signals."""

    def __init__(self, prepared_prices: pd.DataFrame, execution: ExecutionConfig):
        execution.validate()
        if prepared_prices.empty:
            raise ValueError("prepared_prices cannot be empty")
        self.execution = execution
        priced = prepared_prices.copy()
        priced["date"] = pd.to_datetime(priced["date"])
        priced["code"] = priced["code"].astype(str)
        if priced.duplicated(["date", "code"]).any():
            raise ValueError("prepared prices contain duplicate date/code rows")
        self.prices = priced.sort_values(["date", "code"]).reset_index(drop=True)
        self.trading_dates = sorted(self.prices["date"].drop_duplicates().tolist())
        self.rows_by_date = {
            date: group.set_index("code", drop=False)
            for date, group in self.prices.groupby("date", sort=True)
        }
        self._signal_cache: dict[
            StrategyParams, dict[pd.Timestamp, tuple[str, ...]]
        ] = {}

    def _candidate_codes_by_date(
        self, params: StrategyParams
    ) -> dict[pd.Timestamp, tuple[str, ...]]:
        cached = self._signal_cache.get(params)
        if cached is not None:
            return cached

        ready = self.prices[self.prices["indicator_ready"]].copy()
        if ready.empty:
            self._signal_cache[params] = {}
            return {}
        sentiment = ready.groupby("date", sort=False)["is_bullish"].transform(
            "mean"
        )
        candidates = ready[
            (sentiment >= params.market_threshold)
            & ready["gc_trend"]
            & (ready["basis_close"] < ready["ma_short"] * params.dip_threshold)
        ].sort_values(["date", "priority_score", "code"], kind="stable")
        result = {
            date: tuple(group["code"].astype(str))
            for date, group in candidates.groupby("date", sort=False)
        }
        self._signal_cache[params] = result
        return result

    def _sell(
        self,
        position: Position,
        *,
        exit_date: pd.Timestamp,
        raw_price: float,
        reason: str,
    ) -> tuple[float, dict[str, Any]]:
        slippage_rate = self.execution.slippage_bps / 10_000.0
        commission_rate = self.execution.commission_bps / 10_000.0
        exit_fill = raw_price * (1.0 - slippage_rate)
        proceeds = position.qty * exit_fill
        exit_fee = proceeds * commission_rate
        net_proceeds = proceeds - exit_fee
        pnl = net_proceeds - position.entry_cost
        trade = {
            "code": position.code,
            "signal_date": position.signal_date,
            "entry_date": position.entry_date,
            "exit_date": exit_date,
            "qty": position.qty,
            "entry_fill": position.entry_fill,
            "exit_fill": exit_fill,
            "entry_fee": position.entry_fee,
            "exit_fee": exit_fee,
            "net_pnl": pnl,
            "net_return": net_proceeds / position.entry_cost - 1.0,
            "reason": reason,
        }
        return net_proceeds, trade

    def run(
        self,
        params: StrategyParams,
        *,
        start_date: pd.Timestamp | str,
        end_date: pd.Timestamp | str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        params.validate()
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        if end < start:
            raise ValueError("end_date must be on or after start_date")

        dates = [date for date in self.trading_dates if start <= date <= end]
        if not dates:
            return pd.DataFrame(), pd.DataFrame()
        final_date = dates[-1]

        date_indexes = {date: index for index, date in enumerate(self.trading_dates)}
        cash = float(self.execution.initial_capital)
        positions: dict[str, Position] = {}
        equity_rows: list[dict[str, Any]] = []
        trades: list[dict[str, Any]] = []
        commission_rate = self.execution.commission_bps / 10_000.0
        buy_slippage = 1.0 + self.execution.slippage_bps / 10_000.0
        signal_candidates = self._candidate_codes_by_date(params)

        for current_date in dates:
            daily = self.rows_by_date[current_date]
            exited_today: set[str] = set()

            for code, position in list(positions.items()):
                if code not in daily.index:
                    continue
                row = daily.loc[code]
                stop_price = position.entry_fill * (1.0 - params.stop_loss)
                trailing_price = position.high_water * (1.0 - params.trailing_stop)
                trigger = max(stop_price, trailing_price)
                raw_exit: float | None = None
                reason = ""
                if float(row["basis_open"]) <= trigger:
                    raw_exit = float(row["basis_open"])
                    reason = "GapStop"
                elif float(row["basis_low"]) <= trigger:
                    raw_exit = trigger
                    reason = "StopOrTrail"

                if raw_exit is not None:
                    proceeds, trade = self._sell(
                        position,
                        exit_date=current_date,
                        raw_price=raw_exit,
                        reason=reason,
                    )
                    cash += proceeds
                    trades.append(trade)
                    exited_today.add(code)
                    del positions[code]
                else:
                    position.high_water = max(
                        position.high_water, float(row["basis_high"])
                    )
                    position.last_close = float(row["basis_close"])

            current_index = date_indexes[current_date]
            previous_date = (
                self.trading_dates[current_index - 1] if current_index > 0 else None
            )
            if (
                current_date < final_date
                and previous_date is not None
                and len(positions) < self.execution.max_positions
            ):
                excluded = set(positions) | exited_today
                candidates = [
                    code
                    for code in signal_candidates.get(previous_date, ())
                    if code not in excluded and code in daily.index
                ]
                slots = self.execution.max_positions - len(positions)
                open_equity = cash + sum(
                    position.qty
                    * (
                        float(daily.loc[code, "basis_open"])
                        if code in daily.index
                        else position.last_close
                    )
                    for code, position in positions.items()
                )
                target_value = open_equity / self.execution.max_positions
                for code in candidates[:slots]:
                    row = daily.loc[code]
                    raw_open = float(row["basis_open"])
                    entry_fill = raw_open * buy_slippage
                    qty = _round_lot_quantity(
                        cash,
                        target_value,
                        entry_fill,
                        commission_rate,
                        self.execution.lot_size,
                    )
                    if qty <= 0:
                        continue
                    notional = qty * entry_fill
                    entry_fee = notional * commission_rate
                    total_cost = notional + entry_fee
                    if total_cost > cash + 1e-7:
                        raise RuntimeError("cash constraint violated")
                    cash -= total_cost
                    position = Position(
                        code=code,
                        qty=qty,
                        signal_date=previous_date,
                        entry_date=current_date,
                        entry_fill=entry_fill,
                        entry_fee=entry_fee,
                        high_water=raw_open,
                        last_close=float(row["basis_close"]),
                    )

                    same_day_stop = entry_fill * (1.0 - params.stop_loss)
                    if float(row["basis_low"]) <= same_day_stop:
                        proceeds, trade = self._sell(
                            position,
                            exit_date=current_date,
                            raw_price=same_day_stop,
                            reason="EntryDayStop",
                        )
                        cash += proceeds
                        trades.append(trade)
                        exited_today.add(code)
                    else:
                        position.high_water = max(
                            position.high_water, float(row["basis_high"])
                        )
                        positions[code] = position

            if current_date == final_date and positions:
                for code, position in list(positions.items()):
                    raw_exit = (
                        float(daily.loc[code, "basis_close"])
                        if code in daily.index
                        else position.last_close
                    )
                    proceeds, trade = self._sell(
                        position,
                        exit_date=current_date,
                        raw_price=raw_exit,
                        reason="EndOfFold",
                    )
                    cash += proceeds
                    trades.append(trade)
                    del positions[code]

            market_value = 0.0
            for code, position in positions.items():
                if code in daily.index:
                    position.last_close = float(daily.loc[code, "basis_close"])
                market_value += position.qty * position.last_close
            equity_rows.append(
                {
                    "date": current_date,
                    "cash": cash,
                    "market_value": market_value,
                    "equity": cash + market_value,
                    "positions": len(positions),
                }
            )
            if cash < -1e-7 or len(positions) > self.execution.max_positions:
                raise RuntimeError("portfolio constraint violated")

        equity = pd.DataFrame(equity_rows)
        if not equity.empty:
            equity["peak_equity"] = equity["equity"].cummax().clip(
                lower=self.execution.initial_capital
            )
            equity["drawdown"] = equity["equity"] / equity["peak_equity"] - 1.0
        return equity, pd.DataFrame(trades)


def summarize_portfolio(
    equity: pd.DataFrame,
    trades: pd.DataFrame,
    *,
    initial_capital: float,
) -> dict[str, Any]:
    if equity.empty:
        return {}
    final_equity = float(equity.iloc[-1]["equity"])
    first_date = pd.Timestamp(equity.iloc[0]["date"])
    last_date = pd.Timestamp(equity.iloc[-1]["date"])
    years = max((last_date - first_date).days / 365.25, 0.0)
    total_return = final_equity / initial_capital - 1.0
    cagr = (
        (final_equity / initial_capital) ** (1.0 / years) - 1.0
        if years > 0 and final_equity > 0
        else total_return
    )
    returns = equity["equity"].pct_change().dropna()
    volatility = float(returns.std(ddof=1) * math.sqrt(252)) if len(returns) > 1 else 0.0
    annualized_mean = float(returns.mean() * 252) if not returns.empty else 0.0
    sharpe = annualized_mean / volatility if volatility > 0 else None
    return {
        "start_date": first_date.date().isoformat(),
        "end_date": last_date.date().isoformat(),
        "initial_capital": float(initial_capital),
        "final_equity": final_equity,
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": float(equity["drawdown"].min()),
        "annualized_volatility": volatility,
        "sharpe_zero_rate": sharpe,
        "trades": int(len(trades)),
        "win_rate": (
            float((trades["net_return"] > 0).mean()) if not trades.empty else None
        ),
        "average_positions": float(equity["positions"].mean()),
    }


def build_walk_forward_folds(
    dates: Sequence[pd.Timestamp], n_splits: int
) -> list[tuple[list[pd.Timestamp], list[pd.Timestamp]]]:
    if n_splits <= 0:
        raise ValueError("n_splits must be positive")
    ordered = sorted(pd.Timestamp(date) for date in set(dates))
    if len(ordered) < n_splits + 1:
        raise ValueError("not enough dates for requested walk-forward splits")
    blocks = [list(block) for block in np.array_split(ordered, n_splits + 1)]
    folds = []
    for index in range(n_splits):
        train = [date for block in blocks[: index + 1] for date in block]
        test = blocks[index + 1]
        if train and test:
            folds.append((train, test))
    return folds


def _optimization_score(summary: dict[str, Any], minimum_trades: int) -> float:
    if not summary:
        return -math.inf
    trade_penalty = max(0, minimum_trades - int(summary["trades"])) * 0.05
    return float(summary["cagr"] + summary["max_drawdown"] - trade_penalty)


def combine_oos_equity(
    curves: Iterable[pd.DataFrame], initial_capital: float
) -> pd.DataFrame:
    combined: list[pd.DataFrame] = []
    capital = float(initial_capital)
    for curve in curves:
        if curve.empty:
            continue
        current = curve.copy().sort_values("date")
        returns = current["equity"].pct_change()
        returns.iloc[0] = current.iloc[0]["equity"] / initial_capital - 1.0
        current["equity"] = capital * (1.0 + returns).cumprod()
        current["cash"] = np.nan
        current["market_value"] = np.nan
        capital = float(current.iloc[-1]["equity"])
        combined.append(current)
    if not combined:
        return pd.DataFrame()
    result = pd.concat(combined, ignore_index=True).sort_values("date")
    result["peak_equity"] = result["equity"].cummax().clip(lower=initial_capital)
    result["drawdown"] = result["equity"] / result["peak_equity"] - 1.0
    return result


def run_walk_forward(
    prepared_prices: pd.DataFrame,
    *,
    start_date: str,
    end_date: str | None,
    n_splits: int,
    execution: ExecutionConfig,
    param_grid: Sequence[StrategyParams] = DEFAULT_PARAM_GRID,
    minimum_train_trades: int = 5,
) -> dict[str, Any]:
    if not param_grid:
        raise ValueError("param_grid cannot be empty")
    execution.validate()
    simulator = PortfolioSimulator(prepared_prices, execution)
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date) if end_date else max(simulator.trading_dates)
    evaluation_dates = [
        date for date in simulator.trading_dates if start <= date <= end
    ]
    folds = build_walk_forward_folds(evaluation_dates, n_splits)
    fold_rows: list[dict[str, Any]] = []
    oos_curves: list[pd.DataFrame] = []
    oos_trades: list[pd.DataFrame] = []

    for fold_number, (train_dates, test_dates) in enumerate(folds, start=1):
        candidates: list[tuple[float, StrategyParams, dict[str, Any]]] = []
        for params in param_grid:
            train_equity, train_trades = simulator.run(
                params,
                start_date=train_dates[0],
                end_date=train_dates[-1],
            )
            train_summary = summarize_portfolio(
                train_equity,
                train_trades,
                initial_capital=execution.initial_capital,
            )
            candidates.append(
                (
                    _optimization_score(train_summary, minimum_train_trades),
                    params,
                    train_summary,
                )
            )
        _, candidate = max(
            enumerate(candidates),
            key=lambda indexed: (indexed[1][0], -indexed[0]),
        )
        score, best_params, train_summary = candidate
        test_equity, test_trades = simulator.run(
            best_params,
            start_date=test_dates[0],
            end_date=test_dates[-1],
        )
        test_summary = summarize_portfolio(
            test_equity,
            test_trades,
            initial_capital=execution.initial_capital,
        )
        if not test_equity.empty:
            test_equity = test_equity.copy()
            test_equity["fold"] = fold_number
            oos_curves.append(test_equity)
        if not test_trades.empty:
            test_trades = test_trades.copy()
            test_trades["fold"] = fold_number
            oos_trades.append(test_trades)
        fold_rows.append(
            {
                "fold": fold_number,
                "train_start": train_dates[0].date().isoformat(),
                "train_end": train_dates[-1].date().isoformat(),
                "test_start": test_dates[0].date().isoformat(),
                "test_end": test_dates[-1].date().isoformat(),
                **{f"param_{key}": value for key, value in asdict(best_params).items()},
                "optimization_score": score,
                "train_trades": train_summary.get("trades", 0),
                "train_cagr": train_summary.get("cagr"),
                "train_max_drawdown": train_summary.get("max_drawdown"),
                "test_trades": test_summary.get("trades", 0),
                "test_total_return": test_summary.get("total_return"),
                "test_max_drawdown": test_summary.get("max_drawdown"),
                "test_min_cash": (
                    float(test_equity["cash"].min())
                    if not test_equity.empty
                    else None
                ),
                "test_max_positions": (
                    int(test_equity["positions"].max())
                    if not test_equity.empty
                    else 0
                ),
            }
        )

    combined_equity = combine_oos_equity(oos_curves, execution.initial_capital)
    combined_trades = (
        pd.concat(oos_trades, ignore_index=True) if oos_trades else pd.DataFrame()
    )
    summary = summarize_portfolio(
        combined_equity,
        combined_trades,
        initial_capital=execution.initial_capital,
    )
    summary["folds"] = len(folds)
    summary["execution"] = asdict(execution)
    summary["targets"] = {
        "cagr_minimum": TARGET_CAGR,
        "max_drawdown_floor": TARGET_MAX_DRAWDOWN,
        "cagr_passed": bool(summary["cagr"] >= TARGET_CAGR),
        "max_drawdown_passed": bool(
            summary["max_drawdown"] >= TARGET_MAX_DRAWDOWN
        ),
    }
    summary["methodology"] = {
        "signal_time": "session close",
        "entry_time": "next available session open",
        "same_bar_exit_order": "prior-high stop first; new intraday high applies next session",
        "share_basis": "explicit adjustment factors; unverified codes excluded",
        "optimization": "expanding-window train, disjoint next-block test",
        "constraints": "cash >= 0, fixed lot size, maximum positions enforced",
        "universe_limitation": (
            "latest fundamentals.scalecat; historical membership unavailable"
        ),
    }
    return {
        "summary": summary,
        "folds": pd.DataFrame(fold_rows),
        "equity": combined_equity,
        "trades": combined_trades,
    }


def write_results(result: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    result["folds"].to_csv(output_dir / "wfa_folds.csv", index=False)
    result["equity"].to_csv(output_dir / "wfa_equity.csv", index=False)
    result["trades"].to_csv(output_dir / "wfa_trades.csv", index=False)
    (output_dir / "wfa_summary.json").write_text(
        json.dumps(result["summary"], ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DATABASE_PATH)
    parser.add_argument("--start", default=DEFAULT_START_DATE)
    parser.add_argument("--end")
    parser.add_argument("--splits", type=int, default=5)
    parser.add_argument("--initial-capital", type=float, default=3_000_000.0)
    parser.add_argument("--max-positions", type=int, default=20)
    parser.add_argument("--lot-size", type=int, default=100)
    parser.add_argument("--commission-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--output-dir", type=Path, default=REPORTS_DIR / "wfa")
    parser.add_argument("--no-save", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    execution = ExecutionConfig(
        initial_capital=args.initial_capital,
        max_positions=args.max_positions,
        lot_size=args.lot_size,
        commission_bps=args.commission_bps,
        slippage_bps=args.slippage_bps,
    )
    prices = load_price_history(
        args.db,
        start_date=args.start,
        end_date=args.end,
    )
    prepared, quality = prepare_price_history(prices)
    if prepared.empty:
        print(json.dumps({"status": "no_eligible_data", "quality": quality}))
        return 1
    result = run_walk_forward(
        prepared,
        start_date=args.start,
        end_date=args.end,
        n_splits=args.splits,
        execution=execution,
    )
    result["summary"]["data_quality"] = quality
    if not args.no_save:
        write_results(result, args.output_dir)
    print(json.dumps(result["summary"], ensure_ascii=True, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

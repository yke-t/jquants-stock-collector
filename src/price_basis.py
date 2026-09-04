"""Utilities for putting historical OHLC prices on one auditable share basis."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd


DEFAULT_GAP_LOW = 0.70
DEFAULT_GAP_HIGH = 1.40
OHLC_COLUMNS = ("open", "high", "low", "close")


def normalize_price_history(
    prices: pd.DataFrame,
    *,
    gap_low: float = DEFAULT_GAP_LOW,
    gap_high: float = DEFAULT_GAP_HIGH,
    ohlc_columns: Iterable[str] = OHLC_COLUMNS,
) -> pd.DataFrame:
    """Return prices normalized with explicit J-Quants event factors only.

    ``adjustmentfactor`` is treated as an event factor effective on its row.
    Prices before the event are multiplied by the product of later factors so
    every ``basis_*`` field is expressed on the latest share basis in the
    supplied history. Large raw close discontinuities without a valid explicit
    factor are marked ``unverified_gap`` and are never inferred away.
    """
    normalized = prices.copy()
    if normalized.empty:
        return normalized

    required = {"date", "code", "close"}
    missing = required - set(normalized.columns)
    if missing:
        raise ValueError(f"Missing price columns: {', '.join(sorted(missing))}")

    columns = tuple(ohlc_columns)
    missing_ohlc = set(columns) - set(normalized.columns)
    if missing_ohlc:
        raise ValueError(f"Missing OHLC columns: {', '.join(sorted(missing_ohlc))}")

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    invalid_code = normalized["code"].isna() | (
        normalized["code"].astype(str).str.strip() == ""
    )
    if normalized["date"].isna().any() or invalid_code.any():
        raise ValueError("Price history contains an invalid date or code")
    normalized["code"] = normalized["code"].astype(str)
    if normalized.duplicated(["date", "code"]).any():
        raise ValueError("Price history contains duplicate date/code rows")

    for column in (*columns, "adjustmentfactor"):
        if column not in normalized.columns:
            normalized[column] = np.nan
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.sort_values(["code", "date"]).reset_index(drop=True)
    factor_present = normalized["adjustmentfactor"].notna()
    factor_valid = np.isfinite(normalized["adjustmentfactor"]) & (
        normalized["adjustmentfactor"] > 0
    )
    factor_material = (normalized["adjustmentfactor"] - 1.0).abs() > 1e-12
    normalized["invalid_adjustment_factor"] = factor_present & ~factor_valid
    normalized["event_factor"] = normalized["adjustmentfactor"].where(
        factor_present & factor_valid & factor_material,
        1.0,
    )

    reverse_product = normalized.groupby("code", group_keys=False)[
        "event_factor"
    ].transform(lambda values: values.iloc[::-1].cumprod().iloc[::-1])
    normalized["basis_scale"] = reverse_product / normalized["event_factor"]
    for column in columns:
        normalized[f"basis_{column}"] = (
            normalized[column] * normalized["basis_scale"]
        )

    normalized["previous_close"] = normalized.groupby("code")["close"].shift(1)
    normalized["close_ratio"] = (
        normalized["close"] / normalized["previous_close"]
    )
    large_gap = (normalized["close_ratio"] <= gap_low) | (
        normalized["close_ratio"] >= gap_high
    )
    normalized["unverified_gap"] = normalized[
        "invalid_adjustment_factor"
    ] | (large_gap & (normalized["event_factor"] == 1.0))
    return normalized

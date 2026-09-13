"""Read-only evaluation of the frozen P6i forward strategy."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from src.backtest_wfa import (
    DEFAULT_PARAM_GRID,
    ExecutionConfig,
    PortfolioSimulator,
    StrategyParams,
    load_price_history,
    prepare_price_history,
    summarize_portfolio,
)
from src.settings import DATABASE_PATH


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_PATH = PROJECT_ROOT / "config" / "p6i_forward_evaluation.json"
BACKTEST_SOURCE_PATH = PROJECT_ROOT / "src" / "backtest_wfa.py"


def _canonical_json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def manifest_sha256(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json_bytes(manifest)).hexdigest()


def normalized_source_sha256(path: Path) -> str:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _validate_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != 1:
        raise ValueError("manifest schema_version must be 1")
    for key in (
        "protocol_id",
        "selection",
        "execution",
        "forward_window",
        "targets",
        "side_effects",
    ):
        if key not in manifest:
            raise ValueError(f"manifest is missing {key}")

    selection = manifest["selection"]
    selection_start = pd.Timestamp(selection["start_date"])
    selection_end = pd.Timestamp(selection["end_date"])
    max_loaded_date = pd.Timestamp(selection["max_loaded_date"])
    forward = manifest["forward_window"]
    forward_start = pd.Timestamp(forward["start_date"])
    if selection_end < selection_start:
        raise ValueError("selection end_date must not precede start_date")
    if max_loaded_date > selection_end:
        raise ValueError("selection max_loaded_date exceeds the frozen cutoff")
    if forward_start <= selection_end:
        raise ValueError("forward start_date must be after the selection cutoff")

    candidates = selection.get("candidates", [])
    expected_grid = [asdict(params) for params in DEFAULT_PARAM_GRID]
    candidate_grid = [candidate.get("params") for candidate in candidates]
    if candidate_grid != expected_grid:
        raise ValueError("manifest candidate grid differs from DEFAULT_PARAM_GRID")
    if [candidate.get("candidate_index") for candidate in candidates] != list(
        range(1, len(candidates) + 1)
    ):
        raise ValueError("manifest candidate indexes must preserve grid order")
    if any(
        not math.isfinite(float(candidate.get("optimization_score", math.nan)))
        for candidate in candidates
    ):
        raise ValueError("manifest candidate scores must be finite")
    selected_index = selection.get("selected_candidate_index")
    if not isinstance(selected_index, int) or not 1 <= selected_index <= len(candidates):
        raise ValueError("selected_candidate_index is invalid")
    if selection.get("selected_params") != candidates[selected_index - 1]["params"]:
        raise ValueError("selected_params do not match selected_candidate_index")
    expected_selected = max(
        candidates,
        key=lambda candidate: (
            candidate["optimization_score"],
            -candidate["candidate_index"],
        ),
    )
    if selected_index != expected_selected["candidate_index"]:
        raise ValueError("selected candidate does not have the best frozen score")
    StrategyParams(**selection["selected_params"]).validate()

    execution = ExecutionConfig(**manifest["execution"])
    execution.validate()
    if execution.allocation_policy != "fixed-equal-weight":
        raise ValueError("P6i requires the P6f fixed-equal-weight allocation")

    minimum_sessions = forward.get("minimum_sessions")
    milestones = forward.get("interim_milestones")
    if not isinstance(minimum_sessions, int) or minimum_sessions <= 0:
        raise ValueError("minimum_sessions must be a positive integer")
    if (
        not isinstance(milestones, list)
        or milestones != sorted(set(milestones))
        or any(
            not isinstance(value, int) or value <= 0 or value >= minimum_sessions
            for value in milestones
        )
    ):
        raise ValueError("interim_milestones must be unique and below maturity")
    if forward.get("freeze_at_maturity") is not True:
        raise ValueError("P6i requires freeze_at_maturity=true")

    targets = manifest["targets"]
    if not math.isfinite(float(targets["cagr_minimum"])):
        raise ValueError("cagr_minimum must be finite")
    drawdown_floor = float(targets["max_drawdown_floor"])
    if not math.isfinite(drawdown_floor) or not -1 <= drawdown_floor <= 0:
        raise ValueError("max_drawdown_floor must be in [-1, 0]")
    if int(targets["maximum_positions"]) != execution.max_positions:
        raise ValueError("target maximum_positions must match execution")
    if int(targets["lot_size"]) != execution.lot_size:
        raise ValueError("target lot_size must match execution")

    side_effects = manifest["side_effects"]
    if (
        side_effects.get("database_mode") != "read-only"
        or side_effects.get("remote_writes") is not False
        or side_effects.get("broker_orders") is not False
    ):
        raise ValueError("P6i manifest cannot authorize external side effects")


def load_manifest(path: Path = DEFAULT_MANIFEST_PATH) -> tuple[dict[str, Any], str]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    _validate_manifest(manifest)
    return manifest, manifest_sha256(manifest)


def verify_frozen_backtest_source(
    manifest: dict[str, Any],
    source_path: Path = BACKTEST_SOURCE_PATH,
) -> str:
    expected = manifest["selection"]["backtest_source_sha256"]
    actual = normalized_source_sha256(source_path)
    if actual != expected:
        raise RuntimeError(
            "frozen backtest source hash mismatch: "
            f"expected {expected}, found {actual}"
        )
    return actual


def _empty_metrics() -> dict[str, Any]:
    return {
        "start_date": None,
        "end_date": None,
        "initial_capital": None,
        "final_equity": None,
        "total_return": None,
        "cagr": None,
        "max_drawdown": None,
        "annualized_volatility": None,
        "sharpe_zero_rate": None,
        "trades": 0,
        "win_rate": None,
        "average_positions": None,
    }


def evaluate_prepared_forward(
    prepared_prices: pd.DataFrame,
    manifest: dict[str, Any],
    *,
    manifest_digest: str,
    data_quality: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_manifest(manifest)
    execution = ExecutionConfig(**manifest["execution"])
    params = StrategyParams(**manifest["selection"]["selected_params"])
    forward = manifest["forward_window"]
    targets = manifest["targets"]
    start = pd.Timestamp(forward["start_date"])
    minimum_sessions = int(forward["minimum_sessions"])

    available_dates: list[pd.Timestamp] = []
    simulator: PortfolioSimulator | None = None
    if not prepared_prices.empty:
        simulator = PortfolioSimulator(prepared_prices, execution)
        available_dates = [date for date in simulator.trading_dates if date >= start]
    available_sessions = len(available_dates)
    evaluation_dates = available_dates[:minimum_sessions]
    evaluation_sessions = len(evaluation_dates)
    mature = evaluation_sessions >= minimum_sessions

    metrics = _empty_metrics()
    min_cash: float | None = None
    max_positions = 0
    lot_size_violations = 0
    share_basis_warning_trades = 0
    if evaluation_dates and simulator is not None:
        equity, trades = simulator.run(
            params,
            start_date=start,
            end_date=evaluation_dates[-1],
        )
        metrics = summarize_portfolio(
            equity,
            trades,
            initial_capital=execution.initial_capital,
        )
        min_cash = float(equity["cash"].min())
        max_positions = int(equity["positions"].max())
        if not trades.empty:
            lot_size_violations = int(
                (trades["qty"].astype(int) % execution.lot_size != 0).sum()
            )
            warning_codes: set[str] = set()
            if "unverified_gap" in prepared_prices:
                warning_codes = set(
                    prepared_prices.loc[
                        prepared_prices["unverified_gap"].fillna(False), "code"
                    ].astype(str)
                )
            share_basis_warning_trades = int(
                trades["code"].astype(str).isin(warning_codes).sum()
            )

    constraints = {
        "cash_nonnegative": min_cash is None or min_cash >= targets["cash_minimum"],
        "maximum_positions_respected": max_positions <= targets["maximum_positions"],
        "lot_size_respected": lot_size_violations == 0,
        "share_basis_warnings_blocked": share_basis_warning_trades == 0,
        "minimum_cash": min_cash,
        "observed_maximum_positions": max_positions,
        "lot_size_violations": lot_size_violations,
        "share_basis_warning_trades": share_basis_warning_trades,
    }
    constraints_passed = all(
        constraints[key]
        for key in (
            "cash_nonnegative",
            "maximum_positions_respected",
            "lot_size_respected",
            "share_basis_warnings_blocked",
        )
    )
    cagr_passed = (
        bool(metrics["cagr"] >= targets["cagr_minimum"]) if mature else None
    )
    drawdown_passed = (
        bool(metrics["max_drawdown"] >= targets["max_drawdown_floor"])
        if mature
        else None
    )
    final_passed = (
        bool(cagr_passed and drawdown_passed and constraints_passed)
        if mature
        else None
    )
    status = "passed" if final_passed else "failed" if mature else "collecting"

    milestones = [int(value) for value in forward["interim_milestones"]]
    milestones_reached = [value for value in milestones if evaluation_sessions >= value]
    next_milestone = next(
        (value for value in [*milestones, minimum_sessions] if value > evaluation_sessions),
        None,
    )
    observation_end = (
        evaluation_dates[-1].date().isoformat() if evaluation_dates else None
    )
    available_latest = (
        available_dates[-1].date().isoformat() if available_dates else None
    )
    return {
        "protocol_id": manifest["protocol_id"],
        "manifest_sha256": manifest_digest,
        "code_checkpoint": manifest["selection"]["code_checkpoint"],
        "backtest_source_sha256": manifest["selection"][
            "backtest_source_sha256"
        ],
        "status": status,
        "mature": mature,
        "metrics_are_interim": not mature,
        "observation": {
            "start_date": forward["start_date"],
            "end_date": observation_end,
            "available_latest_date": available_latest,
            "available_sessions": available_sessions,
            "evaluation_sessions": evaluation_sessions,
            "remaining_sessions": max(0, minimum_sessions - evaluation_sessions),
            "ignored_sessions_after_maturity": max(
                0, available_sessions - minimum_sessions
            ),
            "milestones_reached": milestones_reached,
            "next_milestone": next_milestone,
        },
        "selected_candidate_index": manifest["selection"][
            "selected_candidate_index"
        ],
        "strategy_params": asdict(params),
        "execution": asdict(execution),
        "metrics": metrics,
        "constraints": constraints,
        "targets": {
            "cagr_minimum": targets["cagr_minimum"],
            "max_drawdown_floor": targets["max_drawdown_floor"],
            "cagr_passed": cagr_passed,
            "max_drawdown_passed": drawdown_passed,
            "constraints_passed": constraints_passed,
            "final_passed": final_passed,
        },
        "data_quality": data_quality or {},
        "methodology": {
            "selection_data_end": manifest["selection"]["end_date"],
            "forward_data_start": forward["start_date"],
            "parameter_reselection": "disabled",
            "maturity_rule": f"{minimum_sessions} trading sessions",
            "interim_target_decisions": "disabled",
            "database_mode": "read-only",
            "remote_writes": False,
            "broker_orders": False,
        },
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DATABASE_PATH)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument(
        "--end",
        help="Optional observation cutoff; cannot precede the forward start date.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional local JSON output. Standard output is always written.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    manifest, digest = load_manifest(args.manifest)
    verify_frozen_backtest_source(manifest)
    forward_start = manifest["forward_window"]["start_date"]
    if args.end and pd.Timestamp(args.end) < pd.Timestamp(forward_start):
        raise ValueError("end cannot precede the forward start date")
    prices = load_price_history(
        args.db,
        start_date=forward_start,
        end_date=args.end,
        lookback_days=int(manifest["selection"]["lookback_days"]),
    )
    prepared, quality = prepare_price_history(prices)
    result = evaluate_prepared_forward(
        prepared,
        manifest,
        manifest_digest=digest,
        data_quality=quality,
    )
    serialized = json.dumps(
        result,
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())

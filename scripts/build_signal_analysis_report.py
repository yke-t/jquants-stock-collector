"""Build a portable technical report from signal-performance audit output.

The script reads only the JSON produced by ``analyze_signal_performance.py``
and writes a canonical Data Analytics artifact plus compact source notes. The
portable HTML is produced separately by the bundled report renderer so its
schema and interactions can be verified.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


TITLE = "J-Quants シグナル判定検証"
SOURCE_ID = "local-signal-audit"


def _find(rows: list[dict[str, Any]], key: str, value: str) -> dict[str, Any]:
    return next(row for row in rows if row[key] == value)


def _percent(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}%"


def build_artifact(summary: dict[str, Any]) -> dict[str, Any]:
    quality = summary["quality"]
    verdict_rows = summary["verdict_summary"]
    entry = _find(verdict_rows, "verdict", "ENTRY")
    watch = _find(verdict_rows, "verdict", "WATCH")
    low_rsi = _find(summary["rsi_bucket_summary"], "rsi_bucket", "<=30")
    bootstrap = summary["entry_minus_watch_bootstrap_pre_guard"]
    bootstrap_ci = bootstrap["mean_return_difference_pct_points_ci95"]
    generated_at = summary["generated_at"]

    source = {
        "id": SOURCE_ID,
        "label": "ローカルSQLite・読み取り専用シグナル監査",
        "path": "analysis_summary.json",
        "query": {
            "engine": "SQLite + pandas",
            "language": "SQL / Python",
            "executed_at": generated_at,
            "tables_used": ["stock_data.db.signals", "stock_data.db.prices"],
            "filters": [
                f"signal_date >= {summary['source']['start_date']}",
                f"価格データ基準日 <= {summary['source']['price_as_of']}",
                "1銘柄内で評価期間が重なる後続シグナルを除外",
                "明示的調整係数で株式単位を検証できない価格窓を除外",
            ],
            "metric_definitions": [
                "20営業日リターン = 20番目の将来終値 / 翌営業日始値 - 1",
                "勝率 = 20営業日リターンが0%超の観測数 / 対象観測数",
                "RSI = 明示的調整係数で基準統一した終値の14観測単純移動RSI",
                "ENTRY-WATCH差の95%区間 = 銘柄単位クラスターブートストラップ5000回",
            ],
            "description": (
                "signalsとpricesを読み取り専用で取得し、株式単位を検証した翌営業日始値から"
                "20営業日後終値までの成績を再計算した。"
            ),
            "sql": (
                "SELECT signal_date, code, name, signal_price, ma25_rate, stop_loss, "
                "take_profit, verdict, reason, news_hit FROM signals "
                "WHERE signal_date >= :start_date ORDER BY signal_date, code;\n"
                "SELECT p.date, p.code, p.open, p.high, p.low, p.close, p.adjustmentfactor "
                "FROM prices p JOIN (SELECT DISTINCT code FROM signals "
                "WHERE signal_date >= :start_date) selected ON selected.code = p.code "
                "WHERE p.date >= :history_start ORDER BY p.code, p.date;"
            ),
        },
    }

    quality_rows = [
        {
            "signals": quality["signals"],
            "eligible": quality["analysis_eligible"],
            "episodes": quality["non_overlapping_episodes"],
            "actionable_episodes": summary["correlation_actionable_episodes"]["observations"],
            "current_policy_signals": quality["current_policy_signals"],
            "current_policy_complete": quality["current_policy_complete"],
        }
    ]

    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": TITLE,
            "description": "RSI、ENTRY/WATCH判定、20営業日後成績の読み取り専用検証",
            "generatedAt": generated_at,
            "sources": [source],
            "cards": [
                {
                    "id": "signals-card",
                    "dataset": "quality",
                    "sourceId": SOURCE_ID,
                    "metrics": [{"label": "保存シグナル", "field": "signals", "format": "number"}],
                },
                {
                    "id": "episodes-card",
                    "dataset": "quality",
                    "sourceId": SOURCE_ID,
                    "metrics": [{"label": "非重複評価", "field": "episodes", "format": "number"}],
                },
                {
                    "id": "current-card",
                    "dataset": "quality",
                    "sourceId": SOURCE_ID,
                    "metrics": [
                        {"label": "現行方針の成熟例", "field": "current_policy_complete", "format": "number"},
                        {"label": "現行方針シグナル", "field": "current_policy_signals", "format": "number"},
                    ],
                },
            ],
            "charts": [
                {
                    "id": "rsi-quintile-chart",
                    "title": "RSI 5分位別の20営業日平均リターン",
                    "subtitle": "低RSI側の2群はプラス、高RSI側の2群はマイナス。ただし対象は59件。",
                    "type": "bar",
                    "dataset": "rsi_quintiles",
                    "sourceId": SOURCE_ID,
                    "encodings": {
                        "x": {"field": "rsi_quintile", "type": "ordinal", "label": "RSI 5分位"},
                        "y": {
                            "field": "mean_return_pct",
                            "type": "quantitative",
                            "label": "平均20営業日リターン",
                            "format": "number",
                            "unit": "%",
                        },
                        "tooltip": [
                            {"field": "observations", "type": "quantitative", "label": "件数"},
                            {"field": "rsi_median", "type": "quantitative", "label": "RSI中央値"},
                            {"field": "win_rate", "type": "quantitative", "label": "勝率", "format": "percent"},
                        ],
                    },
                    "xAxisTitle": "RSI 5分位（低→高）",
                    "yAxisTitle": "平均リターン（%）",
                    "valueFormat": "number",
                    "unit": "%",
                }
            ],
            "tables": [
                {
                    "id": "verdict-table",
                    "title": "保存判定別の非重複20営業日成績",
                    "subtitle": "異なる判定ロジック時期を含む記述統計。因果比較ではない。",
                    "dataset": "verdict_summary",
                    "sourceId": SOURCE_ID,
                    "defaultSort": {"field": "mean_return_pct", "direction": "desc"},
                    "columns": [
                        {"field": "verdict", "label": "判定", "type": "text"},
                        {"field": "observations", "label": "件数", "type": "number"},
                        {"field": "mean_return_pct", "label": "平均", "type": "number", "unit": "%"},
                        {"field": "median_return_pct", "label": "中央値", "type": "number", "unit": "%"},
                        {"field": "win_rate", "label": "勝率", "type": "percent", "format": "percent"},
                        {"field": "stop_hit_rate", "label": "-5%到達率", "type": "percent", "format": "percent"},
                    ],
                }
            ],
            "blocks": [
                {"id": "title", "type": "markdown", "body": f"# {TITLE}"},
                {
                    "id": "technical-summary",
                    "type": "markdown",
                    "sourceId": SOURCE_ID,
                    "body": (
                        "## 技術要約\n"
                        "現時点ではENTRYとWATCHの反転を支持しない。過去ENTRYの平均20営業日リターンは"
                        f"{_percent(entry['mean_return_pct'])}、WATCHは{_percent(watch['mean_return_pct'])}だが、"
                        "ENTRYはすべて反発ガード導入前で、同時期比較の95%区間は0をまたぐ。"
                        f"RSI 30以下は平均{_percent(low_rsi['mean_return_pct'])}・勝率"
                        f"{_percent(low_rsi['win_rate'] * 100, 0)}と相対的に良いが25件のみである。"
                        f"現行ニュース判定後は{quality['current_policy_signals']}件あるものの、20営業日を"
                        "満たした例が0件のため、設定変更は保留する。"
                    ),
                },
                {
                    "id": "metric-strip",
                    "type": "metric-strip",
                    "cardIds": ["signals-card", "episodes-card", "current-card"],
                },
                {
                    "id": "rsi-finding",
                    "type": "markdown",
                    "sourceId": SOURCE_ID,
                    "body": (
                        "## RSIと成績\n"
                        f"ENTRY/WATCHの非重複59件ではRSIと20営業日リターンのSpearman相関は"
                        f"{summary['correlation_actionable_episodes']['spearman']:.2f}。"
                        "低RSIほど成績が良い方向だが、5分位各群は11〜12件にすぎず、探索的な結果として扱う。"
                    ),
                },
                {"id": "rsi-chart", "type": "chart", "chartId": "rsi-quintile-chart"},
                {
                    "id": "verdict-finding",
                    "type": "markdown",
                    "sourceId": SOURCE_ID,
                    "body": (
                        "## ENTRYとWATCH\n"
                        f"全時期を混ぜたENTRY−WATCH平均差の95%区間は"
                        f"{_percent(summary['entry_minus_watch_bootstrap_all_epochs']['mean_return_difference_pct_points_ci95'][0])}〜"
                        f"{_percent(summary['entry_minus_watch_bootstrap_all_epochs']['mean_return_difference_pct_points_ci95'][2])}ポイント。"
                        "ただし制度変更前後が交絡する。同じ反発ガード導入前だけでは"
                        f"{_percent(bootstrap_ci[0])}〜{_percent(bootstrap_ci[2])}ポイントで0をまたぐため、"
                        "判定ラベルを反転する根拠には不足する。"
                    ),
                },
                {"id": "verdict-table-block", "type": "table", "tableId": "verdict-table"},
                {
                    "id": "definitions",
                    "type": "markdown",
                    "sourceId": SOURCE_ID,
                    "body": (
                        "## 対象と指標定義\n"
                        f"対象は{summary['source']['start_date']}以降、価格基準日"
                        f"{summary['source']['price_as_of']}まで。エントリー価格は翌営業日の株式単位調整済み始値、"
                        "結果は20番目の将来終値、勝ちはリターン0%超とした。主分析は同一銘柄で評価期間が"
                        "重ならない最初のシグナルだけを残した。"
                    ),
                },
                {
                    "id": "methodology",
                    "type": "markdown",
                    "sourceId": SOURCE_ID,
                    "body": (
                        "## 分析方法\n"
                        "SQLiteをmode=roで開き、保存判定は改変せず再評価した。OHLCはJ-Quantsの明示的な"
                        "調整係数だけで同一株式単位へ換算し、係数なしで終値が30%以上跳んだ評価窓は"
                        "DATA_WARNING相当として除外した。RSIは同じ基準の終値から再計算した。"
                    ),
                },
                {
                    "id": "limitations",
                    "type": "markdown",
                    "sourceId": SOURCE_ID,
                    "body": (
                        "## 制約と解釈\n"
                        f"全{quality['signals']}件のうち分析対象は{quality['analysis_eligible']}件、"
                        f"主分析は{quality['non_overlapping_episodes']}件。成熟済みでも株式単位を"
                        f"確認できない窓が{quality['mature_unverified_share_basis']}件あり除外した。"
                        f"保存価格の整合確認に通らないシグナルも{quality['invalid_signal_price']}件除外した。"
                        "ニュース判定、反発ガード、市況が時期とともに変わるため、判定間の差を因果効果とは解釈できない。"
                        "既存のポートフォリオ・バックテストは同日終値約定、未調整価格、資金制約などの"
                        "方法論を是正するまで意思決定用に使わない。"
                    ),
                },
                {
                    "id": "recommendations",
                    "type": "markdown",
                    "sourceId": SOURCE_ID,
                    "body": (
                        "## 推奨\n"
                        "1. 現行の反発ガードとENTRY/WATCH判定を維持する。\n"
                        "2. RSI 30以下を候補フラグとして記録し、即時の除外条件にはしない。\n"
                        "3. 現行ニュース判定後の各群で20営業日成熟例が30件以上になるまで再評価を待つ。\n"
                        "4. -5%到達率がENTRY 86.7%、WATCH 81.6%と高いため、次回は約定順序と"
                        "資金制約を備えたウォークフォワード検証を独立実装する。"
                    ),
                },
                {
                    "id": "questions",
                    "type": "markdown",
                    "body": (
                        "## 次回確認\n"
                        "現行方針の成熟例が蓄積した時点で、RSI閾値の事前固定、同一時期内の判定比較、"
                        "手数料・スリッページ込みポートフォリオ評価を再実行する。"
                    ),
                },
            ],
        },
        "snapshot": {
            "version": 1,
            "generatedAt": generated_at,
            "status": "ready",
            "datasets": {
                "quality": quality_rows,
                "rsi_quintiles": summary["rsi_quintile_summary"],
                "verdict_summary": verdict_rows,
            },
            "accessIssues": [],
        },
        "sources": [source],
        "package_info": {
            "generator": "scripts/build_signal_analysis_report.py",
            "analysis": "scripts/analyze_signal_performance.py",
        },
    }
    return artifact


def build_source_notes(summary: dict[str, Any]) -> str:
    return f"""# Signal analysis source notes

- Report spine: technical summary → data-quality metrics → RSI finding/chart → verdict finding/table → definitions → methodology → limitations → recommendations → next check.
- Chart map: `rsi-quintile-chart` uses `rsi_quintiles`; x = ordered RSI quintile, y = mean 20-trading-day return, tooltips = n/median RSI/win rate.
- Primary source: local `stock_data.db`, opened with SQLite URI `mode=ro`; tables `signals` and `prices`; price as of {summary['source']['price_as_of']}.
- Reproducible analysis: `python scripts/analyze_signal_performance.py --output-dir <directory>`.
- Notebook gap: `nbformat`, `nbclient`, and a Jupyter runtime were unavailable in the verified environment. The executed CLI and its CSV/JSON outputs are the reproducible audit trail instead.
- Existing portfolio backtest is excluded from decision-grade evidence until same-day-close execution, raw-price corporate actions, exit ordering, capital constraints, and return aggregation are corrected.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = json.loads(args.summary.read_text(encoding="utf-8"))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = args.output_dir / "artifact.json"
    artifact_path.write_text(
        json.dumps(build_artifact(summary), ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    (args.output_dir / "source_notes.md").write_text(
        build_source_notes(summary), encoding="utf-8"
    )
    print(json.dumps({"artifact": str(artifact_path)}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

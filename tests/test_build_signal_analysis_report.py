import importlib.util
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "build_signal_analysis_report.py"
SPEC = importlib.util.spec_from_file_location("build_signal_analysis_report", MODULE_PATH)
report = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = report
SPEC.loader.exec_module(report)


def sample_summary():
    verdict_row = {
        "observations": 10,
        "codes": 10,
        "mean_return_pct": 1.0,
        "median_return_pct": 0.5,
        "win_rate": 0.6,
        "q25_return_pct": -1.0,
        "q75_return_pct": 2.0,
        "stop_hit_rate": 0.2,
        "take_hit_rate": 0.3,
    }
    return {
        "generated_at": "2026-09-04T10:00:00+09:00",
        "source": {
            "database": "stock_data.db",
            "price_as_of": "2026-09-02",
            "start_date": "2026-01-26",
        },
        "quality": {
            "signals": 100,
            "analysis_eligible": 80,
            "non_overlapping_episodes": 40,
            "current_policy_signals": 20,
            "current_policy_complete": 0,
            "mature_unverified_share_basis": 2,
            "invalid_signal_price": 3,
        },
        "correlation_actionable_episodes": {"observations": 20, "spearman": -0.3},
        "entry_minus_watch_bootstrap_all_epochs": {
            "mean_return_difference_pct_points_ci95": [-5.0, -2.0, -0.1]
        },
        "entry_minus_watch_bootstrap_pre_guard": {
            "mean_return_difference_pct_points_ci95": [-5.0, -1.0, 2.0]
        },
        "verdict_summary": [
            {"verdict": "ENTRY", **verdict_row},
            {"verdict": "WATCH", **verdict_row},
            {"verdict": "REJECT", **verdict_row},
        ],
        "rsi_bucket_summary": [
            {"rsi_bucket": "<=30", **verdict_row},
        ],
        "rsi_quintile_summary": [
            {
                "rsi_quintile": "Q1 (lowest)",
                "observations": 4,
                "codes": 4,
                "rsi_min": 10.0,
                "rsi_max": 20.0,
                "rsi_median": 15.0,
                "mean_return_pct": 1.0,
                "median_return_pct": 0.5,
                "win_rate": 0.5,
                "stop_hit_rate": 0.25,
            }
        ],
    }


class SignalAnalysisReportTest(unittest.TestCase):
    def test_artifact_has_canonical_report_spine_and_provenance(self):
        artifact = report.build_artifact(sample_summary())

        self.assertEqual(artifact["surface"], "report")
        self.assertEqual(artifact["manifest"]["title"], report.TITLE)
        self.assertEqual(
            artifact["manifest"]["blocks"][0]["body"], f"# {report.TITLE}"
        )
        self.assertTrue(
            any(block["type"] == "chart" for block in artifact["manifest"]["blocks"])
        )
        self.assertEqual(
            artifact["manifest"]["charts"][0]["encodings"]["x"]["field"],
            "rsi_quintile",
        )
        self.assertEqual(artifact["manifest"]["sources"][0]["id"], report.SOURCE_ID)
        self.assertEqual(artifact["snapshot"]["status"], "ready")


if __name__ == "__main__":
    unittest.main()

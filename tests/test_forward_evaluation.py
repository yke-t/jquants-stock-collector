import copy
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.forward_evaluation import (
    evaluate_prepared_forward,
    load_manifest,
    manifest_sha256,
    verify_frozen_backtest_source,
)
from tests.test_backtest_wfa import prepared_rows


class ForwardEvaluationTest(unittest.TestCase):
    def setUp(self):
        self.manifest, self.digest = load_manifest()

    def test_manifest_hash_is_canonical_and_serializable(self):
        reordered = json.loads(
            json.dumps(self.manifest, ensure_ascii=False, sort_keys=True)
        )

        self.assertEqual(manifest_sha256(reordered), self.digest)

    def test_collecting_status_does_not_decide_targets(self):
        dates = pd.bdate_range("2026-09-03", periods=10)
        prices = prepared_rows(dates, signal_date=dates[4])

        result = evaluate_prepared_forward(
            prices,
            self.manifest,
            manifest_digest=self.digest,
        )

        self.assertEqual(result["status"], "collecting")
        self.assertFalse(result["mature"])
        self.assertTrue(result["metrics_are_interim"])
        self.assertEqual(result["observation"]["evaluation_sessions"], 10)
        self.assertEqual(result["observation"]["remaining_sessions"], 242)
        self.assertEqual(result["observation"]["next_milestone"], 21)
        self.assertIsNone(result["targets"]["cagr_passed"])
        self.assertIsNone(result["targets"]["max_drawdown_passed"])
        self.assertIsNone(result["targets"]["final_passed"])

    def test_mature_evaluation_freezes_at_configured_session(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["forward_window"]["minimum_sessions"] = 5
        manifest["forward_window"]["interim_milestones"] = [2, 3, 4]
        dates = pd.bdate_range("2026-09-03", periods=8)
        prices = prepared_rows(dates)

        result = evaluate_prepared_forward(
            prices,
            manifest,
            manifest_digest=manifest_sha256(manifest),
        )

        self.assertTrue(result["mature"])
        self.assertFalse(result["metrics_are_interim"])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["observation"]["evaluation_sessions"], 5)
        self.assertEqual(result["observation"]["available_sessions"], 8)
        self.assertEqual(result["observation"]["ignored_sessions_after_maturity"], 3)
        self.assertEqual(
            result["observation"]["end_date"],
            dates[4].date().isoformat(),
        )
        self.assertIsNotNone(result["targets"]["cagr_passed"])
        self.assertIsNotNone(result["targets"]["max_drawdown_passed"])

    def test_empty_forward_window_remains_collecting(self):
        result = evaluate_prepared_forward(
            pd.DataFrame(),
            self.manifest,
            manifest_digest=self.digest,
        )

        self.assertEqual(result["status"], "collecting")
        self.assertEqual(result["observation"]["evaluation_sessions"], 0)
        self.assertIsNone(result["metrics"]["cagr"])

    def test_manifest_rejects_forward_start_before_selection_cutoff(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["forward_window"]["start_date"] = "2026-09-02"
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "must be after"):
                load_manifest(path)

    def test_manifest_rejects_selected_candidate_without_best_score(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["selection"]["selected_candidate_index"] = 1
        manifest["selection"]["selected_params"] = manifest["selection"][
            "candidates"
        ][0]["params"]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "best frozen score"):
                load_manifest(path)

    def test_frozen_source_hash_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "backtest_wfa.py"
            source.write_text("changed\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
                verify_frozen_backtest_source(self.manifest, source)


if __name__ == "__main__":
    unittest.main()

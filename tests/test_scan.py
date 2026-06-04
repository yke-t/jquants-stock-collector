# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import scan


class ReboundGuardTest(unittest.TestCase):
    def test_demotes_deep_dip_entry_to_watch(self):
        signals = [
            {'code': '12340', 'verdict': 'ENTRY', 'ma25_rate': -10.0, 'reason': 'Normal:通常押し目'},
        ]
        count = scan.apply_rebound_guard(signals)
        self.assertEqual(count, 1)
        self.assertEqual(signals[0]['verdict'], 'WATCH')
        self.assertIn('Guard: MA25=-10.0%', signals[0]['reason'])
        self.assertIn('Normal:通常押し目', signals[0]['reason'])

    def test_does_not_demote_shallow_dip_entry(self):
        signals = [
            {'code': '12340', 'verdict': 'ENTRY', 'ma25_rate': -5.0, 'reason': 'Normal'},
        ]
        count = scan.apply_rebound_guard(signals)
        self.assertEqual(count, 0)
        self.assertEqual(signals[0]['verdict'], 'ENTRY')

    def test_does_not_affect_watch_or_reject(self):
        signals = [
            {'code': '12340', 'verdict': 'WATCH', 'ma25_rate': -12.0, 'reason': 'Test'},
            {'code': '56780', 'verdict': 'REJECT', 'ma25_rate': -15.0, 'reason': 'News:bad'},
        ]
        count = scan.apply_rebound_guard(signals)
        self.assertEqual(count, 0)
        self.assertEqual(signals[0]['verdict'], 'WATCH')
        self.assertEqual(signals[1]['verdict'], 'REJECT')

    def test_boundary_at_threshold(self):
        signals = [
            {'code': '12340', 'verdict': 'ENTRY', 'ma25_rate': -8.0, 'reason': ''},
        ]
        count = scan.apply_rebound_guard(signals)
        self.assertEqual(count, 1)
        self.assertEqual(signals[0]['verdict'], 'WATCH')

    def test_just_above_threshold(self):
        signals = [
            {'code': '12340', 'verdict': 'ENTRY', 'ma25_rate': -7.9, 'reason': ''},
        ]
        count = scan.apply_rebound_guard(signals)
        self.assertEqual(count, 0)
        self.assertEqual(signals[0]['verdict'], 'ENTRY')

    def test_handles_string_ma25_rate(self):
        signals = [
            {'code': '12340', 'verdict': 'ENTRY', 'ma25_rate': '-9.5', 'reason': ''},
        ]
        count = scan.apply_rebound_guard(signals)
        self.assertEqual(count, 1)
        self.assertEqual(signals[0]['verdict'], 'WATCH')

    def test_handles_missing_ma25_rate(self):
        signals = [
            {'code': '12340', 'verdict': 'ENTRY', 'reason': ''},
        ]
        count = scan.apply_rebound_guard(signals)
        # ma25_rate missing -> defaults to 0, which is > -8, so no demotion
        self.assertEqual(count, 0)
        self.assertEqual(signals[0]['verdict'], 'ENTRY')

    def test_handles_invalid_ma25_rate(self):
        signals = [
            {'code': '12340', 'verdict': 'ENTRY', 'ma25_rate': 'N/A', 'reason': ''},
        ]
        count = scan.apply_rebound_guard(signals)
        # ValueError -> skip, no demotion
        self.assertEqual(count, 0)
        self.assertEqual(signals[0]['verdict'], 'ENTRY')


if __name__ == '__main__':
    unittest.main()

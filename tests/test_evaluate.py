# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import evaluate


class EvaluateTest(unittest.TestCase):
    def setUp(self):
        evaluate._market_sentiment_cache.clear()

    def tearDown(self):
        evaluate._market_sentiment_cache.clear()

    def test_calculate_performance_uses_next_open(self):
        signals = pd.DataFrame([{
            'signal_date': '2026-01-05',
            'code': '12340',
            'name': 'Sample',
            'signal_price': 9999,
            'ma25_rate': -4.0,
            'stop_loss': 95,
            'take_profit': 120,
            'verdict': 'ENTRY',
            'reason': 'Normal',
            'news_hit': '',
        }])
        prices = pd.DataFrame([
            {'date': '2026-01-06', 'code': '12340', 'open': 100, 'high': 110, 'low': 96, 'close': 105},
            {'date': '2026-01-07', 'code': '12340', 'open': 106, 'high': 121, 'low': 94, 'close': 120},
        ])
        prices['date'] = pd.to_datetime(prices['date'])

        evaluate._market_sentiment_cache['2026-01-05'] = 0.45

        with patch.object(evaluate, 'load_prices_for_evaluation', return_value=prices):
            result = evaluate.calculate_performance(signals, eval_days=2).iloc[0]

        self.assertTrue(result['eval_complete'])
        self.assertEqual(result['entry_price'], 100)
        self.assertEqual(result['entry_price_source'], 'next_open')
        self.assertEqual(result['return_pct'], 20.0)
        self.assertFalse(result['signal_price_sane'])
        self.assertTrue(result['stop_loss_hit'])
        self.assertTrue(result['take_profit_hit'])

    def test_calculate_performance_marks_incomplete(self):
        signals = pd.DataFrame([{
            'signal_date': '2026-01-05',
            'code': '12340',
            'name': 'Sample',
            'signal_price': 100,
            'ma25_rate': -4.0,
            'stop_loss': 95,
            'take_profit': 120,
            'verdict': 'ENTRY',
            'reason': 'Normal',
            'news_hit': '',
        }])
        prices = pd.DataFrame([
            {'date': '2026-01-06', 'code': '12340', 'open': 100, 'high': 105, 'low': 99, 'close': 104},
        ])
        prices['date'] = pd.to_datetime(prices['date'])

        evaluate._market_sentiment_cache['2026-01-05'] = 0.35

        with patch.object(evaluate, 'load_prices_for_evaluation', return_value=prices):
            result = evaluate.calculate_performance(signals, eval_days=2).iloc[0]

        self.assertFalse(result['eval_complete'])
        self.assertEqual(result['eval_observations'], 1)
        self.assertEqual(result['next_open'], 100)
        self.assertTrue(result['signal_price_sane'])
        self.assertTrue(pd.isna(result['return_pct']))

    def test_signal_price_sanity_check(self):
        self.assertTrue(evaluate.is_sane_signal_price(100, 100))
        self.assertFalse(evaluate.is_sane_signal_price(19, 100))
        self.assertFalse(evaluate.is_sane_signal_price(501, 100))

    def test_classify_market_bucket(self):
        self.assertEqual(evaluate.classify_market_bucket(0.30), 'bearish')
        self.assertEqual(evaluate.classify_market_bucket(0.39), 'bearish')
        self.assertEqual(evaluate.classify_market_bucket(0.40), 'neutral')
        self.assertEqual(evaluate.classify_market_bucket(0.49), 'neutral')
        self.assertEqual(evaluate.classify_market_bucket(0.50), 'bullish')
        self.assertEqual(evaluate.classify_market_bucket(0.70), 'bullish')
        self.assertEqual(evaluate.classify_market_bucket(float('nan')), 'unknown')

    def test_classify_ma25_bucket(self):
        self.assertEqual(evaluate.classify_ma25_bucket(-8.0), '<=-8')
        self.assertEqual(evaluate.classify_ma25_bucket(-6.0), '-8..-5')
        self.assertEqual(evaluate.classify_ma25_bucket('-4.0'), '-5..-3')
        self.assertEqual(evaluate.classify_ma25_bucket(-1.0), '-3..0')
        self.assertEqual(evaluate.classify_ma25_bucket(0.0), '>=0')
        self.assertEqual(evaluate.classify_ma25_bucket(float('nan')), 'unknown')

    def test_calculate_performance_includes_market_sentiment(self):
        signals = pd.DataFrame([{
            'signal_date': '2026-01-05',
            'code': '12340',
            'name': 'Sample',
            'signal_price': 100,
            'ma25_rate': -4.0,
            'stop_loss': 95,
            'take_profit': 120,
            'verdict': 'ENTRY',
            'reason': 'Normal',
            'news_hit': '',
        }])
        prices = pd.DataFrame([
            {'date': '2026-01-06', 'code': '12340', 'open': 100, 'high': 110, 'low': 96, 'close': 105},
            {'date': '2026-01-07', 'code': '12340', 'open': 106, 'high': 121, 'low': 94, 'close': 120},
        ])
        prices['date'] = pd.to_datetime(prices['date'])

        # Pre-populate the cache to avoid DB access
        evaluate._market_sentiment_cache['2026-01-05'] = 0.55

        with patch.object(evaluate, 'load_prices_for_evaluation', return_value=prices):
            result = evaluate.calculate_performance(signals, eval_days=2).iloc[0]

        self.assertAlmostEqual(result['market_sentiment'], 0.55)
        self.assertEqual(result['market_bucket'], 'bullish')
        self.assertEqual(result['ma25_rate_num'], -4.0)
        self.assertEqual(result['ma25_bucket'], '-5..-3')


if __name__ == '__main__':
    unittest.main()

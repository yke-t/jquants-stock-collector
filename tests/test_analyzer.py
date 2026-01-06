# -*- coding: utf-8 -*-
"""Test script for news_analyzer"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

from src.news_analyzer import get_nikkei_change, batch_analyze

print("Testing News Analyzer...")
print(f"Nikkei 225 change: {get_nikkei_change():+.1f}%")

# Test with English name (to avoid encoding issues)
signals = [
    {'code': '72030', 'name': 'Toyota', 'dip_pct': -3.5},
]

results = batch_analyze(signals)
for r in results:
    print(f"Result: {r['verdict']} - {r['reason']}")
    if r.get('news_hit'):
        print(f"  News: {r['news_hit']}")

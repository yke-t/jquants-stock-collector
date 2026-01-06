"""
News Analyzer Module

暴落銘柄の「売られる理由」を自動調査し、エントリー可否を判定する。

Phase 1: Event Filter - ネガティブニュース検索（Killer Keywords）
Phase 2: Context Filter - 日経平均との比較（連れ安判定）
"""
import yfinance as yf
from duckduckgo_search import DDGS
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging

# Logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
# Killer Keywords（悪材料検出用）
KILLER_KEYWORDS = [
    "下方修正",
    "減配",
    "不祥事",
    "ストップ安",
    "課徴金",
    "業績悪化",
    "赤字転落",
    "倒産",
    "粉飾",
]

# 判定閾値
MARKET_DROP_THRESHOLD = -2.0  # 市場が-2%以上下落で「地合い悪」
SECTOR_DIP_THRESHOLD = -3.0   # 個別銘柄が-3%以上下落で「押し目候補」


def get_nikkei_change() -> float:
    """日経平均の前日比（%）を取得"""
    try:
        nikkei = yf.Ticker("^N225")
        hist = nikkei.history(period="5d")
        if len(hist) >= 2:
            prev_close = hist['Close'].iloc[-2]
            last_close = hist['Close'].iloc[-1]
            change_pct = (last_close / prev_close - 1) * 100
            return round(change_pct, 2)
    except Exception as e:
        logger.warning(f"[NEWS] Failed to get Nikkei data: {e}")
    return 0.0


def search_news(company_name: str, max_results: int = 3) -> list:
    """
    DuckDuckGoでニュース検索（Killer Keywords）
    
    Args:
        company_name: 銘柄名
        max_results: 検索結果の最大数
    
    Returns:
        ヒットしたニュースのリスト [{'title': str, 'keyword': str}, ...]
    """
    import time
    
    # シンプルなクエリ（レートリミット回避）
    query = f"{company_name} 株価"
    
    hits = []
    try:
        with DDGS() as ddgs:
            # ニュース検索
            results = list(ddgs.news(
                query,
                region="jp-jp",
                safesearch="off",
                timelimit="w",  # 過去1週間
                max_results=max_results
            ))
            
            for result in results:
                title = result.get('title', '')
                body = result.get('body', '')
                text = title + " " + body
                
                # タイトルまたは本文にKiller Keywordが含まれているか確認
                for keyword in KILLER_KEYWORDS:
                    if keyword in text:
                        hits.append({
                            'title': title,
                            'keyword': keyword,
                            'date': result.get('date', ''),
                        })
                        break
        
        # レートリミット回避のため待機（3秒）
        time.sleep(3)
        
    except Exception as e:
        error_str = str(e)
        if "Ratelimit" in error_str:
            logger.warning(f"[NEWS] Rate limited, skipping news search for {company_name}")
        else:
            logger.warning(f"[NEWS] Search failed for {company_name}: {e}")
    
    return hits


def analyze_stock(
    code: str,
    name: str,
    stock_drop: float,
    market_drop: Optional[float] = None
) -> Dict:
    """
    銘柄を分析し、エントリー判定を行う
    
    Args:
        code: 銘柄コード
        name: 銘柄名
        stock_drop: 対象銘柄の前日比（%）
        market_drop: 日経平均の前日比（%）、Noneの場合は自動取得
    
    Returns:
        {
            'verdict': 'ENTRY' | 'WATCH' | 'REJECT',
            'reason': str,
            'news_hit': str | None
        }
    """
    # 日経平均の変動を取得
    if market_drop is None:
        market_drop = get_nikkei_change()
    
    # Phase 1: Event Filter（ニュース検索）
    logger.info(f"[NEWS] Analyzing {code} ({name})...")
    news_hits = search_news(name, max_results=3)
    
    if news_hits:
        # ネガティブニュース検出 → REJECT
        first_hit = news_hits[0]
        return {
            'verdict': 'REJECT',
            'reason': f"News:{first_hit['keyword']}検出",
            'news_hit': first_hit['title'][:50] + "..." if len(first_hit['title']) > 50 else first_hit['title']
        }
    
    # Phase 2: Context Filter（地合い比較）
    # 連れ安判定: 市場が下落していて、個別も下落している
    if market_drop <= MARKET_DROP_THRESHOLD and stock_drop <= SECTOR_DIP_THRESHOLD:
        return {
            'verdict': 'ENTRY',
            'reason': f"Sector:連れ安(市場{market_drop:+.1f}%)",
            'news_hit': None
        }
    
    # 固有下落: 市場は堅調なのに個別だけ下落
    if market_drop > -1.0 and stock_drop <= SECTOR_DIP_THRESHOLD:
        return {
            'verdict': 'WATCH',
            'reason': f"Individual:固有下落(市場{market_drop:+.1f}%)",
            'news_hit': None
        }
    
    # デフォルト: 軽微な下落
    return {
        'verdict': 'ENTRY',
        'reason': f"Normal:通常押し目(市場{market_drop:+.1f}%)",
        'news_hit': None
    }


def batch_analyze(signals: list) -> list:
    """
    複数銘柄を一括分析
    
    Args:
        signals: [{'code': str, 'name': str, 'dip_pct': float}, ...]
    
    Returns:
        [{'code': str, 'verdict': str, 'reason': str, 'news_hit': str}, ...]
    """
    # 日経平均を1回だけ取得
    market_drop = get_nikkei_change()
    logger.info(f"[NEWS] Nikkei 225 change: {market_drop:+.1f}%")
    
    results = []
    for signal in signals:
        result = analyze_stock(
            code=signal['code'],
            name=signal.get('name', ''),
            stock_drop=signal.get('dip_pct', 0),
            market_drop=market_drop
        )
        result['code'] = signal['code']
        results.append(result)
    
    return results


if __name__ == "__main__":
    # テスト実行
    print("Testing News Analyzer...")
    
    # 日経平均の変動を取得
    nikkei_change = get_nikkei_change()
    print(f"Nikkei 225 change: {nikkei_change:+.1f}%")
    
    # テスト銘柄
    test_signals = [
        {'code': '72030', 'name': 'トヨタ自動車', 'dip_pct': -3.5},
        {'code': '67580', 'name': 'ソニーグループ', 'dip_pct': -4.2},
    ]
    
    results = batch_analyze(test_signals)
    for r in results:
        print(f"{r['code']}: {r['verdict']} - {r['reason']}")
        if r['news_hit']:
            print(f"  → {r['news_hit']}")

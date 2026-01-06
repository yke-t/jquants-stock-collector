"""
News Analyzer Module

暴落銘柄の「売られる理由」を自動調査し、エントリー可否を判定する。

Phase 1: Event Filter - ネガティブニュース検索（Killer Keywords）
Phase 2: Context Filter - 日経平均との比較（連れ安判定）

Search Provider: Google Custom Search API (100回/日無料)
"""
import os
import requests
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
# Google Custom Search API
GOOGLE_CSE_API_KEY = os.getenv("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID", "")

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


def search_news_google(company_name: str, max_results: int = 3) -> List[dict]:
    """
    Google Custom Search APIでニュース検索（Killer Keywords）
    
    Args:
        company_name: 銘柄名
        max_results: 検索結果の最大数
    
    Returns:
        ヒットしたニュースのリスト [{'title': str, 'keyword': str}, ...]
    """
    if not GOOGLE_CSE_API_KEY or not GOOGLE_CSE_ID:
        logger.warning("[NEWS] Google CSE API key or ID not configured. Skipping search.")
        return []
    
    # 検索クエリ（銘柄名 + Killer Keywords）
    keywords_query = " OR ".join(KILLER_KEYWORDS[:5])  # 最初の5つのキーワード
    query = f"{company_name} ({keywords_query})"
    
    # Google Custom Search API呼び出し
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_CSE_API_KEY,
        "cx": GOOGLE_CSE_ID,
        "q": query,
        "num": max_results,
        "lr": "lang_ja",  # 日本語
        "dateRestrict": "d7",  # 過去7日
    }
    
    hits = []
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        items = data.get("items", [])
        logger.info(f"[NEWS] Google CSE returned {len(items)} results for {company_name}")
        
        for item in items:
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            text = title + " " + snippet
            
            # Killer Keywordsを検出
            for keyword in KILLER_KEYWORDS:
                if keyword in text:
                    hits.append({
                        'title': title,
                        'keyword': keyword,
                        'link': item.get("link", ""),
                    })
                    break
                    
    except requests.exceptions.RequestException as e:
        logger.warning(f"[NEWS] Google CSE request failed for {company_name}: {e}")
    except Exception as e:
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
    news_hits = search_news_google(name, max_results=3)
    
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
    print("Testing News Analyzer with Google CSE...")
    
    # 設定確認
    if not GOOGLE_CSE_API_KEY or not GOOGLE_CSE_ID:
        print("[ERROR] Google CSE API key or ID not configured.")
        print("Please set GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID in .env")
        exit(1)
    
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

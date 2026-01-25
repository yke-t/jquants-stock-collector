# src/evaluate.py
"""
シグナル判定結果の事後評価モジュール

月次でシグナルの的中率・回避成功率を計算し、日足チャートで可視化する。

Usage:
    python -m src.evaluate --month 2026-01
    python -m src.evaluate --month 2026-01 --charts
"""
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import argparse

# --- Config ---
DB_PATH = Path(__file__).parent.parent / "stock_data.db"
CHARTS_OUTPUT_DIR = Path(__file__).parent.parent / "charts"
EVAL_DAYS = 20  # シグナル後N営業日で評価


def load_signals(year_month: str) -> pd.DataFrame:
    """
    指定月のシグナルをDBから取得
    
    Args:
        year_month: YYYY-MM形式
    
    Returns:
        DataFrame: シグナルデータ
    """
    conn = sqlite3.connect(DB_PATH)
    
    query = """
    SELECT * FROM signals 
    WHERE signal_date LIKE ?
    ORDER BY signal_date, code
    """
    
    df = pd.read_sql(query, conn, params=[f"{year_month}%"])
    conn.close()
    
    if df.empty:
        print(f"[WARN] No signals found for {year_month}")
    else:
        print(f"[INFO] Loaded {len(df)} signals for {year_month}")
    
    return df


def load_prices_for_evaluation(codes: list, start_date: str, end_date: str) -> pd.DataFrame:
    """
    評価用の株価データを取得
    
    Args:
        codes: 銘柄コードのリスト
        start_date: 開始日
        end_date: 終了日
    
    Returns:
        DataFrame: 株価データ
    """
    if not codes:
        return pd.DataFrame()
    
    conn = sqlite3.connect(DB_PATH)
    
    placeholders = ','.join('?' * len(codes))
    query = f"""
    SELECT date, code, open, high, low, close
    FROM prices
    WHERE code IN ({placeholders})
    AND date >= ?
    AND date <= ?
    ORDER BY code, date
    """
    
    params = list(codes) + [start_date, end_date]
    df = pd.read_sql(query, conn, params=params, parse_dates=['date'])
    conn.close()
    
    return df


def calculate_performance(signals_df: pd.DataFrame, eval_days: int = EVAL_DAYS) -> pd.DataFrame:
    """
    シグナル後N日のパフォーマンスを計算
    
    Args:
        signals_df: シグナルデータ
        eval_days: 評価日数
    
    Returns:
        DataFrame: パフォーマンス付きシグナルデータ
    """
    if signals_df.empty:
        return signals_df
    
    # 評価期間を計算
    min_date = pd.to_datetime(signals_df['signal_date'].min())
    max_eval_date = (pd.to_datetime(signals_df['signal_date'].max()) + timedelta(days=eval_days * 2)).strftime('%Y-%m-%d')
    
    # 株価データ取得
    codes = signals_df['code'].unique().tolist()
    prices_df = load_prices_for_evaluation(codes, min_date.strftime('%Y-%m-%d'), max_eval_date)
    
    if prices_df.empty:
        print("[WARN] No price data found for evaluation")
        signals_df['return_pct'] = np.nan
        signals_df['max_gain'] = np.nan
        signals_df['max_loss'] = np.nan
        return signals_df
    
    # 各シグナルに対してパフォーマンスを計算
    results = []
    
    for _, sig in signals_df.iterrows():
        code = sig['code']
        signal_date = pd.to_datetime(sig['signal_date'])
        
        # 該当銘柄の株価を取得
        code_prices = prices_df[prices_df['code'] == code].copy()
        if code_prices.empty:
            results.append({
                **sig.to_dict(),
                'return_pct': np.nan,
                'max_gain': np.nan,
                'max_loss': np.nan,
                'eval_price': np.nan
            })
            continue
        
        # シグナル日以降のデータ
        future_prices = code_prices[code_prices['date'] > signal_date].head(eval_days)
        
        if future_prices.empty:
            results.append({
                **sig.to_dict(),
                'return_pct': np.nan,
                'max_gain': np.nan,
                'max_loss': np.nan,
                'eval_price': np.nan
            })
            continue
        
        signal_price = sig['signal_price']
        if signal_price <= 0:
            signal_price = future_prices.iloc[0]['open']
        
        # N日後の終値でリターン計算
        eval_price = future_prices.iloc[-1]['close']
        return_pct = (eval_price - signal_price) / signal_price * 100
        
        # 期間中の最大上昇/下落
        max_high = future_prices['high'].max()
        min_low = future_prices['low'].min()
        max_gain = (max_high - signal_price) / signal_price * 100
        max_loss = (min_low - signal_price) / signal_price * 100
        
        results.append({
            **sig.to_dict(),
            'return_pct': round(return_pct, 2),
            'max_gain': round(max_gain, 2),
            'max_loss': round(max_loss, 2),
            'eval_price': eval_price
        })
    
    return pd.DataFrame(results)


def generate_report(year_month: str, eval_days: int = EVAL_DAYS):
    """
    月次評価レポートを生成
    
    Args:
        year_month: YYYY-MM形式
        eval_days: 評価日数
    """
    print("=" * 60)
    print(f"シグナル評価レポート: {year_month}")
    print(f"評価期間: シグナル日から{eval_days}営業日後")
    print("=" * 60)
    
    # シグナル取得
    signals_df = load_signals(year_month)
    if signals_df.empty:
        return None
    
    # パフォーマンス計算
    results_df = calculate_performance(signals_df, eval_days)
    
    # 判定別集計
    for verdict in ['ENTRY', 'WATCH', 'REJECT']:
        subset = results_df[results_df['verdict'] == verdict]
        if subset.empty:
            continue
        
        valid_subset = subset.dropna(subset=['return_pct'])
        
        print(f"\n■ {verdict}判定 ({len(subset)}件)")
        print("-" * 40)
        
        if valid_subset.empty:
            print("  評価データなし（株価データ不足）")
            continue
        
        # 的中率/回避成功率
        if verdict == 'ENTRY':
            hit_rate = (valid_subset['return_pct'] > 0).mean() * 100
            print(f"  的中率（プラス終了）: {hit_rate:.1f}%")
        elif verdict == 'REJECT':
            avoid_rate = (valid_subset['return_pct'] < 0).mean() * 100
            print(f"  回避成功率（マイナス終了）: {avoid_rate:.1f}%")
        else:
            plus_rate = (valid_subset['return_pct'] > 0).mean() * 100
            print(f"  プラス終了率: {plus_rate:.1f}%")
        
        # リターン統計
        avg_return = valid_subset['return_pct'].mean()
        median_return = valid_subset['return_pct'].median()
        max_return = valid_subset['return_pct'].max()
        min_return = valid_subset['return_pct'].min()
        
        print(f"  平均リターン: {avg_return:+.2f}%")
        print(f"  中央値: {median_return:+.2f}%")
        print(f"  最大: {max_return:+.2f}% / 最小: {min_return:+.2f}%")
        
        # 上位/下位銘柄
        if len(valid_subset) >= 3:
            print("\n  [TOP 3]")
            for _, row in valid_subset.nlargest(3, 'return_pct').iterrows():
                print(f"    {row['code']} {row['name'][:10]}: {row['return_pct']:+.1f}%")
            
            print("  [BOTTOM 3]")
            for _, row in valid_subset.nsmallest(3, 'return_pct').iterrows():
                print(f"    {row['code']} {row['name'][:10]}: {row['return_pct']:+.1f}%")
    
    print("\n" + "=" * 60)
    
    return results_df


def plot_signal_charts(results_df: pd.DataFrame, output_dir: Path = CHARTS_OUTPUT_DIR):
    """
    各シグナルの日足チャートをPNG出力
    
    Args:
        results_df: 評価結果データ
        output_dir: 出力ディレクトリ
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        plt.rcParams['font.family'] = 'MS Gothic'  # 日本語フォント
    except ImportError:
        print("[ERROR] matplotlib not installed. Run: pip install matplotlib")
        return
    
    output_dir.mkdir(exist_ok=True)
    
    if results_df.empty:
        print("[WARN] No data to plot")
        return
    
    # 株価データ取得（前後20日）
    codes = results_df['code'].unique().tolist()
    min_date = (pd.to_datetime(results_df['signal_date'].min()) - timedelta(days=40)).strftime('%Y-%m-%d')
    max_date = (pd.to_datetime(results_df['signal_date'].max()) + timedelta(days=40)).strftime('%Y-%m-%d')
    
    prices_df = load_prices_for_evaluation(codes, min_date, max_date)
    
    if prices_df.empty:
        print("[WARN] No price data for charts")
        return
    
    chart_count = 0
    
    for _, sig in results_df.iterrows():
        code = sig['code']
        signal_date = pd.to_datetime(sig['signal_date'])
        verdict = sig['verdict']
        name = sig['name'][:15] if sig['name'] else code
        
        # 該当銘柄のデータ
        code_prices = prices_df[prices_df['code'] == code].copy()
        
        # シグナル日の前後20営業日を抽出
        before = code_prices[code_prices['date'] < signal_date].tail(20)
        after = code_prices[code_prices['date'] >= signal_date].head(21)
        chart_data = pd.concat([before, after])
        
        if len(chart_data) < 5:
            continue
        
        # チャート作成
        fig, ax = plt.subplots(figsize=(10, 5))
        
        # ローソク足（簡易版：終値折れ線）
        ax.plot(chart_data['date'], chart_data['close'], 'b-', linewidth=1.5)
        ax.fill_between(chart_data['date'], chart_data['low'], chart_data['high'], alpha=0.3)
        
        # シグナル日に縦線
        ax.axvline(x=signal_date, color='red', linestyle='--', linewidth=2, label='Signal Date')
        
        # 判定結果に応じた色
        verdict_colors = {'ENTRY': 'green', 'WATCH': 'orange', 'REJECT': 'red'}
        title_color = verdict_colors.get(verdict, 'black')
        
        return_str = f"{sig.get('return_pct', 0):+.1f}%" if pd.notna(sig.get('return_pct')) else "N/A"
        ax.set_title(f"[{verdict}] {code} {name} (Return: {return_str})", fontsize=12, color=title_color)
        
        ax.set_xlabel('Date')
        ax.set_ylabel('Price')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        plt.xticks(rotation=45)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # 保存（ファイル名の不正文字をサニタイズ）
        safe_verdict = verdict.replace('/', '_').replace('\\', '_').replace(':', '_')
        filename = f"{sig['signal_date']}_{code}_{safe_verdict}.png"
        filepath = output_dir / filename
        plt.savefig(filepath, dpi=100)
        plt.close()
        
        chart_count += 1
    
    print(f"[INFO] Generated {chart_count} charts in {output_dir}")


def import_from_sheets():
    """
    Google Sheetsの過去シート（Signals_YYYYMMDD）からシグナルをインポート
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("[ERROR] gspread not installed. Run: pip install gspread google-auth")
        return
    
    SECRET_KEY_PATH = Path(__file__).parent.parent / "secret_key.json"
    SPREADSHEET_KEY = "1Hejm_UXA3xvn5rEXUhMkpHPtSjM2-foq-t1Su96gGYo"
    
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    
    if not SECRET_KEY_PATH.exists():
        print(f"[ERROR] Secret key not found: {SECRET_KEY_PATH}")
        return
    
    print("[INFO] Connecting to Google Sheets...")
    creds = Credentials.from_service_account_file(str(SECRET_KEY_PATH), scopes=SCOPES)
    client = gspread.authorize(creds)
    
    sh = client.open_by_key(SPREADSHEET_KEY)
    worksheets = sh.worksheets()
    
    # Signals_YYYYMMDD形式のシートを検索
    signal_sheets = [ws for ws in worksheets if ws.title.startswith('Signals_') and len(ws.title) == 16]
    
    print(f"[INFO] Found {len(signal_sheets)} signal sheets")
    
    from src.database import StockDatabase
    db = StockDatabase()
    
    total_imported = 0
    
    for ws in signal_sheets:
        # シート名から日付を抽出
        date_str = ws.title.replace('Signals_', '')
        try:
            signal_date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError:
            print(f"[WARN] Invalid sheet name: {ws.title}")
            continue
        
        # データ取得
        records = ws.get_all_records()
        if not records:
            continue
        
        # DB保存用に変換
        signals = []
        for row in records:
            if not row.get('銘柄コード'):
                continue
            
            signals.append({
                'code': str(row.get('銘柄コード', '')),
                'name': str(row.get('銘柄名', '')),
                'current_price': row.get('現在値', 0),
                'ma25_rate': row.get('MA25乖離率(%)', 0.0),
                'stop_loss': row.get('損切りライン', 0),
                'take_profit': row.get('利確目標(MA25)', 0),
                'verdict': str(row.get('判定結果', 'N/A')),
                'reason': str(row.get('判定理由', '')),
                'news_hit': str(row.get('News Hit', ''))
            })
        
        if signals:
            saved = db.save_signals(signals, signal_date)
            total_imported += saved
            print(f"  {ws.title}: {saved} signals imported")
    
    print(f"\n[INFO] Total imported: {total_imported} signals")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Signal Evaluation Tool")
    parser.add_argument("--month", type=str, help="Target month (YYYY-MM)")
    parser.add_argument("--charts", action="store_true", help="Generate charts")
    parser.add_argument("--import-sheets", action="store_true", help="Import from Google Sheets")
    parser.add_argument("--days", type=int, default=EVAL_DAYS, help=f"Evaluation days (default: {EVAL_DAYS})")
    
    args = parser.parse_args()
    
    if args.import_sheets:
        import_from_sheets()
    elif args.month:
        results = generate_report(args.month, args.days)
        if args.charts and results is not None:
            plot_signal_charts(results)
    else:
        # デフォルト: 今月を評価
        current_month = datetime.now().strftime('%Y-%m')
        print(f"[INFO] No month specified. Using current month: {current_month}")
        results = generate_report(current_month, args.days)
        if args.charts and results is not None:
            plot_signal_charts(results)

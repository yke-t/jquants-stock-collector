# src/evaluate.py
"""
シグナル判定結果の事後評価モジュール

月次でシグナルの的中率・回避成功率を計算し、日足チャートで可視化する。

Usage:
    python -m src.evaluate --month 2026-01
    python -m src.evaluate --month 2026-01 --charts
"""
import sqlite3
import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
DB_PATH = Path(__file__).parent.parent / "stock_data.db"
CHARTS_OUTPUT_DIR = Path(__file__).parent.parent / "charts"
REPORTS_OUTPUT_DIR = Path(__file__).parent.parent / "reports"
EVAL_DAYS = 20  # シグナル後N営業日で評価
SIGNAL_PRICE_MIN_RATIO = 0.2
SIGNAL_PRICE_MAX_RATIO = 5.0


def coerce_numeric(value):
    """カンマや空文字を含む値を安全に数値化する"""
    if pd.isna(value):
        return np.nan
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if not value:
            return np.nan
    return pd.to_numeric(value, errors="coerce")


def is_sane_signal_price(signal_price, next_open) -> bool:
    """signal_priceが翌日始値に対して桁違いでないか判定する"""
    if pd.isna(signal_price) or pd.isna(next_open) or signal_price <= 0 or next_open <= 0:
        return False
    ratio = signal_price / next_open
    return SIGNAL_PRICE_MIN_RATIO <= ratio <= SIGNAL_PRICE_MAX_RATIO


def build_unevaluated_result(sig, eval_observations=0, next_open=np.nan):
    """評価不能または未成熟なシグナルの結果行を作る"""
    signal_price_num = coerce_numeric(sig.get('signal_price', np.nan))
    signal_price_ratio = signal_price_num / next_open if pd.notna(signal_price_num) and pd.notna(next_open) and next_open > 0 else np.nan
    return {
        **sig.to_dict(),
        'entry_price': np.nan,
        'entry_price_source': 'next_open',
        'next_open': next_open,
        'signal_price_num': signal_price_num,
        'signal_price_ratio': round(signal_price_ratio, 4) if pd.notna(signal_price_ratio) else np.nan,
        'signal_price_sane': is_sane_signal_price(signal_price_num, next_open),
        'eval_complete': False,
        'eval_observations': eval_observations,
        'return_pct': np.nan,
        'max_gain': np.nan,
        'max_loss': np.nan,
        'eval_price': np.nan,
        'stop_loss_hit': np.nan,
        'take_profit_hit': np.nan,
    }


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
        return pd.DataFrame([build_unevaluated_result(sig) for _, sig in signals_df.iterrows()])
    
    # 各シグナルに対してパフォーマンスを計算
    results = []
    
    for _, sig in signals_df.iterrows():
        code = sig['code']
        signal_date = pd.to_datetime(sig['signal_date'])
        
        # 該当銘柄の株価を取得
        code_prices = prices_df[prices_df['code'] == code].copy()
        if code_prices.empty:
            results.append(build_unevaluated_result(sig))
            continue
        
        # シグナル日以降のデータ
        future_prices = code_prices[code_prices['date'] > signal_date].head(eval_days)
        eval_observations = len(future_prices)
        
        if eval_observations < eval_days:
            next_open = future_prices.iloc[0]['open'] if not future_prices.empty else np.nan
            results.append(build_unevaluated_result(sig, eval_observations=eval_observations, next_open=next_open))
            continue
        
        next_open = future_prices.iloc[0]['open']
        entry_price = next_open
        signal_price_num = coerce_numeric(sig.get('signal_price', np.nan))
        signal_price_ratio = signal_price_num / next_open if pd.notna(signal_price_num) and next_open > 0 else np.nan
        signal_price_sane = is_sane_signal_price(signal_price_num, next_open)
        stop_loss = coerce_numeric(sig.get('stop_loss', np.nan))
        take_profit = coerce_numeric(sig.get('take_profit', np.nan))
        
        # N日後の終値でリターン計算
        eval_price = future_prices.iloc[-1]['close']
        return_pct = (eval_price - entry_price) / entry_price * 100
        
        # 期間中の最大上昇/下落
        max_high = future_prices['high'].max()
        min_low = future_prices['low'].min()
        max_gain = (max_high - entry_price) / entry_price * 100
        max_loss = (min_low - entry_price) / entry_price * 100
        stop_loss_hit = bool(pd.notna(stop_loss) and stop_loss > 0 and (future_prices['low'] <= stop_loss).any())
        take_profit_hit = bool(pd.notna(take_profit) and take_profit > 0 and (future_prices['high'] >= take_profit).any())
        
        results.append({
            **sig.to_dict(),
            'entry_price': entry_price,
            'entry_price_source': 'next_open',
            'next_open': next_open,
            'signal_price_num': signal_price_num,
            'signal_price_ratio': round(signal_price_ratio, 4) if pd.notna(signal_price_ratio) else np.nan,
            'signal_price_sane': signal_price_sane,
            'eval_complete': True,
            'eval_observations': eval_observations,
            'return_pct': round(return_pct, 2),
            'max_gain': round(max_gain, 2),
            'max_loss': round(max_loss, 2),
            'eval_price': eval_price,
            'stop_loss_hit': stop_loss_hit,
            'take_profit_hit': take_profit_hit,
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
    completed_df = results_df[results_df.get('eval_complete', False) == True]
    incomplete_count = len(results_df) - len(completed_df)
    bad_price_count = len(results_df[results_df.get('signal_price_sane', True) == False])
    
    print("\n■ データ品質")
    print("-" * 40)
    print(f"  総シグナル数: {len(results_df)}")
    print(f"  評価完了（{eval_days}営業日到達）: {len(completed_df)}")
    print(f"  評価未完了（集計除外）: {incomplete_count}")
    print(f"  signal_price異常/欠損: {bad_price_count}")
    
    # 判定別集計
    for verdict in ['ENTRY', 'WATCH', 'REJECT']:
        subset = results_df[results_df['verdict'] == verdict]
        if subset.empty:
            continue
        
        valid_subset = subset[(subset['eval_complete'] == True) & subset['return_pct'].notna()]
        
        print(f"\n■ {verdict}判定 ({len(subset)}件)")
        print("-" * 40)
        
        if valid_subset.empty:
            print("  評価データなし（20営業日未達または株価データ不足）")
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
        print(f"  平均最大上昇: {valid_subset['max_gain'].mean():+.2f}%")
        print(f"  平均最大下落: {valid_subset['max_loss'].mean():+.2f}%")
        print(f"  損切り到達率: {valid_subset['stop_loss_hit'].mean() * 100:.1f}%")
        print(f"  利確到達率: {valid_subset['take_profit_hit'].mean() * 100:.1f}%")
        
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


def summarize_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Markdownレポート用の判定別サマリーを作る"""
    rows = []
    for verdict in ['ENTRY', 'WATCH', 'REJECT', 'N/A']:
        subset = df[df['verdict'] == verdict]
        if subset.empty:
            continue
        valid = subset[(subset['eval_complete'] == True) & subset['return_pct'].notna()]
        rows.append({
            'verdict': verdict,
            'signals': len(subset),
            'evaluated': len(valid),
            'incomplete': len(subset) - len(valid),
            'avg_return_pct': valid['return_pct'].mean() if not valid.empty else np.nan,
            'median_return_pct': valid['return_pct'].median() if not valid.empty else np.nan,
            'win_rate_pct': (valid['return_pct'] > 0).mean() * 100 if not valid.empty else np.nan,
            'avg_max_gain_pct': valid['max_gain'].mean() if not valid.empty else np.nan,
            'avg_max_loss_pct': valid['max_loss'].mean() if not valid.empty else np.nan,
            'stop_hit_pct': valid['stop_loss_hit'].mean() * 100 if not valid.empty else np.nan,
            'take_hit_pct': valid['take_profit_hit'].mean() * 100 if not valid.empty else np.nan,
        })
    return pd.DataFrame(rows)


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    """DataFrameを依存追加なしでMarkdownテーブルに変換する"""
    if df.empty:
        return "_No data_"
    display_df = df.copy()
    for col in display_df.columns:
        if pd.api.types.is_float_dtype(display_df[col]):
            display_df[col] = display_df[col].map(lambda x: "" if pd.isna(x) else f"{x:.2f}")
    headers = list(display_df.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in display_df.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in headers) + " |")
    return "\n".join(lines)


def write_markdown_report(year_month: str, results_df: pd.DataFrame, eval_days: int = EVAL_DAYS) -> Path:
    """AIレビューに渡しやすいMarkdown評価レポートを書き出す"""
    REPORTS_OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = REPORTS_OUTPUT_DIR / f"signal_performance_{year_month}.md"
    completed_df = results_df[(results_df['eval_complete'] == True) & results_df['return_pct'].notna()]
    invalid_price_df = results_df[results_df['signal_price_sane'] == False]
    summary_df = summarize_performance(results_df)
    
    month_summary = pd.DataFrame()
    if not completed_df.empty:
        month_summary = (
            completed_df.assign(month=pd.to_datetime(completed_df['signal_date']).dt.strftime('%Y-%m'))
            .groupby(['month', 'verdict'])
            .agg(
                evaluated=('return_pct', 'size'),
                avg_return_pct=('return_pct', 'mean'),
                median_return_pct=('return_pct', 'median'),
                win_rate_pct=('return_pct', lambda x: (x > 0).mean() * 100),
                stop_hit_pct=('stop_loss_hit', lambda x: x.mean() * 100),
                take_hit_pct=('take_profit_hit', lambda x: x.mean() * 100),
            )
            .reset_index()
        )
    
    lines = [
        f"# Signal Performance Report: {year_month}",
        "",
        "## Evaluation Rules",
        f"- Entry price: next trading day's open after `signal_date`.",
        f"- Evaluation window: {eval_days} future trading days.",
        "- Incomplete signals are kept in diagnostics but excluded from performance aggregates.",
        f"- `signal_price` sanity check: {SIGNAL_PRICE_MIN_RATIO} <= signal_price / next_open <= {SIGNAL_PRICE_MAX_RATIO}.",
        "",
        "## Data Quality",
        f"- total_signals: {len(results_df)}",
        f"- evaluated_complete: {len(completed_df)}",
        f"- incomplete_or_missing_price: {len(results_df) - len(completed_df)}",
        f"- invalid_signal_price: {len(invalid_price_df)}",
        "",
        "## Verdict Summary",
        dataframe_to_markdown(summary_df),
        "",
        "## Month x Verdict Summary",
        dataframe_to_markdown(month_summary),
        "",
        "## Invalid signal_price Sample",
        dataframe_to_markdown(
            invalid_price_df[
                ['signal_date', 'code', 'name', 'verdict', 'signal_price', 'next_open', 'signal_price_ratio']
            ].head(20)
        ),
        "",
        "## Best 10",
        dataframe_to_markdown(
            completed_df.nlargest(10, 'return_pct')[
                ['signal_date', 'code', 'name', 'verdict', 'return_pct', 'max_gain', 'max_loss', 'reason']
            ]
        ),
        "",
        "## Worst 10",
        dataframe_to_markdown(
            completed_df.nsmallest(10, 'return_pct')[
                ['signal_date', 'code', 'name', 'verdict', 'return_pct', 'max_gain', 'max_loss', 'reason']
            ]
        ),
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


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
    
    default_secret_key_path = Path(__file__).parent.parent / "secret_key.json"
    SECRET_KEY_PATH = Path(os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(default_secret_key_path)))
    SPREADSHEET_KEY = os.getenv(
        "GOOGLE_SHEETS_SPREADSHEET_KEY",
        "1Hejm_UXA3xvn5rEXUhMkpHPtSjM2-foq-t1Su96gGYo",
    )
    
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
    parser.add_argument("--prev-month", action="store_true", help="Evaluate previous month")
    parser.add_argument("--charts", action="store_true", help="Generate charts")
    parser.add_argument("--report", action="store_true", help="Write Markdown report")
    parser.add_argument("--import-sheets", action="store_true", help="Import from Google Sheets")
    parser.add_argument("--days", type=int, default=EVAL_DAYS, help=f"Evaluation days (default: {EVAL_DAYS})")
    
    args = parser.parse_args()
    
    if args.import_sheets:
        import_from_sheets()
    elif args.month:
        results = generate_report(args.month, args.days)
        if args.charts and results is not None:
            plot_signal_charts(results)
        if args.report and results is not None:
            report_path = write_markdown_report(args.month, results, args.days)
            print(f"[INFO] Report written: {report_path}")
    elif args.prev_month:
        # 先月を評価（毎月1日のバッチ実行用）
        today = datetime.now()
        first_of_this_month = today.replace(day=1)
        prev_month = (first_of_this_month - timedelta(days=1)).strftime('%Y-%m')
        print(f"[INFO] Target: Previous month ({prev_month})")
        results = generate_report(prev_month, args.days)
        if args.charts and results is not None:
            plot_signal_charts(results)
        if args.report and results is not None:
            report_path = write_markdown_report(prev_month, results, args.days)
            print(f"[INFO] Report written: {report_path}")
    else:
        # デフォルト: 今月を評価
        current_month = datetime.now().strftime('%Y-%m')
        print(f"[INFO] No month specified. Using current month: {current_month}")
        results = generate_report(current_month, args.days)
        if args.charts and results is not None:
            plot_signal_charts(results)
        if args.report and results is not None:
            report_path = write_markdown_report(current_month, results, args.days)
            print(f"[INFO] Report written: {report_path}")

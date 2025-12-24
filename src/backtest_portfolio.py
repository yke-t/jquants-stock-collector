"""
Phase 2.5: Portfolio Backtest Engine
単利合算ではなく、実際の資金推移（Equity Curve）をシミュレーションする。
"""
import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
from pathlib import Path

# --- Config ---
DB_PATH = Path(__file__).parent.parent / "stock_data.db"
INITIAL_CAPITAL = 3000000  # 初期資金 300万円

# Emergency Tuning: 分散 + 厳格化
MAX_POSITIONS = 15         # 銘柄数を増やしてリスク分散（1銘柄 6.6%）
STOP_LOSS_PCT = 0.07       # 損切りを浅くする（-10% -> -7%）
DIP_THRESHOLD = 0.95       # より深く押した（割安な）タイミングだけ拾う
TRAILING_STOP_PCT = 0.20   # 利益は伸ばす（-15% -> -20%）

USE_SCALECAT_FILTER = True
SCALECAT_TARGETS = ['TOPIX Small 1', 'TOPIX Small 2', 'TOPIX Mid400']

class PortfolioBacktester:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        
    def load_data(self, start_date="2015-01-01"):
        print("[INFO] Loading data...")
        if USE_SCALECAT_FILTER:
            scalecat_list = "', '".join(SCALECAT_TARGETS)
            query = f"""
            SELECT p.date, p.code, p.open, p.high, p.low, p.close, f.scalecat
            FROM prices p
            LEFT JOIN fundamentals f ON p.code = f.code
            WHERE p.date >= '{start_date}'
            AND f.scalecat IN ('{scalecat_list}')
            ORDER BY p.date ASC
            """
        else:
            query = f"SELECT date, code, open, high, low, close FROM prices WHERE date >= '{start_date}' ORDER BY date ASC"

        self.df = pd.read_sql(query, self.conn, parse_dates=['date'])
        self.df = self.df.sort_values(['date', 'code'])
        print(f"[INFO] Loaded {len(self.df)} records.")

    def calculate_signals(self):
        """全銘柄のテクニカル指標とシグナルを一括計算"""
        print("[INFO] Calculating indicators...")
        # 処理速度向上のため、groupbyで計算
        self.df['ma_short'] = self.df.groupby('code')['close'].transform(lambda x: x.rolling(25).mean())
        self.df['ma_long'] = self.df.groupby('code')['close'].transform(lambda x: x.rolling(75).mean())
        
        # エントリー条件: GC発生中 AND 押し目
        self.df['gc_trend'] = self.df['ma_short'] > self.df['ma_long']
        self.df['entry_signal'] = self.df['gc_trend'] & (self.df['close'] < self.df['ma_short'] * DIP_THRESHOLD)
        
        # 優先順位用スコア（乖離率：より深く押しているものを優先）
        self.df['priority_score'] = self.df['close'] / self.df['ma_short']

    def run_simulation(self):
        print(f"[INFO] Running Portfolio Simulation (Cap={INITIAL_CAPITAL:,}, MaxPos={MAX_POSITIONS})...")
        
        # 口座状態
        cash = INITIAL_CAPITAL
        positions = {}  # code -> {qty, entry_price, highest_price}
        equity_curve = []
        trade_log = []

        # 日付ごとにループ（ユニークな日付リスト）
        dates = self.df['date'].unique()
        
        # 高速アクセスのため辞書化またはGrouper利用
        # メモリ効率のため、日付ごとのSliceを使用
        for current_date in tqdm(dates, desc="Simulating Days"):
            # その日のデータ（現在保有中の銘柄 + シグナル銘柄）
            # ※本来は全銘柄見るべきだが、高速化のため当該日付のレコードのみ抽出
            daily_data = self.df[self.df['date'] == current_date]
            if daily_data.empty: continue
            
            # --- 1. Exit Processing (保有銘柄の決済判定) ---
            # 売りは「始値」や「安値」で判定したいが、日足バックテストの限界として
            # 「安値が逆指値に触れたら決済」とする
            
            codes_to_sell = []
            
            for code, pos in positions.items():
                row = daily_data[daily_data['code'] == code]
                if row.empty: continue # データ欠損時はスキップ（持ち越し）
                
                price_high = row.iloc[0]['high']
                price_low = row.iloc[0]['low']
                price_close = row.iloc[0]['close']
                
                # 最高値更新チェック
                if price_high > pos['highest_price']:
                    positions[code]['highest_price'] = price_high
                
                # Exit条件計算
                stop_price = pos['entry_price'] * (1 - STOP_LOSS_PCT)
                trail_price = positions[code]['highest_price'] * (1 - TRAILING_STOP_PCT)
                exit_trigger_price = max(stop_price, trail_price)
                
                if price_low <= exit_trigger_price:
                    # 決済実行（スリッページ考慮せず、トリガー価格で約定と仮定）
                    # 実際には始値と比較が必要だが、簡易的にトリガー価格で決済
                    sell_price = exit_trigger_price
                    revenue = sell_price * pos['qty']
                    cash += revenue
                    
                    profit = revenue - (pos['entry_price'] * pos['qty'])
                    profit_pct = (sell_price - pos['entry_price']) / pos['entry_price']
                    
                    trade_log.append({
                        'entry_date': pos['entry_date'],
                        'exit_date': current_date,
                        'code': code,
                        'profit': profit,
                        'return': profit_pct,
                        'reason': 'Stop/Trail'
                    })
                    codes_to_sell.append(code)
            
            # 辞書から削除
            for code in codes_to_sell:
                del positions[code]

            # --- 2. Entry Processing (新規エントリー) ---
            # ポジション枠に空きがあるか？
            open_slots = MAX_POSITIONS - len(positions)
            
            if open_slots > 0:
                # 買いシグナル銘柄を抽出
                candidates = daily_data[daily_data['entry_signal']].copy()
                
                # 既に保有している銘柄は除外
                candidates = candidates[~candidates['code'].isin(positions.keys())]
                
                if not candidates.empty:
                    # 優先順位（乖離率が低い順＝安値拾い）でソート
                    candidates = candidates.sort_values('priority_score')
                    
                    # 枠数分だけ購入
                    targets = candidates.head(open_slots)
                    
                    # 1銘柄あたりの予算（現金均等配分）
                    # ※常にフルインベストメントに近い形を目指す場合
                    # allocation = cash / open_slots 
                    # ここでは「初期資金ベースの固定枠」とする戦略もあれば、「残余現金配分」もある
                    # シンプルに「現在現金の均等配分」とする
                    if cash > 0:
                        allocation = cash / open_slots
                        
                        for _, row in targets.iterrows():
                            price = row['close'] # 終値でエントリーと仮定
                            qty = int(allocation // price)
                            
                            if qty > 0:
                                cost = qty * price
                                cash -= cost
                                positions[row['code']] = {
                                    'entry_date': current_date,
                                    'entry_price': price,
                                    'highest_price': price,
                                    'qty': qty
                                }

            # --- 3. Record Equity (資産集計) ---
            # 現在の保有株の評価額
            market_value = 0
            for code, pos in positions.items():
                row = daily_data[daily_data['code'] == code]
                if not row.empty:
                    market_value += row.iloc[0]['close'] * pos['qty']
                else:
                    # データがない日は前日評価額（＝現在値不明のため推定）
                    # 厳密には保持すべきだが、ここでは簡易化
                    market_value += pos['highest_price'] * pos['qty'] # 仮

            total_equity = cash + market_value
            equity_curve.append({'date': current_date, 'equity': total_equity})

        return pd.DataFrame(equity_curve), pd.DataFrame(trade_log)

    def print_results(self, equity_df, trades_df):
        print("\n=== PORTFOLIO SIMULATION RESULTS ===")
        if equity_df.empty:
            print("No results.")
            return

        initial = equity_df.iloc[0]['equity']
        final = equity_df.iloc[-1]['equity']
        total_ret = (final - initial) / initial
        
        # DD計算
        equity_df['max_equity'] = equity_df['equity'].cummax()
        equity_df['drawdown'] = (equity_df['equity'] - equity_df['max_equity']) / equity_df['max_equity']
        max_dd = equity_df['drawdown'].min()
        
        # 年利換算
        days = (equity_df.iloc[-1]['date'] - equity_df.iloc[0]['date']).days
        years = days / 365.25
        cagr = (final / initial) ** (1/years) - 1 if years > 0 else 0

        print(f"Period: {equity_df.iloc[0]['date'].date()} -> {equity_df.iloc[-1]['date'].date()} ({years:.1f} years)")
        print(f"Initial Capital: {initial:,.0f} JPY")
        print(f"Final Equity:    {final:,.0f} JPY")
        print(f"Total Return:    {total_ret:.2%}")
        print(f"CAGR (Annual):   {cagr:.2%}  <-- Target: 15%")
        print(f"Max Drawdown:    {max_dd:.2%} <-- Target: > -20%")
        print(f"Total Trades:    {len(trades_df)}")
        if not trades_df.empty:
            print(f"Win Rate:        {(trades_df['profit'] > 0).mean():.2%}")

if __name__ == "__main__":
    bt = PortfolioBacktester()
    bt.load_data() # 2015年から全期間
    bt.calculate_signals()
    equity, trades = bt.run_simulation()
    bt.print_results(equity, trades)

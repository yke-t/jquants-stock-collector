"""
Phase 2.6: Portfolio Backtest Engine (with Market Filter)
市場環境フィルターを追加し、ドローダウンを抑制する。
"""
import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
from pathlib import Path

# --- Config (The Safety First - Coward's Strategy) ---
DB_PATH = Path(__file__).parent.parent / "stock_data.db"
INITIAL_CAPITAL = 3000000  # 300万円

# 防御最優先設定
MAX_POSITIONS = 20         # さらに分散 (1銘柄 5% まで)
STOP_LOSS_PCT = 0.05       # 損切り -5% (ノイズで狩られても良い、大怪我を防ぐ)
TRAILING_STOP_PCT = 0.10   # トレーリング -10% (利益が出たら即確保)
DIP_THRESHOLD = 0.97       # 押し目 0.97 (極端な安値を待たない)

# 市場環境フィルター (大幅強化)
# 「小型株の4割以上が上昇トレンド」の時しか戦わない
MARKET_BULLISH_THRESHOLD = 0.40

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
        
        # トレンド判定用（市場環境フィルターに使用）
        self.df['is_bullish'] = self.df['close'] > self.df['ma_long']

        # エントリー条件: GC発生中 AND 押し目
        self.df['gc_trend'] = self.df['ma_short'] > self.df['ma_long']
        self.df['entry_signal'] = self.df['gc_trend'] & (self.df['close'] < self.df['ma_short'] * DIP_THRESHOLD)
        
        # 優先順位用スコア（乖離率：より深く押しているものを優先）
        self.df['priority_score'] = self.df['close'] / self.df['ma_short']

    def run_simulation(self):
        print(f"[INFO] Running Portfolio Simulation (Market Filter > {MARKET_BULLISH_THRESHOLD:.0%})...")
        
        cash = INITIAL_CAPITAL
        positions = {}
        equity_curve = []
        trade_log = []

        dates = self.df['date'].unique()
        
        for current_date in tqdm(dates, desc="Simulating Days"):
            daily_data = self.df[self.df['date'] == current_date]
            if daily_data.empty: continue
            
            # --- 0. Market Environment Check (NEW) ---
            # その日の全銘柄のうち、MA75を超えている銘柄の割合を計算
            # ※ daily_dataはフィルタリング済みの小型株ユニバースなので、これを市場指数として扱う
            n_stocks = len(daily_data)
            n_bullish = daily_data['is_bullish'].sum()
            market_sentiment = n_bullish / n_stocks if n_stocks > 0 else 0
            
            # 市場が「弱気（全面安）」なら、新規買いを禁止する
            allow_entry = market_sentiment >= MARKET_BULLISH_THRESHOLD
            
            # --- 1. Exit Processing ---
            codes_to_sell = []
            for code, pos in positions.items():
                row = daily_data[daily_data['code'] == code]
                if row.empty: continue
                
                price_high = row.iloc[0]['high']
                price_low = row.iloc[0]['low']
                
                if price_high > pos['highest_price']:
                    positions[code]['highest_price'] = price_high
                
                stop_price = pos['entry_price'] * (1 - STOP_LOSS_PCT)
                trail_price = positions[code]['highest_price'] * (1 - TRAILING_STOP_PCT)
                exit_trigger_price = max(stop_price, trail_price)
                
                if price_low <= exit_trigger_price:
                    sell_price = exit_trigger_price
                    revenue = sell_price * pos['qty']
                    cash += revenue
                    profit = revenue - (pos['entry_price'] * pos['qty'])
                    profit_pct = (sell_price - pos['entry_price']) / pos['entry_price']
                    trade_log.append({
                        'code': code, 'profit': profit, 'return': profit_pct, 
                        'reason': 'Stop/Trail', 'exit_date': current_date
                    })
                    codes_to_sell.append(code)
            
            for code in codes_to_sell:
                del positions[code]

            # --- 2. Entry Processing ---
            # 市場環境が良いときだけエントリー
            open_slots = MAX_POSITIONS - len(positions)
            if open_slots > 0 and allow_entry:
                candidates = daily_data[daily_data['entry_signal']].copy()
                candidates = candidates[~candidates['code'].isin(positions.keys())]
                
                if not candidates.empty:
                    candidates = candidates.sort_values('priority_score')
                    targets = candidates.head(open_slots)
                    
                    if cash > 0:
                        allocation = cash / open_slots
                        for _, row in targets.iterrows():
                            price = row['close']
                            qty = int(allocation // price)
                            if qty > 0:
                                cost = qty * price
                                cash -= cost
                                positions[row['code']] = {
                                    'entry_price': price, 'highest_price': price, 
                                    'qty': qty, 'entry_date': current_date
                                }

            # --- 3. Record Equity ---
            market_value = 0
            for code, pos in positions.items():
                row = daily_data[daily_data['code'] == code]
                if not row.empty:
                    market_value += row.iloc[0]['close'] * pos['qty']
                else:
                    market_value += pos['highest_price'] * pos['qty']

            total_equity = cash + market_value
            equity_curve.append({'date': current_date, 'equity': total_equity})

        return pd.DataFrame(equity_curve), pd.DataFrame(trade_log)

    def print_results(self, equity_df, trades_df):
        print("\n=== PORTFOLIO SIMULATION RESULTS (Final) ===")
        if equity_df.empty: return

        initial = equity_df.iloc[0]['equity']
        final = equity_df.iloc[-1]['equity']
        
        equity_df['max_equity'] = equity_df['equity'].cummax()
        equity_df['drawdown'] = (equity_df['equity'] - equity_df['max_equity']) / equity_df['max_equity']
        max_dd = equity_df['drawdown'].min()
        
        days = (equity_df.iloc[-1]['date'] - equity_df.iloc[0]['date']).days
        years = days / 365.25
        cagr = (final / initial) ** (1/years) - 1 if years > 0 else 0

        print(f"Period: {equity_df.iloc[0]['date'].date()} -> {equity_df.iloc[-1]['date'].date()} ({years:.1f} years)")
        print(f"Initial Capital: {initial:,.0f} JPY")
        print(f"Final Equity:    {final:,.0f} JPY")
        print(f"Total Return:    {(final-initial)/initial:.2%}")
        print(f"CAGR (Annual):   {cagr:.2%}  <-- Target: 15%")
        print(f"Max Drawdown:    {max_dd:.2%} <-- Target: > -20%")
        print(f"Total Trades:    {len(trades_df)}")
        if not trades_df.empty:
            print(f"Win Rate:        {(trades_df['return'] > 0).mean():.2%}")

if __name__ == "__main__":
    bt = PortfolioBacktester()
    bt.load_data()
    bt.calculate_signals()
    equity, trades = bt.run_simulation()
    bt.print_results(equity, trades)

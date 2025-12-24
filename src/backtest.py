"""
Phase 2: Backtest Engine

トレンドフォロー戦略（25/75日線GC + 押し目買い）のバックテスト。
ウォークフォワード分析（WFA）による過学習防止。

Target KPI:
- 年次リターン: 15%以上
- 最大ドローダウン: -20%以内
"""

import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime, timedelta
from pathlib import Path

# --- Config ---
DB_PATH = Path(__file__).parent.parent / "stock_data.db"

# TODO: Premium Plan契約後、market_capでフィルタリングに戻す
# MIN_CAP = 5e9   # 50億円
# MAX_CAP = 50e10 # 500億円
USE_SCALECAT_FILTER = True  # scalecatで代替フィルタリング
SCALECAT_TARGETS = ['TOPIX Small 1', 'TOPIX Small 2', 'TOPIX Mid400']  # 小型・中型株


class NisaJQuantBacktester:
    """NISA小型株トレンドフォロー戦略のバックテスター"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = Path(db_path)
        self.conn = None
        self.df = pd.DataFrame()
        
    def connect(self):
        """DBに接続"""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        self.conn = sqlite3.connect(self.db_path)
        print(f"[DB] Connected to {self.db_path}")
        
    def close(self):
        """DB接続を閉じる"""
        if self.conn:
            self.conn.close()
            
    def load_data(self, start_date="2014-01-01"):
        """データベースから価格とファンダメンタルズデータを読み込む"""
        print("[INFO] Loading data from DB...")
        
        if USE_SCALECAT_FILTER:
            # scalecatでフィルタリング（market_cap代替）
            scalecat_list = "', '".join(SCALECAT_TARGETS)
            # FIX: AND f.scalecat IN (...) を追加
            query = f"""
            SELECT p.date, p.code, p.open, p.high, p.low, p.close, 
                   p.adjustmentclose as adj_close, p.volume, f.scalecat
            FROM prices p
            LEFT JOIN fundamentals f ON p.code = f.code
            WHERE p.date >= '{start_date}'
            AND f.scalecat IN ('{scalecat_list}')
            ORDER BY p.code, p.date ASC
            """
        else:
            # TODO: market_capフィルタリング（将来実装）
            query = f"""
            SELECT p.date, p.code, p.open, p.high, p.low, p.close,
                   p.adjustmentclose as adj_close, p.volume
            FROM prices p
            WHERE p.date >= '{start_date}'
            ORDER BY p.code, p.date ASC
            """
        
        try:
            self.df = pd.read_sql(query, self.conn, parse_dates=['date'])
            print(f"[INFO] Loaded {len(self.df)} records, {self.df['code'].nunique()} stocks")
            
            # データ期間を表示
            if not self.df.empty:
                min_date = self.df['date'].min()
                max_date = self.df['date'].max()
                print(f"[INFO] Date range: {min_date.date()} to {max_date.date()}")
                
        except Exception as e:
            print(f"[ERROR] Failed to load data: {e}")
            self.df = pd.DataFrame()

    def calculate_indicators(self, df, ma_short=25, ma_long=75):
        """テクニカル指標の計算"""
        df = df.copy()
        
        # 移動平均線
        df['ma_short'] = df.groupby('code')['close'].transform(
            lambda x: x.rolling(ma_short, min_periods=ma_short).mean()
        )
        df['ma_long'] = df.groupby('code')['close'].transform(
            lambda x: x.rolling(ma_long, min_periods=ma_long).mean()
        )
        
        # ゴールデンクロス条件（トレンドフィルター）
        df['gc_trend'] = df['ma_short'] > df['ma_long']
        
        # 25日線からの乖離率
        df['ma_deviation'] = df['close'] / df['ma_short']
        
        return df

    def run_strategy(self, df, params):
        """
        戦略ロジックの実行
        
        Entry: GC形成中 かつ 25日線乖離率が閾値以下（押し目）
        Exit: トレーリングストップ または 損切り
        """
        trades = []
        
        dip_threshold = params.get('dip_threshold', 0.98)  # 25日線の98%以下で買い
        stop_loss_pct = params.get('stop_loss', 0.10)      # -10%で損切り
        trailing_stop_pct = params.get('trailing_stop', 0.15)  # 最高値から-15%で決済

        for code, sub_df in df.groupby('code'):
            sub_df = sub_df.sort_values('date')
            position = None
            entry_price = 0
            entry_date = None
            high_since_entry = 0

            for _, row in sub_df.iterrows():
                if pd.isna(row['ma_short']) or pd.isna(row['ma_long']):
                    continue

                # --- Entry Logic ---
                if position is None:
                    # トレンドが上向き(GC) かつ 押し目(価格 < MA短期 * 閾値)
                    if row['gc_trend'] and (row['close'] < row['ma_short'] * dip_threshold):
                        position = 'LONG'
                        entry_price = row['close']
                        entry_date = row['date']
                        high_since_entry = entry_price
                
                # --- Exit Logic ---
                elif position == 'LONG':
                    # 高値更新
                    if row['high'] > high_since_entry:
                        high_since_entry = row['high']
                    
                    # 1. Stop Loss
                    if row['low'] <= entry_price * (1 - stop_loss_pct):
                        exit_price = entry_price * (1 - stop_loss_pct)
                        trades.append({
                            'code': code,
                            'entry_date': entry_date,
                            'exit_date': row['date'],
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'return': (exit_price - entry_price) / entry_price,
                            'reason': 'StopLoss'
                        })
                        position = None
                    
                    # 2. Trailing Stop
                    elif row['low'] <= high_since_entry * (1 - trailing_stop_pct):
                        exit_price = high_since_entry * (1 - trailing_stop_pct)
                        trades.append({
                            'code': code,
                            'entry_date': entry_date,
                            'exit_date': row['date'],
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'return': (exit_price - entry_price) / entry_price,
                            'reason': 'Trailing'
                        })
                        position = None

        return pd.DataFrame(trades)

    def walk_forward_analysis(self, n_splits=5):
        """
        ウォークフォワード分析
        In-Sampleで最適化 → Out-of-Sampleで検証を繰り返す
        """
        if self.df.empty:
            print("[ERROR] No data to backtest.")
            return None

        print(f"\n{'='*50}")
        print(f"Walk-Forward Analysis (Splits: {n_splits})")
        print('='*50)
        
        # データにインジケーターを計算
        df = self.calculate_indicators(self.df)
        
        # 全期間のユニークな日付を取得して分割
        dates = sorted(df['date'].unique())
        
        if len(dates) < n_splits + 1:
            print(f"[WARN] Not enough data for {n_splits} splits. Using available data.")
            n_splits = max(1, len(dates) // 2)
        
        split_size = len(dates) // (n_splits + 1)
        
        if split_size == 0:
            print("[ERROR] Not enough data for walk-forward analysis.")
            return None
        
        total_results = []
        
        # パラメータグリッド（最適化対象）
        param_grid = [
            {'dip_threshold': 0.98, 'stop_loss': 0.10, 'trailing_stop': 0.10},
            {'dip_threshold': 0.95, 'stop_loss': 0.10, 'trailing_stop': 0.15},
            {'dip_threshold': 1.00, 'stop_loss': 0.08, 'trailing_stop': 0.10},
            {'dip_threshold': 0.97, 'stop_loss': 0.12, 'trailing_stop': 0.12},
        ]

        for i in range(n_splits):
            # 期間設定
            train_start_idx = i * split_size
            train_end_idx = (i + 1) * split_size
            test_end_idx = min((i + 2) * split_size, len(dates) - 1)
            
            train_start = dates[train_start_idx]
            train_end = dates[train_end_idx]
            test_start = train_end
            test_end = dates[test_end_idx]
            
            print(f"\nPeriod {i+1}: Train[{pd.Timestamp(train_start).date()} ~ {pd.Timestamp(train_end).date()}]")
            print(f"          Test[{pd.Timestamp(test_start).date()} ~ {pd.Timestamp(test_end).date()}]")

            # --- Optimization Phase (In-Sample) ---
            train_data = df[(df['date'] >= train_start) & (df['date'] < train_end)]
            
            best_param = None
            best_score = -999
            
            for param in param_grid:
                trades = self.run_strategy(train_data, param)
                if not trades.empty:
                    score = trades['return'].mean()
                else:
                    score = 0
                
                if score > best_score:
                    best_score = score
                    best_param = param
            
            if best_param:
                print(f"  Best Param: dip={best_param['dip_threshold']}, "
                      f"sl={best_param['stop_loss']}, ts={best_param['trailing_stop']} "
                      f"(Score: {best_score:.2%})")

            # --- Validation Phase (Out-of-Sample) ---
            test_data = df[(df['date'] >= test_start) & (df['date'] < test_end)]
            
            if best_param and not test_data.empty:
                oos_trades = self.run_strategy(test_data, best_param)
                if not oos_trades.empty:
                    oos_trades['period'] = i + 1
                    total_results.append(oos_trades)
                    
                    win_rate = (oos_trades['return'] > 0).mean()
                    print(f"  Test: Return={oos_trades['return'].mean():.2%}, "
                          f"WinRate={win_rate:.2%}, Trades={len(oos_trades)}")
                else:
                    print("  Test: No trades in test period.")

        # --- Final KPI Calculation ---
        if total_results:
            all_trades = pd.concat(total_results)
            return self.calculate_kpi(all_trades)
        else:
            print("\n[WARN] No trades generated across all periods.")
            return None

    def calculate_kpi(self, trades):
        """目標KPIに対する評価"""
        print("\n" + "="*50)
        print("Final Walk-Forward KPI")
        print("="*50)
        
        total_trades = len(trades)
        avg_return = trades['return'].mean()
        win_rate = (trades['return'] > 0).mean()
        
        # Drawdown Calculation
        trades = trades.sort_values('exit_date')
        trades['cum_return'] = trades['return'].cumsum()
        trades['rolling_max'] = trades['cum_return'].cummax()
        trades['drawdown'] = trades['cum_return'] - trades['rolling_max']
        max_dd = trades['drawdown'].min()

        print(f"Total Trades: {total_trades}")
        print(f"Avg Return per Trade: {avg_return:.2%}")
        print(f"Win Rate: {win_rate:.2%}")
        print(f"Max Drawdown: {max_dd:.2%}")
        
        # KPI評価
        print("\n--- KPI Evaluation ---")
        
        dd_passed = max_dd >= -0.20
        print(f"Max DD > -20%: {'PASSED' if dd_passed else 'FAILED'} ({max_dd:.2%})")
        
        # 年換算リターン（概算）
        if total_trades > 0:
            # 平均保有期間を計算
            trades['hold_days'] = (trades['exit_date'] - trades['entry_date']).dt.days
            avg_hold_days = trades['hold_days'].mean()
            trades_per_year = 252 / avg_hold_days if avg_hold_days > 0 else 0
            annual_return = avg_return * trades_per_year
            
            return_passed = annual_return >= 0.15
            print(f"Annual Return > 15%: {'PASSED' if return_passed else 'FAILED'} ({annual_return:.2%})")
        
        return {
            'total_trades': total_trades,
            'avg_return': avg_return,
            'win_rate': win_rate,
            'max_drawdown': max_dd,
            'trades': trades
        }

    def run(self, start_date="2014-01-01"):
        """バックテストを実行"""
        try:
            self.connect()
            self.load_data(start_date)
            
            if self.df.empty:
                print("[ERROR] No data available for backtest.")
                return None
            
            return self.walk_forward_analysis()
            
        finally:
            self.close()


if __name__ == "__main__":
    bt = NisaJQuantBacktester()
    results = bt.run()

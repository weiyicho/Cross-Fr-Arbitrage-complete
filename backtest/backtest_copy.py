"""
回測系統模組

提供資金費率套利策略的回測功能，支援：
- 跨交易所套利
- 單交易所策略
- 投資組合管理
- 性能可視化
"""

# Standard library
import os
import math
from pathlib import Path
from functools import reduce

# Third-party libraries
import numpy as np
import pandas as pd
import seaborn as sns
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


class Backtester_Copy():
    """Backtester supporting both cross-exchange and single-exchange strategies."""
    
    def __init__(self, exchange1, exchange2=None):
        """
        Initialize backtester.
        
        Args:
            exchange1: First exchange name (e.g., 'binance')
            exchange2: Second exchange name (None for single-exchange mode)
        """
        TIME = "Time"
        self.TIME = TIME
        self.exchange1 = exchange1
        self.exchange2 = exchange2
        self.is_cross_exchange = exchange2 is not None
        
        # Set path based on mode
        if self.is_cross_exchange:
            exchanges = sorted([exchange1, exchange2])
            self.exchange1 = exchanges[0]
            self.exchange2 = exchanges[1]
            self.path = Path(__file__).parent.parent / "data" / "merge" / f"{self.exchange1}_{self.exchange2}" / "funding_rates"
            mode_info = f"Cross-exchange mode: {self.exchange1} vs {self.exchange2}"
        else:
            self.path = Path(__file__).parent.parent / "data" / "clean" / exchange1 / "funding_rates"
            mode_info = f"Single-exchange mode: {self.exchange1}"
        
        self.shared_symbols = [f.stem for f in self.path.glob("*.parquet")]
        
        # 簡潔的初始化信息
        print(f"Backtester: {mode_info} | Symbols: {len(self.shared_symbols)}")
        if not os.path.exists(self.path):
            print(f"⚠️ Warning: Directory not found at {self.path}")
    
    def _normalize_symbol(self, symbol: str) -> str:
        """
        Converts a trading symbol to a normalized format for file reading.
        Handles various formats:
        - 'BTC/USDT' → 'BTCUSDT'
        - 'BTC/USDT:USDT' → 'BTCUSDT'
        - 'ETHUSDT:USDT' → 'ETHUSDT'
        - 'BTCUSDT' → 'BTCUSDT' (already normalized)
        - 'ETH' → 'ETHUSDT' (assumes USDT quote)
        - 'BTC' → 'BTCUSDT' (assumes USDT quote)
        
        Args:
            symbol: The trading symbol
            
        Returns:
            str: Normalized symbol representation
        """
        # Handle perpetual futures notation (BTC/USDT:USDT or ETHUSDT:USDT)
        if ':' in symbol:
            symbol = symbol.split(':')[0]  # Take the part before the colon
            
        # Handle standard notation (BTC/USDT)
        if '/' in symbol:
            base, quote = symbol.split('/')
            return f"{base}{quote}"
            
        # Handle other separators like dash (BTC-USDT)
        if '-' in symbol:
            base, quote = symbol.split('-')
            return f"{base}{quote}"
            
        # Handle base currency only (e.g., 'ETH' → 'ETHUSDT', 'FARTCOIN' → 'FARTCOINUSDT', '4' → '4USDT')
        if '/' not in symbol and ':' not in symbol and '-' not in symbol:
            if not symbol.endswith('USDT'):
                return f"{symbol}USDT"
            return symbol
            
        # Fallback
        return symbol.replace('/', '').replace('-', '').replace(':', '')

    # ============================================================
    # Main Backtesting Logic
    # ============================================================
    
    def backtest_fundingrate(self, symbol, n_days=2, threshold=0.1,
                         out_threshold=0.05, value_threshold=1000, value_out_threshold=1000,
                         fee=0.0005, position=10000, leverage=1, start_date=None, spread_threshold=0.01,spread_enter =False):
        """
        Backtest funding rate arbitrage for a single symbol.
        
        Strategy:
            - Cross-exchange: Profit from funding rate differences between exchanges
            - Single-exchange: Profit from directional funding rate positions
        
        Args:
            symbol: Trading pair (e.g., 'BTC/USDT', 'ETHUSDT')
            n_days: Rolling window for signal calculation (default: 2 days)
            threshold: Entry threshold - annualized FR % (default: 0.1 = 10%)
            out_threshold: Exit threshold - annualized FR % (default: 0.05 = 5%)
            value_threshold: Min volume×price to enter (default: 1000 USD)
            value_out_threshold: Min volume×price to stay in (default: 1000 USD)
            fee: Trading fee per side (default: 0.0005 = 0.05%)
            position: Position size in USD (default: 10000)
            leverage: Leverage multiplier (default: 1)
            start_date: Backtest start date (default: None = use all data)
            spread_threshold: Min price spread % to enter (default: 0.01 = 1%)
        
        Returns:
            (DataFrame, symbol): Backtest results with PnL columns, or (None, symbol) if failed
        """
        # Load and validate data
        normalized_symbol = self._normalize_symbol(symbol)
        df = pd.read_parquet(os.path.join(self.path, f"{normalized_symbol}.parquet"))
        if df.empty or len(df) == 0:
            print(f"[{symbol}] ⚠️ No data available")
            return None, symbol
        
        # Time filtering
        df['Time'] = pd.to_datetime(df['Time'])
        if start_date is not None:
            df = df[df['Time'] >= start_date].reset_index(drop=True)
        
        # Convert parameters to hourly basis
        window_hours = int(n_days * 24)
        entry_threshold_hourly = threshold / (365 * 24)
        exit_threshold_hourly = out_threshold / (365 * 24)

        # ---- Calculate rolling mean signal ----
        if self.is_cross_exchange:
            # Cross-exchange: use funding rate difference
            df['diff_mean'] = df['Diff'].rolling(window=window_hours).mean()
            signal_mean = df['diff_mean']
        else:
            # Single-exchange: use absolute funding rate
            if 'FundingRate_hourly' in df.columns:
                df['fr_mean'] = df['FundingRate_hourly'].rolling(window=window_hours).mean()
            else:
                print(f"[{symbol}] ❌ Missing 'FundingRate_hourly' | Available: {list(df.columns)}")
                return None, symbol
            signal_mean = df['fr_mean']
        
        # ---- Volume filters ----
        value_cols = [col for col in df.columns if col.endswith("_Value")]
        volume_condition = True
        exit_volume_condition = False
        
        if self.is_cross_exchange:
            if len(value_cols) != 2:
                print(f"[{symbol}] ❌ Expected 2 value columns, found {len(value_cols)}")
                return None, symbol
            value_mean1 = df[value_cols[0]].rolling(window=window_hours).mean()
            value_mean2 = df[value_cols[1]].rolling(window=window_hours).mean()
            volume_condition = (value_mean1 > value_threshold) & (value_mean2 > value_threshold)
            exit_volume_condition = (value_mean1 < value_out_threshold) | (value_mean2 < value_out_threshold)
        else:
            # Single exchange: only one volume column
            if 'Value' not in df.columns:
                print(f"[{symbol}] ❌ Missing 'Value' column")
                return None, symbol
            
            value_mean = df['Value'].rolling(window=window_hours).mean()
            volume_condition = value_mean > value_threshold
            exit_volume_condition = value_mean < value_out_threshold 
            
        
        # ---- Entry/exit conditions ----
        entry_condition = (abs(signal_mean) > entry_threshold_hourly) & volume_condition
        exit_condition = exit_volume_condition

        can_enter_long = entry_condition & (signal_mean < 0)  # Long when FR diff is negative
        can_enter_short = entry_condition & (signal_mean > 0)  # Short when FR diff is positive
        
        # ---- Position tracking arrays (for fast vectorized access) ----
        signals = np.zeros(len(df), dtype=int)
        position_state = 0  # 0=flat, 1=long, -1=short

        if self.is_cross_exchange:
            signal_arr = df['diff_mean'].values
            price_exchange1 = df[self.exchange1.capitalize() + '_Open'].values
            price_exchange2 = df[self.exchange2.capitalize() + '_Open'].values
        else:
            signal_arr = df['fr_mean'].values
        
        can_enter_long_arr = can_enter_long.values
        can_enter_short_arr = can_enter_short.values
        exit_cond_arr = exit_condition.values
        
        # Single-exchange mode: disable long entries (only short funding rate)
        if not self.is_cross_exchange:
            can_enter_long_arr = np.zeros(len(can_enter_long), dtype=bool)
        
        # ---- Position tracking loop ----
        for i in range(len(df)):
            current_signal = signal_arr[i]
            
            if position_state == 0:
                # No position: check entry conditions
                if can_enter_long_arr[i]:
                    if self.is_cross_exchange and spread_enter:
                        # Long: buy cheaper exchange, sell expensive exchange
                        spread = (price_exchange2[i] - price_exchange1[i]) / price_exchange2[i]
                        if spread > spread_threshold:
                            position_state = 1
                    else:
                        # Single-exchange: always allow long (no spread check needed)
                        position_state = 1
                    
                elif can_enter_short_arr[i]:
                    if self.is_cross_exchange and spread_enter:
                        # Short: sell expensive exchange, buy cheaper exchange
                        spread = (price_exchange1[i] - price_exchange2[i]) / price_exchange1[i]
                        if spread > spread_threshold:
                            position_state = -1
                    else:
                        # Single-exchange: always allow short (no spread check needed)
                        position_state = -1
            else:
                # Already in position: check exit conditions
                should_exit = False
                
                if exit_cond_arr[i]:
                    should_exit = True
                elif exit_threshold_hourly > 0:
                    if position_state == 1:
                        # Long: exit if signal weakens or reverses
                        should_exit = (abs(current_signal) <= exit_threshold_hourly) or (current_signal >= 0)
                    else:
                        # Short: exit if signal weakens or reverses
                        should_exit = (abs(current_signal) <= exit_threshold_hourly) or (current_signal <= 0)
                else:
                    # Negative threshold logic (legacy)
                    if position_state == 1:
                        should_exit = (current_signal * -1 < exit_threshold_hourly)
                    else:
                        should_exit = (current_signal < exit_threshold_hourly)
                
                if should_exit:
                    position_state = 0
            
            signals[i] = position_state

        # ---- Calculate PnL components ----
        df['signal'] = signals
        df['position'] = position
        entry_flag = (df['signal'] != 0) & (df['signal'].shift(fill_value=0) == 0)
        exit_flag = (df['signal'] == 0) & (df['signal'].shift(fill_value=0) != 0)
        should_count_funding = (df['signal'] != 0) & (~entry_flag)

        # Base PnL: funding rate payments (no fees)
        if self.is_cross_exchange:
            pnl = df['signal'] * df[self.exchange1.capitalize() + '_FR_1H'] * -1 + df['signal'] * df[self.exchange2.capitalize() + '_FR_1H']
            base_return = np.where(should_count_funding, pnl, 0)
        else:
            base_return = np.where(should_count_funding, df['FundingRate_hourly'] * df['signal'] * (-1), 0)
        
        # Trading fees
        open_fee = np.where(entry_flag, -fee, 0)
        close_fee = np.where(exit_flag, -fee, 0)

        # ---- Spread PnL (cross-exchange only) ----
        if self.is_cross_exchange:
            signal_arr = df['signal'].values
            
            # Find entry and exit points (vectorized)
            signal_shift_prev = np.concatenate([[0], signal_arr[:-1]])
            signal_shift_next = np.concatenate([signal_arr[1:], [0]])
            
            entry_points = (signal_arr != 0) & (signal_shift_prev == 0)
            exit_points = (signal_arr != 0) & (signal_shift_next == 0)
            
            trade_points = entry_points | exit_points
            result_indices = np.where(trade_points)[0]
            
            df['spread_pnl'] = 0.0
            
            open_col = [col for col in df.columns if col.endswith("_Open")]
            if len(open_col) >= 2:
                exchange1_open_arr = df[open_col[0]].values
                exchange2_open_arr = df[open_col[1]].values
                
                # Calculate spread PnL at each trade point
                if len(result_indices) > 1:
                    for idx in result_indices[:-1]:
                        signal_val = signal_arr[idx]
                        open1 = exchange1_open_arr[idx]
                        open2 = exchange2_open_arr[idx]
                        spread_pnl = (open1 - open2) / open2 * signal_val
                        df.at[idx, 'spread_pnl'] = spread_pnl
        else:
            df['spread_pnl'] = 0
        
        # ---- Final PnL calculations ----
        df['base_pnl'] = base_return * leverage
        if self.is_cross_exchange:
            df['base_pnl_with_fee_no_spread'] = base_return + (open_fee + close_fee) * 2
            df['base_pnl_with_fee'] = base_return + (open_fee + close_fee) * 2 + df['spread_pnl'].fillna(0)
        else:
            df['base_pnl_with_fee_no_spread'] = base_return + (open_fee + close_fee)
            df['base_pnl_with_fee'] = base_return + (open_fee + close_fee) + df['spread_pnl'].fillna(0)
        
        df['cumulative_pnl'] = df['base_pnl'].cumsum()
        df['cumulative_pnl_with_fee_no_spread'] = df['base_pnl_with_fee_no_spread'].cumsum()
        df['cumulative_pnl_with_fee'] = df['base_pnl_with_fee'].cumsum()
        df['prev_signal'] = df['signal'].shift(1, fill_value=0)
        df['n_0to1'] = ((df['prev_signal'] == 0) & (df['signal'] != 0)).astype(int)
        df['n_1to0'] = ((df['prev_signal'] != 0) & (df['signal'] == 0)).astype(int)
        df['drawdown'] = df['cumulative_pnl_with_fee'].cummax() - df['cumulative_pnl_with_fee']
        
        return df, symbol

    # ============================================================
    # Portfolio Backtesting
    # ============================================================
        
    def process_mutiple_symbols(self, symbols, **kwargs):
        """
        Process multiple symbols in parallel for portfolio backtesting.
        
        Args:
            symbols: List of trading symbols to backtest
            **kwargs: Arguments passed to backtest_fundingrate()
            
        Returns:
            (all_hourly_data, failed_symbols): Merged DataFrame and list of failed symbols
        """
        symbol_data_list = []
        failed_symbols = []

        # Parallel processing with thread pool
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_symbol = {
                executor.submit(self.backtest_fundingrate, symbol, **kwargs): symbol
                for symbol in symbols
            }
            with tqdm(total=len(symbols), desc="Processing symbols") as pbar:
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        hourly_data, symbol = future.result()
                        if hourly_data is not None and not hourly_data.empty and len(hourly_data) > 0:
                            # Extract relevant columns for portfolio
                            symbol_data = hourly_data[[self.TIME, 'signal', 'n_0to1', 'base_pnl', 
                                                      'base_pnl_with_fee_no_spread', 'base_pnl_with_fee']].copy()
                            symbol_data = symbol_data.rename(columns={
                                'base_pnl': f'{symbol}_base_pnl',
                                'n_0to1': f'{symbol}_n_0to1',
                                'signal': f'{symbol}_signal',
                                'base_pnl_with_fee_no_spread': f'{symbol}_base_pnl_with_fee_no_spread',
                                'base_pnl_with_fee': f'{symbol}_base_pnl_with_fee'
                            })
                            symbol_data_list.append(symbol_data)
                        else:
                            print(f"[{symbol}] ⚠️ Warning: No data returned")
                            failed_symbols.append(symbol)
                    except Exception as e:
                        print(f"[{symbol}] ❌ Error: {e}")
                        failed_symbols.append(symbol)
                    pbar.update(1)
        
        # Batch merge for better performance
        print(f"\nMerging {len(symbol_data_list)} symbols...")
        
        if symbol_data_list:
            batch_size = 50
            num_batches = math.ceil(len(symbol_data_list) / batch_size)
            
            batch_results = []
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(symbol_data_list))
                batch = symbol_data_list[start_idx:end_idx]
                
                batch_result = reduce(
                    lambda left, right: pd.merge(left, right, on=self.TIME, how='outer'),
                    batch
                )
                batch_results.append(batch_result)
            
            # Final merge
            if len(batch_results) == 1:
                all_hourly_data = batch_results[0]
            else:
                all_hourly_data = reduce(
                    lambda left, right: pd.merge(left, right, on=self.TIME, how='outer'),
                    batch_results
                )
            
            # Sort and fill NaN
            all_hourly_data = all_hourly_data.sort_values(self.TIME).reset_index(drop=True)
            signal_cols = [col for col in all_hourly_data.columns if '_signal' in col or '_base_pnl' in col]
            all_hourly_data[signal_cols] = all_hourly_data[signal_cols].fillna(0)
        else:
            all_hourly_data = pd.DataFrame(columns=[self.TIME, 'signal', 'n_0to1', 'base_pnl', 
                                                    'base_pnl_with_fee_no_spread', 'base_pnl_with_fee'])
        
        print(f"✓ Merge completed | Success: {len(symbol_data_list)}/{len(symbols)} | Failed: {len(failed_symbols)}")
        if failed_symbols and len(failed_symbols) <= 5:
            print(f"  Failed: {', '.join(failed_symbols)}")
        elif failed_symbols:
            print(f"  Failed: {', '.join(failed_symbols[:5])} ... (+{len(failed_symbols)-5} more)")
        
        return all_hourly_data, failed_symbols

    def create_portfolio_summary(self, df, max_active_positions=3, **kwargs):
        """
        Construct portfolio from multiple symbols using top PnL selection.
        
        Strategy:
            - Select top max_active_positions symbols by current PnL
            - Equal weight allocation (1/N per symbol)
            - Track entry-time allocation for accurate exit fee calculation
        
        Args:
            df: Merged DataFrame with all symbols' data
            max_active_positions: Maximum concurrent positions (default: 3)
            
        Returns:
            (portfolio_df, stats, coin_portfolio_df): Portfolio summary, statistics, and per-coin tracking
        """
        active_symbols = []
        symbol_columns = [c for c in df.columns if c.endswith('_base_pnl')]
        symbols = [c[: -len('_base_pnl')] for c in symbol_columns]
        if not symbols:
            raise ValueError("No symbols detected from '*_base_pnl' columns.")

        # Initialize portfolio tracking dictionaries
        portfolio_data = {
            self.TIME: [],
            'n_0to1': [],
            'n_1to0': [],
            'active_symbols': [],
            'active_positions': [],
            'allocation': [],
            'base_pnl': [],
            'base_pnl_with_fee_no_spread': [],
            'base_pnl_with_fee': [],
        }
        
        # Add exchange-specific position columns
        if self.is_cross_exchange:
            portfolio_data[self.exchange1] = []
            portfolio_data[self.exchange2] = []
        else:
            portfolio_data['long_trades'] = []
            portfolio_data['short_trades'] = []
        
        signal_cols = [f"{symbol}_signal" for symbol in symbols]
        signal_dict = {i: symbols[i] for i in range(len(symbols))}
        pnl_dict = {symbols[i]: f"{symbols[i]}_base_pnl" for i in range(len(symbols))}
        
        # Pre-convert to NumPy arrays for fast vectorized access
        time_arr = df[self.TIME].values
        signal_arrays = {col: df[col].values for col in signal_cols}
        base_pnl_arrays = {f"{s}_base_pnl": df[f"{s}_base_pnl"].values for s in symbols}
        fee_arrays = {f"{s}_base_pnl_with_fee": df[f"{s}_base_pnl_with_fee"].values for s in symbols}
        fee_no_spread_arrays = {f"{s}_base_pnl_with_fee_no_spread": df[f"{s}_base_pnl_with_fee_no_spread"].values for s in symbols}
        
        # Pre-compute all signals matrix (rows=time, cols=symbols)
        all_signals = np.column_stack([signal_arrays[col] for col in signal_cols])
        
        # Per-coin portfolio tracking: [base_pnl, fee_no_spread, total_fee]
        coin_portfolio_data = [[], [], []]
        prev_signals = None
        
        # ---- Main portfolio construction loop ----
        for idx in tqdm(range(len(df)), desc="Building portfolio"):
            # Get active signals at this timestamp
            row_signals = all_signals[idx]
            potential_indices = np.where(row_signals != 0)[0]
            potential_symbols = [signal_dict[i] for i in potential_indices] 
            
            # Update active positions
            prev_active_symbols = active_symbols.copy()
            active_symbols = [s for s in active_symbols if s in potential_symbols]
            new_potential_symbols = [s for s in potential_symbols if s not in active_symbols]

            # Add new positions if slots available (select top PnL)
            if len(active_symbols) < max_active_positions and len(new_potential_symbols) > 0:
                slots_available = max_active_positions - len(active_symbols)
                pnl_values = {s: base_pnl_arrays[pnl_dict[s]][idx] for s in new_potential_symbols}
                new_symbols_to_add = sorted(pnl_values.keys(), key=lambda x: pnl_values[x], reverse=True)[:slots_available]
                active_symbols += new_symbols_to_add
            
            # Calculate position metrics
            current_signals = row_signals
            prev_signals_row = prev_signals if prev_signals is not None else np.zeros(len(symbols))
            
            n_0to1 = ((prev_signals_row == 0) & (current_signals != 0)).sum()  # New entries
            n_1to0 = ((prev_signals_row != 0) & (current_signals == 0)).sum()  # Exits
            long_trades = int((current_signals > 0).sum())
            short_trades = int((current_signals < 0).sum())
            prev_signals = current_signals.copy()
            
            # Initialize coin-level tracking
            coin_row_base_pnl = {s: 0 for s in symbols}
            coin_row_fee_no_spread = {s: 0 for s in symbols}
            coin_row_total_fee = {s: 0 for s in symbols}
            
            # Calculate portfolio PnL with equal weighting
            allocation = 1
            if len(active_symbols) > 0:
                if len(active_symbols) > 1:
                    allocation = 1 / len(active_symbols)
                
                # Aggregate PnL from active symbols (single pass)
                total_base_pnl = 0
                total_fee_no_spread = 0
                total_fee = 0
                
                for s in active_symbols:
                    base_pnl_val = base_pnl_arrays[f"{s}_base_pnl"][idx]
                    fee_no_spread_val = fee_no_spread_arrays[f"{s}_base_pnl_with_fee_no_spread"][idx]
                    total_fee_val = fee_arrays[f"{s}_base_pnl_with_fee"][idx]
                    
                    coin_row_base_pnl[s] = base_pnl_val * allocation
                    coin_row_fee_no_spread[s] = fee_no_spread_val * allocation
                    coin_row_total_fee[s] = total_fee_val * allocation
                    
                    total_base_pnl += base_pnl_val * allocation
                    total_fee_no_spread += fee_no_spread_val * allocation
                    total_fee += total_fee_val * allocation
            else:
                total_base_pnl = 0
                total_fee_no_spread = 0
                total_fee = 0
                allocation = 0
            
            # Handle exited symbols: apply previous allocation to exit fees
            if len(active_symbols) < len(prev_active_symbols):
                exited_symbols = set(prev_active_symbols) - set(active_symbols)
                exit_fee_no_spread = 0
                exit_total_fee = 0
                prev_allocation = 1 / len(prev_active_symbols)
                
                for s in exited_symbols:
                    fee_no_spread_val = fee_no_spread_arrays[f"{s}_base_pnl_with_fee_no_spread"][idx]
                    total_fee_val = fee_arrays[f"{s}_base_pnl_with_fee"][idx]
                    
                    # Track coin-level exit fees
                    coin_row_fee_no_spread[s] = fee_no_spread_val * prev_allocation
                    coin_row_total_fee[s] = total_fee_val * prev_allocation
                    
                    # Add to portfolio exit fees
                    exit_fee_no_spread += fee_no_spread_val * prev_allocation
                    exit_total_fee += total_fee_val * prev_allocation
                
                total_fee_no_spread += exit_fee_no_spread
                total_fee += exit_total_fee
            
            # Record portfolio state
            portfolio_data[self.TIME].append(time_arr[idx])
            
            if self.is_cross_exchange:
                portfolio_data[self.exchange1].append(long_trades)
                portfolio_data[self.exchange2].append(short_trades)
            else:
                portfolio_data['long_trades'].append(long_trades)
                portfolio_data['short_trades'].append(short_trades)
            
            portfolio_data['n_0to1'].append(n_0to1)
            portfolio_data['n_1to0'].append(n_1to0)
            portfolio_data['active_symbols'].append(active_symbols.copy())
            portfolio_data['active_positions'].append(len(active_symbols))
            portfolio_data['allocation'].append(allocation)
            portfolio_data['base_pnl'].append(total_base_pnl)
            portfolio_data['base_pnl_with_fee_no_spread'].append(total_fee_no_spread)
            portfolio_data['base_pnl_with_fee'].append(total_fee)
            
            # Track coin-level allocations
            coin_portfolio_data[0].append(coin_row_base_pnl.copy())
            coin_portfolio_data[1].append(coin_row_fee_no_spread.copy())
            coin_portfolio_data[2].append(coin_row_total_fee.copy())
        
        # Build final portfolio DataFrame
        portfolio_df = pd.DataFrame(portfolio_data)
        portfolio_df['cumulative_pnl'] = portfolio_df['base_pnl'].cumsum()
        portfolio_df['cumulative_pnl_with_fee_no_spread'] = portfolio_df['base_pnl_with_fee_no_spread'].cumsum() 
        portfolio_df['cumulative_pnl_with_fee'] = portfolio_df['base_pnl_with_fee'].cumsum()
        portfolio_df['drawdown'] = portfolio_df['cumulative_pnl_with_fee'].cummax() - portfolio_df['cumulative_pnl_with_fee']
        
        # Calculate statistics
        stats = self.calculate_stats(portfolio_df, single=False)
        
        # Build coin-level portfolio DataFrames [base_pnl, fee_no_spread, total_fee]
        coin_portfolio_df = [pd.DataFrame(data, index=portfolio_df[self.TIME]) for data in coin_portfolio_data]
        
        return portfolio_df, stats, coin_portfolio_df

    def backtest_portfolio(self, symbols=None, all_symbols=False, max_active_positions=3, **kwargs):
        """
        Portfolio backtest workflow.
        
        Args:
            symbols: List of symbols (ignored if all_symbols=True)
            all_symbols: If True, use all symbols from merge folder
            max_active_positions: Max concurrent positions (default: 3)
            **kwargs: Parameters for backtest_fundingrate()
        
        Returns:
            (portfolio_df, stats, all_symbols_df, failed_symbols)
        """
        # Load all symbols from merge folder if all_symbols=True
        if all_symbols:
            symbols = self.shared_symbols
            print(f"Auto-loaded {len(symbols)} symbols | Max positions: {max_active_positions}")
        elif symbols is None:
            raise ValueError("Either provide 'symbols' list or set 'all_symbols=True'")
            
        df, failed_symbols = self.process_mutiple_symbols(symbols, **kwargs)
        df.fillna(0, inplace=True)
        portfolio_df,stat,coin_porfolio_df = self.create_portfolio_summary(df, max_active_positions)
        return portfolio_df,stat,coin_porfolio_df,failed_symbols



    def symbols_summary(self, df, time_col='Time'):
        """Summary stats for coin-level DataFrame (columns=symbols, values=PnL)."""
        symbol_columns = [c for c in df.columns if c != time_col]
        result = []
        
        for symbol in symbol_columns:
            pnl = df[symbol]
            is_active = (pnl != 0).astype(int)
            transitions = ((is_active.shift(fill_value=0) == 0) & (is_active == 1)).sum() + \
                         ((is_active.shift(fill_value=0) == 1) & (is_active == 0)).sum()
            
            result.append({
                'symbol': symbol,
                'enter_exit_time': transitions,
                'time in the market': (pnl != 0).sum(),
                'pnl': pnl.sum(),
            })
        
        return pd.DataFrame(result)
        
        
        
    def calculate_stats(self, df_, single=True):
        """Calculate performance stats: PnL, MDD, Sharpe, Sortino, trades."""
        
        # Calculate annual rate based on time period
        if self.TIME in df_.columns and len(df_) > 0:
            time_col = pd.to_datetime(df_[self.TIME])
            days_elapsed = (time_col.iloc[-1] - time_col.iloc[0]).total_seconds() / (24 * 3600)
            years_elapsed = days_elapsed / 365.25 if days_elapsed > 0 else np.nan
        else:
            years_elapsed = np.nan

        symbol_stats = {
            'cumulative_pnl':
                df_['cumulative_pnl'].iloc[-1]
                if ('cumulative_pnl' in df_.columns and not df_['cumulative_pnl'].empty)
                else (
                    np.nan
                ),

            'cumulative_pnl_with_fee_no_spread': (
                df_['cumulative_pnl_with_fee_no_spread'].iloc[-1]
                if ('cumulative_pnl_with_fee_no_spread' in df_.columns )
                    else np.nan
                ),

            'cumulative_pnl_with_fee': (
                df_['cumulative_pnl_with_fee'].iloc[-1]
                if ('cumulative_pnl_with_fee' in df_.columns and not df_['cumulative_pnl_with_fee'].empty)
                    else np.nan
                ),

            # 'time_in_market': (df_['signal'] != 0).sum() if single and 'signal' in df_.columns else (df_['active_positions'] != 0).sum(),

            'max_drawdown': (
                lambda df: (
                    (equity := (1 + df['cumulative_pnl_with_fee'])).cummax() - equity
                ).max() if 'cumulative_pnl_with_fee' in df.columns else np.nan
            )(df_),

            'sharpe_ratio': (df_['base_pnl_with_fee'].mean() * 365 * 24) / (df_['base_pnl_with_fee'].std() * np.sqrt(365 * 24)),
            'sortino_ratio': (df_['base_pnl_with_fee'].mean() * 365 * 24) / (np.sqrt(((np.minimum(df_['base_pnl_with_fee'], 0))**2).mean()) * np.sqrt(365 * 24)),
            'trades_entered': df_['n_0to1'].sum(),
            'trades_exited': df_['n_1to0'].sum(),
            'avg_daily_pnl_with_fee': df_['base_pnl_with_fee'].mean() / 24,
            'annual_rate': (
                (df_['cumulative_pnl_with_fee'].iloc[-1] / years_elapsed)
                if (years_elapsed and not np.isnan(years_elapsed) and years_elapsed > 0 
                    and 'cumulative_pnl_with_fee' in df_.columns 
                    and not df_['cumulative_pnl_with_fee'].empty)
                else np.nan
            ),
        }

        return symbol_stats

    # ============================================================
    # Visualization
    # ============================================================
    
    def plot_symbols(self, symbol, title="Funding Rate Comparison", start_date=None):
        """
        Visualize funding rate data for a single symbol.
        
        Args:
            symbol: Trading symbol to plot
            title: Chart title (default: "Funding Rate Comparison")
            start_date: Optional start date to filter data
        """
        normalized_symbol = self._normalize_symbol(symbol)
        df = pd.read_parquet(os.path.join(self.path, f"{normalized_symbol}.parquet"))
        if len(df) == 0:
            print(f"[{symbol}] ⚠️ No data available. Skipping plot.")
            return
        
        df['Time'] = pd.to_datetime(df['Time'])
        if start_date is not None:
            df = df[df['Time'] >= start_date].reset_index(drop=True)
        
        if self.is_cross_exchange:
            # Cross-exchange mode: plot both exchanges and difference
            plt.figure(figsize=(20, 10))
            ax1 = plt.subplot(2, 2, 1)
            ax2 = plt.subplot(2, 2, 2)
            ax3 = plt.subplot(2, 2, 3)
            ax4 = plt.subplot(2, 2, 4)
            
            ax1.plot(df.Time, df[f'{self.exchange1.capitalize()}_FR_1H'], label=f"{self.exchange1}", alpha=0.7)
            ax1.plot(df.Time, df[f'{self.exchange2.capitalize()}_FR_1H'], label=f"{self.exchange2}", alpha=0.7, linestyle='--')
            ax1.set_title('Hourly Funding Rates')
            ax1.set_xlabel('Time')
            ax1.tick_params(axis='x', rotation=45)
            ax1.set_ylabel('Funding Rate')
            ax1.legend()
            ax1.grid(True)

            ax2.plot(df.Time, df.Diff, label=f"Diff", alpha=0.7)
            ax2.set_title(f'Difference ({self.exchange1} - {self.exchange2})')
            ax2.set_xlabel('Time')
            ax2.tick_params(axis='x', rotation=45)
            ax2.set_ylabel('Funding Rate Difference')
            ax2.legend()
            ax2.grid(True)

            ax3.plot(df.Time, df.Diff_cumsum, label='Cumulative Diff')
            ax3.set_title('Cumulative Sum of Difference')
            ax3.set_xlabel('Time')
            ax3.tick_params(axis='x', rotation=45)
            ax3.set_ylabel('Cumulative Difference')
            ax3.legend()
            ax3.grid(True)

            ax4.plot(df.Time, abs(df.Diff).cumsum(), label='Abs Cumulative Diff')
            ax4.set_title('Abs Cumulative Sum of Difference')
            ax4.set_xlabel('Time')
            ax4.tick_params(axis='x', rotation=45)
            ax4.set_ylabel('Cumulative Difference')
            ax4.legend()
            ax4.grid(True)

            plt.suptitle(f'{symbol} - {title}')
            plt.tight_layout(rect=[0, 0.03, 1, 0.95])
            plt.show()
        else:
            # Single-exchange mode: plot funding rate and cumulative
            plt.figure(figsize=(20, 6))
            ax1 = plt.subplot(1, 3, 1)
            ax2 = plt.subplot(1, 3, 2)
            ax3 = plt.subplot(1, 3, 3)
            fr_col = 'FundingRate_hourly' if 'FundingRate_hourly' in df.columns else 'FundingRate'
            
            ax1.plot(df.Time, df[fr_col], label=f"{self.exchange1} Funding Rate", alpha=0.7, color='blue')
            ax1.axhline(y=0, color='red', linestyle='--', alpha=0.3)
            ax1.set_title(f'{self.exchange1} Hourly Funding Rate')
            ax1.set_xlabel('Time')
            ax1.tick_params(axis='x', rotation=45)
            ax1.set_ylabel('Funding Rate')
            ax1.legend()
            ax1.grid(True)

            ax2.plot(df.Time, df[fr_col].cumsum(), label='Cumulative FR', alpha=0.7, color='green')
            ax2.set_title('Cumulative Funding Rate')
            ax2.set_xlabel('Time')
            ax2.tick_params(axis='x', rotation=45)
            ax2.set_ylabel('Cumulative Rate')
            ax2.legend()
            ax2.grid(True)

            ax3.plot(df.Time, abs(df[fr_col]).cumsum(), label='Abs Cumulative FR', alpha=0.7, color='green')
            ax3.set_title('Cumulative Funding Rate (Abs)')
            ax3.set_xlabel('Time')
            ax3.tick_params(axis='x', rotation=45)
            ax3.set_ylabel('Cumulative Rate')
            ax3.legend()
            ax3.grid(True)

            plt.suptitle(f'{symbol} - {self.exchange1} {title}')
            plt.tight_layout(rect=[0, 0.03, 1, 0.95])
            plt.show()


    def plot_strategy_performance(self, df, figsize=(15, 8), title="Strategy Return & MDD", new_equity=True):
        """
        Visualize strategy performance with equity curve, drawdown, and positions.
        
        Features:
            - Auto-scales fonts, line widths, and markers based on figure size
            - Highlights new equity highs
            - Shows comprehensive statistics panel
            - Adaptive date axis formatting
        
        Args:
            df: DataFrame with cumulative PnL columns and signals
            figsize: Figure dimensions (width, height) in inches
            title: Chart title
            new_equity: Whether to highlight new equity highs with markers
        """
        time_col = 'Time'
        df = df.copy()
        df[time_col] = pd.to_datetime(df[time_col])
        df = df.sort_values(time_col).reset_index(drop=True)

        # ---- Calculate responsive scaling factors ----
        base_w, base_h = 15.0, 8.0
        w, h = float(figsize[0]), float(figsize[1])
        scale = max(0.6, min(1.8, 0.5 * (w / base_w + h / base_h)))  # Linear scale for lines/fonts
        area_scale = max(0.5, min(2.0, math.sqrt((w * h) / (base_w * base_h))))  # Area scale for dense text

        # Font sizes
        title_fs = int(16 * scale)
        label_fs = int(12 * scale)
        legend_fs = int(10 * scale)
        stats_fs = int(max(7, min(18, round(10 * area_scale))))  # Area-based for readability
        tick_fs = int(10 * scale)
        
        # Line widths and marker sizes
        lw_main = 1.5 * scale
        lw_secondary = 1.0 * scale
        scatter_s = 4 * (scale ** 2)

        # ---- Prepare data ----
        multiple_symbols = 'active_positions' in df.columns
        statistics = self.calculate_stats(df)

        # Find new equity highs
        simple_cum = df['cumulative_pnl']
        simple_cum_max = simple_cum.cummax()
        is_new_high = (simple_cum == simple_cum_max) & (simple_cum > simple_cum.shift(1).fillna(-np.inf))
        new_high_times = df.loc[is_new_high, time_col]
        new_high_values = simple_cum[is_new_high]

        # ---- Create figure ----        plt.style.use('default')

        fig, (ax1, ax2, ax3) = plt.subplots(
            3, 1, figsize=figsize, gridspec_kw={'height_ratios': [3, 1, 1]}
        )
        fig.suptitle(title, fontsize=title_fs, fontweight='bold')

        if len(new_high_times) > 0 and new_equity:
            ax1.scatter(new_high_times, new_high_values, color='lime', s=scatter_s,
                        label='New Equity High', zorder=5, alpha=0.8)

        ax1.plot(df[time_col], simple_cum, color='#1f77b4', linewidth=lw_main,
                 label='Cumulative Return (No Fee and No Spread)', zorder=3)
        ax1.plot(df[time_col], df['cumulative_pnl_with_fee'], color='#ff7f0e',
                 linewidth=lw_secondary, linestyle='--', label='Cumulative Return (With Fee)', zorder=4)
        ax1.plot(df[time_col], df['cumulative_pnl_with_fee_no_spread'], color='#2ca02c',
                 linewidth=lw_secondary, linestyle='--', label='Cumulative Return (No Spread)')

        stats_text = (
            f"Annualized Return: {statistics['annual_rate']*100:.2f}%\n"
            f"MDD: {statistics['max_drawdown']*100:.2f}%\n"
            f"Sharpe Ratio: {statistics['sharpe_ratio']:.2f}\n"
            f"Sortino Ratio: {statistics['sortino_ratio']:.2f}\n"
            f"Cumulative PnL: {statistics['cumulative_pnl']*100:.2f}%\n"
            f"Cumulative PnL (No Spread): {statistics['cumulative_pnl_with_fee_no_spread']*100:.2f}%\n"
            f"Cumulative PnL (With Fee): {statistics['cumulative_pnl_with_fee']*100:.2f}%"
        )
        # Adjust padding with area scale to keep the box compact on small figures
        stats_box_pad = max(0.35, min(0.8, 0.45 * area_scale))
        stats_text_obj = ax1.text(
            0.99, 0.15, stats_text, transform=ax1.transAxes,
            verticalalignment='bottom', horizontalalignment='right',
            bbox=dict(boxstyle=f'round,pad={stats_box_pad}', facecolor='white', alpha=0.95,
                      edgecolor='darkgray', linewidth=max(0.8, 1.0 * scale)),
            fontsize=stats_fs, family='monospace'
        )
        # Tighten or relax line spacing depending on space available
        try:
            if area_scale < 0.85:
                stats_text_obj.set_linespacing(0.95)
            elif area_scale < 1.25:
                stats_text_obj.set_linespacing(1.10)
            else:
                stats_text_obj.set_linespacing(1.20)
        except Exception:
            # Fallback harmless if backend doesn't support linespacing adjustment
            pass

        ax1.set_ylabel("Return (Decimal)", fontsize=label_fs)
        ax1.legend(loc='upper left', fontsize=legend_fs)
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(df[time_col].min(), df[time_col].max())
        ax1.tick_params(axis='both', labelsize=tick_fs)

        ax2.fill_between(
            df[time_col], df['drawdown'] * 100, 0,
            color='#ff9999', alpha=0.6, label='Drawdown'
        )
        ax2.plot(df[time_col], df['drawdown'] * 100, color='red',
                 linewidth=max(0.6, 0.8 * scale), alpha=0.8)
        ax2.set_ylabel('Drawdown (%)', fontsize=label_fs)
        ax2.legend(loc='lower left', fontsize=legend_fs)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(df[time_col].min(), df[time_col].max())
        ax2.tick_params(axis='both', labelsize=tick_fs)

        if multiple_symbols:
            ax3.plot(df[time_col], df['active_positions'], color='purple',
                     linewidth=max(0.6, 0.8 * scale), label='Active Positions', alpha=0.6)
            ax3.set_ylabel('Active Positions', fontsize=label_fs)
        else:
            ax3.plot(df[time_col], df['signal'], color='purple',
                     linewidth=max(0.6, 0.8 * scale), label='Position', alpha=0.6)
            ax3.set_ylabel(self.exchange1, fontsize=label_fs)
        ax3.legend(loc='upper right', fontsize=legend_fs)
        ax3.tick_params(axis='y', labelcolor='purple')
        ax3.set_xlabel('Time', fontsize=label_fs)
        ax3.grid(True, alpha=0.3)
        ax3.set_xlim(df[time_col].min(), df[time_col].max())
        ax3.tick_params(axis='both', labelsize=tick_fs)

        time_span = (df[time_col].max() - df[time_col].min()).days
        if time_span <= 90:
            major_locator = mdates.WeekdayLocator(interval=2)
            major_formatter = mdates.DateFormatter('%m-%d')
            minor_locator = mdates.DayLocator(interval=7)
        elif time_span <= 365:
            major_locator = mdates.MonthLocator(interval=1)
            major_formatter = mdates.DateFormatter('%Y-%m')
            minor_locator = mdates.WeekdayLocator(interval=2)
        elif time_span <= 730:
            major_locator = mdates.MonthLocator(interval=2)
            major_formatter = mdates.DateFormatter('%Y-%m')
            minor_locator = mdates.MonthLocator(interval=1)
        else:
            major_locator = mdates.MonthLocator(interval=3)
            major_formatter = mdates.DateFormatter('%Y-%m')
            minor_locator = mdates.MonthLocator(interval=1)
        for ax in [ax1, ax2]:
            ax.xaxis.set_major_locator(major_locator)
            ax.xaxis.set_major_formatter(major_formatter)
            ax.xaxis.set_minor_locator(minor_locator)
        ax1.tick_params(axis='x', labelbottom=False)
        ax2.tick_params(axis='x', rotation=45)

        plt.tight_layout(rect=[0, 0, 1, 0.97])
        plt.show()

        print(f"Plot: {len(df):,} points | {time_span} days | {df[time_col].min().date()} to {df[time_col].max().date()}")


    def plot_fr_and_diff_cumsum(self, symbol, title=None, start_date=None):
        """
        Plot two exchanges' funding rates (top) and FR diff cumulative sum (bottom).

        Only works in cross-exchange mode, where the parquet has:
            - <Exchange1>_FR_1H
            - <Exchange2>_FR_1H
            - Diff
            - Diff_cumsum
        """
        if not self.is_cross_exchange:
            raise ValueError("plot_fr_and_diff_cumsum only supports cross-exchange mode.")

        normalized_symbol = self._normalize_symbol(symbol)
        df = pd.read_parquet(os.path.join(self.path, f"{normalized_symbol}.parquet"))
        if len(df) == 0:
            print(f"[{symbol}] ⚠️ No data available. Skipping plot.")
            return

        df["Time"] = pd.to_datetime(df["Time"])
        if start_date is not None:
            df = df[df["Time"] >= start_date].reset_index(drop=True)
        if df.empty:
            print(f"[{symbol}] ⚠️ No data after start_date. Skipping plot.")
            return

        # 確保有 Diff 或 Diff_cumsum 欄位
        if ("Diff_cumsum" not in df.columns) and ("Diff" not in df.columns):
            raise ValueError("DataFrame must contain 'Diff' or 'Diff_cumsum' column.")

        # 為了讓圖上的曲線「從 0 開始」，對切完的這一段重新平移 / 累積
        if "Diff" in df.columns:
            # 直接在目前這一段資料上重新做 cumsum（起點 = 0）
            df["Diff_cumsum_plot"] = df["Diff"].cumsum()
        else:
            # 沒有 Diff，只能用原本的 Diff_cumsum，並把第一點平移到 0
            base = df["Diff_cumsum"].iloc[0]
            df["Diff_cumsum_plot"] = df["Diff_cumsum"] - base

        ex1_col = f"{self.exchange1.capitalize()}_FR_1H"
        ex2_col = f"{self.exchange2.capitalize()}_FR_1H"
        if ex1_col not in df.columns or ex2_col not in df.columns:
            raise ValueError(f"Expected columns '{ex1_col}' and '{ex2_col}' in parquet file.")

        if title is None:
            title = f"{symbol} – {self.exchange1.upper()} vs {self.exchange2.upper()}"

        plt.style.use("default")
        fig, (ax1, ax2) = plt.subplots(
            2, 1,
            figsize=(16, 8),
            gridspec_kw={"height_ratios": [2, 1]},
            sharex=True,
        )
        fig.suptitle(title, fontsize=16, fontweight="bold")

        # ---- 上圖：兩個交易所 FR ----
        ax1.plot(df["Time"], df[ex1_col],
                 label=f"{self.exchange1.capitalize()} FR",
                 color="#1f77b4", alpha=0.8, linewidth=1.2)
        ax1.plot(df["Time"], df[ex2_col],
                 label=f"{self.exchange2.capitalize()} FR",
                 color="#ff7f0e", alpha=0.8, linewidth=1.2)
        ax1.axhline(0, color="gray", linestyle="--", linewidth=0.8, alpha=0.5)

        ax1.set_ylabel("Funding Rate", fontsize=12)
        ax1.legend(loc="upper right", fontsize=10)
        ax1.grid(True, alpha=0.3)

        # ---- 下圖：FR diff 的 cumsum（從 0 開始）----
        ax2.plot(df["Time"], df["Diff_cumsum_plot"],
                 label="FR Diff Cumsum", color="#2ca02c",
                 linewidth=1.2)
        ax2.axhline(0, color="gray", linestyle="--", linewidth=0.8, alpha=0.5)

        ax2.set_xlabel("Time", fontsize=12)
        ax2.set_ylabel("Diff Cumsum", fontsize=12)
        ax2.legend(loc="upper left", fontsize=10)
        ax2.grid(True, alpha=0.3)

        # x 軸格式
        time_span = (df["Time"].max() - df["Time"].min()).days
        if time_span <= 3:
            major_locator = mdates.HourLocator(interval=6)
            major_formatter = mdates.DateFormatter("%m-%d\n%H:%M")
        elif time_span <= 10:
            major_locator = mdates.DayLocator(interval=1)
            major_formatter = mdates.DateFormatter("%m-%d")
        else:
            major_locator = mdates.WeekdayLocator(interval=1)
            major_formatter = mdates.DateFormatter("%m-%d")

        ax2.xaxis.set_major_locator(major_locator)
        ax2.xaxis.set_major_formatter(major_formatter)
        fig.autofmt_xdate(rotation=30)

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.show()

    def plot_top_earning_losing(self, df, top):
        """Plot top N earning/losing symbols with fee comparisons."""
        # ---- data prep ----
        symbol_summary_pnl = self.symbols_summary(df[0])
        symbol_summary_pnl_fee_no_spread = self.symbols_summary(df[1])
        symbol_summary_pnl_fee = self.symbols_summary(df[2])
        symbol_summary = symbol_summary_pnl.copy()
        
        symbol_summary['pnl_no_spread'] = symbol_summary_pnl_fee_no_spread['pnl']
        symbol_summary['pnl_with_fee'] = symbol_summary_pnl_fee['pnl']
        top_earning = symbol_summary.sort_values('pnl_with_fee', ascending=False).head(top)
        top_losing = symbol_summary.sort_values('pnl_with_fee', ascending=True).head(top)

        x_earning = np.arange(len(top_earning))
        x_losing = np.arange(len(top_losing))
        
        bar_width = 0.6

        # Modern color palette
        colors = ['#E8F4F8', '#82C4E5', '#1E88E5']
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), dpi=120)
        fig.patch.set_facecolor('white')

        # ========= Top Earning =========
        bars1 = ax1.bar(x_earning, top_earning['pnl'], bar_width, 
                       label='Base PnL', color=colors[0], edgecolor='#B0BEC5', linewidth=1.5, zorder=3)
        bars2 = ax1.bar(x_earning, top_earning['pnl_no_spread'], bar_width,
                       label='With Fee (No Spread)', color=colors[1], edgecolor='#90A4AE', linewidth=1.5, zorder=4)
        bars3 = ax1.bar(x_earning, top_earning['pnl_with_fee'], bar_width,
                       label='With Fee + Spread', color=colors[2], edgecolor='#546E7A', linewidth=1.5, zorder=5)

        # Add value labels on bars
        for i, (base, no_spread, with_fee) in enumerate(zip(
            top_earning['pnl'], top_earning['pnl_no_spread'], top_earning['pnl_with_fee']
        )):
            # Only show final value to reduce clutter
            ax1.text(i, with_fee, f'{with_fee*100:.2f}%', 
                    ha='center', va='bottom', fontsize=10, fontweight='bold', color='#1E88E5')

        ax1.set_title('Top Earning Symbols', fontsize=16, fontweight='bold', pad=20, color='#263238')
        ax1.set_ylabel('PnL (%)', fontsize=13, fontweight='600', color='#455A64')
        ax1.set_xticks(x_earning)
        ax1.set_xticklabels(top_earning['symbol'], rotation=45, ha='right', fontsize=11)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y*100:.1f}%'))
        ax1.grid(axis='y', alpha=0.3, linestyle='--', linewidth=0.7)
        ax1.legend(loc='upper right', frameon=True, fancybox=True, shadow=True, fontsize=11)
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        ax1.set_facecolor('#FAFAFA')

        # ========= Top Losing =========
        bars1 = ax2.bar(x_losing, top_losing['pnl'], bar_width,
                       label='Base PnL', color=colors[0], edgecolor='#B0BEC5', linewidth=1.5, zorder=3)
        bars2 = ax2.bar(x_losing, top_losing['pnl_no_spread'], bar_width,
                       label='With Fee (No Spread)', color=colors[1], edgecolor='#90A4AE', linewidth=1.5, zorder=4)
        bars3 = ax2.bar(x_losing, top_losing['pnl_with_fee'], bar_width,
                       label='With Fee + Spread', color=colors[2], edgecolor='#546E7A', linewidth=1.5, zorder=5)

        for i, (base, no_spread, with_fee) in enumerate(zip(
            top_losing['pnl'], top_losing['pnl_no_spread'], top_losing['pnl_with_fee']
        )):
            va = 'top' if with_fee < 0 else 'bottom'
            ax2.text(i, with_fee, f'{with_fee*100:.2f}%',
                    ha='center', va=va, fontsize=10, fontweight='bold', color='#D32F2F')

        ax2.set_title('Top Losing Symbols', fontsize=16, fontweight='bold', pad=20, color='#263238')
        ax2.set_xlabel('Symbol', fontsize=13, fontweight='600', color='#455A64')
        ax2.set_ylabel('PnL (%)', fontsize=13, fontweight='600', color='#455A64')
        ax2.set_xticks(x_losing)
        ax2.set_xticklabels(top_losing['symbol'], rotation=45, ha='right', fontsize=11)
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y*100:.1f}%'))
        ax2.grid(axis='y', alpha=0.3, linestyle='--', linewidth=0.7)
        ax2.legend(loc='upper right', frameon=True, fancybox=True, shadow=True, fontsize=11)
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.set_facecolor('#FAFAFA')

        plt.tight_layout()
        plt.show()
        
        print(f"\n📈 Top {top} Earning: {', '.join(top_earning['symbol'].tolist())}")
        print(f"📉 Top {top} Losing: {', '.join(top_losing['symbol'].tolist())}")
            
            
            
            
    # def plot_top_time_in_market(self, df, top): 這個要改
    #     # ---- data prep ----
    #     symbol_summary = self.symbols_summary(df)
    #     top_time_in_market = symbol_summary.sort_values('time in the market', ascending=False).head(top)

    #     time_symbols = top_time_in_market['symbol']
    #     time_values = top_time_in_market['time in the market']
    #     x_time = np.arange(len(time_symbols))

    #     # ---- style: same color, alpha layers ----
    #     base_color          = "#4883D6"  # 清爽藍
    #     alpha_with_fee      = 0.90       # With Fee（最深）
    #     edgecolor           = "white"    # 柔化邊界
    #     linewidth           = 0.8

    #     fig, ax1 = plt.subplots(1, 1, figsize=(14, 6), dpi=150)

    #     # ========= Top Time in Market =========
    #     ax1.bar(x_time, time_values,
    #             width=0.5, color=base_color, alpha=alpha_with_fee,
    #             label='Time in Market', edgecolor=edgecolor, linewidth=linewidth, zorder=3)

    #     # labels
    #     for i, row in enumerate(top_time_in_market.itertuples(index=False)):
    #         ax1.text(i, row._2,               f"{row._2:.0f}",               ha='center', va='bottom', fontsize=9)

    #     ax1.set_title('Top Symbols by Time in Market', fontsize=14, fontweight='bold')
    #     ax1.set_ylabel('Time in Market (Hours)', fontsize=12)
    #     ax1.set_xticks(x_time)
    #     ax1.set_xticklabels(time_symbols, rotation=45, ha='right')
    #     ax1.grid(True, axis='y', alpha=0.25)
    #     ax1.legend(frameon=False, ncol=3, loc='upper right')

    #     plt.tight_layout()
    #     plt.show()
    
    
    # def plot_top_enter_exit(self, df, top):
    #     symbol_summary = self.symbols_summary(df)
    #     top_enter_exit = symbol_summary.sort_values('enter_exit_time', ascending=False).head(top)
        
    #     fig, ax = plt.subplots(1, 1, figsize=(14, 6), dpi=150)
    #     symbols = top_enter_exit['symbol']
    #     x = np.arange(len(symbols))
        
    #     ax.bar(x, top_enter_exit['enter_exit_time'], color='#4883D6', alpha=0.7, 
    #            edgecolor='white', linewidth=0.8, label='Enter/Exit Count')
        
    #     # Add value labels on top of bars
    #     for i, row in enumerate(top_enter_exit.itertuples(index=False)):
    #         ax.text(i, row.enter_exit_time + 1, f"{row.enter_exit_time:.0f}", 
    #                ha='center', va='bottom', fontsize=10, fontweight='bold')
        
    #     ax.set_title('Top Symbols by Number of Entries and Exits', fontsize=14, fontweight='bold')
    #     ax.set_ylabel('Number of Entries and Exits', fontsize=12)
    #     ax.set_xlabel('Symbols', fontsize=12)
    #     ax.set_xticks(x)
    #     ax.set_xticklabels(symbols, rotation=45, ha='right')
    #     ax.grid(True, axis='y', alpha=0.3)
    #     ax.legend(frameon=True, loc='upper right')
        
    #     plt.tight_layout()
    #     plt.show()
    
    def plot_position_heatmap(self, df):
        """Plot calendar heatmap of active positions by day (separate plot per year)."""
        daily = df.copy().groupby(pd.Grouper(freq='D', key='Time')).sum().reset_index()
        daily['active_symbols'] = daily['active_symbols'].apply(lambda x: list(set(x)))
        daily['active_positions'] = daily['active_symbols'].apply(len)
        daily['active_positions'] = daily['active_positions'].astype(int)
        daily['Time'] = pd.to_datetime(daily['Time'])
        daily['Time'] = pd.to_datetime(daily['Time']).dt.date

        daily['year_month'] = pd.to_datetime(daily['Time']).dt.to_period('M').astype(str)   # e.g. '2024-05'
        daily['year'] = pd.to_datetime(daily['Time']).dt.year
        daily['month'] = pd.to_datetime(daily['Time']).dt.month
        daily['day'] = pd.to_datetime(daily['Time']).dt.day
        for year in daily.year.unique():
            df = daily[daily['year'] == year].pivot_table(index='month', columns='day', values='active_positions', fill_value=0)
            plt.figure(figsize=(20, 8))
            sns.heatmap(
                df,
                cmap="YlGnBu",
                linewidths=0.5,
                linecolor='gray',
                cbar_kws={'label': 'Number of Positions'}
            )

            plt.title('Average Number of Positions by Calendar Day in ' + str(year), fontsize=16, fontweight='bold')
            plt.xlabel('Day of Month')
            plt.ylabel('Month')
            plt.tight_layout()
            plt.show()

    def portfolio_by_month(self, df):
        """Aggregate and plot monthly portfolio performance. Returns monthly_summary df."""
        df = df.copy()
        df[self.TIME] = pd.to_datetime(df[self.TIME])
        df['year_month'] = df[self.TIME].dt.to_period('M').astype(str)   # e.g. '2024-05'
        monthly_summary = df.groupby('year_month').agg({
            'base_pnl': 'sum',
            'base_pnl_with_fee_no_spread': 'sum',
            'base_pnl_with_fee': 'sum',
            'n_0to1': 'sum',
            'n_1to0': 'sum',
        }).reset_index()
        fig = plt.figure(figsize=(12, 6))
        ax = fig.add_subplot(111)
        ax.bar(monthly_summary['year_month'], monthly_summary['base_pnl'], label='Base PnL', alpha=0.7)
        ax.bar(monthly_summary['year_month'], monthly_summary['base_pnl_with_fee_no_spread'], label='PnL with Fee (No Spread)', alpha=0.7)
        ax.bar(monthly_summary['year_month'], monthly_summary['base_pnl_with_fee'], label='PnL with Fee', alpha=0.7)
        ax.set_title('Monthly Portfolio Performance')
        ax.set_xlabel('Year-Month')
        ax.set_ylabel('PnL')
        ax.legend()
        plt.xticks(rotation=45,size=10)
        plt.tight_layout()
        plt.show()

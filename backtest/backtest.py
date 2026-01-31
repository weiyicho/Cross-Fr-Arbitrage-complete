"""
Backtesting System Module.

This module provides functionality for backtesting funding rate arbitrage strategies,
supporting both cross-exchange arbitrage and single-exchange directional strategies.
It includes tools for data loading, signal generation, portfolio management, and
performance visualization.
"""

import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import reduce
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from tqdm import tqdm


class Backtester:
    """
    Backtester for funding rate arbitrage strategies.
    
    Supports:
    - Cross-exchange arbitrage (e.g., Binance vs OKX).
    - Single-exchange directional funding rate strategies.
    """

    def __init__(self, exchange1: str, exchange2: Optional[str] = None) -> None:
        """
        Initialize the backtester.

        Args:
            exchange1: Name of the first exchange (e.g., 'binance').
            exchange2: Name of the second exchange for cross-exchange mode.
                       If None, operates in single-exchange mode.
        """
        self.TIME_COL = "Time"
        self.exchange1 = exchange1
        self.exchange2 = exchange2
        self.is_cross_exchange = exchange2 is not None

        # Set data path based on mode
        if self.is_cross_exchange:
            # Sort exchanges to ensure consistent path naming
            exchanges = sorted([exchange1, exchange2])  # type: ignore # exchange2 is not None here
            self.exchange1 = exchanges[0]
            self.exchange2 = exchanges[1]
            self.path = (
                Path(__file__).resolve().parent.parent 
                / "data" / "merge" / f"{self.exchange1}_{self.exchange2}" / "funding_rates"
            )
            mode_info = f"Cross-exchange mode: {self.exchange1} vs {self.exchange2}"
        else:
            self.path = (
                Path(__file__).resolve().parent.parent 
                / "data" / "clean" / exchange1 / "funding_rates"
            )
            mode_info = f"Single-exchange mode: {self.exchange1}"

        # Initialize shared symbols list
        self.shared_symbols: List[str] = []
        if self.path.exists():
            self.shared_symbols = [f.stem for f in self.path.glob("*.parquet")]
        else:
            print(f"Warning: Directory not found at {self.path}")

        print(f"Backtester: {mode_info} | Symbols: {len(self.shared_symbols)}")
    
    def _normalize_symbol(self, symbol: str) -> str:
        """
        Convert a trading symbol to a normalized format for file reading.
        
        Handles:
        - 'BTC/USDT' -> 'BTCUSDT'
        - 'BTC/USDT:USDT' -> 'BTCUSDT'
        - 'ETH' -> 'ETHUSDT' (assumes USDT quote)

        Args:
            symbol: The trading symbol to normalize.
            
        Returns:
            str: Normalized symbol string (e.g., 'BTCUSDT').
        """
        # Handle perpetual futures notation (BTC/USDT:USDT or ETHUSDT:USDT)
        if ':' in symbol:
            symbol = symbol.split(':')[0]
            
        # Handle standard notation (BTC/USDT)
        if '/' in symbol:
            base, quote = symbol.split('/')
            return f"{base}{quote}"
            
        # Handle other separators like dash (BTC-USDT)
        if '-' in symbol:
            base, quote = symbol.split('-')
            return f"{base}{quote}"
            
        # Handle base currency only (e.g., 'ETH' -> 'ETHUSDT')
        if '/' not in symbol and ':' not in symbol and '-' not in symbol:
            if not symbol.endswith('USDT'):
                return f"{symbol}USDT"
            return symbol
            
        # Fallback: remove all separators
        return symbol.replace('/', '').replace('-', '').replace(':', '')

    # ============================================================
    # Main Backtesting Logic
    # ============================================================
    
    # ============================================================
    # Main Backtesting Logic
    # ============================================================

    def backtest_fundingrate(
        self,
        symbol: str,
        n_days: float = 2,
        threshold: float = 0.1,
        out_threshold: float = 0.05,
        value_threshold: float = 1000,
        value_out_threshold: float = 1000,
        fee: float = 0.0005,
        position: float = 10000,
        leverage: float = 1,
        start_date: Optional[str] = None,
        spread_threshold: float = 0.01,
        spread_enter: bool = False,
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Backtest funding rate arbitrage for a single symbol.

        Strategy:
            - Cross-exchange: Profit from funding rate differences between exchanges.
            - Single-exchange: Profit from directional funding rate positions.

        Args:
            symbol: Trading pair (e.g., 'BTC/USDT', 'ETHUSDT').
            n_days: Rolling window for signal calculation (default: 2 days).
            threshold: Entry threshold - annualized FR % (default: 0.1 = 10%).
            out_threshold: Exit threshold - annualized FR % (default: 0.05 = 5%).
            value_threshold: Min volume*price to enter (default: 1000 USD).
            value_out_threshold: Min volume*price to stay in (default: 1000 USD).
            fee: Trading fee per side (default: 0.0005 = 0.05%).
            position: Position size in USD (default: 10000).
            leverage: Leverage multiplier (default: 1).
            start_date: Backtest start date (default: None = use all data).
            spread_threshold: Min price spread % to enter (default: 0.01 = 1%).
            spread_enter: If True, entry also requires a sufficient price spread.

        Returns:
            Tuple[Optional[pd.DataFrame], str]: The backtest results DataFrame and the symbol name.
                                                Returns (None, symbol) if data is missing or invalid.
        """
        # Load and validate data
        normalized_symbol = self._normalize_symbol(symbol)
        file_path = self.path / f"{normalized_symbol}.parquet"
        
        if not file_path.exists():
            print(f"[{symbol}] File not found: {file_path}")
            return None, symbol

        df = pd.read_parquet(file_path)
        if df.empty or len(df) == 0:
            print(f"[{symbol}] No data available")
            return None, symbol

        # Time filtering
        df[self.TIME_COL] = pd.to_datetime(df[self.TIME_COL])
        if start_date is not None:
            df = df[df[self.TIME_COL] >= start_date].reset_index(drop=True)

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
                print(f"[{symbol}] Missing 'FundingRate_hourly' | Available: {list(df.columns)}")
                return None, symbol
            signal_mean = df['fr_mean']

        # ---- Volume filters ----
        value_cols = [col for col in df.columns if col.endswith("_Value")]
        volume_condition = pd.Series(True, index=df.index)
        exit_volume_condition = pd.Series(False, index=df.index)

        if self.is_cross_exchange:
            if len(value_cols) != 2:
                print(f"[{symbol}] Expected 2 value columns, found {len(value_cols)}")
                return None, symbol
            value_mean1 = df[value_cols[0]].rolling(window=window_hours).mean()
            value_mean2 = df[value_cols[1]].rolling(window=window_hours).mean()
            volume_condition = (value_mean1 > value_threshold) & (value_mean2 > value_threshold)
            exit_volume_condition = (value_mean1 < value_out_threshold) | (value_mean2 < value_out_threshold)
        else:
            # Single exchange: only one volume column
            if 'Value' not in df.columns:
                print(f"[{symbol}] Missing 'Value' column")
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
            price_exchange1 = df[self.exchange1.capitalize() + '_Open'].values  # type: ignore
            price_exchange2 = df[self.exchange2.capitalize() + '_Open'].values  # type: ignore
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
            pnl = (
                df['signal'] * df[self.exchange1.capitalize() + '_FR_1H'] * -1  # type: ignore
                + df['signal'] * df[self.exchange2.capitalize() + '_FR_1H']  # type: ignore
            )
            base_return = np.where(should_count_funding, pnl, 0)
        else:
            base_return = np.where(
                should_count_funding, 
                df['FundingRate_hourly'] * df['signal'] * (-1), 
                0
            )

        # Trading fees
        open_fee = np.where(entry_flag, -fee, 0)
        close_fee = np.where(exit_flag, -fee, 0)

        # ---- Spread PnL (cross-exchange only) ----
        if self.is_cross_exchange:
            signal_arr = df['signal'].values
            
            # Find entry and exit points (vectorized)
            signal_shift_prev = np.concatenate([[0], signal_arr[:-1]])  # type: ignore
            signal_shift_next = np.concatenate([signal_arr[1:], [0]])  # type: ignore
            
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

    def process_multiple_symbols(
        self, 
        symbols: List[str], 
        **kwargs: Any
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Process multiple symbols in parallel for portfolio backtesting.

        Args:
            symbols: List of trading symbols to backtest.
            **kwargs: Arguments passed to backtest_fundingrate().

        Returns:
            Tuple[pd.DataFrame, List[str]]: 
                - Merged DataFrame containing hourly data for all successful symbols.
                - List of symbols that failed to process.
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
                            symbol_data = hourly_data[[
                                self.TIME_COL, 'signal', 'n_0to1', 'base_pnl',
                                'base_pnl_with_fee_no_spread', 'base_pnl_with_fee'
                            ]].copy()
                            
                            symbol_data = symbol_data.rename(columns={
                                'base_pnl': f'{symbol}_base_pnl',
                                'n_0to1': f'{symbol}_n_0to1',
                                'signal': f'{symbol}_signal',
                                'base_pnl_with_fee_no_spread': f'{symbol}_base_pnl_with_fee_no_spread',
                                'base_pnl_with_fee': f'{symbol}_base_pnl_with_fee'
                            })
                            symbol_data_list.append(symbol_data)
                        else:
                            print(f"[{symbol}] Warning: No data returned")
                            failed_symbols.append(symbol)
                    except Exception as e:
                        print(f"[{symbol}] Error: {e}")
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
                    lambda left, right: pd.merge(left, right, on=self.TIME_COL, how='outer'),
                    batch
                )
                batch_results.append(batch_result)

            # Final merge
            if len(batch_results) == 1:
                all_hourly_data = batch_results[0]
            else:
                all_hourly_data = reduce(
                    lambda left, right: pd.merge(left, right, on=self.TIME_COL, how='outer'),
                    batch_results
                )

            # Sort and fill NaN
            all_hourly_data = all_hourly_data.sort_values(self.TIME_COL).reset_index(drop=True)
            signal_cols = [col for col in all_hourly_data.columns if '_signal' in col or '_base_pnl' in col]
            all_hourly_data[signal_cols] = all_hourly_data[signal_cols].fillna(0)
        else:
            all_hourly_data = pd.DataFrame(columns=[
                self.TIME_COL, 'signal', 'n_0to1', 'base_pnl',
                'base_pnl_with_fee_no_spread', 'base_pnl_with_fee'
            ])

        print(f"Merge completed | Success: {len(symbol_data_list)}/{len(symbols)} | Failed: {len(failed_symbols)}")
        if failed_symbols:
            if len(failed_symbols) <= 5:
                print(f"  Failed: {', '.join(failed_symbols)}")
            else:
                print(f"  Failed: {', '.join(failed_symbols[:5])} ... (+{len(failed_symbols)-5} more)")

        return all_hourly_data, failed_symbols

    def create_portfolio_summary(
        self, 
        df: pd.DataFrame, 
        max_active_positions: int = 3, 
        **kwargs: Any
    ) -> Tuple[pd.DataFrame, Dict[str, float], List[pd.DataFrame]]:
        """
        Construct portfolio from multiple symbols using top PnL selection strategy.

        Strategy:
            - Select top `max_active_positions` symbols by current period PnL.
            - Equal weight allocation (1/N per symbol).
            - Tracks entry-time allocation for accurate exit fee calculation.

        Args:
            df: Merged DataFrame with data for all symbols.
            max_active_positions: Maximum concurrent positions (default: 3).
            **kwargs: Additional arguments.

        Returns:
            Tuple[pd.DataFrame, Dict[str, float], List[pd.DataFrame]]:
                - Portfolio summary DataFrame.
                - Portfolio performance statistics dictionary.
                - List of DataFrames tracking coin-level contributions.
        """
        active_symbols: List[str] = []
        symbol_columns = [c for c in df.columns if c.endswith('_base_pnl')]
        symbols = [c[: -len('_base_pnl')] for c in symbol_columns]
        
        if not symbols:
            raise ValueError("No symbols detected from '*_base_pnl' columns.")

        # Initialize portfolio tracking dictionaries
        portfolio_data: Dict[str, List[Any]] = {
            self.TIME_COL: [],
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
            portfolio_data[self.exchange1] = []  # type: ignore
            portfolio_data[self.exchange2] = []  # type: ignore
        else:
            portfolio_data['long_trades'] = []
            portfolio_data['short_trades'] = []

        signal_cols = [f"{symbol}_signal" for symbol in symbols]
        signal_dict = {i: symbols[i] for i in range(len(symbols))}
        pnl_dict = {symbols[i]: f"{symbols[i]}_base_pnl" for i in range(len(symbols))}

        # Pre-convert to NumPy arrays for fast vectorized access
        time_arr = df[self.TIME_COL].values
        signal_arrays = {col: df[col].values for col in signal_cols}
        base_pnl_arrays = {f"{s}_base_pnl": df[f"{s}_base_pnl"].values for s in symbols}
        fee_arrays = {f"{s}_base_pnl_with_fee": df[f"{s}_base_pnl_with_fee"].values for s in symbols}
        fee_no_spread_arrays = {f"{s}_base_pnl_with_fee_no_spread": df[f"{s}_base_pnl_with_fee_no_spread"].values for s in symbols}

        # Pre-compute all signals matrix (rows=time, cols=symbols)
        all_signals = np.column_stack([signal_arrays[col] for col in signal_cols])

        # Per-coin portfolio tracking: [base_pnl, fee_no_spread, total_fee]
        coin_portfolio_data: List[List[Dict[str, float]]] = [[], [], []]
        prev_signals: Optional[np.ndarray] = None

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
                new_symbols_to_add = sorted(
                    pnl_values.keys(), 
                    key=lambda x: pnl_values[x], 
                    reverse=True
                )[:slots_available]
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
            coin_row_base_pnl = {s: 0.0 for s in symbols}
            coin_row_fee_no_spread = {s: 0.0 for s in symbols}
            coin_row_total_fee = {s: 0.0 for s in symbols}

            # Calculate portfolio PnL with equal weighting
            allocation = 1.0
            if len(active_symbols) > 0:
                if len(active_symbols) > 1:
                    allocation = 1.0 / len(active_symbols)

                # Aggregate PnL from active symbols (single pass)
                total_base_pnl = 0.0
                total_fee_no_spread = 0.0
                total_fee = 0.0

                for s in active_symbols:
                    base_pnl_val = base_pnl_arrays[f"{s}_base_pnl"][idx]
                    fee_no_spread_val = fee_no_spread_arrays[f"{s}_base_pnl_with_fee_no_spread"][idx]
                    total_fee_val = fee_arrays[f"{s}_base_pnl_with_fee"][idx]

                    # Per-coin tracking
                    coin_row_base_pnl[s] = base_pnl_val * allocation
                    coin_row_fee_no_spread[s] = fee_no_spread_val * allocation
                    coin_row_total_fee[s] = total_fee_val * allocation

                    # Total tracking
                    total_base_pnl += base_pnl_val * allocation
                    total_fee_no_spread += fee_no_spread_val * allocation
                    total_fee += total_fee_val * allocation
            else:
                total_base_pnl = 0.0
                total_fee_no_spread = 0.0
                total_fee = 0.0
                allocation = 0.0

            # Handle exited symbols: apply previous allocation to exit fees
            if len(active_symbols) < len(prev_active_symbols):
                exited_symbols = set(prev_active_symbols) - set(active_symbols)
                exit_fee_no_spread = 0.0
                exit_total_fee = 0.0
                # Prevent division by zero if prev_active_symbols was somehow empty (unlikely here)
                prev_allocation = 1.0 / len(prev_active_symbols) if prev_active_symbols else 0.0

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
            portfolio_data[self.TIME_COL].append(time_arr[idx])

            if self.is_cross_exchange:
                portfolio_data[self.exchange1].append(long_trades)  # type: ignore
                portfolio_data[self.exchange2].append(short_trades)  # type: ignore
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
            # coin_portfolio_data[0] -> base_pnl
            # coin_portfolio_data[1] -> fee_no_spread
            # coin_portfolio_data[2] -> total_fee
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

        # Build coin-level portfolio DataFrames
        coin_portfolio_dfs = [
            pd.DataFrame(data, index=portfolio_df[self.TIME_COL]) 
            for data in coin_portfolio_data
        ]

        return portfolio_df, stats, coin_portfolio_dfs

    def backtest_portfolio(
        self, 
        symbols: Optional[List[str]] = None, 
        all_symbols: bool = False, 
        max_active_positions: int = 3, 
        **kwargs: Any
    ) -> Tuple[pd.DataFrame, Dict[str, float], List[pd.DataFrame], List[str]]:
        """
        Run portfolio backtest workflow.

        Args:
            symbols: List of symbols (ignored if all_symbols=True).
            all_symbols: If True, use all symbols from the merge folder.
            max_active_positions: Max concurrent positions (default: 3).
            **kwargs: Parameters for backtest_fundingrate().

        Returns:
            Tuple[pd.DataFrame, Dict[str, float], List[pd.DataFrame], List[str]]:
                - Portfolio summary DataFrame.
                - Portfolio statistics.
                - List of coin-level DataFrames.
                - List of failed symbols.
        """
        # Load all symbols from merge folder if all_symbols=True
        if all_symbols:
            symbols = self.shared_symbols
            print(f"Auto-loaded {len(symbols)} symbols | Max positions: {max_active_positions}")
        elif symbols is None:
            raise ValueError("Either provide 'symbols' list or set 'all_symbols=True'")

        # Process symbols in parallel
        df, failed_symbols = self.process_multiple_symbols(symbols, **kwargs)
        df.fillna(0, inplace=True)

        # Create portfolio summary
        portfolio_df, stats, coin_portfolio_df = self.create_portfolio_summary(
            df, max_active_positions
        )
        
        return portfolio_df, stats, coin_portfolio_df, failed_symbols

    def symbols_summary(self, df: pd.DataFrame, time_col: str = 'Time') -> pd.DataFrame:
        """
        Calculate summary statistics for each symbol in the DataFrame.

        Args:
            df: DataFrame containing symbol PnL data.
            time_col: Name of the time column to exclude.

        Returns:
            pd.DataFrame: Summary statistics DataFrame.
        """
        symbol_columns = [c for c in df.columns if c != time_col]
        result = []

        for symbol in symbol_columns:
            pnl = df[symbol]
            is_active = (pnl != 0).astype(int)
            # Calculate transitions (entries + exits)
            transitions = (
                ((is_active.shift(fill_value=0) == 0) & (is_active == 1)).sum() +
                ((is_active.shift(fill_value=0) == 1) & (is_active == 0)).sum()
            )

            result.append({
                'symbol': symbol,
                'enter_exit_time': transitions,
                'time in the market': (pnl != 0).sum(),
                'pnl': pnl.sum(),
            })

        return pd.DataFrame(result)

    def calculate_stats(
        self, 
        df_: pd.DataFrame, 
        single: bool = True
    ) -> Dict[str, float]:
        """
        Calculate performance statistics: PnL, MDD, Sharpe, Sortino, trades.

        Args:
            df_: DataFrame containing backtest results.
            single: Unused parameter (kept for compatibility or future use).

        Returns:
            Dict[str, float]: Dictionary of calculated statistics.
        """
        # Calculate annual rate based on time period
        years_elapsed: float = np.nan
        if self.TIME_COL in df_.columns and len(df_) > 0:
            time_col = pd.to_datetime(df_[self.TIME_COL])
            duration = time_col.iloc[-1] - time_col.iloc[0]
            days_elapsed = duration.total_seconds() / (24 * 3600)
            if days_elapsed > 0:
                years_elapsed = days_elapsed / 365.25

        # Helper to get last value safely
        def get_last(col: str) -> float:
            if col in df_.columns and not df_[col].empty:
                return float(df_[col].iloc[-1])
            return np.nan

        cumulative_pnl = get_last('cumulative_pnl')
        cumulative_pnl_with_fee_no_spread = get_last('cumulative_pnl_with_fee_no_spread')
        cumulative_pnl_with_fee = get_last('cumulative_pnl_with_fee')

        # Calculate Max Drawdown
        max_drawdown = np.nan
        if 'cumulative_pnl_with_fee' in df_.columns:
            equity = 1 + df_['cumulative_pnl_with_fee']
            max_drawdown = (equity.cummax() - equity).max()

        # Ratios (assuming hourly data)
        hours_per_year = 365 * 24
        
        sharpe_ratio = np.nan
        sortino_ratio = np.nan
        avg_daily_pnl = np.nan

        if 'base_pnl_with_fee' in df_.columns:
            returns = df_['base_pnl_with_fee']
            std_dev = returns.std()
            if std_dev != 0:
                sharpe_ratio = (returns.mean() * hours_per_year) / (std_dev * np.sqrt(hours_per_year))
            
            downside = returns[returns < 0]
            downside_std = np.sqrt((downside**2).mean())
            if downside_std != 0:
                sortino_ratio = (returns.mean() * hours_per_year) / (downside_std * np.sqrt(hours_per_year))
                
            avg_daily_pnl = returns.mean() * 24

        # Calculate annual rate
        annual_rate = np.nan
        if not np.isnan(years_elapsed) and years_elapsed > 0 and not np.isnan(cumulative_pnl_with_fee):
            annual_rate = cumulative_pnl_with_fee / years_elapsed

        symbol_stats = {
            'cumulative_pnl': cumulative_pnl,
            'cumulative_pnl_with_fee_no_spread': cumulative_pnl_with_fee_no_spread,
            'cumulative_pnl_with_fee': cumulative_pnl_with_fee,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'trades_entered': float(df_['n_0to1'].sum()) if 'n_0to1' in df_.columns else 0.0,
            'trades_exited': float(df_['n_1to0'].sum()) if 'n_1to0' in df_.columns else 0.0,
            'avg_daily_pnl_with_fee': avg_daily_pnl,
            'annual_rate': annual_rate,
        }

        return symbol_stats

    # ============================================================
    # Visualization
    # ============================================================

    def plot_symbols(
        self, 
        symbol: str, 
        title: str = "Funding Rate Comparison", 
        start_date: Optional[str] = None
    ) -> None:
        """
        Visualize funding rate data for a single symbol.

        Args:
            symbol: Trading symbol to plot.
            title: Chart title (default: "Funding Rate Comparison").
            start_date: Optional start date to filter data.
        """
        normalized_symbol = self._normalize_symbol(symbol)
        file_path = self.path / f"{normalized_symbol}.parquet"
        
        if not file_path.exists():
             # Try fallback to standard storage path structure if file not found in current self.path
             # This handles cases where self.path might be set for one mode but we verify existence
             pass

        try:
             df = pd.read_parquet(file_path)
        except Exception as e:
            print(f"[{symbol}] Failed to read data: {e}")
            return

        if len(df) == 0:
            print(f"[{symbol}] No data available. Skipping plot.")
            return

        df[self.TIME_COL] = pd.to_datetime(df[self.TIME_COL])
        if start_date is not None:
            df = df[df[self.TIME_COL] >= start_date].reset_index(drop=True)

        if self.is_cross_exchange:
            # Cross-exchange mode: plot both exchanges and difference
            plt.figure(figsize=(20, 10))
            ax1 = plt.subplot(2, 2, 1)
            ax2 = plt.subplot(2, 2, 2)
            ax3 = plt.subplot(2, 2, 3)
            ax4 = plt.subplot(2, 2, 4)
            
            # Ensure exchange names are available
            ex1 = self.exchange1.capitalize() if self.exchange1 else "Exchange1"
            ex2 = self.exchange2.capitalize() if self.exchange2 else "Exchange2"

            ax1.plot(df[self.TIME_COL], df[f'{ex1}_FR_1H'], label=f"{self.exchange1}", alpha=0.7)
            ax1.plot(df[self.TIME_COL], df[f'{ex2}_FR_1H'], label=f"{self.exchange2}", alpha=0.7, linestyle='--')
            ax1.set_title('Hourly Funding Rates')
            ax1.set_xlabel('Time')
            ax1.tick_params(axis='x', rotation=45)
            ax1.set_ylabel('Funding Rate')
            ax1.legend()
            ax1.grid(True)

            ax2.plot(df[self.TIME_COL], df.Diff, label='Diff', alpha=0.7)
            ax2.set_title(f'Difference ({self.exchange1} - {self.exchange2})')
            ax2.set_xlabel('Time')
            ax2.tick_params(axis='x', rotation=45)
            ax2.set_ylabel('Funding Rate Difference')
            ax2.legend()
            ax2.grid(True)

            ax3.plot(df[self.TIME_COL], df.Diff_cumsum, label='Cumulative Diff')
            ax3.set_title('Cumulative Sum of Difference')
            ax3.set_xlabel('Time')
            ax3.tick_params(axis='x', rotation=45)
            ax3.set_ylabel('Cumulative Difference')
            ax3.legend()
            ax3.grid(True)

            ax4.plot(df[self.TIME_COL], abs(df.Diff).cumsum(), label='Abs Cumulative Diff')
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
            # Single-exchange mode
            plt.figure(figsize=(20, 6))
            ax1 = plt.subplot(1, 3, 1)
            ax2 = plt.subplot(1, 3, 2)
            ax3 = plt.subplot(1, 3, 3)
            
            fr_col = 'FundingRate_hourly' if 'FundingRate_hourly' in df.columns else 'FundingRate'
            ex1 = self.exchange1.capitalize() if self.exchange1 else "Exchange"

            ax1.plot(df[self.TIME_COL], df[fr_col], label=f"{self.exchange1} Funding Rate", alpha=0.7, color='blue')
            ax1.axhline(y=0, color='red', linestyle='--', alpha=0.3)
            ax1.set_title(f'{ex1} Hourly Funding Rate')
            ax1.set_xlabel('Time')
            ax1.tick_params(axis='x', rotation=45)
            ax1.set_ylabel('Funding Rate')
            ax1.legend()
            ax1.grid(True)

            ax2.plot(df[self.TIME_COL], df[fr_col].cumsum(), label='Cumulative FR', alpha=0.7, color='green')
            ax2.set_title('Cumulative Funding Rate')
            ax2.set_xlabel('Time')
            ax2.tick_params(axis='x', rotation=45)
            ax2.set_ylabel('Cumulative Rate')
            ax2.legend()
            ax2.grid(True)

            ax3.plot(df[self.TIME_COL], abs(df[fr_col]).cumsum(), label='Abs Cumulative FR', alpha=0.7, color='green')
            ax3.set_title('Cumulative Funding Rate (Abs)')
            ax3.set_xlabel('Time')
            ax3.tick_params(axis='x', rotation=45)
            ax3.set_ylabel('Cumulative Rate')
            ax3.legend()
            ax3.grid(True)

            plt.suptitle(f'{symbol} - {self.exchange1} {title}')
            plt.tight_layout(rect=[0, 0.03, 1, 0.95])
            plt.show()

    def plot_strategy_performance(
        self, 
        df: pd.DataFrame, 
        figsize: Tuple[float, float] = (15, 8), 
        title: str = "Strategy Return & MDD", 
        new_equity: bool = True
    ) -> None:
        """
        Visualize strategy performance with equity curve, drawdown, and positions.

        Features:
            - Auto-scales fonts, line widths, and markers based on figure size.
            - Highlights new equity highs.
            - Shows comprehensive statistics panel.
            - Adaptive date axis formatting.

        Args:
            df: DataFrame with cumulative PnL columns and signals.
            figsize: Figure dimensions (width, height) in inches.
            title: Chart title.
            new_equity: Whether to highlight new equity highs with markers.
        """
        df = df.copy()
        df[self.TIME_COL] = pd.to_datetime(df[self.TIME_COL])
        df = df.sort_values(self.TIME_COL).reset_index(drop=True)

        # ---- Calculate responsive scaling factors ----
        base_w, base_h = 15.0, 8.0
        w, h = float(figsize[0]), float(figsize[1])
        scale = max(0.6, min(1.8, 0.5 * (w / base_w + h / base_h)))
        area_scale = max(0.5, min(2.0, math.sqrt((w * h) / (base_w * base_h))))

        # Font sizes
        title_fs = int(16 * scale)
        label_fs = int(12 * scale)
        legend_fs = int(10 * scale)
        stats_fs = int(max(7, min(18, round(10 * area_scale))))
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
        new_high_times = df.loc[is_new_high, self.TIME_COL]
        new_high_values = simple_cum[is_new_high]

        # ---- Create figure ----
        plt.style.use('default')

        fig, (ax1, ax2, ax3) = plt.subplots(
            3, 1, figsize=figsize, gridspec_kw={'height_ratios': [3, 1, 1]}
        )
        fig.suptitle(title, fontsize=title_fs, fontweight='bold')

        if len(new_high_times) > 0 and new_equity:
            ax1.scatter(new_high_times, new_high_values, color='lime', s=scatter_s,
                        label='New Equity High', zorder=5, alpha=0.8)

        ax1.plot(df[self.TIME_COL], simple_cum, color='#1f77b4', linewidth=lw_main,
                 label='Cumulative Return (No Fee and No Spread)', zorder=3)
        ax1.plot(df[self.TIME_COL], df['cumulative_pnl_with_fee'], color='#ff7f0e',
                 linewidth=lw_secondary, linestyle='--', label='Cumulative Return (With Fee)', zorder=4)
        ax1.plot(df[self.TIME_COL], df['cumulative_pnl_with_fee_no_spread'], color='#2ca02c',
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
        
        # Adjust padding with area scale
        stats_box_pad = max(0.35, min(0.8, 0.45 * area_scale))
        stats_text_obj = ax1.text(
            0.99, 0.15, stats_text, transform=ax1.transAxes,
            verticalalignment='bottom', horizontalalignment='right',
            bbox=dict(boxstyle=f'round,pad={stats_box_pad}', facecolor='white', alpha=0.95,
                      edgecolor='darkgray', linewidth=max(0.8, 1.0 * scale)),
            fontsize=stats_fs, family='monospace'
        )
        
        try:
            if area_scale < 0.85:
                stats_text_obj.set_linespacing(0.95)
            elif area_scale < 1.25:
                stats_text_obj.set_linespacing(1.10)
            else:
                stats_text_obj.set_linespacing(1.20)
        except Exception:
            pass

        ax1.set_ylabel("Return (Decimal)", fontsize=label_fs)
        ax1.legend(loc='upper left', fontsize=legend_fs)
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(df[self.TIME_COL].min(), df[self.TIME_COL].max())
        ax1.tick_params(axis='both', labelsize=tick_fs)

        ax2.fill_between(
            df[self.TIME_COL], df['drawdown'] * 100, 0,
            color='#ff9999', alpha=0.6, label='Drawdown'
        )
        ax2.plot(df[self.TIME_COL], df['drawdown'] * 100, color='red',
                 linewidth=max(0.6, 0.8 * scale), alpha=0.8)
        ax2.set_ylabel('Drawdown (%)', fontsize=label_fs)
        ax2.legend(loc='lower left', fontsize=legend_fs)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(df[self.TIME_COL].min(), df[self.TIME_COL].max())
        ax2.tick_params(axis='both', labelsize=tick_fs)

        if multiple_symbols:
            ax3.plot(df[self.TIME_COL], df['active_positions'], color='purple',
                     linewidth=max(0.6, 0.8 * scale), label='Active Positions', alpha=0.6)
            ax3.set_ylabel('Active Positions', fontsize=label_fs)
        else:
            position_label = self.exchange1 if self.exchange1 else "Position"
            ax3.plot(df[self.TIME_COL], df['signal'], color='purple',
                     linewidth=max(0.6, 0.8 * scale), label='Position', alpha=0.6)
            ax3.set_ylabel(position_label, fontsize=label_fs)
        
        ax3.legend(loc='upper right', fontsize=legend_fs)
        ax3.tick_params(axis='y', labelcolor='purple')
        ax3.set_xlabel('Time', fontsize=label_fs)
        ax3.grid(True, alpha=0.3)
        ax3.set_xlim(df[self.TIME_COL].min(), df[self.TIME_COL].max())
        ax3.tick_params(axis='both', labelsize=tick_fs)

        time_span = (df[self.TIME_COL].max() - df[self.TIME_COL].min()).days
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

        print(f"Plot: {len(df):,} points | {time_span} days | {df[self.TIME_COL].min().date()} to {df[self.TIME_COL].max().date()}")


    def plot_fr_and_diff_cumsum(
        self, 
        symbol: str, 
        title: Optional[str] = None, 
        start_date: Optional[str] = None
    ) -> None:
        """
        Plot two exchanges' funding rates (top) and FR diff cumulative sum (bottom).

        Only supports cross-exchange mode. Requires parquet file with:
            - <Exchange1>_FR_1H
            - <Exchange2>_FR_1H
            - Diff
            - Diff_cumsum

        Args:
            symbol: Trading symbol.
            title: Optional chart title.
            start_date: Optional start date filter.
        """
        if not self.is_cross_exchange:
            raise ValueError("plot_fr_and_diff_cumsum only supports cross-exchange mode.")

        normalized_symbol = self._normalize_symbol(symbol)
        file_path = self.path / f"{normalized_symbol}.parquet"
        
        if not file_path.exists():
            # Fallback logic if needed, or just let read_parquet fail/return empty
            pass

        try:
             df = pd.read_parquet(file_path)
        except Exception:
             print(f"[{symbol}] Failed to read data.")
             return

        if len(df) == 0:
            print(f"[{symbol}] No data available. Skipping plot.")
            return

        df[self.TIME_COL] = pd.to_datetime(df[self.TIME_COL])
        if start_date is not None:
            df = df[df[self.TIME_COL] >= start_date].reset_index(drop=True)
        if df.empty:
            print(f"[{symbol}] No data after start_date. Skipping plot.")
            return

        # Ensure Diff or Diff_cumsum exists
        if ("Diff_cumsum" not in df.columns) and ("Diff" not in df.columns):
            raise ValueError("DataFrame must contain 'Diff' or 'Diff_cumsum' column.")

        # Recalculate cumsum to start from 0 for the plotted period
        if "Diff" in df.columns:
            df["Diff_cumsum_plot"] = df["Diff"].cumsum()
        else:
            base = df["Diff_cumsum"].iloc[0]
            df["Diff_cumsum_plot"] = df["Diff_cumsum"] - base

        ex1_col = f"{self.exchange1.capitalize()}_FR_1H"
        ex2_col = f"{self.exchange2.capitalize()}_FR_1H"
        
        # Check if columns exist
        if ex1_col not in df.columns or ex2_col not in df.columns:
             # Try fallback capitalization if standard fails
             ex1_col = f"{self.exchange1}_FR_1H" 
             ex2_col = f"{self.exchange2}_FR_1H"
             
        if ex1_col not in df.columns or ex2_col not in df.columns:
            print(f"Missing columns {ex1_col} or {ex2_col} in parquet.")
            return

        if title is None:
            title = f"{symbol} - {self.exchange1.upper()} vs {self.exchange2.upper()}"

        plt.style.use("default")
        fig, (ax1, ax2) = plt.subplots(
            2, 1,
            figsize=(16, 8),
            gridspec_kw={"height_ratios": [2, 1]},
            sharex=True,
        )
        fig.suptitle(title, fontsize=16, fontweight="bold")

        # Top: Exchange Funding Rates
        ax1.plot(df[self.TIME_COL], df[ex1_col],
                 label=f"{self.exchange1.capitalize()} FR",
                 color="#1f77b4", alpha=0.8, linewidth=1.2)
        ax1.plot(df[self.TIME_COL], df[ex2_col],
                 label=f"{self.exchange2.capitalize()} FR",
                 color="#ff7f0e", alpha=0.8, linewidth=1.2)
        ax1.axhline(0, color="gray", linestyle="--", linewidth=0.8, alpha=0.5)

        ax1.set_ylabel("Funding Rate", fontsize=12)
        ax1.legend(loc="upper right", fontsize=10)
        ax1.grid(True, alpha=0.3)

        # Bottom: FR Diff Cumsum
        ax2.plot(df[self.TIME_COL], df["Diff_cumsum_plot"],
                 label="FR Diff Cumsum", color="#2ca02c",
                 linewidth=1.2)
        ax2.axhline(0, color="gray", linestyle="--", linewidth=0.8, alpha=0.5)

        ax2.set_xlabel("Time", fontsize=12)
        ax2.set_ylabel("Diff Cumsum", fontsize=12)
        ax2.legend(loc="upper left", fontsize=10)
        ax2.grid(True, alpha=0.3)

        # X-axis formatting
        time_span = (df[self.TIME_COL].max() - df[self.TIME_COL].min()).days
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

    def plot_top_earning_losing(self, df: List[pd.DataFrame], top: int) -> None:
        """
        Plot top N earning and losing symbols with fee comparisons.
        
        Args:
            df: List of DataFrames [base_pnl, pnl_with_fee_no_spread, pnl_with_fee].
            top: Number of symbols to show.
        """
        # ---- Data Prep ----
        symbol_summary_pnl = self.symbols_summary(df[0], time_col=self.TIME_COL)
        symbol_summary_pnl_fee_no_spread = self.symbols_summary(df[1], time_col=self.TIME_COL)
        symbol_summary_pnl_fee = self.symbols_summary(df[2], time_col=self.TIME_COL)
        
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
        ax1.bar(x_earning, top_earning['pnl'], bar_width, 
                label='Base PnL', color=colors[0], edgecolor='#B0BEC5', linewidth=1.5, zorder=3)
        ax1.bar(x_earning, top_earning['pnl_no_spread'], bar_width,
                label='With Fee (No Spread)', color=colors[1], edgecolor='#90A4AE', linewidth=1.5, zorder=4)
        ax1.bar(x_earning, top_earning['pnl_with_fee'], bar_width,
                label='With Fee + Spread', color=colors[2], edgecolor='#546E7A', linewidth=1.5, zorder=5)

        # Add value labels
        for i, with_fee in enumerate(top_earning['pnl_with_fee']):
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
        ax2.bar(x_losing, top_losing['pnl'], bar_width,
                label='Base PnL', color=colors[0], edgecolor='#B0BEC5', linewidth=1.5, zorder=3)
        ax2.bar(x_losing, top_losing['pnl_no_spread'], bar_width,
                label='With Fee (No Spread)', color=colors[1], edgecolor='#90A4AE', linewidth=1.5, zorder=4)
        ax2.bar(x_losing, top_losing['pnl_with_fee'], bar_width,
                label='With Fee + Spread', color=colors[2], edgecolor='#546E7A', linewidth=1.5, zorder=5)

        for i, with_fee in enumerate(top_losing['pnl_with_fee']):
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
        
        print(f"\nTop {top} Earning: {', '.join(top_earning['symbol'].tolist())}")
        print(f"Top {top} Losing: {', '.join(top_losing['symbol'].tolist())}")

    def plot_position_heatmap(self, df: pd.DataFrame) -> None:
        """
        Plot calendar heatmap of active positions by day (separate plot per year).

        Args:
            df: DataFrame containing backtest results with 'active_symbols' or 'active_positions'.
        """
        if 'active_symbols' not in df.columns:
            print("'active_symbols' column missing in DataFrame. Cannot plot heatmap.")
            return

        df_copy = df.copy()
        # Group by day
        daily = df_copy.groupby(pd.Grouper(freq='D', key=self.TIME_COL)).sum().reset_index()
        
        # Calculate active positions per day (this logic assumes summing hourly 'active_positions' which 
        # gives total position-hours, or we want max positions? The original code did set(symbols) then len.
        # Original logic:
        # daily['active_symbols'] = daily['active_symbols'].apply(lambda x: list(set(x))) 
        # But 'active_symbols' in original df is a list of strings [S1, S2]. Summing lists concatenates them.
        # So sum() makes a giant list of all symbols active in that day. set() gets unique. len() gets count.
        
        # Replicating original logic safely:
        # Note: sum() on object column (lists) works in pandas to concatenate.
        daily['active_symbols'] = daily['active_symbols'].apply(lambda x: list(set(x)) if isinstance(x, list) else [])
        daily['active_positions'] = daily['active_symbols'].apply(len).astype(int)
        
        daily[self.TIME_COL] = pd.to_datetime(daily[self.TIME_COL])
        daily['year'] = daily[self.TIME_COL].dt.year
        daily['month'] = daily[self.TIME_COL].dt.month
        daily['day'] = daily[self.TIME_COL].dt.day

        for year in daily.year.unique():
            pivot_df = daily[daily['year'] == year].pivot_table(
                index='month', columns='day', values='active_positions', fill_value=0
            )
            plt.figure(figsize=(20, 8))
            sns.heatmap(
                pivot_df,
                cmap="YlGnBu",
                linewidths=0.5,
                linecolor='gray',
                cbar_kws={'label': 'Unique Active Symbols per Day'}
            )

            plt.title(f'Active Symbols Count by Calendar Day in {year}', fontsize=16, fontweight='bold')
            plt.xlabel('Day of Month')
            plt.ylabel('Month')
            plt.tight_layout()
            plt.show()

    def portfolio_by_month(self, df: pd.DataFrame) -> None:
        """
        Aggregate and plot monthly portfolio performance.

        Args:
            df: DataFrame containing backtest results.
        """
        df = df.copy()
        df[self.TIME_COL] = pd.to_datetime(df[self.TIME_COL])
        df['year_month'] = df[self.TIME_COL].dt.to_period('M').astype(str)

        monthly_summary = df.groupby('year_month').agg({
            'base_pnl': 'sum',
            'base_pnl_with_fee_no_spread': 'sum',
            'base_pnl_with_fee': 'sum',
            'n_0to1': 'sum',
            'n_1to0': 'sum',
        }).reset_index()

        fig = plt.figure(figsize=(12, 6))
        ax = fig.add_subplot(111)
        
        x = monthly_summary['year_month']
        w = 0.25
        x_idx = np.arange(len(x))

        ax.bar(x_idx - w, monthly_summary['base_pnl'], width=w, label='Base PnL', alpha=0.7)
        ax.bar(x_idx, monthly_summary['base_pnl_with_fee_no_spread'], width=w, label='PnL with Fee (No Spread)', alpha=0.7)
        ax.bar(x_idx + w, monthly_summary['base_pnl_with_fee'], width=w, label='PnL with Fee', alpha=0.7)

        ax.set_title('Monthly Portfolio Performance')
        ax.set_xlabel('Year-Month')
        ax.set_ylabel('PnL')
        ax.legend()
        ax.set_xticks(x_idx)
        ax.set_xticklabels(x, rotation=45, size=10)
        
        plt.tight_layout()
        plt.show()

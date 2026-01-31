"""
Funding Rate Data Transformation and Loading Module.

This module is responsible for loading raw funding rate data, transforming it
into a standardized format, and managing the "clean" data storage.
"""

import concurrent.futures
from abc import ABC
from typing import Optional, List, Tuple, Union

import pandas as pd
from tqdm import tqdm

from src import FundingRateStorage, KlinesStorage
from .storage import CleanDataStorage


def calculate_time_diff_in_hours(df: pd.DataFrame) -> float:
    """
    Calculate the time difference in hours between the last two data points.
    
    Args:
        df (pd.DataFrame): The dataframe containing a 'Time' column.
        
    Returns:
        float: Time difference in hours, minimum 1.0.
    """
    if df.empty or 'Time' not in df.columns or len(df) < 2:
        return 0.0
    
    # Ensure sorted order for calculation
    # Note: creating a copy to avoid side effects if not intended
    df_sorted = df.sort_values('Time')
    time_diff = (df_sorted['Time'].iloc[-1] - df_sorted['Time'].iloc[-2]).total_seconds() / 3600
    
    return time_diff if time_diff > 0 else 1.0


class DataTransform(ABC):
    """
    Data Transformer for Funding Rates.
    
    Handles the ETL process: Extracting from raw storage, Transforming (resampling,
    normalizing), and Loading into clean storage.
    """
    
    TIME_COL = 'Time'
    
    def __init__(self, exchange_id: str):
        """
        Initialize the DataTransform pipeline.

        Args:
            exchange_id (str): The exchange identifier.
        """
        self.exchange_id = exchange_id
        self.funding_rate_storage = FundingRateStorage(exchange_id)
        self.clean_storage = CleanDataStorage(exchange_id)
        self.klines_storage = KlinesStorage(exchange_id)
    
    # ==================== Helper Functions ====================
    
    def _load_raw_data(self, symbol: str) -> pd.DataFrame:
        """
        Load raw funding rate data for a symbol.
        
        Args:
            symbol (str): The trading symbol.
            
        Returns:
            pd.DataFrame: Raw data dataframe.
        """
        print(f"[{self.exchange_id}] Loading raw data for {symbol}")
        return self.funding_rate_storage.read(symbol)
    
    def _validate_data(self, df: pd.DataFrame, 
                       required_columns: Optional[List[str]] = None, 
                       symbol: Optional[str] = None) -> bool:
        """
        Validate dataframe contents.
        
        Args:
            df (pd.DataFrame): Dataframe to check.
            required_columns (Optional[List[str]]): List of columns that must exist.
            symbol (Optional[str]): Symbol name for logging.
            
        Returns:
            bool: True if valid, False otherwise.
        """
        prefix = f"[{self.exchange_id}|{symbol}]" if symbol else f"[{self.exchange_id}]"
        
        if df.empty:
            print(f"{prefix} Data is empty")
            return False
        
        if required_columns:
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                print(f"{prefix} Missing required columns: {missing_cols}")
                print(f"{prefix} Available columns: {list(df.columns)}")
                return False
        
        return True
    
    def _check_data_freshness(self, clean_df: pd.DataFrame, raw_df: pd.DataFrame) -> bool:
        """
        Check if raw data has newer entries than clean data.
        
        Args:
            clean_df (pd.DataFrame): Existing clean data.
            raw_df (pd.DataFrame): Newly loaded raw data.
            
        Returns:
            bool: True if raw data contains newer timestamps.
        """
        if not self._validate_data(clean_df, [self.TIME_COL]) or \
           not self._validate_data(raw_df, [self.TIME_COL]):
            return True
            
        return raw_df[self.TIME_COL].max() > clean_df[self.TIME_COL].max()
        
    def _resample_data(self, df: pd.DataFrame, freq: str = '1h', 
                       symbol: Optional[str] = None) -> pd.DataFrame:
        """
        Resample data to a fixed frequency using forward fill.
        
        Args:
            df (pd.DataFrame): Input dataframe.
            freq (str): Target frequency (e.g., '1h').
            symbol (Optional[str]): Symbol used for logging.
            
        Returns:
            pd.DataFrame: Resampled dataframe.
        """
        if not self._validate_data(df, [self.TIME_COL], symbol):
            return df
        
        prefix = f"[{self.exchange_id}|{symbol}]" if symbol else f"[{self.exchange_id}]"
        print(f"{prefix} Resampling data to frequency: {freq}")
        
        df = df.set_index(self.TIME_COL)
        df = df.resample(freq).ffill().reset_index()
        return df
    
    def _prepare_funding_data(self, df: pd.DataFrame, symbol: Optional[str] = None) -> pd.DataFrame:
        """
        Normalize funding rate data and calculate hourly rates.
        
        Args:
            df (pd.DataFrame): Raw funding data.
            symbol (Optional[str]): Symbol for logging.
            
        Returns:
            pd.DataFrame: Processed dataframe with 'FundingRate_hourly'.
        """
        if not self._validate_data(df, [self.TIME_COL, 'FundingRate'], symbol):
            return df
            
        df = df.copy()
        df[self.TIME_COL] = pd.to_datetime(df[self.TIME_COL], utc=True)
        df['FundingRate'] = pd.to_numeric(df['FundingRate'], errors='coerce')
        
        # Calculate time interval to next row (in hours)
        df['Interval_hours'] = (df['Time'].shift(-1) - df['Time']).dt.total_seconds() / 3600
        
        # Handle the last row
        if len(df) > 1:
            # Use interval from the second to last row
            last_interval = df['Interval_hours'].iloc[-2]
            if pd.notna(last_interval):
                df.loc[df.index[-1], 'Interval_hours'] = last_interval
            else:
                # Fallback calculation
                time_diff = (df['Time'].iloc[-1] - df['Time'].iloc[-2]).total_seconds() / 3600
                df.loc[df.index[-1], 'Interval_hours'] = time_diff
        else:
            # Default to 8 hours (standard crypto funding interval)
            df.loc[df.index[-1], 'Interval_hours'] = 8.0
        
        # Normalize to hourly rate
        df['FundingRate_hourly'] = df['FundingRate'] / df['Interval_hours']
        
        return df

    def _prepare_volume_data(self, volume_df: pd.DataFrame, 
                             symbol: Optional[str] = None) -> pd.DataFrame:
        """
        Prepare volume/kline data for merging.
        
        Args:
            volume_df (pd.DataFrame): Raw kline data.
            symbol (Optional[str]): Symbol for validation.
            
        Returns:
            pd.DataFrame: Processed volume data with 'Value' column.
        """
        if not self._validate_data(volume_df, ['Time', 'Open', 'Volume'], symbol):
            return pd.DataFrame()
            
        volume_df = volume_df.copy()
        volume_df['Value'] = volume_df['Open'] * volume_df['Volume']
        volume_df = volume_df[['Time', 'Value', 'Open']]
        volume_df['Time'] = pd.to_datetime(volume_df['Time'], utc=True)
        return volume_df

    def _process_symbol_data(self, symbol: str, include_volume: bool = True) -> pd.DataFrame:
        """
        Full processing pipeline for a single symbol.
        
        Args:
            symbol (str): The trading symbol.
            include_volume (bool): Whether to merge volume data.
            
        Returns:
            pd.DataFrame: Transformed and merged data.
        """
        print(f"[{self.exchange_id}|{symbol}] Processing symbol data...")
        
        # Load and transform funding data
        funding_df = self._load_raw_data(symbol)
        transformed_df = self.transform_raw_data(funding_df, symbol=symbol)
        
        # Merge volume data if requested
        if include_volume:
            print(f"[{self.exchange_id}|{symbol}] Loading volume data...")
            volume_df = self.klines_storage.read(symbol)
            
            # Validate volume data
            if volume_df.empty or not self._validate_data(volume_df, ['Time', 'Open', 'Volume'], symbol):
                print(f"[{self.exchange_id}|{symbol}] No valid volume data, skipping volume merge.")
            else:
                transformed_df = self.merge_volume_data(transformed_df, volume_df, symbol)
        
        print(f"[{self.exchange_id}|{symbol}] ✓ Processing completed")
        return transformed_df
    
    # ==================== Public Methods ====================
        
    def transform_raw_data(self, df: pd.DataFrame, freq: str = '1h', 
                           symbol: Optional[str] = None) -> pd.DataFrame:
        """
        Transform raw DataFrame: Normalize columns, calculate rates, resample.
        
        Args:
            df (pd.DataFrame): Raw input data.
            freq (str): Resample frequency.
            symbol (Optional[str]): Symbol identifier.
            
        Returns:
            pd.DataFrame: Transformed data.
        """
        df = self._prepare_funding_data(df, symbol)
        df = self._resample_data(df, freq, symbol)
        return df
    
    def merge_volume_data(self, funding_df: pd.DataFrame, volume_df: pd.DataFrame, 
                          symbol: Optional[str] = None) -> pd.DataFrame:
        """
        Merge transformed funding data with volume data.
        
        Args:
            funding_df (pd.DataFrame): Transformed funding data.
            volume_df (pd.DataFrame): Raw volume data.
            symbol (Optional[str]): Symbol identifier.
            
        Returns:
            pd.DataFrame: Merged dataframe.
        """
        prefix = f"[{self.exchange_id}|{symbol}]" if symbol else f"[{self.exchange_id}]"
        print(f"{prefix} Merging funding rate data with volume data: {len(funding_df)} rows + {len(volume_df)} rows")
        
        # Prepare volume data
        volume_df = self._prepare_volume_data(volume_df, symbol)
        
        if volume_df.empty:
            print(f"{prefix} Volume data is empty, returning funding data without volume.")
            return funding_df
        
        # Check for Time column presence
        if 'Time' not in funding_df.columns:
            print(f"{prefix} Warning: 'Time' column not found in funding_df")
            return funding_df
        if 'Time' not in volume_df.columns:
            print(f"{prefix} Warning: 'Time' column not found in volume_df")
            return funding_df
        
        # Merge operation
        merged_df = pd.merge(funding_df, volume_df, on='Time', how='left')
        
        print(f"{prefix} ✓ Merge completed: {len(merged_df)} rows")
        return merged_df

    def transform_symbol(self, symbol: str) -> pd.DataFrame:
        """
        Execute transformation pipeline and save clean data for a symbol.
        
        Args:
            symbol (str): The trading symbol.
            
        Returns:
            pd.DataFrame: The saved clean dataframe.
        """
        print(f"[{self.exchange_id}|{symbol}] Starting transformation...")
        transformed_df = self._process_symbol_data(symbol)
        self.clean_storage.write(transformed_df, symbol)
        print(f"[{self.exchange_id}|{symbol}] ✓ Transformation completed and saved\n")
        return transformed_df
        
    def load_symbol(self, symbol: str) -> pd.DataFrame:
        """
        Load clean data from storage.
        
        Args:
            symbol (str): The trading symbol.
            
        Returns:
            pd.DataFrame: Clean data.
        """
        print(f"[{self.exchange_id}|{symbol}] Loading clean data...")
        return self.clean_storage.read(symbol)

    def update_symbol(self, symbol: str) -> pd.DataFrame:
        """
        Incrementally update smooth data for a symbol.
        
        Args:
            symbol (str): The trading symbol.
            
        Returns:
            pd.DataFrame: Updated dataframe.
        """
        print(f"[{self.exchange_id}|{symbol}] Checking for updates...")
        
        # Check if clean data exists
        clean_df = self.clean_storage.read(symbol)
        if not self._validate_data(clean_df, [self.TIME_COL], symbol):
            print(f"[{self.exchange_id}|{symbol}] Clean data does not exist. Starting full transformation...")
            return self.transform_symbol(symbol)

        # Check raw data freshness
        raw_df = self._load_raw_data(symbol)
        if self._check_data_freshness(clean_df, raw_df):
            print(f"[{self.exchange_id}|{symbol}] New data available, updating...")
            
            last_time = clean_df[self.TIME_COL].max()
            
            # Filter new data
            new_funding_data = raw_df[raw_df[self.TIME_COL] > last_time]
            print(f"[{self.exchange_id}|{symbol}] Processing {len(new_funding_data)} new rows...")
            
            # Transform new chunk
            new_transformed = self.transform_raw_data(new_funding_data, symbol=symbol)
            
            # Merge volume if available
            volume_df = self.klines_storage.read(symbol)
            if not volume_df.empty and self._validate_data(volume_df, ['Time', 'Open', 'Volume'], symbol):
                volume_df['Time'] = pd.to_datetime(volume_df['Time'], utc=True)
                new_volume_data = volume_df[volume_df['Time'] > last_time]
                
                if not new_volume_data.empty:
                    new_transformed = self.merge_volume_data(new_transformed, new_volume_data, symbol)
            
            # Append and Save
            clean_df = pd.concat([clean_df, new_transformed], ignore_index=True)
            self.clean_storage.write(clean_df, symbol)
            print(f"[{self.exchange_id}|{symbol}] ✓ Update completed\n")
            return clean_df
        else:
            print(f"[{self.exchange_id}|{symbol}] Data is up-to-date\n")
            return clean_df

    def reset_symbol(self, symbol: str) -> pd.DataFrame:
        """
        Force re-calculation of clean data for a symbol (delete and regenerate).
        
        Args:
            symbol (str): The trading symbol.
            
        Returns:
            pd.DataFrame: Regenerated clean data.
        """
        print(f"[{self.exchange_id}|{symbol}] Resetting clean data...")
        if self.clean_storage.exists(symbol):
            self.clean_storage.delete(symbol)
            print(f"[{self.exchange_id}|{symbol}] Existing data deleted")
        
        return self.transform_symbol(symbol)
    
    def _update_symbol_safe(self, symbol: str) -> Tuple[str, bool, Optional[str]]:
        """
        Parallel processing wrapper for update_symbol.
        
        Args:
            symbol (str): The trading symbol.
            
        Returns:
            Tuple[str, bool, Optional[str]]: (symbol, success_flag, error_message)
        """
        try:
            self.update_symbol(symbol)
            return symbol, True, None
        except Exception as e:
            error_msg = str(e)
            print(f"\n[{self.exchange_id}|{symbol}] ❌ ERROR: {error_msg}")
            return symbol, False, error_msg
    
    def transform_all_symbols(self, freq: str = '1h', max_workers: int = 4) -> None:
        """
        Batch process and transform all available symbols concurrently.
        
        Args:
            freq (str): Sampling frequency.
            max_workers (int): Number of parallel threads.
        """
        symbols = self.funding_rate_storage.list_symbols()
        print(f"\n{'='*60}")
        print(f"[{self.exchange_id}] Starting batch transformation")
        print(f"[{self.exchange_id}] Total symbols: {len(symbols)}")
        print(f"[{self.exchange_id}] Frequency: {freq}")
        print(f"[{self.exchange_id}] Max workers: {max_workers}")
        print(f"{'='*60}\n")
        
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self._update_symbol_safe, symbol): symbol 
                for symbol in symbols
            }
            
            for future in tqdm(concurrent.futures.as_completed(future_to_symbol), 
                               total=len(symbols),
                               desc=f"[{self.exchange_id}] Transforming"):
                symbol, success, error = future.result()
                results.append((symbol, success, error))
        
        # Summary
        success_count = sum(1 for _, success, _ in results if success)
        failed_count = len(results) - success_count
        
        print(f"\n{'='*60}")
        print(f"[{self.exchange_id}] ✓ Batch transformation completed")
        print(f"[{self.exchange_id}] Success: {success_count}/{len(symbols)}")
        
        if failed_count > 0:
            print(f"[{self.exchange_id}] Failed: {failed_count}/{len(symbols)}")
            print(f"[{self.exchange_id}] Failed symbols:")
            for symbol, success, error in results:
                if not success:
                    print(f"  - {symbol}: {error}")
        print(f"{'='*60}\n")
    
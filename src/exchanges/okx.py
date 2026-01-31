"""
OKX exchange data fetchers.

This module contains the specific implementations for fetching funding rates
and klines (OHLCV) from the OKX exchange using the CCXT library.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import pandas as pd
from tqdm import tqdm

from ..adapters.Exchange_base import ExchangeFetcher
from ..adapters.storage import FundingRateStorage, KlinesStorage


class OkxFundingFetcher(ExchangeFetcher):
    """
    Fetcher for OKX Funding Rate data.
    """
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = FundingRateStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch raw funding rate history from OKX.
        
        Args:
            symbol (str): The trading symbol.
            since (Optional[str]): Start date string.
            limit (Optional[int]): Not used by OKX implementation currently.

        Returns:
            List[Dict[str, Any]]: List of raw funding rate records.
        """
        all_infos = []
        print(f"Fetching {symbol} funding rate history from {self._get_exchange().id} since {since}")
        
        # Normalize 'since' to milliseconds epoch (UTC)
        if since is None:
            # Default to 3 months ago if not provided (OKX limitation)
            since_api = int((pd.Timestamp.now(tz='UTC') - pd.DateOffset(months=3)).timestamp() * 1000)
        elif isinstance(since, (int, float)):
             # If numeric, assume ms when > 1e12 else seconds
            since_api = int(since if since > 1_000_000_000_000 else since * 1000)
        else:
            # String/datetime-like
            since_api = int(pd.to_datetime(since, utc=True).timestamp() * 1000)

        # OKX typically limits historical retrieval to ~3 months
        three_months_ago_ms = int((pd.Timestamp.now(tz='UTC') - pd.DateOffset(months=3)).timestamp() * 1000)
        
        if since_api < three_months_ago_ms:
            print("Warning: OKX API may not return full historical data when 'since' is older than ~3 months.")
            
        try:
            # OKX endpoint usually returns minimal history unless iterated, 
            # but current implementation seems to rely on single call or specific CCXT behavior?
            # Warning: fetch_funding_rate_history behavior varies by exchange in CCXT.
            # Assuming current behavior is desired, just wrapping in try/except and type hint.
            fetched_data = self.exchange.fetch_funding_rate_history(symbol=symbol, since=since_api)
            all_infos.extend(fetched_data)
        except Exception as e:
            print(f"Error fetching data from OKX: {e}")
            
        return all_infos

    def _normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw OKX data into a standard DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data)
        
        # Drop unnecessary columns first
        df.drop(columns=['info', 'datetime'], errors='ignore', inplace=True)
        
        # Standardize column names
        column_mapping = {
            'symbol': 'Symbol',
            'timestamp': 'Time',
            'fundingRate': 'FundingRate',
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Convert Time to datetime
        df['Time'] = pd.to_datetime(df['Time'], unit='ms', utc=True)
        df['Exchange'] = self._get_exchange().id
        
        return df
    
    
class OkxKlinesFetcher(ExchangeFetcher):
    """
    Fetcher for OKX Kline (OHLCV) data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = KlinesStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[List[float]]:
        """
        Fetch raw kline data from OKX.
        
        Note: OKX history can be sparse or require searching, implemented here
        with a "scan forward if empty" strategy.
        """
        all_infos = []
        batch = []
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        print(f"Fetching {symbol} klines history from {self._get_exchange().id} since {since}")
        since_api = int(pd.to_datetime(since).timestamp() * 1000)  # Convert to ms
        
        with tqdm(desc=f"{self._get_exchange().id} {symbol} klines history", unit="batch") as pbar:
            while True:
                try:
                    # Using 'HistoryCandles' usually allows more depth than standard candles
                    batch = self.exchange.fetch_ohlcv(
                        symbol,
                        timeframe='1h',
                        since=since_api,
                        limit=100,  # Lower limit for safety in pagination
                        params={'type': 'HistoryCandles'}
                    )
                except Exception as e:
                    print(f"Error fetching klines: {e}")
                    break
                
                if not batch:
                    # If no data found, move forward by 3 days and try again (skip gaps)
                    since_api += 86400000 * 3  # Add three days in milliseconds
                    
                    # Stop if we've reached current time
                    if since_api >= now_ts:
                        print("Reached current date without finding more data")
                        break
                else:
                    all_infos.extend(batch)
                    last_timestamp = batch[-1][0]
                    since_api = last_timestamp + 1  
                    pbar.update(1)

        return all_infos
    
    def _normalize_data(self, raw_data: List[List[float]]) -> pd.DataFrame:
        """
        Normalize raw kline lists into a DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data, columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
        
        # Ensure numeric types
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # Standardize Time column
        df['Time'] = pd.to_numeric(df['Time'], errors='coerce')
        df['Time'] = pd.to_datetime(df['Time'], unit='ms', errors='coerce', utc=True)
        
        return df
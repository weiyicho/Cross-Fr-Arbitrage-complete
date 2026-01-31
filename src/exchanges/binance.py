"""
Binance exchange data fetchers.

This module contains the specific implementations for fetching funding rates
and klines (OHLCV) from the Binance exchange using the CCXT library.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import pandas as pd
from tqdm import tqdm

from ..adapters.Exchange_base import ExchangeFetcher
from ..adapters.storage import FundingRateStorage, KlinesStorage


class BinanceFundingFetcher(ExchangeFetcher):
    """
    Fetcher for Binance Funding Rate data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since=None):
        super().__init__(exchange, since)
        self.storage = FundingRateStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch raw funding rate history from Binance.
        
        Args:
            symbol (str): The trading symbol (e.g., 'BTC/USDT').
            since (Optional[str]): Start date string.
            limit (Optional[int]): Number of records to fetch.

        Returns:
            List[Dict[str, Any]]: List of raw funding rate dictionaries.
        """
        all_infos = []
        batch = []
        
        print(f"Fetching {symbol} funding rate history from {self._get_exchange().id} since {since}")
        
        # Convert start time to milliseconds timestamp
        since_api = int(pd.to_datetime(since).timestamp() * 1000)
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        prev_last_ts = 0
        page = 0
        limit_per_page = limit if limit and limit < 1000 else 1000  # Binance supports up to 1000
        
        with tqdm(desc=f"{self._get_exchange().id} {symbol} funding history", unit="page") as pbar:
            while True:
                try:
                    batch = self._get_exchange().fetch_funding_rate_history(
                        symbol,
                        since=since_api,
                        limit=limit_per_page
                    )
                except Exception as e:
                    print(f"Error fetching {symbol} on {self._get_exchange().id}: {e}")
                    break
                
                if not batch:
                    break
                
                # Extract 'info' usage varies by exchange, but here we consolidate
                # In CCXT `fetch_funding_rate_history`, we usually get a standardized structure.
                # However, the original code extracted 'info' if available. 
                # We will keep the logic to collect `info` or the item itself.
                all_infos.extend([fr.get('info', fr) for fr in batch])

                last_fr = batch[-1]
                
                # improved timestamp extraction
                if 'timestamp' in last_fr and last_fr['timestamp']:
                     last_ts = last_fr['timestamp']
                else:
                    # Fallback to info dictionary
                    last_fr_info = last_fr.get('info', {})
                    last_ts = (
                        last_fr_info.get('timestamp') or 
                        last_fr_info.get('t') or 
                        last_fr_info.get('fundingRateTimestamp')
                    )

                if last_ts is None:
                    print(f"No timestamp found in last item at page {page}")
                    break
                    
                if last_ts >= now_ts + 1:
                    print(f"Reached current data at page {page}")
                    break

                if prev_last_ts is not None and last_ts <= prev_last_ts:
                    print(f"Timestamp stalled at page {page} ({last_ts} <= {prev_last_ts})")
                    break

                prev_last_ts = last_ts
                since_api = last_ts + 1  # Increment to fetch next batch
                
                # Break if we got less than expected (likely end of data)
                if len(batch) < limit_per_page:
                    break
                
                page += 1
                pbar.update(1)
        
        return all_infos
        
    def _normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw Binance data into a standard DataFrame.

        Args:
            raw_data (List[Dict[str, Any]]): Raw data list.

        Returns:
            pd.DataFrame: Normalized DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data)
        
        # Standardize column names
        column_mapping = {
            'symbol': 'Symbol',
            'fundingTime': 'Time',
            'fundingRate': 'FundingRate',
            'markPrice': 'MarkPrice',  # Standardize camelCase check
            'markprice': 'MarkPrice',  # For robust matching
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Ensure required columns exist
        required_columns = ['Symbol', 'Time', 'FundingRate']
        for col in required_columns:
            if col not in df.columns:
                print(f"Warning: Missing expected column '{col}' in data")
                
        df['Exchange'] = self._get_exchange().id 
        
        # Convert Time to numeric first to avoid FutureWarning
        df['Time'] = pd.to_numeric(df['Time'], errors='coerce')
        df['Time'] = pd.to_datetime(df['Time'], unit='ms', errors='coerce', utc=True)
        df["Time"] = df["Time"].dt.floor("s")
        
        # Convert FundingRate to numeric
        df['FundingRate'] = pd.to_numeric(df['FundingRate'], errors='coerce')
        
        return df
        
    def _normalize_symbol_name(self, symbol: str) -> str:
        """
        Normalize symbol name (remove separate base/quote logic if needed).
        Current implementation just takes the base part if split by '/'.
        """
        return symbol.split("/")[0]

    # ------------- NEW FUNCTIONALITY ------------- #
    def get_perpetual_symbols(self) -> None:
        """
        Filter symbols to keep only USDT perpetuals (ending in :USDT).
        Updates self.symbols in place.
        """
        perpetuals = [s for s in self._get_symbols() if s.endswith(":USDT")]
        self.symbols = perpetuals


class BinanceKlinesFetcher(ExchangeFetcher):
    """
    Fetcher for Binance Kline (OHLCV) data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since=None):
        super().__init__(exchange, since)
        self.storage = KlinesStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[List[float]]:
        """
        Fetch raw kline data.

        Returns:
            List[List[float]]: List of OHLCV lists [timestamp, open, high, low, close, volume].
        """
        all_infos = []
        batch = []
        
        print(f"Fetching {symbol} klines history from {self._get_exchange().id} since {since}")
        since_api = int(pd.to_datetime(since).timestamp() * 1000)
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        prev_last_ts = 0
        page = 0
        limit_per_page = limit if limit and limit < 1000 else 1000
        
        with tqdm(desc=f"{self._get_exchange().id} {symbol} klines history", unit="page") as pbar:
            while True:
                try:
                    batch = self._get_exchange().fetch_ohlcv(
                        symbol,
                        timeframe='1h',
                        since=since_api,
                        limit=limit_per_page
                    )
                except Exception as e:
                    print(f"Error fetching {symbol} on {self._get_exchange().id}: {e}")
                    break
                
                if not batch:
                    break
                    
                all_infos.extend(batch)

                last_kline = batch[-1]
                if len(last_kline) > 0:
                    last_ts = last_kline[0]
                else:
                    print(f"No data in last kline at page {page}")
                    break

                if last_ts is None:
                    print(f"No timestamp found in last item at page {page}")
                    break
                    
                if last_ts >= now_ts + 1:
                    print(f"Reached current data at page {page}")
                    break

                if prev_last_ts is not None and last_ts <= prev_last_ts:
                    print(f"Timestamp stalled at page {page} ({last_ts} <= {prev_last_ts})")
                    break

                prev_last_ts = last_ts
                since_api = last_ts + 1 
                
                if len(batch) < limit_per_page:
                    break
                
                page += 1
                pbar.update(1)
        
        return all_infos
        
    def _normalize_data(self, raw_data: List[List[float]]) -> pd.DataFrame:
        """
        Normalize raw kline lists into a DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data, columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Time'] = pd.to_datetime(df['Time'], unit='ms', errors='coerce', utc=True)
        return df
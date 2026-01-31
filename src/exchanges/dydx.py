"""
dYdX exchange data fetchers.

This module contains the specific implementations for fetching funding rates
and klines (OHLCV) from the dYdX exchange using the CCXT library.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import pandas as pd
from tqdm import tqdm

from ..adapters.Exchange_base import ExchangeFetcher
from ..adapters.storage import FundingRateStorage, KlinesStorage


class DydxFundingFetcher(ExchangeFetcher):
    """
    Fetcher for dYdX Funding Rate data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = FundingRateStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch raw funding rate history from dYdX.
        
        Args:
            symbol (str): The trading symbol.
            since (str): Start date string.
            limit (int): Limit per page.
            
        Returns:
            List[Dict[str, Any]]: Funding rate records.
        """
        all_infos = []
        batch = []
        
        print(f"Fetching {symbol} funding rate history from {self._get_exchange().id} since {since}")
        
        since_api = int(pd.to_datetime(since).timestamp() * 1000)  # Convert to ms
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        prev_last_ts = 0
        page = 0
        limit_per_page = limit if limit and limit < 1000 else 100  # dYdX limits might vary
        
        with tqdm(desc=f"{self._get_exchange().id} {symbol} funding history", unit="page") as pbar:
            while True:
                try:
                    batch = self.exchange.fetch_funding_rate_history(
                        symbol,
                        since=since_api,
                        limit=limit_per_page
                    )
                except Exception as e:
                    print(f"Error fetching {symbol} on {self._get_exchange().id}: {e}")
                    break
                    
                if not batch:
                    break
                    
                # Extract 'info' if present, or use item itself
                all_infos.extend([fr.get('info', fr) for fr in batch])
                
                # Timestamp logic
                last_fr = batch[-1]
                last_ts = last_fr.get('timestamp')
                
                if last_ts is None:
                    # Try to look into info
                    info = last_fr.get('info', {})
                    last_ts = info.get('timestamp') or info.get('t') or info.get('fundingRateTimestamp')

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
        
    def _normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw dYdX data into a standard DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data)
        
        # Standardize column names
        # Note: adjust source column names based on actual API response if needed
        column_mapping = {
            'symbol': 'Symbol',
            'fundingTime': 'Time',
            'fundingRate': 'FundingRate',
            'markPrice': 'MarkPrice',
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Ensure required columns exist
        required_columns = ['Symbol', 'Time', 'FundingRate']
        for col in required_columns:
            if col not in df.columns:
                print(f"Warning: Missing expected column '{col}' in data. Columns found: {df.columns.tolist()}")
                
        df['Exchange'] = self._get_exchange().id 
        
        # Convert Time to numeric first to avoid the FutureWarning
        if 'Time' in df.columns:
            df['Time'] = pd.to_numeric(df['Time'], errors='coerce')
            df['Time'] = pd.to_datetime(df['Time'], unit='ms', errors='coerce', utc=True)
            df["Time"] = df["Time"].dt.floor("s")

        # Convert FundingRate to numeric
        if 'FundingRate' in df.columns:
            df['FundingRate'] = pd.to_numeric(df['FundingRate'], errors='coerce')
        
        return df
        
    def _normalize_symbol_name(self, symbol: str) -> str:
        """Normalize symbol name."""
        return symbol.split("/")[0]

    # ------------- NEW FUNCTIONALITY ------------- #
    def get_perpetual_symbols(self) -> None:
        """Filter for USDT perpetual symbols."""
        # dYdX symbols might not end with :USDT always, assume standard check
        perpetuals = [s for s in self._get_symbols() if s.endswith(":USDT") or 'PERP' in s]
        self.symbols = perpetuals


class DydxKlinesFetcher(ExchangeFetcher):
    """
    Fetcher for dYdX Kline (OHLCV) data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = KlinesStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[List[float]]:
        """
        Fetch raw kline data from dYdX.
        """
        all_infos = []
        batch = []
        
        print(f"Fetching {symbol} klines history from {self._get_exchange().id} since {since}")
        
        since_api = int(pd.to_datetime(since).timestamp() * 1000)  # Convert to ms
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        prev_last_ts = 0
        page = 0
        limit_per_page = limit if limit and limit < 1000 else 200
        
        with tqdm(desc=f"{self._get_exchange().id} {symbol} klines history", unit="page") as pbar:
            while True:
                try:
                    batch = self.exchange.fetch_ohlcv(
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
        
        # Ensure data types
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_cols:
             df[col] = pd.to_numeric(df[col], errors='coerce')
             
        df['Time'] = pd.to_datetime(df['Time'], unit='ms', errors='coerce', utc=True)
        return df
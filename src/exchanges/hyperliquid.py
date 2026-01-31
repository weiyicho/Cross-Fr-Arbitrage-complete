"""
Hyperliquid exchange data fetchers.

This module contains the specific implementations for fetching funding rates
and klines (OHLCV) from the Hyperliquid exchange using the CCXT library.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from time import sleep

import pandas as pd
from tqdm import tqdm

from ..adapters.Exchange_base import ExchangeFetcher
from ..adapters.storage import FundingRateStorage, KlinesStorage


class HyperliquidFundingFetcher(ExchangeFetcher):
    """
    Fetcher for Hyperliquid Funding Rate data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = FundingRateStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch raw funding rate history from Hyperliquid.
        
        Args:
            symbol (str): The trading symbol.
            since (str): Not fully utilized in the loop logic but passed to API.
            limit (int): Pagination limit.
            
        Returns:
            List[Dict[str, Any]]: List of funding rate records.
        """
        all_infos = []
        
        print(f"Fetching {symbol} funding rate history since {since}")
        
        # Hyperliquid API specifics might vary; assuming CCXT implementation allows 'since'
        # or pagination via time.
        since_api = 0
        limit_per_page = 100
        prev_last_ts = 0
        
        # NOTE: Infinite loop potential if not handled carefully with timestamps
        
        while True:
            sleep(0.2)  # Avoid rate limiting
            try:
                # Assuming custom method or passing through CCXT
                # The original code called self._get_exchange_id().fetchFundingRateHistory
                # self._get_exchange_id() likely returns strings? 
                # ExchangeFetcher._get_exchange() returns the object. 
                # Let's assume self.exchange is what we want.
                
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
                
            all_infos.extend(batch)
            
            # Logic from original code: update since_api based on last item
            # Hyperliquid structure in CCXT needs verification, but preserving logic:
            try:
                # Original: batch[-1]['info']['time'] + 1
                # But we should be careful if 'info' is missing.
                last_item = batch[-1]
                last_ts = last_item.get('timestamp')
                
                if last_ts is None and 'info' in last_item:
                     last_ts = last_item['info'].get('time')
                
                if last_ts is None:
                    print("No timestamp found in last item.")
                    break
                    
                since_api = last_ts + 1
                
                # Check for stalled progress
                if prev_last_ts is not None and last_ts <= prev_last_ts:
                    print(f"Timestamp stalled ({last_ts} <= {prev_last_ts})")
                    break
                    
                prev_last_ts = last_ts
                
            except Exception as e:
                print(f"Error processing batch timestamps: {e}")
                break
            
            if len(batch) < limit_per_page:
                break

        return all_infos
    
    def _normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw Hyperliquid data into a standard DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data)
        if df.empty:
            return df
            
        df.drop(columns=['info'], errors='ignore', inplace=True)
        
        # Rename columns if they exist
        rename_map = {
            'fundingRate': 'FundingRate',
            'datetime': 'Time',
            'symbol': 'Symbol'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Clean symbol logic
        if 'Symbol' in df.columns:
            df['Symbol'] = df['Symbol'].str.split(':').str[0]
            df['Symbol'] = df['Symbol'].str.replace('/', '')
            
        # Ensure types
        if 'Time' in df.columns:
            df['Time'] = pd.to_datetime(df['Time'], errors='coerce', utc=True)
            
        if 'FundingRate' in df.columns:
            df['FundingRate'] = pd.to_numeric(df['FundingRate'], errors='coerce')
            
        df['Exchange'] = self._get_exchange().id
            
        return df


class HyperliquidKlinesFetcher(ExchangeFetcher):
    """
    Fetcher for Hyperliquid Kline (OHLCV) data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = KlinesStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[List[float]]:
        """
        Fetch raw kline data from Hyperliquid.
        """
        all_infos = []
        batch = []
        
        print(f"Fetching {symbol} klines history from {self._get_exchange().id} since {since}")
        
        since_api = int(pd.to_datetime(since).timestamp() * 1000)  # Convert to ms
        limit_per_page = limit if limit and limit < 1000 else 200
        prev_last_ts = 0
        page = 0
        
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
                last_ts = last_kline[0] if len(last_kline) > 0 else None
                
                if last_ts is None:
                    print(f"No timestamp found in last item at page {page}")
                    break

                since_api = last_ts + 1  # Increment for next batch

                if prev_last_ts is not None and last_ts <= prev_last_ts:
                    print(f"Timestamp stalled at page {page} ({last_ts} <= {prev_last_ts})")
                    break
                
                prev_last_ts = last_ts

                # Break if we got less than expected (likely end of data)
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
        
        # Ensure numeric
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        df['Time'] = pd.to_datetime(df['Time'], unit='ms', errors='coerce', utc=True)
        return df
    
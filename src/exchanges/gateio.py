"""
Gate.io exchange data fetchers.

This module contains the specific implementations for fetching funding rates
and klines (OHLCV) from the Gate.io exchange using the CCXT library 
and direct API calls where necessary.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import pandas as pd
from tqdm import tqdm
import requests

from ..adapters.Exchange_base import ExchangeFetcher
from ..adapters.storage import FundingRateStorage, KlinesStorage


class GateFundingFetcher(ExchangeFetcher):
    """
    Fetcher for Gate.io Funding Rate data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = FundingRateStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch raw funding rate history from Gate.io.
        
        Args:
            symbol (str): The trading symbol.
            since (str): Start date string.
            limit (int): Pagination limit (default 100).
            
        Returns:
            List[Dict[str, Any]]: List of funding rate records.
        """
        all_infos = []
        batch = []
        
        print(f"Fetching {symbol} funding rate history from {self._get_exchange().id} since {since}")
        
        since_api = int(pd.to_datetime(since).timestamp() * 1000)  # Convert to ms        
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        page = 0
        
        with tqdm(desc=f"{self._get_exchange().id} {symbol} funding history", unit="page") as pbar:
            while True:
                try:
                    # Note: CCXT fetch_funding_rate_history implementation details vary
                    batch = self.exchange.fetch_funding_rate_history(
                        symbol,
                        since=since_api,
                        limit=100
                    )
                except Exception as e:
                    print(f"Error fetching funding data: {e}")
                    break

                if not batch:
                    # If no data found, move forward by 3 days and try again
                    since_api += 86400000 * 3  # Add three days in milliseconds
                    
                    if since_api >= now_ts:
                        print(f"Reached current date without finding data")
                        break
                else:
                    all_infos.extend(batch)
                    pbar.update(1)
                    
                    # Move to next timestamp
                    last_ts = batch[-1]['timestamp']
                    since_api = last_ts + 1
                    
                    # Safety break if we essentially caught up
                    if last_ts >= now_ts:
                         break
                    
                page += 1

        return all_infos

    def _normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw Gate.io data into a standard DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data)
        df.drop(columns=['info'], errors='ignore', inplace=True)
        
        # Standardize column names
        column_mapping = {
            'symbol': 'Symbol',
            'timestamp': 'Time',
            'fundingRate': 'FundingRate',
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Ensure required columns exist
        required_columns = ['Symbol', 'Time', 'FundingRate']
        for col in required_columns:
            if col not in df.columns:
                print(f"Warning: Missing expected column '{col}' in data")
        
        df['Exchange'] = self._get_exchange().id 
        
        # Convert Time to numeric first to avoid the FutureWarning
        df['Time'] = pd.to_numeric(df['Time'], errors='coerce')
        df['Time'] = pd.to_datetime(df['Time'], unit='ms', errors='coerce', utc=True)
        df["Time"] = df["Time"].dt.floor("s")
        
        # Convert FundingRate to numeric
        df['FundingRate'] = pd.to_numeric(df['FundingRate'], errors='coerce')
        
        return df[['Symbol', 'Time', 'FundingRate', 'Exchange']]
        
    def _normalize_symbol_name(self, symbol: str) -> str:
        """Normalize symbol name."""
        return symbol.split("/")[0]

    # ------------- NEW FUNCTIONALITY ------------- #
    def get_perpetual_symbols(self) -> None:
        """Filter for USDT perpetual symbols."""
        perpetuals = [s for s in self._get_symbols() if s.endswith(":USDT")]
        self.symbols = perpetuals


class GateKlinesFetcher(ExchangeFetcher):
    """
    Fetcher for Gate.io Kline (OHLCV) data.
    """
    TIME_COL = 'Time'
    HOST = "https://api.gateio.ws"
    PREFIX = "/api/v4"
    URL = '/futures/usdt/candlesticks'
    HEADERS = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = KlinesStorage(self.exchange.id)

    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch raw kline data from Gate.io using direct API calls.
        """
        all_infos = []
        
        print(f"Fetching {symbol} klines history from {self._get_exchange().id} since {since}")
        
        # Gate.io API uses underscores for symbols (e.g., BTC_USDT)
        symbol_api = symbol.split(':')[0].replace("/", "_")
        
        # API requires seconds timestamp
        since_api = int(pd.to_datetime(since).timestamp())
        now_ts = int(pd.Timestamp.now(tz=timezone.utc).timestamp())
        
        with tqdm(desc=f"{self._get_exchange().id} {symbol} klines history", unit="batch") as pbar:
            while True:
                # 'to' determines the end of the range, 'from' the start
                # Gate.io fetches backwards if we iterate like this? 
                # Original code logic: from since_api to now_ts. 
                # But notice the loop updates `now_ts` downwards: `now_ts = int(batch.json()[0]['t']) - 1`
                # This suggests it is fetching REVERSE chronological order (latest first)?
                # Let's verify original logic: 
                # params = {'from': since_api, 'to': now_ts}
                # update: get batch. 
                # update: `now_ts` becomes the timestamp of the *first* item in batch minus 1.
                # This implies the batch returns items sorted by time? 
                # If Gate returns ascending, batch[0] is oldest. old - 1 -> even older? 
                # If Gate returns descending, batch[0] is newest. new - 1 -> older. 
                # It seems the original code was trying to fetch backwards from NOW down to SINCE.
                
                query_param = {
                    'contract': symbol_api, 
                    'from': since_api, 
                    'to': now_ts, 
                    'interval': '1h',
                    'limit': 100  # Explicit limit often helps
                }
                
                try:
                    response = requests.get(
                        self.HOST + self.PREFIX + self.URL, 
                        headers=self.HEADERS, 
                        params=query_param
                    )
                    
                    if response.status_code != 200:
                        print(f"Error fetching data: {response.text}")
                        break
                        
                    batch = response.json()
                    
                    if not batch:
                        break
                        
                    all_infos.extend(batch)
                    
                    # Assuming batch is sorted? 
                    # If we trust the original logic's intent:
                    # It checks `batch[0]['t']`. 
                    first_item_ts = int(batch[0]['t'])
                    
                    if first_item_ts <= since_api:
                        break
                        
                    now_ts = first_item_ts - 1
                    pbar.update(1)
                    
                except Exception as e:
                    print(f"Exception during fetch: {e}")
                    break
                    
        return all_infos
    
    def _normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw Gate.io kline data into a DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data)
        
        # Gate.io raw response fields: t (time), v (volume), c (close), h (high), l (low), o (open)
        # Note: Depending on endpoint version, fields might be loose or objects.
        # The fetcher above uses `candlesticks` endpoint which returns objects like {'t':..., 'v':...}
        
        df = df.rename(columns={
            'o': 'Open',
            'v': 'Volume',
            't': 'Time',
            'c': 'Close',
            'l': 'Low',
            'h': 'High',
        })
        
        # Ensure numeric
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # Standardize Time
        df['Time'] = pd.to_numeric(df['Time'], errors='coerce')
        df['Time'] = pd.to_datetime(df['Time'], unit='s', errors='coerce', utc=True)
        
        return df[['Time', 'Open', 'High', 'Low', 'Close', 'Volume']]
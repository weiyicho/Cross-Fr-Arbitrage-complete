"""
Bitget exchange data fetchers.

This module contains the specific implementations for fetching funding rates
and klines (OHLCV) from the Bitget exchange using the CCXT library.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import pandas as pd
from tqdm import tqdm
import requests
import ccxt

from ..adapters.Exchange_base import ExchangeFetcher
from ..adapters.storage import FundingRateStorage, KlinesStorage


class BitgetFundingFetcher(ExchangeFetcher):
    """
    Fetcher for Bitget Funding Rate data.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = FundingRateStorage(self.exchange.id)
        
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch funding rate history from Bitget.
        
        Args:
            symbol (str): Trading pair symbol.
            since (str): Starting date string.
            limit (int): Not strictly used as pagination is handled manually.
            
        Returns:
            List[Dict[str, Any]]: List of funding rate records.
        """
        all_infos = []
        batch = []
        
        print(f"Fetching {symbol} funding rate history from {self._get_exchange().id} since {since}")
        since_api = int(pd.to_datetime(since).timestamp() * 1000)  # Convert to ms        
        
        page_num = 1
        
        with tqdm(desc=f"{self._get_exchange().id} {symbol} funding history", unit="page") as pbar:
            while True:
                try:
                    # Bitget API pagination uses pageNo
                    batch = self.exchange.fetch_funding_rate_history(
                        symbol,
                        since=since_api,
                        params={
                            "pageNo": page_num,
                            "pageSize": 100  # Max page size
                        }
                    )
                except Exception as e:
                    print(f"Error fetching funding data: {e}")
                    break

                if not batch:
                    break
                
                # Extract info dicts if available, or use the record itself
                data = [dat.get('info', dat) for dat in batch]
                all_infos.extend(data)
                
                if len(batch) < 100:
                    # print("No more data available from API.")
                    break
                
                page_num += 1
                pbar.update(1)

        return all_infos

    def _normalize_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalize raw Bitget data into a standard DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data)
        
        # Standardize column names
        column_mapping = {
            'symbol': 'Symbol',
            'fundingTime': 'Time',
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
        
        return df
        
    def _normalize_symbol_name(self, symbol: str) -> str:
        """Normalize symbol name."""
        return symbol.split("/")[0]

    # ------------- NEW FUNCTIONALITY ------------- #
    def get_perpetual_symbols(self) -> None:
        """Filter for USDT perpetual symbols."""
        perpetuals = [s for s in self._get_symbols() if s.endswith(":USDT")]
        self.symbols = perpetuals


class BitgetKlinesFetcher(ExchangeFetcher):
    """
    Fetcher for Bitget Kline (OHLCV) data.
    
    Uses direct API calls for fetching history candles to bypass some CCXT limitations
    or to use specific endpoints optimized for history.
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange, since: Optional[str] = None):
        super().__init__(exchange, since)
        self.storage = KlinesStorage(self.exchange.id)

    def _normalize_symbol_for_validation(self, symbol: str) -> str:
        """Convert raw symbol format to CCXT format for validation."""
        if '/' in symbol:
            return symbol
        return f"{symbol[:-4]}/USDT:USDT"
    
    def _normalize_symbol_for_api(self, symbol: str) -> str:
        """Convert CCXT format to raw format for API calls."""
        if '/' not in symbol:
            return symbol
        return symbol.split(':')[0].replace("/", "")

    def fetch_data(self, symbol: str, since: Optional[str] = None, 
                   limit: Optional[int] = None) -> pd.DataFrame:
        """
        Override to handle both raw and CCXT symbol formats transparently.
        """
        # Normalize for validation
        ccxt_symbol = self._normalize_symbol_for_validation(symbol)
        if ccxt_symbol not in self.symbols:
            raise ValueError(f"{symbol} not supported in {self.exchange.id}")
        
        # Normalize for API call
        api_symbol = self._normalize_symbol_for_api(symbol)
        since = since or self.since
        limit = limit or self.DEFAULT_LIMIT
        
        raw_data = self._fetch_raw_data(api_symbol, since, limit)
        processed_data = self._normalize_data(raw_data)
        processed_data = self._deduplicate(processed_data)
        return processed_data

    def _fetch_raw_data(self, symbol: str, since: str = '2023-01-01', 
                        limit: Optional[int] = None) -> List[List[float]]:
        """
        Fetch all 1H candles for `symbol` from `since` to now using direct API calls.
        
        Returns:
            List[List[float]]: List of [timestamp_ms, open, high, low, close, volume]
        """
        BITGET_URL = "https://api.bitget.com/api/v2/mix/market/history-candles"
        TF_MS = 60 * 60 * 1000              # 1 hour in ms
        MAX_PER_CALL = 200
        MAX_90D_MS = 90 * 24 * 60 * 60 * 1000  # 90 days
        
        # Helper to convert various inputs to milliseconds
        def _to_ms(ts):
            if isinstance(ts, (int, float)):
                return int(ts if ts > 10**12 else ts * 1000)
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return int(ts.timestamp() * 1000)
            return int(pd.to_datetime(ts, utc=True).timestamp() * 1000)
        
        start_ms = _to_ms(since if since else '2023-06-01')
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Round both down to the 1H boundary
        start_ms -= start_ms % TF_MS
        now_ms -= now_ms % TF_MS

        if start_ms >= now_ms:
            print("Warning: Start time is in the future - nothing to fetch.")
            return []

        all_rows = []
        cursor = start_ms
        call_count = 0

        print(f"Fetching {symbol} from {datetime.fromtimestamp(start_ms/1000, timezone.utc)} "
              f"to {datetime.fromtimestamp(now_ms/1000, timezone.utc)}")

        while cursor < now_ms:
            call_count += 1
            # Obey both the 200-candle and 90-day limits of the endpoint
            span_200 = (MAX_PER_CALL - 1) * TF_MS
            end_ms = min(cursor + span_200, cursor + MAX_90D_MS - 1, now_ms - 1)
            end_ms -= end_ms % TF_MS
            
            # Ensure startTime < endTime
            if cursor >= end_ms:
                break

            params = {
                "symbol": symbol,
                "productType": "USDT-FUTURES",
                "granularity": "1H",
                "startTime": str(cursor),
                "endTime": str(end_ms),
            }

            try:
                r = requests.get(BITGET_URL, params=params, timeout=10)
                r.raise_for_status()
            except Exception as e:
                print(f"HTTP error: {e}")
                break

            payload = r.json()
            if payload.get("code") != "00000":
                print(f"Bitget API error: {payload.get('msg')}")
                break

            rows = payload.get("data") or []
            if not rows:
                cursor += 24 * TF_MS * 3  # Advance by 3 days if no data to skip gaps
                continue

            for row in rows:
                ts = int(row[0])
                o, h, l, c, v = map(float, row[1:6])
                all_rows.append([ts, o, h, l, c, v])

            last_ts = int(rows[-1][0])
            cursor = last_ts + TF_MS

        print(f"Finished: {len(all_rows)} candles in {call_count} API calls")
        
        # De-duplicate and sort based on timestamp
        all_rows = [v for _, v in sorted({r[0]: r for r in all_rows}.items())]
        return all_rows

    def _normalize_data(self, raw_data: List[List[float]]) -> pd.DataFrame:
        """
        Normalize raw kline lists into a DataFrame.
        """
        if not raw_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(raw_data, columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Time'] = pd.to_datetime(df['Time'], unit='ms', errors='coerce', utc=True)
        return df
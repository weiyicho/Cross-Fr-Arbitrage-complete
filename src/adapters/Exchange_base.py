"""
Base classes for exchange data fetching.

This module defines the abstract base classes `ExchangeFetcher` and `DexFetcher` which
standardize the interface for fetching, normalizing, and storing funding rate and
OHLCV (Kline) data from various cryptocurrency exchanges.
"""

from abc import abstractmethod, ABC
from datetime import datetime, timedelta
from typing import Union, Optional, List, Dict, Any, Tuple
import time

import ccxt
import pandas as pd
from tqdm import tqdm

from .storage import FundingRateStorage, KlinesStorage


class ExchangeFetcher(ABC):
    """
    Abstract base class for fetching and managing exchange data (CEX).

    This class handles the connection to the exchange (via ccxt), manages
    supported symbols/markets, and orchestrates the fetching, normalization,
    and storage of data.

    Attributes:
        DEFAULT_LIMIT (int): Default number of records to fetch per request.
        TIME_COL (str): The column name used for timestamps in DataFrames.
        exchange (ccxt.Exchange): The ccxt exchange instance.
        since (str): The default start date for fetching data if not specified.
        storage (FundingRateStorage): Storage handler for funding rates.
        markets (Dict): Dictionary of market data loaded from the exchange.
        symbols (List[str]): List of supported symbol names.
        USDT_SYMBOLS (List[str]): Filtered list of symbols ending with ':USDT'.
    """

    DEFAULT_LIMIT: int = 150
    TIME_COL: str = 'Time'

    def __init__(self, exchange: Union[str, ccxt.Exchange], since: Optional[str] = None):
        """
        Initialize the ExchangeFetcher.

        Args:
            exchange (Union[str, ccxt.Exchange]): The exchange ID (str) or a ccxt exchange instance.
            since (Optional[str]): The start date for data fetching (YYYY-MM-DD).
                                   Defaults to 5 years ago if None.
        """
        if isinstance(exchange, str):
            # Dynamically instantiate the exchange from ccxt
            self.exchange: ccxt.Exchange = getattr(ccxt, exchange)()
        else:
            self.exchange: ccxt.Exchange = exchange

        self.since: str = since if since is not None else self._default_since()
        
        # Initialize storage (defaulting to FundingRateStorage, subclasses may override)
        self.storage = FundingRateStorage(self.exchange.id)
        
        self.markets: Dict[str, Any] = self._load_markets()
        self.symbols: List[str] = self._load_supported_symbols(self.markets, self.exchange)
        self.USDT_SYMBOLS: List[str] = [s for s in self._get_symbols() if s.endswith(":USDT")]

        # If data is missing (e.g., due to transient network error), try a one-time refresh
        if not self.markets or not self.symbols:
            try:
                self.exchange.load_markets()
                self.markets = getattr(self.exchange, 'markets', {}) or {}
                self.symbols = self._load_supported_symbols(self.markets, self.exchange)
                self.USDT_SYMBOLS = [s for s in self._get_symbols() if s.endswith(":USDT")]
            except Exception as e:
                print(f"Warning: Retry loading markets for {self.exchange.id} failed: {e}")

    # ------------- UTILS ------------- #

    def _get_symbols(self) -> List[str]:
        """Returns the list of supported symbols."""
        return self.symbols

    def _get_exchange(self) -> ccxt.Exchange:
        """Returns the ccxt exchange instance."""
        return self.exchange

    def _get_exchange_id(self) -> str:
        """Returns the exchange ID."""
        return self.exchange.id

    def _get_usdt_symbols(self) -> List[str]:
        """Returns the list of USDT-margined symbols."""
        return self.USDT_SYMBOLS

    def _default_since(self) -> str:
        """Returns the default start date (2021-01-01)."""
        return '2021-01-01'

    def _load_markets(self) -> Dict[str, Any]:
        """
        Load markets with a retry mechanism to handle transient network issues.

        Returns:
            Dict[str, Any]: A dictionary of market data, or empty dict on failure.
        """
        last_err = None
        for attempt in range(3):
            try:
                return self.exchange.load_markets()
            except Exception as e:
                last_err = e
                # Exponential backoff: 0.5, 1.0, 1.5 seconds
                time.sleep(0.5 * (attempt + 1))
        
        print(f"Error loading markets for {self.exchange.id} after retries: {last_err}")
        return {}

    def _ensure_markets(self) -> None:
        """Unconditionally reload markets if they are currently empty."""
        if not self.markets or not self.symbols:
            try:
                self.exchange.load_markets()
                self.markets = getattr(self.exchange, 'markets', {}) or {}
                self.symbols = self._load_supported_symbols(self.markets, self.exchange)
                self.USDT_SYMBOLS = [s for s in self._get_symbols() if s.endswith(":USDT")]
            except Exception as e:
                print(f"Error ensuring markets for {self.exchange.id}: {e}")

    def _load_supported_symbols(self, markets: Dict[str, Any], exchange: ccxt.Exchange) -> List[str]:
        """
        Filter and return supported symbols (Swap + Contract).

        Args:
            markets (Dict): The markets dictionary from ccxt.
            exchange (ccxt.Exchange): The exchange instance.

        Returns:
            List[str]: A list of symbol names.
        """
        return [
            symbol for symbol, market in markets.items()
            if market.get('swap', False) and market.get('contract', False)
        ]

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate records based on the timestamp column.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: A cleaned DataFrame with duplicates removed and sorted by time.
        """
        if df is None or df.empty:
            return pd.DataFrame()
        
        df[self.TIME_COL] = pd.to_datetime(df[self.TIME_COL], errors='coerce', utc=True)
        df = df.drop_duplicates(subset=[self.TIME_COL])
        df = df.sort_values(by=self.TIME_COL).reset_index(drop=True)
        return df

    # ------------- ABSTRACT METHODS ------------- #

    @abstractmethod
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, limit: Optional[int] = None) -> List[Any]:
        """
        Fetch raw data from the exchange.

        Args:
            symbol (str): The market symbol.
            since (Optional[str]): The start time.
            limit (Optional[int]): The maximum number of records.
        
        Returns:
            List[Any]: A list of raw data records.
        """
        pass

    @abstractmethod
    def _normalize_data(self, raw_data: List[Any]) -> pd.DataFrame:
        """
        Normalize raw data into a standardized pandas DataFrame.

        Args:
            raw_data (List[Any]): The raw data fetched from exchange.

        Returns:
            pd.DataFrame: DataFrame containing standardized columns.
        """
        pass

    # ------------- MAIN METHODS ------------- #

    def fetch_data(self, symbol: str, since: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch and normalize data for a specific symbol.

        Args:
            symbol (str): The trading symbol.
            since (Optional[str]): Start date/time.
            limit (Optional[int]): Limit of records.

        Returns:
            pd.DataFrame: The processed data.
        """
        self._ensure_markets()
        if symbol not in self.symbols:
            raise ValueError(f"{symbol} not supported in {self.exchange.id}")
        
        since = since or self.since
        limit = limit or self.DEFAULT_LIMIT
        
        raw_data = self._fetch_raw_data(symbol, since, limit)
        processed_data = self._normalize_data(raw_data)
        processed_data = self._deduplicate(processed_data)
        return processed_data

    def load_historical_data(self, symbol: str) -> Tuple[pd.DataFrame, Optional[pd.Timestamp]]:
        """
        Load locally stored historical data.

        Returns:
            Tuple[pd.DataFrame, Optional[pd.Timestamp]]: A tuple containing the dataframe
            and the last timestamp found, or None.
        """
        historical_data = pd.DataFrame()
        last_timestamp = None
        
        try:
            data = self.storage.search(symbol)
            if data is not None and not data.empty:
                historical_data = data
                last_timestamp = historical_data[self.TIME_COL].max()
                if last_timestamp is not None:
                    last_timestamp = pd.to_datetime(last_timestamp, utc=True)
        except Exception as e:
            # Optionally log the error; currently supressed as per original logic
            pass
            
        return historical_data, last_timestamp

    def get_data(self, symbol: str) -> pd.DataFrame:
        """
        Get data for a symbol, either by fetching new data or appending to historical data.

        Args:
            symbol (str): The symbol to query.

        Returns:
            pd.DataFrame: The complete dataset (historical + fresh).
        """
        time_col = self.TIME_COL
        historical_data, last_timestamp = self.load_historical_data(symbol)
        now = pd.Timestamp.now(tz="UTC")

        if last_timestamp is not None:
            print(f"Last data time for {symbol}: {last_timestamp}")
        
        # If data is recent (less than 1 hour old), just return it
        if last_timestamp and (now - last_timestamp).total_seconds() < 3600:
            print(f"Data for {symbol} is up-to-date. No update needed.")
            return historical_data
        
        fresh_data = self.fetch_data(symbol, since=last_timestamp)
        
        if fresh_data is None:
            fresh_data = pd.DataFrame()

        if historical_data.empty and fresh_data.empty:
            return pd.DataFrame()
        elif fresh_data.empty and not historical_data.empty:
            return historical_data

        combined_data = pd.concat([historical_data, fresh_data], ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset=[time_col])
        combined_data[time_col] = pd.to_datetime(combined_data[time_col], errors='coerce', utc=True)
        combined_data = combined_data.sort_values(by=time_col)
        combined_data = self._deduplicate(combined_data)
        
        self.storage.write(combined_data, symbol)
        print(f"Updated data for {symbol}. Total records: {len(combined_data)}")
        return combined_data

    def get_all_data(self, usdt_pairs_only: bool = True) -> None:
        """
        Update data for all supported symbols.

        Args:
            usdt_pairs_only (bool): If True, only update USDT pairs.
        """
        self._ensure_markets()
        symbols = self.USDT_SYMBOLS if usdt_pairs_only else self.symbols
        
        count = len(symbols)
        label = "USDT symbols" if usdt_pairs_only else "symbols"
        print(f"Updating funding rates for {count} {label} on {self.exchange.id}...")
        
        for symbol in tqdm(symbols, desc="Updating symbols"):
            # print(f"Updating {symbol}...")  # Reduce noise, tqdm shows progress
            try:
                self.get_data(symbol)
            except Exception as e:
                print(f"Error updating {symbol}: {e}")
        
        print("All symbols updated.")

    def reset_data(self, symbol: str) -> pd.DataFrame:
        """
        Delete existing data for a symbol and re-fetch from scratch.

        Args:
            symbol (str): The symbol to reset.

        Returns:
            pd.DataFrame: The newly fetched data.
        """
        if self.storage.exists(symbol):
            print(f"Resetting data for {symbol}")
            self.storage.delete(symbol)
        
        df = self.fetch_data(symbol, since=self._default_since())
        if df is not None and not df.empty:
            self.storage.write(df, symbol)
            print(f"Data for {symbol} reset. Total records: {len(df)}")
        return df

    def reset_all_data(self) -> None:
        """Reset data for all supported symbols."""
        for symbol in self.symbols:
            self.reset_data(symbol)
        print("All data reset.")


class DexFetcher(ABC):
    """
    Abstract base class for fetching data from Decentralized Exchanges (DEX).
    """
    DEFAULT_LIMIT: int = 150
    TIME_COL: str = 'Time'

    def __init__(self, exchange: Any, since: Optional[str] = None):
        self.exchange = exchange
        self.since = since if since is not None else self._default_since()
        self.storage = FundingRateStorage(self.exchange)
        self.symbols = self._load_supported_symbols()
        self.USDT_SYMBOLS = self._usdt_symbols()
        
    # ------------- UTILS ------------- #
    def _get_exchange(self) -> Any:
        return self.exchange

    def _default_since(self) -> str:
        return '2021-01-01'

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        df[self.TIME_COL] = pd.to_datetime(df[self.TIME_COL], errors='coerce', utc=True)
        df = df.drop_duplicates(subset=[self.TIME_COL])
        df = df.sort_values(by=self.TIME_COL).reset_index(drop=True)
        return df
    
    # ------------- ABSTRACT METHODS ------------- #
    @abstractmethod
    def _fetch_raw_data(self, symbol: str, since: Optional[str] = None, limit: Optional[int] = None) -> Any:
        pass

    @abstractmethod
    def _normalize_data(self, raw_data: Any) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def _load_supported_symbols(self) -> List[str]:
        pass

    @abstractmethod
    def _usdt_symbols(self) -> List[str]:
        pass
    
    # ------------- MAIN METHODS ------------- #
    def fetch_data(self, symbol: str, since: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
        if symbol not in self.symbols:
            raise ValueError(f"{symbol} not supported in {self.exchange.id}")
        
        since = since or self.since
        limit = limit or self.DEFAULT_LIMIT
        
        raw_data = self._fetch_raw_data(symbol, since, limit)
        processed_data = self._normalize_data(raw_data)
        processed_data = self._deduplicate(processed_data)
        return processed_data

    def load_historical_data(self, symbol: str) -> Tuple[pd.DataFrame, Optional[pd.Timestamp]]:
        historical_data = pd.DataFrame()
        last_timestamp = None
        
        data = self.storage.search(symbol)
        if data is not None:
            try:
                historical_data = data
                last_timestamp = historical_data[self.TIME_COL].max() if not historical_data.empty else None
                if last_timestamp is not None:
                    last_timestamp = pd.to_datetime(last_timestamp, utc=True)
            except Exception:
                pass
        return historical_data, last_timestamp

    def get_data(self, symbol: str) -> pd.DataFrame:
        time_col = self.TIME_COL
        historical_data, last_timestamp = self.load_historical_data(symbol)
        now = pd.Timestamp.now(tz="UTC")
        
        if last_timestamp is not None:
            print(f"Last data time for {symbol}: {last_timestamp}")
        
        # DEX might have different update frequency, keep 8 hours logic as per original
        if last_timestamp and (now - last_timestamp).total_seconds() < 3600 * 8:
            print(f"Data for {symbol} is up-to-date. No update needed.")
            return historical_data
        
        fresh_data = self.fetch_data(symbol, since=last_timestamp)
        
        if fresh_data is None:
            fresh_data = pd.DataFrame()

        if historical_data.empty and fresh_data.empty:
            return pd.DataFrame()
        elif fresh_data.empty and not historical_data.empty:
            return historical_data

        combined_data = pd.concat([historical_data, fresh_data], ignore_index=True)
        combined_data = combined_data.drop_duplicates(subset=[time_col])
        combined_data[time_col] = pd.to_datetime(combined_data[time_col], errors='coerce', utc=True)
        combined_data = combined_data.sort_values(by=time_col)
        combined_data = self._deduplicate(combined_data)
        
        self.storage.write(combined_data, symbol)
        print(f"Updated data for {symbol}. Total records: {len(combined_data)}")
        return combined_data

    def get_all_data(self, usdt_pairs_only: bool = True) -> None:
        print(f"Updating funding rates for {len(self.symbols)} symbols on {self.exchange.id}...")
        symbols = self.USDT_SYMBOLS if usdt_pairs_only else self.symbols
        
        for symbol in tqdm(symbols, desc="Updating symbols"):
            # print(f"Updating {symbol}...")
            try:
                self.get_data(symbol)
            except Exception as e:
                print(f"Error updating {symbol}: {e}")
        print("All symbols updated.")

    def reset_data(self, symbol: str) -> pd.DataFrame:
        if self.storage.exists(symbol):
            print(f"Resetting data for {symbol}")
            self.storage.delete(symbol)
            
        df = self.fetch_data(symbol, since=self._default_since())
        if df is not None and not df.empty:
            self.storage.write(df, symbol)
            print(f"Data for {symbol} reset. Total records: {len(df)}")
        return df

    def reset_all_data(self) -> None:
        for symbol in self.symbols:
            self.reset_data(symbol)
        print("All data reset.")


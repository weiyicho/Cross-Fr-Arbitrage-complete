"""
Concrete storage adapter implementations.

This module contains the concrete implementations of `BaseDataStorage` for managing
exchange-specific data types, such as funding rates and Klines (OHLCV).
"""

from pathlib import Path
from typing import List

from ..core.storage import BaseDataStorage, PARQUET_AVAILABLE


class FundingRateStorage(BaseDataStorage):
    """
    Storage handler for Funding Rate data.
    
    Data is stored in `data/raw/<exchange_id>/funding_rates/<symbol>.parquet`.
    """

    TIME_COL = 'Time'

    def __init__(self, exchange_id: str, folder: str = "funding_rates", project_root: str = None):
        """
        Initialize the funding rate storage for a specific exchange.

        Args:
            exchange_id (str): The ID of the exchange (e.g., 'binance').
            folder (str): The folder name for this data type.
            project_root (str): Optional override for project root.
        """
        # Create the data_subdir path
        data_subdir = Path("data") / 'raw' / exchange_id / folder
        # Call parent constructor
        super().__init__(base_dir=project_root, data_subdir=str(data_subdir), time_column=self.TIME_COL)
        self.exchange_id = exchange_id

    def _file_name_save(self, symbol: str) -> str:
        """
        Convert trading symbol to a filename-friendly format.
        
        Handles various formats:
        - 'BTC/USDT' -> 'BTCUSDT'
        - 'BTC/USDT:USDT' -> 'BTCUSDT' (perpetual notation)
        - 'BTCUSDT' -> 'BTCUSDT' (already normalized)
        - 'BTC-USDT' -> 'BTCUSDT'

        Args:
            symbol (str): The trading symbol.

        Returns:
            str: Filename-friendly symbol representation.
        """
        # Handle already normalized symbols (no separators)
        if '/' not in symbol and ':' not in symbol and '-' not in symbol:
            return symbol
            
        # Handle perpetual futures notation (BTC/USDT:USDT)
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
            
        # Fallback
        return symbol.replace('/', '').replace('-', '').replace(':', '')
    
    def list_symbols(self) -> List[str]:
        """
        List all symbols stored for this exchange's funding rates.

        Returns:
            List[str]: List of symbol names (stems of the files).
        """
        if not self.data_dir.exists():
            return []
            
        extension = "parquet" if PARQUET_AVAILABLE else "csv"
        files = self.data_dir.glob(f"*.{extension}")
        symbols = [f.stem for f in files]
        return list(symbols)


class KlinesStorage(BaseDataStorage):
    """
    Storage handler for Klines (OHLCV) data.
    
    Data is stored in `data/raw/<exchange_id>/klines/<symbol>.parquet`.
    """

    TIME_COL = 'Time'

    def __init__(self, exchange_id: str, folder: str = "klines", project_root: str = None):
        """
        Initialize the klines storage for a specific exchange.

        Args:
            exchange_id (str): The ID of the exchange.
            folder (str): The folder name for this data type.
            project_root (str): Optional override for project root.
        """
        # Create the data_subdir path
        data_subdir = Path("data") / 'raw' / exchange_id / folder
        # Call parent constructor
        super().__init__(base_dir=project_root, data_subdir=str(data_subdir), time_column=self.TIME_COL)
        self.exchange_id = exchange_id

    def _file_name_save(self, symbol: str) -> str:
        """
        Convert trading symbol to a filename-friendly format.

        Args:
            symbol (str): The trading symbol.

        Returns:
            str: Filename-friendly symbol representation.
        """
         # Handle already normalized symbols (no separators)
        if '/' not in symbol and ':' not in symbol and '-' not in symbol:
            return symbol
            
        # Handle perpetual futures notation (BTC/USDT:USDT)
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
            
        # Fallback
        return symbol.replace('/', '').replace('-', '').replace(':', '')

    def list_symbols(self) -> List[str]:
        """
        List all symbols stored for this exchange's klines.

        Returns:
            List[str]: List of symbol names.
        """
        if not self.data_dir.exists():
            return []
            
        extension = "parquet" if PARQUET_AVAILABLE else "csv"
        files = self.data_dir.glob(f"*.{extension}")
        symbols = [f.stem for f in files]
        return list(symbols)
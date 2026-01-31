"""
Pipeline Storage Module.

This module provides storage implementations for the data pipeline,
specifically for handling 'clean' and 'merged' data stages.
"""

from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

# Import the BaseDataStorage class
from src.core.storage import BaseDataStorage

try:
    import pyarrow
    PARQUET_AVAILABLE = True
except ImportError:
    print("Warning: pyarrow not installed. Please install with: pip install pyarrow")
    print("Falling back to CSV storage.")
    PARQUET_AVAILABLE = False


class CleanDataStorage(BaseDataStorage):
    """
    Storage handler for cleaned funding rate data.
    
    Stores data in: data/clean/{exchange_id}/{folder}
    """
    TIME_COL = 'Time'

    def __init__(self, exchange_id: str, folder: str = "funding_rates", 
                 project_root: Optional[Union[str, Path]] = None):
        """
        Initialize CleanDataStorage.

        Args:
            exchange_id (str): The exchange identifier (e.g., 'binance').
            folder (str): Subfolder name (default: 'funding_rates').
            project_root (Optional[Union[str, Path]]): Override project root path.
        """
        # Create the data_subdir path
        data_subdir = Path("data") / 'clean' / exchange_id / folder
        
        # Call parent constructor
        super().__init__(project_root, data_subdir, self.TIME_COL)
        self.exchange_id = exchange_id
        
    def _file_name_save(self, symbol: str) -> str:
        """
        Convert a trading symbol to a filename-friendly format.
        
        Args:
            symbol (str): The trading symbol (e.g., 'BTC/USDT').
            
        Returns:
            str: Filename-friendly string (e.g., 'BTCUSDT').
        """
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
            
        # Handle base currency only (e.g., 'ETH' -> 'ETHUSDT')
        if '/' not in symbol and ':' not in symbol and '-' not in symbol:
            if len(symbol) <= 4 and symbol.isupper() and not symbol.endswith('USDT'):
                return f"{symbol}USDT"
            return symbol
            
        # Fallback
        return symbol.replace('/', '').replace('-', '').replace(':', '')

    def list_symbols(self) -> List[str]:
        """
        List all symbols for which data files exist in the storage directory.
        
        Returns:
            List[str]: List of symbols (derived from filenames without extensions).
        """
        extension = "parquet" if PARQUET_AVAILABLE else "csv"
        files = self.data_dir.glob(f"*.{extension}")
        symbols = [f.stem for f in files]
        return list(symbols)


class MergeDataStorage(BaseDataStorage):
    """
    Storage handler for merged data between two exchanges.
    
    Stores data in: data/merge/{exchange1}_{exchange2}/{folder}
    """
    TIME_COL = 'Time'
    
    def __init__(self, exchange1: str, exchange2: str, 
                 folder: str = "funding_rates", 
                 project_root: Optional[Union[str, Path]] = None):
        """
        Initialize MergeDataStorage.

        Args:
            exchange1 (str): First exchange ID.
            exchange2 (str): Second exchange ID.
            folder (str): Subfolder name.
            project_root (Optional[Union[str, Path]]): Override project root.
        """
        exchanges = sorted([exchange1, exchange2])
        self.exchange1_id = exchanges[0]
        self.exchange2_id = exchanges[1]
        
        # Create the data_subdir path
        data_subdir = Path("data") / 'merge' / f"{self.exchange1_id}_{self.exchange2_id}" / folder
        
        # Call parent constructor
        super().__init__(project_root, data_subdir, self.TIME_COL)
        
    def _file_name_save(self, symbol: str) -> str:
        """
        Convert a trading symbol to a filename-friendly format.

        Args:
            symbol (str): The trading symbol.

        Returns:
            str: Filename-friendly string.
        """
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
            
        # Handle base currency only (e.g., 'ETH' -> 'ETHUSDT')
        if '/' not in symbol and ':' not in symbol and '-' not in symbol:
            if len(symbol) <= 4 and symbol.isupper() and not symbol.endswith('USDT'):
                return f"{symbol}USDT"
            return symbol
            
        # Fallback
        return symbol.replace('/', '').replace('-', '').replace(':', '')
    
    def list_symbols(self) -> List[str]:
        """
        List all symbols for which data files exist in the storage directory.
        
        Returns:
            List[str]: List of symbols (derived from filenames without extensions).
        """
        extension = "parquet" if PARQUET_AVAILABLE else "csv"
        files = self.data_dir.glob(f"*.{extension}")
        symbols = [f.stem for f in files]
        return list(symbols)


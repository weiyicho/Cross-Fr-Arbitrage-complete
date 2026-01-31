"""
Core storage module for data persistence.

This module provides the `BaseDataStorage` abstract base class, which defines the
standard interface for reading and writing data (CSV or Parquet) within the application.
It handles directory resolution, file path management, and basic I/O operations.
"""

from abc import abstractmethod, ABC
from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Union, Optional, List
import time

import pandas as pd
from tqdm import tqdm

# Import pyarrow for Parquet support
try:
    import pyarrow
    PARQUET_AVAILABLE = True
except ImportError:
    print("Warning: pyarrow not installed. Please install with: pip install pyarrow")
    print("Falling back to CSV storage.")
    PARQUET_AVAILABLE = False


class BaseDataStorage(ABC):
    """
    Abstract base storage class for all data storage implementations.

    This class handles common storage operations like reading, writing,
    and managing data files in a consistent way across the application.
    It supports both CSV and Parquet formats (preferring Parquet if available).

    Attributes:
        dataset_name (str): The name involved in the path (e.g., 'funding_rate').
        base_dir (Path): The root directory for data storage.
        data_dir (Path): The specific subdirectory for this dataset.
        time_col (str): The name of the timestamp column in the data.
    """
    
    def __init__(self, base_dir: Optional[Union[str, Path]] = None, 
                 data_subdir: Optional[str] = None, 
                 time_column: str = 'Time'):
        """
        Initialize the base storage.
        
        Args:
            base_dir (Optional[Union[str, Path]]): Explicit base directory path.
            data_subdir (Optional[str]): Subdirectory path relative to base_dir
                                         where data is stored.
            time_column (str): Name of the timestamp column in data.
        """
        self.base_dir = self._resolve_base_dir(base_dir)
        if data_subdir:
            self.data_dir = self.base_dir / data_subdir
            self.data_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.data_dir = self.base_dir
        self.time_col = time_column
    
    def _resolve_base_dir(self, provided_dir: Optional[Union[str, Path]] = None) -> Path:
        """
        Resolve the base directory from provided dir, env var, or auto-detect.
        
        Args:
            provided_dir (Optional[Union[str, Path]]): Explicitly provided directory path.
            
        Returns:
            Path: The resolved base directory path.
        """
        if provided_dir:
            return Path(provided_dir)
        
        # Try environment variable
        elif os.environ.get('PROJECT_ROOT'):
            return Path(os.environ.get('PROJECT_ROOT'))
        
        else:
            # Auto-detect project root (main repository directory)
            current_dir = Path(__file__).resolve().parent

            # Prefer any directory that contains a "data" folder while walking up.
            while current_dir != current_dir.parent:  # Stop at filesystem root
                # If this directory contains a top-level "data" folder, use it as base.
                if (current_dir / 'data').exists():
                    return current_dir

                # Preserve previous behavior: detect .git or src as project root
                if (current_dir / '.git').exists() or (current_dir / 'src').exists():
                    return current_dir

                current_dir = current_dir.parent
            
            # If no markers found, use default 3 levels up (src/core -> src -> root)
            return Path(__file__).resolve().parent.parent.parent
        
    @abstractmethod
    def _file_name_save(self, symbol: str) -> str:
        """
        Convert symbol to filename format.
        
        Args:
            symbol (str): Symbol to convert.
            
        Returns:
            str: The filename-friendly symbol representation.
        """
        pass
        
    def path_for(self, symbol: str) -> Path:
        """
        Get full path for a symbol.
        
        Args:
            symbol (str): Symbol to get path for.
            
        Returns:
            Path: Full path to the data file for the symbol.
        """
        extension = "parquet" if PARQUET_AVAILABLE else "csv"
        file_name = f"{self._file_name_save(symbol)}.{extension}"
        return self.data_dir / file_name
        
    def read(self, symbol: str) -> pd.DataFrame:
        """
        Read data for symbol.
        
        Args:
            symbol (str): Symbol to read data for.
            
        Returns:
            pd.DataFrame: The data for the symbol, or empty DataFrame if not found or error.
        """
        p = self.path_for(symbol)
        if not p.exists():
            print(f"File not found: {p}")
            return pd.DataFrame()
        try:
            if PARQUET_AVAILABLE and p.suffix == '.parquet':
                return pd.read_parquet(p)
            else:
                return pd.read_csv(p, parse_dates=[self.time_col])
        except Exception as e:
            print(f"Error reading {p}: {e}")
            return pd.DataFrame()
        
    def write(self, df: pd.DataFrame, symbol: str) -> Optional[Path]:
        """
        Write data for symbol.
        
        Args:
            df (pd.DataFrame): DataFrame to write.
            symbol (str): Symbol to write data for.
            
        Returns:
            Optional[Path]: The path where data was written, or None on failure.
        """
        if df is None or df.empty:
            print(f"No data to write for {symbol}")
            return None
            
        p = self.path_for(symbol)
        p.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Convert date columns to datetime if they aren't already
            if self.time_col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[self.time_col]):
                df[self.time_col] = pd.to_datetime(df[self.time_col])
            
            if PARQUET_AVAILABLE and p.suffix == '.parquet':
                df.to_parquet(p, engine='pyarrow', index=False)
            else:
                df.to_csv(p, index=False)
                
            print(f"Successfully wrote {len(df)} rows to {p}")
            return p
        except Exception as e:
            print(f"Error writing to {p}: {e}")
            return None
        
    def exists(self, symbol: str) -> bool:
        """
        Check if data exists for symbol.
        
        Args:
            symbol (str): Symbol to check.
            
        Returns:
            bool: True if data exists for the symbol, False otherwise.
        """
        return self.path_for(symbol).exists()
        
    def search(self, symbol: str) -> pd.DataFrame:
        """
        Search for data by symbol.
        
        Args:
            symbol (str): Symbol to search for.
            
        Returns:
            pd.DataFrame: The data for the symbol if found, empty DataFrame otherwise.
        """
        if self.exists(symbol):
            return self.read(symbol)
        return pd.DataFrame()
        
    def delete(self, symbol: str) -> bool:
        """
        Delete data for symbol.
        
        Args:
            symbol (str): Symbol to delete data for.
            
        Returns:
            bool: True if data was deleted, False otherwise.
        """
        p = self.path_for(symbol)
        if p.exists():
            try:
                p.unlink()
                print(f"Deleted file: {p}")
                return True
            except Exception as e:
                print(f"Error deleting {p}: {e}")
                return False
        else:
            print(f"File not found for deletion: {p}")
            return False

    @abstractmethod
    def list_symbols(self) -> List[str]:
        """
        List all symbols for which data files exist in the storage directory.
        
        Returns:
            List[str]: List of symbols (derived from filenames without extensions).
        """
        pass

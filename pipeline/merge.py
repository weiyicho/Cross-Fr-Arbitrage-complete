"""
Data Merging Module.

This module combines data from two different exchanges, calculating spreads
and cumulative differences in funding rates.
"""

from abc import ABC
from typing import Optional, Tuple, Set

import pandas as pd
from tqdm import tqdm

from pipeline.storage import CleanDataStorage, MergeDataStorage


class DataMerge(ABC):
    """
    Data Merging Pipeline.
    
    Combines funding rate and volume data from two exchanges into a single
    normalized dataset with spread calculation.
    """
    
    def __init__(self, exchange1: str, exchange2: str):
        """
        Initialize the DataMerge pipeline.

        Args:
            exchange1 (str): First exchange ID.
            exchange2 (str): Second exchange ID.
        """
        # Ensure consistent order for storage paths (e.g., binance_bybit vs bybit_binance)
        exchanges = sorted([exchange1, exchange2])
        self.exchange1_id = exchanges[0]
        self.exchange2_id = exchanges[1]
        
        self.TIME_COL = 'Time'
        
        self.storage1 = CleanDataStorage(self.exchange1_id)
        self.storage2 = CleanDataStorage(self.exchange2_id)
        self.storage3 = MergeDataStorage(self.exchange1_id, self.exchange2_id)
        
    def load_clean_data(self, symbol: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load clean data for a symbol from both exchanges.
        
        Args:
            symbol (str): The trading symbol.
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (df1, df2)
        """
        df1 = self.storage1.read(symbol)
        df2 = self.storage2.read(symbol)
        return df1, df2
    
    def combined_data(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """
        Combine two dataframes and calculate funding rate spreads.
        
        Args:
            df1 (pd.DataFrame): Data from exchange 1.
            df2 (pd.DataFrame): Data from exchange 2.
            
        Returns:
            pd.DataFrame: Combined dataframe with diffs and cumulative sums.
        """
        if df1.empty and df2.empty:
            return pd.DataFrame()
        if df1.empty:
            return df2
        if df2.empty:
            return df1
            
        # Align time ranges
        if self.TIME_COL in df1.columns and self.TIME_COL in df2.columns:
            start = max(df1[self.TIME_COL].min(), df2[self.TIME_COL].min())
            end = min(df1[self.TIME_COL].max(), df2[self.TIME_COL].max())
            
            df1_filtered = df1[(df1[self.TIME_COL] >= start) & (df1[self.TIME_COL] <= end)]
            df2_filtered = df2[(df2[self.TIME_COL] >= start) & (df2[self.TIME_COL] <= end)]
        else:
            return pd.DataFrame() # Cannot align without time

        # Set index for alignment
        df1_indexed = df1_filtered.set_index(self.TIME_COL)
        df2_indexed = df2_filtered.set_index(self.TIME_COL)
        
        # Inner join to ensure we only have overlapping timestamps
        df1_aligned, df2_aligned = df1_indexed.align(df2_indexed, join='inner')
        
        # Calculate Spread
        diff = df1_aligned['FundingRate_hourly'] - df2_aligned['FundingRate_hourly']
        
        # Build Result DataFrame
        df = pd.DataFrame()
        df['Time'] = df1_aligned.index
        df['Diff'] = diff.values
        df['Diff_cumsum'] = diff.values.cumsum()

        # Store individual rates
        ex1_cap = self.exchange1_id.capitalize()
        ex2_cap = self.exchange2_id.capitalize()
        
        df[f'{ex1_cap}_FR_1H'] = df1_aligned['FundingRate_hourly'].values
        df[f'{ex2_cap}_FR_1H'] = df2_aligned['FundingRate_hourly'].values
        
        # --- Handle 'Value' (Volume*Close) Logic ---
        # Fallback Strategy: If one exchange has volume data and other doesn't,
        # fill gaps with the available one.
        
        val_col_1 = next((c for c in df1_aligned.columns if 'Value' in c), 'Value')
        val_col_2 = next((c for c in df2_aligned.columns if 'Value' in c), 'Value')

        has_val1 = val_col_1 in df1_aligned.columns
        has_val2 = val_col_2 in df2_aligned.columns

        if has_val1 and has_val2:
            val1 = df1_aligned[val_col_1].fillna(df2_aligned[val_col_2])
            val2 = df2_aligned[val_col_2].fillna(df1_aligned[val_col_1])
            df[f'{ex1_cap}_Value'] = val1.values
            df[f'{ex2_cap}_Value'] = val2.values
        elif has_val1:
            df[f'{ex1_cap}_Value'] = df1_aligned[val_col_1].values
            df[f'{ex2_cap}_Value'] = df1_aligned[val_col_1].values # Proxy
        elif has_val2:
            df[f'{ex1_cap}_Value'] = df2_aligned[val_col_2].values # Proxy
            df[f'{ex2_cap}_Value'] = df2_aligned[val_col_2].values

        # --- Handle 'Open' Price Logic ---
        open_col_1 = next((c for c in df1_aligned.columns if 'Open' in c), 'Open')
        open_col_2 = next((c for c in df2_aligned.columns if 'Open' in c), 'Open')
        
        has_open1 = open_col_1 in df1_aligned.columns
        has_open2 = open_col_2 in df2_aligned.columns

        if has_open1 and has_open2:
            op1 = df1_aligned[open_col_1].fillna(df2_aligned[open_col_2])
            op2 = df2_aligned[open_col_2].fillna(df1_aligned[open_col_1])
            df[f'{ex1_cap}_Open'] = op1.values
            df[f'{ex2_cap}_Open'] = op2.values
        elif has_open1:
            df[f'{ex1_cap}_Open'] = df1_aligned[open_col_1].values
            df[f'{ex2_cap}_Open'] = df1_aligned[open_col_1].values
        elif has_open2:
            df[f'{ex1_cap}_Open'] = df2_aligned[open_col_2].values
            df[f'{ex2_cap}_Open'] = df2_aligned[open_col_2].values

        return df

    def load_merged_data(self, symbol: str) -> pd.DataFrame:
        """Load previously merged data."""
        return self.storage3.read(symbol)

    def update_merged_data(self, symbol: str) -> pd.DataFrame:
        """
        Update merged data for a specific symbol incrementally.
        
        Args:
            symbol (str): Trading symbol.
            
        Returns:
            pd.DataFrame: Updated merged data.
        """
        df = self.storage3.read(symbol)
        
        # Case 1: No existing merged data
        if df.empty or self.TIME_COL not in df.columns:
            # print(f"Merge data for {symbol} does not exist. Merging full history...")
            df1, df2 = self.load_clean_data(symbol)
            combined_df = self.combined_data(df1, df2)
            self.storage3.write(combined_df, symbol)
            return combined_df        

        # Case 2: Existing data, check for updates
        clean_df = df
        funding_df1, funding_df2 = self.load_clean_data(symbol)
        
        if funding_df1.empty or funding_df2.empty or self.TIME_COL not in funding_df1.columns:
            return clean_df

        last_merged_time = clean_df[self.TIME_COL].max()
        last_df1_time = funding_df1[self.TIME_COL].max()
        last_df2_time = funding_df2[self.TIME_COL].max()

        if last_df1_time > last_merged_time and last_df2_time > last_merged_time:
            print(f"New data available for {symbol}. Merging updates...")
            
            funding_df1_new = funding_df1[funding_df1[self.TIME_COL] > last_merged_time]
            funding_df2_new = funding_df2[funding_df2[self.TIME_COL] > last_merged_time]
            
            new_df = self.combined_data(funding_df1_new, funding_df2_new)
            
            if not new_df.empty:
                clean_df = pd.concat([clean_df, new_df], ignore_index=True)
                # Recalculate cumsum on the full dataset to ensure accuracy
                clean_df["Diff_cumsum"] = clean_df["Diff"].cumsum()
                self.storage3.write(clean_df, symbol)
                
        return clean_df
    
    def reset_merged_data(self, symbol: str) -> pd.DataFrame:
        """Force full re-merge of data for a symbol."""
        if self.storage3.exists(symbol):
            print(f"Resetting merged data for {symbol}")
            self.storage3.delete(symbol)
            
        df1, df2 = self.load_clean_data(symbol)
        combined_df = self.combined_data(df1, df2)
        self.storage3.write(combined_df, symbol)
        return combined_df
    
    def merge_all_symbols(self) -> None:
        """Merge all symbols common to both exchanges."""
        symbols1 = set(self.storage1.list_symbols())
        symbols2 = set(self.storage2.list_symbols())
        shared_symbols = symbols1.intersection(symbols2)
        
        print(f"Merging data for {len(shared_symbols)} shared symbols between {self.exchange1_id} and {self.exchange2_id}...")
        
        for symbol in tqdm(shared_symbols, desc="Merging symbols"):
            try:
                self.update_merged_data(symbol)
            except Exception as e:
                print(f"Error merging {symbol}: {e}")
                
        print("Merging completed for all shared symbols.")
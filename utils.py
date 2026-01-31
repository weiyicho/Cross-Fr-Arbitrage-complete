def find_symbols_missing_klines(exchange_id):
    """
    Find symbols that have funding rate data but no klines data for a specific exchange.
    
    Parameters:
    -----------
    exchange_id : str
        The exchange identifier (e.g., 'bitget', 'okx', 'gateio', 'binance', 'bybit')
    
    Returns:
    --------
    dict : A dictionary containing:
        - 'missing_klines': list of symbols with funding rates but no klines
        - 'has_both': list of symbols with both funding rates and klines
        - 'total_funding': total number of symbols with funding rate data
        - 'total_klines': total number of symbols with klines data
    """
    import os
    from pathlib import Path
    
    # Define paths
    funding_path = Path(f'data/raw/{exchange_id}/funding_rates')
    klines_path = Path(f'data/raw/{exchange_id}/klines')
    
    # Check if directories exist
    if not funding_path.exists():
        print(f"Warning: Funding rates directory not found: {funding_path}")
        return None
    
    if not klines_path.exists():
        print(f"Warning: Klines directory not found: {klines_path}")
        return None
    
    # Get all parquet files (symbols) from each directory
    funding_files = set([f.stem for f in funding_path.glob('*.parquet')])
    klines_files = set([f.stem for f in klines_path.glob('*.parquet')])
    
    # Find symbols with funding rates but no klines
    missing_klines = sorted(list(funding_files - klines_files))
    has_both = sorted(list(funding_files & klines_files))
    
    # Create result dictionary
    result = {
        'missing_klines': missing_klines,
        'has_both': has_both,
        'total_funding': len(funding_files),
        'total_klines': len(klines_files),
        'missing_count': len(missing_klines)
    }
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Exchange: {exchange_id.upper()}")
    print(f"{'='*60}")
    print(f"Total symbols with funding rates: {result['total_funding']}")
    print(f"Total symbols with klines data: {result['total_klines']}")
    print(f"Symbols with BOTH data types: {len(has_both)}")
    print(f"Symbols MISSING klines data: {result['missing_count']}")
    print(f"{'='*60}\n")
    
    if missing_klines:
        print(f"Symbols with funding rates but NO klines data ({len(missing_klines)}):")
        print("-" * 60)
        # Print in columns for better readability
        for i in range(0, len(missing_klines), 4):
            row = missing_klines[i:i+4]
            print("  ".join(f"{s:<15}" for s in row))
        print()
    
    return result
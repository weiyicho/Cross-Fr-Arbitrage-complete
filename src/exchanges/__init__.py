"""
交易所模組 - 提供特定交易所的實作

這個模組包含不同交易所的資料獲取實作，如:
- Binance
- Bybit
- 其他交易所...
"""

# 從各個交易所的檔案中導出類別
from .binance import BinanceFundingFetcher, BinanceKlinesFetcher
from .bybit import BybitFundingFetcher, BybitKlinesFetcher
from .bitget import BitgetFundingFetcher, BitgetKlinesFetcher
from .okx import OkxFundingFetcher, OkxKlinesFetcher
from .gateio import GateFundingFetcher, GateKlinesFetcher
from .dydx import DydxFundingFetcher, DydxKlinesFetcher
from .hyperliquid import HyperliquidFundingFetcher, HyperliquidKlinesFetcher

# 註冊支援的交易所
__all__ = [
    'BinanceFundingFetcher', 'BinanceKlinesFetcher',
    'BybitFundingFetcher', 'BybitKlinesFetcher', 
    'BitgetFundingFetcher', 'BitgetKlinesFetcher', 
    'OkxFundingFetcher', 'OkxKlinesFetcher',
    'GateFundingFetcher', 'GateKlinesFetcher',
    'DydxFundingFetcher', 'DydxKlinesFetcher',
    'HyperliquidFundingFetcher', 'HyperliquidKlinesFetcher'
]
"""
Cross Exchange Funding Rate Arbitrage - Core Package

這個套件包含所有關於加密貨幣交易所資料處理的核心功能。
"""

from pathlib import Path

# 定義常用的路徑
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'

# 從各模組導出常用的類別，讓使用者可以直接從 src 套件引入
from .adapters.storage import FundingRateStorage, KlinesStorage
from .adapters.Exchange_base import ExchangeFetcher
from .exchanges import (
    BinanceFundingFetcher, BybitFundingFetcher, 
    BinanceKlinesFetcher, BybitKlinesFetcher,
    BitgetFundingFetcher, BitgetKlinesFetcher, 
    OkxFundingFetcher, OkxKlinesFetcher,
    GateFundingFetcher, GateKlinesFetcher,
    DydxFundingFetcher, DydxKlinesFetcher,
    HyperliquidFundingFetcher, HyperliquidKlinesFetcher
)

__all__ = [
    'ExchangeFetcher',
    'FundingRateStorage', 
    'KlinesStorage', 
    'BinanceFundingFetcher',
    'BybitFundingFetcher',
    'BinanceKlinesFetcher',
    'BybitKlinesFetcher',
    'Create_funding_fetcher',
    'Create_klines_fetcher',
    'BitgetKlinesFetcher',
    'BitgetFundingFetcher',
    'FundingFetcher',
    'KlinesFetcher',
    'OkxFundingFetcher',
    'OkxKlinesFetcher',
    'GateFundingFetcher',
    'GateKlinesFetcher',
    'DydxFundingFetcher',
    'DydxKlinesFetcher',
    'HyperliquidFundingFetcher',
    'HyperliquidKlinesFetcher'
]

exchange_registry = {
    'binance': BinanceFundingFetcher,
    'bybit': BybitFundingFetcher,
    'bitget': BitgetFundingFetcher,
    'okx': OkxFundingFetcher,
    'gateio': GateFundingFetcher,
    'gate': GateFundingFetcher,
    'dydx': DydxFundingFetcher,
    'hyperliquid': HyperliquidFundingFetcher,
}

klines_supported_exchanges = {
    'binance': BinanceKlinesFetcher,
    'bybit': BybitKlinesFetcher,
    'bitget': BitgetKlinesFetcher,
    'okx': OkxKlinesFetcher,
    'gateio': GateKlinesFetcher,
    'gate': GateKlinesFetcher,
    'dydx': DydxKlinesFetcher,
    'hyperliquid': HyperliquidKlinesFetcher,
}

def Create_funding_fetcher(exchange_id: str, since=None):
    """Factory function to create funding rate fetcher for an exchange"""
    if exchange_id not in exchange_registry:
        raise ValueError(f"Exchange {exchange_id} not supported for funding rates")
    return exchange_registry[exchange_id](exchange_id, since=since)

def Create_klines_fetcher(exchange_id: str, since=None):
    """Factory function to create klines fetcher for an exchange"""
    if exchange_id not in klines_supported_exchanges:
        raise ValueError(f"Exchange {exchange_id} not supported for klines")
    return klines_supported_exchanges[exchange_id](exchange_id, since=since)

# Lazy proxy to avoid heavy initialization at import time
class _LazyFetcher:
    def __init__(self, cls, exchange_id):
        self._cls = cls
        self._exchange_id = exchange_id
        self._instance = None

    def _get(self):
        if self._instance is None:
            self._instance = self._cls(self._exchange_id)
        return self._instance

    def __getattr__(self, name):
        return getattr(self._get(), name)

class FundingFetcher:
    # Usage: FundingFetcher.binance.get_data(symbol)
    binance = _LazyFetcher(BinanceFundingFetcher, 'binance')
    bybit = _LazyFetcher(BybitFundingFetcher, 'bybit')
    bitget = _LazyFetcher(BitgetFundingFetcher, 'bitget')
    okx = _LazyFetcher(OkxFundingFetcher, 'okx')
    gateio = _LazyFetcher(GateFundingFetcher, 'gateio')
    gate = _LazyFetcher(GateFundingFetcher, 'gate')
    dydx = _LazyFetcher(DydxFundingFetcher, 'dydx')
    hyperliquid = _LazyFetcher(HyperliquidFundingFetcher, 'hyperliquid')

    
class KlinesFetcher:
    # Usage: KlinesFetcher.binance.get_data(symbol)
    binance = _LazyFetcher(BinanceKlinesFetcher, 'binance')
    bybit = _LazyFetcher(BybitKlinesFetcher, 'bybit')
    bitget = _LazyFetcher(BitgetKlinesFetcher, 'bitget')
    okx = _LazyFetcher(OkxKlinesFetcher, 'okx')
    gateio = _LazyFetcher(GateKlinesFetcher, 'gateio')
    gate = _LazyFetcher(GateKlinesFetcher, 'gate')
    dydx = _LazyFetcher(DydxKlinesFetcher, 'dydx')
    hyperliquid = _LazyFetcher(HyperliquidKlinesFetcher, 'hyperliquid')

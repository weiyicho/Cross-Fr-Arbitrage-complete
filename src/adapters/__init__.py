"""
適配器模組 - 提供與外部系統和資料存取的介面

這個模組包含:
1. 資料存儲類別 (FundingRateStorage, KlinesStorage)
2. 交易所資料獲取基類 (ExchangeFetcher)
"""

# 從 Exchange_base.py 導出主要的類別
from .Exchange_base import ExchangeFetcher
from .storage import FundingRateStorage, KlinesStorage

# 定義這個套件公開的類別列表
__all__ = ['FundingRateStorage', 'KlinesStorage', 'ExchangeFetcher']
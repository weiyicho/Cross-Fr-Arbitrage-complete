# Cross-Exchange Funding Rate Arbitrage Backtester
This project is aim to build a backtesting framework designed to evaluate arbitrage strategies across multiple cryptocurrency exchanges including CEX, and DEX. 

This project allows researchers and traders to simulate, test, and validate strategies before moving into production. Moreover, it remains space for all contributor to download it and includes different exchanges when you need. 

---

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Structures](#Structures)
- [Install](#Install)
- [Usage](#usage)
- [Roadmap](#roadmap)
- [License](#license)
- [Contact](#contact)

---

## Overview

- Funding rates differ between exchanges due to liquidity, demand, and market inefficiencies.  
- By systematically collecting and backtesting historical funding data, this project helps to identify opportunities 

---

## Features

- Fetch and normalize funding rate data from multiple exchanges    
- Backtesting engine for cross-exchange arbitrage strategies  
- Configurable strategy parameters (time window, thresholds, fees, slippage)  
- Extensible design for adding new exchanges or strategies  
---

## Structures

### Project Architecture Overview

```
┌───────────────────────────────────────────────────────────────┐
│                     External Exchanges from CCXT              │
│       (Binance, Bybit, Bitget, Hyperliquid, etc.)             │
└───────────────────┬───────────────────────────────────────────┘
                    │ API Calls
                    ▼
┌───────────────────────────────────────────────────────────────┐
│                        src/                                   │
├───────────────┬─────────────────────┬────────────────────--───┤
│  exchanges/   │     adapters/       │      core/              │
│ BinanceFundingFetcher│  ExchangeDataFetcher│   BaseDataStorage       │
│ BybitFundingFetcher│  FundingrateStorage │                         │
└───────┬───────┴──────────┬──────────┴────────────┬──────--────┘
        │                  │                       │
        └──────────────────┼───────────────────────┘
                           │ Data Flow
                           ▼
┌───────────────────────────────────────────────────────────────┐
│                      pipeline/                                │
├─────────────────┬───────────────────┬─────────────────────────┤
│    Loader.py    │     merge.py      │      storage.py         │
│  DataTransform  │    DataMerge      │   CleanDataStorage      │
│                 │                   │   MergeDataStorage      │
└─────────────────┴─────────┬─────────┴─────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                           data/                               │
├─────────────────────┬─────────────────────┬───────────────────┤
│        raw/         │       clean/        │       merge/      │
│     Raw Data        │       Data          │   Exchange Data   │
└─────────────────────┴─────────────────────┴──────────┬────────┘
                                                       │
                                                       ▼
┌───────────────────────────────────────────────────────────────┐
│                         backtest/                             │
├─────────────────────┬─────────────────────────────────────────┤
│                     │             backtest.py                 │
│     Backtester      │  Analysis & Performance Visualization   │
└─────────────────────┴──────────────────────────────┬──────────┘
                                                     │
                                                     ▼
┌───────────────────────────────────────────────────────────────┐
│                         result/                               │
├─────────────────────┬─────────────────────┬───────────────────┤
│  Equity Curve with  │  Top Earning &      │  Top Symbols in   │
│  MDD & Active       │  Losing Symbols     │  Market           │
│  Position           │                     │                   │
├─────────────────────┼─────────────────────┼───────────────────┤
│  Top Symbols Enter  │  Heat Map for       │  Portfolio by     │
│  & Exit             │  Active Positions   │  Month            │
│                     │  Each Day           │                   │
└─────────────────────┴─────────────────────┴──────────┬────────┘
                                                       │
                                                       ▼
┌───────────────────────────────────────────────────────────────┐
│                         monitor/                              │
├─────────────────┬──────────────────┬──────────────────────────┤
│    main.py      │    B2B.py        │     fr_monitor.py        │
│ Exchange & B2B  │ Exchange-to-     │    Discord               │
│ Opportunities   │ Exchange Data    │    Notifications         │
└─────────────────┴──────────────────┴──────────────────────────┘
```

## Install

Clone this repository and install dependencies:

```bash
git clone https://github.com/weiyicho/Cross_fr_arbitrage.git
pip install -r requirements.txt or 
pip3 install  -r requirements.txt
```

## Usaged

## License

## Contact
- Email: james93612421@gmail.com
- Linkedin: https://www.linkedin.com/in/weiyi-cho/  

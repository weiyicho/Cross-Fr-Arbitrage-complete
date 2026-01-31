### Exchange Fundingrate data 統一命名column 格式

#### 單一幣種
- Symbol,FundingTime,FundingRate, Exchange
    - FundingTime 一律是UTC 且 Y-M-M H-M-S
    - Symbol 一律變成 symbol + USDT/USDC or bla 
    - FundinRate float
    - Exchange capital Ex. Binance

#### two exchanges combine 之後的格式
- FundingTime
- Symbol
- Exchange1 FR
- Exchange2 FR
- DIFF
- DIFF_CUMSUM



data storage method: parquet, or feathe


-raw data
    - binance
        - price
            - 4h
            - 15min 
                - BTC
                    
                    - 2020-1
                    - 2020-2
                - ETH
                    - 2020-1
                    - 2020-2

        -funding
            - BTC
            - ETH
            
    - bitget
        - price
        - DOGE
        - ETH




-raw data
    - biance
        - funding
            - 2020-01
        -klines
            -BTC 

        -funding
            - BTC
            - ETH
            
    - bitget
        - price
        - DOGE
        - ETH



### research
- parallel processing
- async
- 程式規範

### 可能會遇到的問題
- data missing
- coin already 下市
- 新標地


-data
    -exchange
        - type
            - frequency 
                - symbol
                    - BTC
                    - ETH
                    - DOGE



- merge can be more flexible n exchange 
- also include one exchange 


需要改:
important and urgent
- backtest column name (profolio part)  check
- 參數增加註解 check
- 新增直接測全部幣種 check
- backtest 邏輯檢查 check  


important not urgent
- 集合成一個大class like ccxt check
- update all_symbol_df so ugly check 
- backtest 主要功能都寫出來：單幣、雙幣 check
- monitor 用json 去設定 check
- 變成矩陣套利形式
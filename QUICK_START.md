# 快速開始指南 - B2B Monitor 單/跨交易所模式

## 🚀 5 分鐘快速上手

### 📋 前置需求
- Python 3.8+
- 已收集的交易所數據（在 `data/clean/` 或 `data/merge/` 中）
- Discord webhook URL（用於通知）

---

## 單交易所模式（監控單一交易所）

### 1️⃣ 設置配置
```bash
cd monitor
cp config.example.single.json config.json
```

編輯 `config.json`：
```json
{
  "notifier": {
    "webhook_url": "YOUR_WEBHOOK_URL"  // 填入你的 Discord webhook
  },
  "monitor": {
    "exchange1_id": "binance",  // 選擇交易所
    "exchange2_id": null,       // 保持為 null
    "threshold": 0.15           // 15% 年化資金費率閾值
  }
}
```

### 2️⃣ 運行
```bash
python fr_monitor.py
```

### 3️⃣ 預期輸出
```
Initialized B2B_monitor (Single-exchange): binance | Threshold: 0.15
Starting funding rate check for binance (single-exchange mode)...
Single-exchange (binance) - Symbols meeting conditions: ['BTCUSDT', 'ETHUSDT']
```

---

## 跨交易所模式（套利監控）

### 1️⃣ 設置配置
```bash
cd monitor
cp config.example.cross.json config.json
```

編輯 `config.json`：
```json
{
  "notifier": {
    "webhook_url": "YOUR_WEBHOOK_URL"
  },
  "monitor": {
    "exchange1_id": "binance",
    "exchange2_id": "bybit",    // 指定第二個交易所
    "threshold": 0.10           // 10% 資金費率差異閾值
  }
}
```

### 2️⃣ 運行
```bash
python fr_monitor.py
```

### 3️⃣ 預期輸出
```
Initialized B2B_monitor (Cross-exchange): binance vs bybit | Threshold: 0.1
Starting funding rate arbitrage check for binance and bybit...
Cross-exchange (binance/bybit) - Symbols meeting conditions: ['BTCUSDT']
```

---

## 📊 在 Python 中使用

### 單交易所模式
```python
from monitor.B2B import B2B_monitor

# 創建監控器
monitor = B2B_monitor('binance', threshold=0.15)

# 開始監控
message, result_df = monitor.start_monitoring()

# 查看結果
print(message)
print(result_df)
```

### 跨交易所模式
```python
from monitor.B2B import B2B_monitor

# 創建監控器
monitor = B2B_monitor('binance', 'bybit', threshold=0.10)

# 開始監控
message, result_df = monitor.start_monitoring()

# 查看結果
print(message)
print(result_df)
```

---

## 🔧 配置參數速查

| 參數 | 單交易所建議值 | 跨交易所建議值 | 說明 |
|------|-------------|-------------|------|
| `threshold` | 0.15 - 0.25 | 0.08 - 0.15 | 年化資金費率閾值 |
| `n_days` | 2 | 2 | 滾動窗口天數 |
| `value_threshold` | 3000000 | 3000000 | 最小成交量 |

---

## 📈 結果解讀

### 單交易所返回數據
```
   symbol  annual_fr  position
0  BTCUSDT      0.25        -1  ← Short（收資金費率）
1  ETHUSDT     -0.18         1  ← Long（負資金費率）
```

- `position = -1`: 做空收取資金費率
- `position = 1`: 做多（負資金費率時也是收益）

### 跨交易所返回數據
```
   symbol  binance_annual_fr  bybit_annual_fr  annual_fr_diff  spread  binance_long/short
0  BTCUSDT             0.15             0.08           -0.07   0.001                   1
```

- `binance_long/short = 1`: Binance 做多，Bybit 做空
- `binance_long/short = -1`: Binance 做空，Bybit 做多

---

## ⏰ 設置定時任務

### Crontab（Linux/Mac）
```bash
# 編輯 crontab
crontab -e

# 每 8 小時運行（配合資金費率結算）
0 */8 * * * cd /path/to/monitor && python fr_monitor.py

# 或每小時運行
0 * * * * cd /path/to/monitor && python fr_monitor.py
```

### Windows Task Scheduler
1. 打開「任務計劃程序」
2. 創建基本任務
3. 觸發器：每 8 小時
4. 操作：運行 `python fr_monitor.py`

---

## 🎯 常見使用場景

### 場景 1: 監控 Binance 高資金費率
```json
{
  "monitor": {
    "exchange1_id": "binance",
    "exchange2_id": null,
    "threshold": 0.20  // 20% 以上才通知
  }
}
```

### 場景 2: Binance vs Bybit 套利
```json
{
  "monitor": {
    "exchange1_id": "binance",
    "exchange2_id": "bybit",
    "threshold": 0.10  // 10% 差異
  }
}
```

### 場景 3: 低門檻監控
```json
{
  "monitor": {
    "exchange1_id": "binance",
    "exchange2_id": null,
    "threshold": 0.05,
    "value_threshold": 1000000  // 降低流動性要求
  }
}
```

---

## 🐛 故障排除

### 問題 1: No data available
**原因：** 沒有對應交易所的數據  
**解決：** 先運行數據收集腳本

### 問題 2: Missing 'FundingRate_hourly' column
**原因：** 數據格式不正確  
**解決：** 確保已運行 `DataTransform` 處理數據

### 問題 3: Discord webhook error
**原因：** Webhook URL 不正確  
**解決：** 檢查 Discord webhook 設置

---

## 📚 更多資源

- **詳細文檔**: [MONITOR_SINGLE_EXCHANGE.md](MONITOR_SINGLE_EXCHANGE.md)
- **使用示例**: [monitor/example_usage.py](monitor/example_usage.py)
- **升級說明**: [UPGRADE_SUMMARY.md](UPGRADE_SUMMARY.md)
- **Monitor README**: [monitor/README.md](monitor/README.md)

---

## 💡 最佳實踐

1. **資金費率結算時間**：每天 0:00, 8:00, 16:00 UTC
2. **推薦運行頻率**：每 8 小時或每小時
3. **閾值設置**：
   - 高頻交易：0.05 - 0.10
   - 中頻交易：0.10 - 0.15
   - 保守交易：0.15+
4. **流動性檢查**：確保 `value_threshold` 足夠大以避免滑點

---

**快速測試：**
```bash
cd monitor
python example_usage.py
```

祝你交易順利！🚀



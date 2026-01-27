# ClickHouse Metrics Overview - Binance Collector

## Raw Data (direkt von WebSockets)

### 1. **binance_trades_raw**
- **Quelle**: WebSocket `@aggTrade`
- **Frequenz**: Bei jedem Trade-Event
- **Felder**:
  - `ts` - Timestamp
  - `symbol` - Trading Pair
  - `price` - Trade Price
  - `qty` - Trade Quantity
  - `is_buyer_maker` - 0=Taker Buy, 1=Taker Sell
  - `trade_id` - Unique Trade ID

### 2. **binance_ob_diffs_raw**
- **Quelle**: WebSocket `@depth`
- **Frequenz**: Bei jedem Orderbook-Update (sehr häufig)
- **Felder**:
  - `ts` - Timestamp
  - `symbol` - Trading Pair
  - `U` - First update ID in event
  - `u` - Final update ID in event
  - `bids_prices` - JSON Array der Bid-Preise
  - `bids_qtys` - JSON Array der Bid-Quantities
  - `asks_prices` - JSON Array der Ask-Preise
  - `asks_qtys` - JSON Array der Ask-Quantities

### 3. **binance_ob_top20_100ms**
- **Quelle**: Rekonstruiertes Orderbook aus Depth-Updates
- **Frequenz**: Snapshot alle 100ms
- **Felder**:
  - `ts` - Timestamp
  - `symbol` - Trading Pair
  - `best_bid` - Best Bid Price
  - `best_bid_qty` - Best Bid Quantity
  - `best_ask` - Best Ask Price
  - `best_ask_qty` - Best Ask Quantity
  - `bid_price` - JSON Array Top-20 Bid Prices
  - `bid_qty` - JSON Array Top-20 Bid Quantities
  - `ask_price` - JSON Array Top-20 Ask Prices
  - `ask_qty` - JSON Array Top-20 Ask Quantities

### 4. **binance_l1_200ms**
- **Quelle**: Berechnet aus Orderbook + Trades
- **Frequenz**: Alle 200ms
- **Felder**:
  - `ts` - Timestamp
  - `symbol` - Trading Pair
  - `best_bid` - Best Bid
  - `best_ask` - Best Ask
  - `bid_qty` - Best Bid Quantity
  - `ask_qty` - Best Ask Quantity
  - `spread` - Bid-Ask Spread
  - `mid` - Mid Price
  - `microprice` - Weighted Mid Price
  - `ofi_200ms` - Order Flow Imbalance
  - `trade_count` - Number of Trades
  - `vol_base` - Base Volume
  - `vol_quote` - Quote Volume

### 5. **binance_liquidations**
- **Quelle**: WebSocket `@forceOrder`
- **Frequenz**: Bei jedem Liquidations-Event
- **Felder**:
  - `ts` - Timestamp
  - `symbol` - Trading Pair
  - `side` - BUY/SELL
  - `price` - Liquidation Price
  - `qty` - Liquidation Quantity
  - `order_type` - Order Type
  - `time_in_force` - Time in Force

### 6. **binance_mark_price**
- **Quelle**: WebSocket `@markPrice`
- **Frequenz**: Bei jedem Mark-Price-Update (~3-5s)
- **Felder**:
  - `ts` - Timestamp
  - `symbol` - Trading Pair
  - `mark_price` - Mark Price
  - `index_price` - Index Price
  - `est_settle_price` - Estimated Settlement Price
  - `funding_rate` - Current Funding Rate
  - `next_funding_ms` - Next Funding Time (ms)

### 7. **binance_open_interest**
- **Quelle**: REST API `/fapi/v1/openInterest`
- **Frequenz**: Polling (vermutlich 5-10s)
- **Felder**:
  - `ts` - Timestamp
  - `symbol` - Trading Pair
  - `open_interest` - Open Interest (Contracts)
  - `open_interest_value` - Open Interest (Notional Value)

### 8. **binance_long_short_ratio**
- **Quelle**: REST API `/futures/data/globalLongShortAccountRatio` + `/topLongShortAccountRatio` + `/topLongShortPositionRatio`
- **Frequenz**: Polling (vermutlich 5-60s)
- **Felder**:
  - `ts` - Timestamp
  - `symbol` - Trading Pair
  - `long_short_ratio` - Long/Short Ratio
  - `long_account` - Long Account Percentage
  - `short_account` - Short Account Percentage

---

## Berechnete Kennzahlen (Aggregationen)

### 9. **binance_agg_1s5** (1.5s Window Aggregation)

**Window-Info**:
- `symbol` - Trading Pair
- `window_start_ms` - Window Start (ms)
- `window_end_ms` - Window End (ms)

#### **A. L1 Best Bid/Ask (4 Kennzahlen)**
- `best_bid` - Best Bid Price
- `best_bid_qty` - Best Bid Quantity
- `best_ask` - Best Ask Price
- `best_ask_qty` - Best Ask Quantity

#### **B. Top-20 Orderbook Arrays (4 Arrays)**
- `bid_prices` - Array(Float64) - Top 20 Bid Prices
- `bid_qtys` - Array(Float64) - Top 20 Bid Quantities
- `ask_prices` - Array(Float64) - Top 20 Ask Prices
- `ask_qtys` - Array(Float64) - Top 20 Ask Quantities

#### **C. Orderbook Metriken (24 Kennzahlen)**
1. `spread` - Absolute Spread
2. `relative_spread` - Relative Spread (bps)
3. `microprice` - Weighted Mid Price
4. `orderbook_imbalance_l1` - L1 Imbalance

**Depth Sums (6)**:
5. `depth_sum_bid_1_5` - Sum Bid Depth Levels 1-5
6. `depth_sum_ask_1_5` - Sum Ask Depth Levels 1-5
7. `depth_sum_bid_1_10` - Sum Bid Depth Levels 1-10
8. `depth_sum_ask_1_10` - Sum Ask Depth Levels 1-10
9. `depth_sum_bid_1_20` - Sum Bid Depth Levels 1-20
10. `depth_sum_ask_1_20` - Sum Ask Depth Levels 1-20

**VWAP (6)**:
11. `vwap_bid_1_5` - VWAP Bid Levels 1-5
12. `vwap_ask_1_5` - VWAP Ask Levels 1-5
13. `vwap_bid_1_10` - VWAP Bid Levels 1-10
14. `vwap_ask_1_10` - VWAP Ask Levels 1-10
15. `vwap_bid_1_20` - VWAP Bid Levels 1-20
16. `vwap_ask_1_20` - VWAP Ask Levels 1-20

**Weitere OB-Metriken (8)**:
17. `ofi_sum` - Order Flow Imbalance (summed over window)
18. `microprice_drift` - Microprice Change
19. `l1_jump_rate` - L1 Price Jump Frequency
20. `replenishment_rate` - Orderbook Replenishment Rate
21. `slope_bid` - Bid Side Slope
22. `slope_ask` - Ask Side Slope
23. `curvature_bid` - Bid Side Curvature
24. `curvature_ask` - Ask Side Curvature
25. `spread_regime` - Spread Regime Classification
26. `depth_update_rate_hz` - Depth Update Rate (Hz)

#### **D. OHLC (4 Kennzahlen)**
27. `ohlc_open` - Open Price
28. `ohlc_high` - High Price
29. `ohlc_low` - Low Price
30. `ohlc_close` - Close Price

#### **E. Trade Aggregates (13 Kennzahlen)**
31. `trade_count` - Number of Trades
32. `vol_base` - Total Base Volume
33. `vol_quote` - Total Quote Volume
34. `taker_buy_vol` - Taker Buy Volume
35. `taker_sell_vol` - Taker Sell Volume
36. `first_trade_id` - First Trade ID in Window
37. `last_trade_id` - Last Trade ID in Window
38. `avg_trade_size` - Average Trade Size
39. `trade_rate_hz` - Trade Rate (trades/second)
40. `buy_sell_imbalance_trades` - Buy/Sell Trade Count Imbalance
41. `buy_sell_imbalance_volume` - Buy/Sell Volume Imbalance
42. `cvd_cum` - Cumulative Volume Delta
43. `itt_median_ms` - Median Inter-Trade Time (ms)
44. `burst_score` - Trade Burst Score
45. `sweep_flag` - Sweep Detection Flag

#### **F. Liquidations Aggregates (6 Kennzahlen)**
46. `liq_count` - Number of Liquidations
47. `liq_buy_vol` - Liquidation Buy Volume
48. `liq_sell_vol` - Liquidation Sell Volume
49. `liq_notional` - Total Liquidation Notional
50. `avg_liq_size` - Average Liquidation Size
51. `max_liq_size` - Max Liquidation Size

#### **G. Mark Price / Funding (8 Kennzahlen)**
52. `mark_price` - Mark Price
53. `index_price` - Index Price
54. `est_settle_price` - Estimated Settlement Price
55. `funding_rate` - Funding Rate
56. `next_funding_ms` - Next Funding Time (ms)
57. `mark_premium_bps` - Mark Premium (bps)
58. `time_to_next_funding_s` - Time to Next Funding (seconds)
59. `mark_stale_flag` - Mark Price Stale Flag

#### **H. Volatility (6 Kennzahlen)**
60. `rv_3s` - Realized Volatility 3s
61. `rv_ewma_1m` - RV EWMA 1 minute
62. `rv_ewma_5m` - RV EWMA 5 minutes
63. `rv_ewma_15m` - RV EWMA 15 minutes
64. `parkinson_1m` - Parkinson Volatility 1 minute
65. `l1_price_jump` - L1 Price Jump Size

#### **I. Open Interest & Long/Short (6 Kennzahlen, Nullable)**
66. `open_interest` - Open Interest (Nullable)
67. `open_interest_value` - Open Interest Value (Nullable)
68. `ls_ratio_global` - Global Long/Short Ratio (Nullable)
69. `ls_ratio_top_accounts` - Top Accounts Long/Short Ratio (Nullable)
70. `ls_ratio_top_positions` - Top Positions Long/Short Ratio (Nullable)
71. `ls_period_ms` - Long/Short Data Period (ms, Nullable)

#### **J. Quality Flags (8 Flags)**
72. `has_l1` - Has L1 Data
73. `has_trades` - Has Trade Data
74. `has_depth` - Has Depth Data
75. `has_liq` - Has Liquidation Data
76. `has_mark` - Has Mark Price Data
77. `crossed_book` - Crossed Book Flag
78. `resynced_this_window` - Resynced Flag
79. `mark_age_ms` - Mark Price Age (ms)

---

## Zusammenfassung

### Raw Data Tables: 8
- Trades Raw
- OB Diffs Raw
- OB Top20 100ms
- L1 200ms
- Liquidations
- Mark Price
- Open Interest
- Long/Short Ratio

### Berechnete Kennzahlen in `binance_agg_1s5`: 79
- Orderbook: 24 Metriken
- OHLC: 4 Metriken
- Trades: 13 Metriken
- Liquidations: 6 Metriken
- Mark/Funding: 8 Metriken
- Volatility: 6 Metriken
- OI/LS: 6 Metriken
- Quality Flags: 8 Flags
- L1 + Arrays: 4 + 4 Arrays

**Total: 87 Felder** in der Aggregations-Tabelle

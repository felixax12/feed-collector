SUPPORTED_CHANNELS = {
    "trades": {"description": "Echtzeit Trades", "stream": "{symbol}@trade"},
    "l1": {"description": "Best Bid/Ask via bookTicker", "stream": "{symbol}@bookTicker"},
    "ob_top5": {"description": "Top 5 Orderbuch-Linien", "stream": "{symbol}@depth5{speed}"},
    "ob_top20": {"description": "Top 20 Orderbuch-Linien", "stream": "{symbol}@depth20{speed}"},
    "ob_diff": {"description": "Orderbuch-Deltas", "stream": "{symbol}@depth{speed}"},
    "liquidations": {"description": "Zwangsliquidationen", "stream": "{symbol}@forceOrder"},
    "mark_price": {"description": "Mark- und Indexpreis", "stream": "{symbol}@markPrice@1s"},
    "funding": {"description": "Funding Informationen (aus markPrice)", "stream": "{symbol}@markPrice@1s"},
    "klines": {"description": "Kline Candles (konfigurierbar)", "stream": "{symbol}@kline_{interval}"},
    "advanced_metrics": {"description": "Abgeleitete Kennzahlen", "stream": None},
}


BASE_URLS = {
    "spot": "wss://stream.binance.com:9443",
    "perp_linear": "wss://fstream.binance.com",
    "perp_inverse": "wss://dstream.binance.com",
    "delivery": "wss://dstream.binance.com",
}

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict, validator


class Channel(str, Enum):
    trades = "trades"
    agg_trades_5s = "agg_trades_5s"
    l1 = "l1"
    ob_top5 = "ob_top5"
    ob_top20 = "ob_top20"
    ob_diff = "ob_diff"
    liquidations = "liquidations"
    klines = "klines"
    mark_price = "mark_price"
    funding = "funding"
    advanced_metrics = "advanced_metrics"


class BaseEvent(BaseModel):
    instrument: str
    channel: Channel
    ts_event_ns: int = Field(..., ge=0)
    ts_recv_ns: int = Field(..., ge=0)

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
    )

    @validator("instrument")
    def _upper(cls, value: str) -> str:
        return value.upper()


class TradeEvent(BaseEvent):
    price: Decimal
    qty: Decimal
    side: str
    trade_id: Optional[str] = None
    is_aggressor: Optional[bool] = None


class AggTrade5sEvent(BaseEvent):
    interval_s: int
    window_start_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    notional: Decimal
    trade_count: int
    buy_qty: Decimal
    sell_qty: Decimal
    buy_notional: Decimal
    sell_notional: Decimal
    first_trade_id: Optional[str] = None
    last_trade_id: Optional[str] = None


class OrderBookDepthEvent(BaseEvent):
    depth: int
    bid_prices: List[Decimal]
    bid_qtys: List[Decimal]
    ask_prices: List[Decimal]
    ask_qtys: List[Decimal]

    @validator("depth")
    def _supported_depth(cls, value: int) -> int:
        if value not in {1, 5, 10, 20, 50, 100}:
            raise ValueError("Nicht unterstützte Tiefe.")
        return value


class OrderBookDiffEvent(BaseEvent):
    sequence: int
    prev_sequence: int
    bids: Dict[Decimal, Decimal]
    asks: Dict[Decimal, Decimal]

    @validator("sequence", "prev_sequence")
    def _positive(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Sequenzen müssen >= 0 sein.")
        return value


class LiquidationEvent(BaseEvent):
    side: str
    price: Decimal
    qty: Decimal
    order_id: Optional[str] = None
    reason: Optional[str] = None


class KlineEvent(BaseEvent):
    interval: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int
    is_closed: bool


class MarkPriceEvent(BaseEvent):
    mark_price: Decimal
    index_price: Optional[Decimal] = None


class FundingEvent(BaseEvent):
    funding_rate: Decimal
    next_funding_ts_ns: int


class AdvancedMetricsEvent(BaseEvent):
    metrics: Dict[str, Decimal]

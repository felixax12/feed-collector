from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Optional

from ...core.events import (
    AdvancedMetricsEvent,
    Channel,
    FundingEvent,
    KlineEvent,
    LiquidationEvent,
    MarkPriceEvent,
    OrderBookDepthEvent,
    OrderBookDiffEvent,
    TradeEvent,
)
from ...utils.decimal import to_decimal


def trade_from_stream(
    exchange: str,
    market_type: str,
    symbol: str,
    payload: dict,
    ts_recv_ns: int,
) -> TradeEvent:
    return TradeEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=Channel.trades,
        ts_event_ns=int(payload.get("T") or payload.get("E")),
        ts_recv_ns=ts_recv_ns,
        price=to_decimal(payload["p"]),
        qty=to_decimal(payload["q"]),
        side="SELL" if payload.get("m") else "BUY",
        trade_id=str(payload.get("t")),
        is_aggressor=not payload.get("m"),
    )


def l1_from_stream(
    exchange: str,
    market_type: str,
    symbol: str,
    payload: dict,
    ts_recv_ns: int,
) -> OrderBookDepthEvent:
    event_ts = payload.get("E")
    ts_event_ms = int(event_ts) if event_ts is not None else int(ts_recv_ns // 1_000_000)
    return OrderBookDepthEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=Channel.l1,
        ts_event_ns=ts_event_ms,
        ts_recv_ns=ts_recv_ns,
        depth=1,
        bid_prices=[to_decimal(payload["b"])],
        bid_qtys=[to_decimal(payload["B"])],
        ask_prices=[to_decimal(payload["a"])],
        ask_qtys=[to_decimal(payload["A"])],
    )


def depth_from_snapshot(
    exchange: str,
    market_type: str,
    symbol: str,
    payload: dict,
    ts_recv_ns: int,
    depth: int,
    channel: Channel,
) -> OrderBookDepthEvent:
    bids = _pairs_to_dec(payload.get("bids", []), depth)
    asks = _pairs_to_dec(payload.get("asks", []), depth)
    event_ts = payload.get("E")
    ts_event_ms = int(event_ts) if event_ts is not None else int(ts_recv_ns // 1_000_000)
    return OrderBookDepthEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=channel,
        ts_event_ns=ts_event_ms,
        ts_recv_ns=ts_recv_ns,
        depth=depth,
        bid_prices=[px for px, _ in bids],
        bid_qtys=[qty for _, qty in bids],
        ask_prices=[px for px, _ in asks],
        ask_qtys=[qty for _, qty in asks],
    )


def diff_from_stream(
    exchange: str,
    market_type: str,
    symbol: str,
    payload: dict,
    ts_recv_ns: int,
) -> OrderBookDiffEvent:
    bids = {to_decimal(price): to_decimal(qty) for price, qty in payload.get("b", [])}
    asks = {to_decimal(price): to_decimal(qty) for price, qty in payload.get("a", [])}
    return OrderBookDiffEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=Channel.ob_diff,
        ts_event_ns=int(payload["E"]),
        ts_recv_ns=ts_recv_ns,
        sequence=int(payload["u"]),
        prev_sequence=int(payload["U"]),
        bids=bids,
        asks=asks,
    )


def liquidation_from_stream(
    exchange: str,
    market_type: str,
    symbol: str,
    payload: dict,
    ts_recv_ns: int,
) -> LiquidationEvent:
    order = payload["o"]
    return LiquidationEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=Channel.liquidations,
        ts_event_ns=int(order["T"]),
        ts_recv_ns=ts_recv_ns,
        side=order["S"],
        price=to_decimal(order["L"]),
        qty=to_decimal(order["z"]),
        order_id=str(order.get("i") or order.get("O") or ""),
        reason=order.get("X"),
    )


def mark_price_from_stream(
    exchange: str,
    market_type: str,
    symbol: str,
    payload: dict,
    ts_recv_ns: int,
) -> MarkPriceEvent:
    return MarkPriceEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=Channel.mark_price,
        ts_event_ns=int(payload["E"]),
        ts_recv_ns=ts_recv_ns,
        mark_price=to_decimal(payload["p"]),
        index_price=to_decimal(payload["i"]) if payload.get("i") else None,
    )


def funding_from_stream(
    exchange: str,
    market_type: str,
    symbol: str,
    payload: dict,
    ts_recv_ns: int,
) -> FundingEvent:
    return FundingEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=Channel.funding,
        ts_event_ns=int(payload["E"]),
        ts_recv_ns=ts_recv_ns,
        funding_rate=to_decimal(payload["r"] if "r" in payload else payload["f"]),
        next_funding_ts_ns=int(payload["T"]),
    )


def kline_from_stream(
    exchange: str,
    market_type: str,
    symbol: str,
    payload: dict,
    ts_recv_ns: int,
) -> KlineEvent:
    k = payload["k"]
    return KlineEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=Channel.klines,
        ts_event_ns=int(payload["E"]),
        ts_recv_ns=ts_recv_ns,
        interval=k["i"],
        open=to_decimal(k["o"]),
        high=to_decimal(k["h"]),
        low=to_decimal(k["l"]),
        close=to_decimal(k["c"]),
        volume=to_decimal(k["v"]),
        trade_count=int(k["n"]),
        is_closed=bool(k["x"]),
    )


def metrics_from_state(
    exchange: str,
    market_type: str,
    symbol: str,
    ts_event_ns: int,
    ts_recv_ns: int,
    best_bid: Optional[Decimal],
    best_ask: Optional[Decimal],
    top5: Optional[Dict[str, List[Decimal]]] = None,
) -> Optional[AdvancedMetricsEvent]:
    if best_bid is None or best_ask is None:
        return None
    spread = best_ask - best_bid
    mid = (best_ask + best_bid) / Decimal("2")
    metrics: Dict[str, Decimal] = {
        "spread_px": spread,
        "mid_px": mid,
        "spread_bps": (spread / mid * Decimal("10000")) if mid > 0 else Decimal("0"),
    }
    if top5:
        bid_qty_total = sum(top5.get("bid_qtys", []))
        ask_qty_total = sum(top5.get("ask_qtys", []))
        total = bid_qty_total + ask_qty_total
        if total > 0:
            metrics["imbalance_5"] = (bid_qty_total - ask_qty_total) / total
    return AdvancedMetricsEvent(
        exchange=exchange,
        market_type=market_type,
        instrument=symbol,
        channel=Channel.advanced_metrics,
        ts_event_ns=ts_event_ns,
        ts_recv_ns=ts_recv_ns,
        metrics=metrics,
    )


def _pairs_to_dec(
    entries: List[List[str]],
    depth: int,
) -> List[tuple[Decimal, Decimal]]:
    result = []
    for price_str, qty_str in entries[:depth]:
        result.append((to_decimal(price_str), to_decimal(qty_str)))
    return result

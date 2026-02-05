from __future__ import annotations

"""
Redis Sink fuer Live-Trading-Lesezugriffe.

TTL-Policy (bewusst kurz fuer "letzter Stand"):
- mark_price: 3 Sekunden
- agg_trades_5s: 10 Sekunden
- klines: 120 Sekunden (2 Minuten)

Hinweis:
- Hash-Keys bilden den jeweils letzten Zustand je Symbol/Intervall ab.
- Streams (trades/liquidations) bleiben ueber MAXLEN begrenzt und nutzen kein TTL.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional

try:
    from redis.asyncio import Redis  # type: ignore
except ImportError:  # pragma: no cover
    Redis = Any  # type: ignore

from ..core.events import (
    AdvancedMetricsEvent,
    AggTrade5sEvent,
    BaseEvent,
    Channel,
    FundingEvent,
    KlineEvent,
    LiquidationEvent,
    MarkPriceEvent,
    OrderBookDepthEvent,
    TradeEvent,
)
from ..core.pipeline import EventWriter
from ..utils.decimal import to_decimal


@dataclass
class RedisCommand:
    name: str
    key: str
    payload: Optional[Dict[str, str]] = None
    maxlen: Optional[int] = None
    ttl_s: Optional[int] = None
    channel: Optional[str] = None
    count_for_channel: bool = False


class RedisWriter(EventWriter):
    MARK_PRICE_TTL_S = 3
    AGG_TRADES_5S_TTL_S = 10
    KLINES_TTL_S = 120

    def __init__(
        self,
        client: Redis,
        *,
        pipeline_size: int,
        flush_interval_ms: int,
        stream_maxlen: int,
        namespace: str = "marketdata",
    ) -> None:
        super().__init__("redis", flush_interval_ms)
        self._client = client
        self._pipeline_size = pipeline_size
        self._stream_maxlen = stream_maxlen
        self._namespace = namespace
        self._buffer: List[RedisCommand] = []
        self._events_by_channel: Dict[str, int] = {}
        self._flushed_by_channel: Dict[str, int] = {}

    async def enqueue(self, event: BaseEvent) -> None:
        channel, commands = self._build_commands(event)
        if not commands:
            return
        self._record_event(len(commands))
        self._events_by_channel[channel] = self._events_by_channel.get(channel, 0) + 1
        for command in commands:
            self._buffer.append(command)
        if len(self._buffer) >= self._pipeline_size:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        buffered = len(self._buffer)
        pipe = self._client.pipeline(transaction=False)
        flushed_by_channel_delta: Dict[str, int] = {}
        try:
            for command in self._buffer:
                if command.name == "hset":
                    await pipe.hset(command.key, mapping=command.payload or {})
                elif command.name == "xadd":
                    await pipe.xadd(
                        command.key,
                        command.payload or {},
                        maxlen=command.maxlen,
                        approximate=True,
                    )
                elif command.name == "expire":
                    if command.ttl_s is None:
                        continue
                    await pipe.expire(command.key, command.ttl_s)
                if command.count_for_channel and command.channel:
                    flushed_by_channel_delta[command.channel] = (
                        flushed_by_channel_delta.get(command.channel, 0) + 1
                    )
            await pipe.execute()
        finally:
            await pipe.aclose()
            self._buffer.clear()
            self._record_flush(buffered)
            for channel, count in flushed_by_channel_delta.items():
                self._flushed_by_channel[channel] = self._flushed_by_channel.get(channel, 0) + count

    def stats(self) -> dict:
        base = super().stats()
        base["events_by_channel"] = dict(self._events_by_channel)
        base["flushed_by_channel"] = dict(self._flushed_by_channel)
        return base

    def _build_commands(self, event: BaseEvent) -> tuple[str, List[RedisCommand]]:
        if isinstance(event, TradeEvent):
            return Channel.trades.value, [self._build_trade_command(event)]
        if isinstance(event, OrderBookDepthEvent):
            return event.channel.value, [self._build_depth_command(event)]
        if isinstance(event, MarkPriceEvent):
            return Channel.mark_price.value, self._with_ttl(
                self._build_mark_command(event),
                self.MARK_PRICE_TTL_S,
                Channel.mark_price.value,
            )
        if isinstance(event, FundingEvent):
            return Channel.funding.value, [self._build_funding_command(event)]
        if isinstance(event, AggTrade5sEvent):
            return Channel.agg_trades_5s.value, self._with_ttl(
                self._build_agg_trades_5s_command(event),
                self.AGG_TRADES_5S_TTL_S,
                Channel.agg_trades_5s.value,
            )
        if isinstance(event, KlineEvent):
            return Channel.klines.value, self._with_ttl(
                self._build_klines_command(event),
                self.KLINES_TTL_S,
                Channel.klines.value,
            )
        if isinstance(event, AdvancedMetricsEvent):
            return Channel.advanced_metrics.value, [self._build_metrics_command(event)]
        if isinstance(event, LiquidationEvent):
            return Channel.liquidations.value, [self._build_liquidation_command(event)]
        return event.channel.value, []

    def _with_ttl(self, command: RedisCommand, ttl_s: int, channel: str) -> List[RedisCommand]:
        command.channel = channel
        command.count_for_channel = True
        return [
            command,
            RedisCommand(name="expire", key=command.key, ttl_s=ttl_s),
        ]

    def _build_trade_command(self, event: TradeEvent) -> RedisCommand:
        key = self._key("stream", "trades", event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
            "px": _str(event.price),
            "qty": _str(event.qty),
            "side": event.side,
        }
        if event.trade_id:
            payload["trade_id"] = event.trade_id
        if event.is_aggressor is not None:
            payload["is_aggressor"] = "1" if event.is_aggressor else "0"
        return RedisCommand(
            "xadd",
            key,
            payload,
            maxlen=self._stream_maxlen,
            channel=Channel.trades.value,
            count_for_channel=True,
        )

    def _build_depth_command(self, event: OrderBookDepthEvent) -> RedisCommand:
        prefix = {
            1: "last:l1",
            5: "last:top5",
            20: "last:top20",
            10: "last:top10",
            50: "last:top50",
            100: "last:top100",
        }[event.depth]
        key = self._key(prefix, event.instrument)
        payload: Dict[str, str] = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
        }
        for idx, (price, qty) in enumerate(zip(event.bid_prices, event.bid_qtys), start=1):
            payload[f"b{idx}_px"] = _str(price)
            payload[f"b{idx}_sz"] = _str(qty)
        for idx, (price, qty) in enumerate(zip(event.ask_prices, event.ask_qtys), start=1):
            payload[f"a{idx}_px"] = _str(price)
            payload[f"a{idx}_sz"] = _str(qty)
        return RedisCommand("hset", key, payload, channel=event.channel.value, count_for_channel=True)

    def _build_mark_command(self, event: MarkPriceEvent) -> RedisCommand:
        key = self._key("last:mark", event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
            "mark_px": _str(event.mark_price),
        }
        if event.index_price is not None:
            payload["index_px"] = _str(event.index_price)
        return RedisCommand(
            "hset",
            key,
            payload,
            channel=Channel.mark_price.value,
            count_for_channel=True,
        )

    def _build_funding_command(self, event: FundingEvent) -> RedisCommand:
        key = self._key("last:funding", event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
            "funding_rate": _str(event.funding_rate),
            "next_funding_ts_ns": str(event.next_funding_ts_ns),
        }
        return RedisCommand(
            "hset",
            key,
            payload,
            channel=Channel.funding.value,
            count_for_channel=True,
        )

    def _build_agg_trades_5s_command(self, event: AggTrade5sEvent) -> RedisCommand:
        key = self._key("last:agg_trades_5s", event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
            "interval_s": str(event.interval_s),
            "window_start_ns": str(event.window_start_ns),
            "open": _str(event.open),
            "high": _str(event.high),
            "low": _str(event.low),
            "close": _str(event.close),
            "volume": _str(event.volume),
            "notional": _str(event.notional),
            "trade_count": str(event.trade_count),
            "buy_qty": _str(event.buy_qty),
            "sell_qty": _str(event.sell_qty),
            "buy_notional": _str(event.buy_notional),
            "sell_notional": _str(event.sell_notional),
        }
        if event.first_trade_id is not None:
            payload["first_trade_id"] = event.first_trade_id
        if event.last_trade_id is not None:
            payload["last_trade_id"] = event.last_trade_id
        return RedisCommand(
            "hset",
            key,
            payload,
            channel=Channel.agg_trades_5s.value,
            count_for_channel=True,
        )

    def _build_klines_command(self, event: KlineEvent) -> RedisCommand:
        key = self._key("last:klines", event.interval, event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
            "interval": event.interval,
            "open": _str(event.open),
            "high": _str(event.high),
            "low": _str(event.low),
            "close": _str(event.close),
            "volume": _str(event.volume),
            "quote_volume": _str(event.quote_volume),
            "taker_buy_base_volume": _str(event.taker_buy_base_volume),
            "taker_buy_quote_volume": _str(event.taker_buy_quote_volume),
            "trade_count": str(event.trade_count),
            "is_closed": "1" if event.is_closed else "0",
        }
        return RedisCommand(
            "hset",
            key,
            payload,
            channel=Channel.klines.value,
            count_for_channel=True,
        )

    def _build_metrics_command(self, event: AdvancedMetricsEvent) -> RedisCommand:
        key = self._key("last:adv", event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
        }
        for name, value in event.metrics.items():
            payload[name] = _str(value)
        return RedisCommand(
            "hset",
            key,
            payload,
            channel=Channel.advanced_metrics.value,
            count_for_channel=True,
        )

    def _build_liquidation_command(self, event: LiquidationEvent) -> RedisCommand:
        key = self._key("stream", "liquidations", event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
            "side": event.side,
            "px": _str(event.price),
            "qty": _str(event.qty),
        }
        if event.order_id:
            payload["order_id"] = event.order_id
        if event.reason:
            payload["reason"] = event.reason
        return RedisCommand(
            "xadd",
            key,
            payload,
            maxlen=self._stream_maxlen,
            channel=Channel.liquidations.value,
            count_for_channel=True,
        )

    def _key(self, *parts: str) -> str:
        return ":".join([self._namespace, *parts])


def _str(value: Decimal) -> str:
    return str(to_decimal(value))

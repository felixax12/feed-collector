from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from redis.asyncio import Redis  # type: ignore
except ImportError:  # pragma: no cover
    Redis = Any  # type: ignore

from ..core.events import (
    AdvancedMetricsEvent,
    BaseEvent,
    Channel,
    FundingEvent,
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
    payload: Dict[str, str]
    maxlen: Optional[int] = None


class RedisWriter(EventWriter):
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

    async def enqueue(self, event: BaseEvent) -> None:
        commands = list(self._build_commands(event))
        if not commands:
            return
        self._record_event(len(commands))
        for command in commands:
            self._buffer.append(command)
        if len(self._buffer) >= self._pipeline_size:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        buffered = len(self._buffer)
        pipe = self._client.pipeline(transaction=False)
        try:
            for command in self._buffer:
                if command.name == "hset":
                    await pipe.hset(command.key, mapping=command.payload)
                elif command.name == "xadd":
                    await pipe.xadd(
                        command.key,
                        command.payload,
                        maxlen=command.maxlen,
                        approximate=True,
                    )
            await pipe.execute()
        finally:
            await pipe.close()
            self._buffer.clear()
            self._record_flush(buffered)

    def _build_commands(self, event: BaseEvent) -> Iterable[RedisCommand]:
        if isinstance(event, TradeEvent):
            return [self._build_trade_command(event)]
        if isinstance(event, OrderBookDepthEvent):
            return [self._build_depth_command(event)]
        if isinstance(event, MarkPriceEvent):
            return [self._build_mark_command(event)]
        if isinstance(event, FundingEvent):
            return [self._build_funding_command(event)]
        if isinstance(event, AdvancedMetricsEvent):
            return [self._build_metrics_command(event)]
        if isinstance(event, LiquidationEvent):
            return [self._build_liquidation_command(event)]
        return []

    def _build_trade_command(self, event: TradeEvent) -> RedisCommand:
        key = self._key("stream", "trades", event.exchange, event.instrument)
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
        return RedisCommand("xadd", key, payload, maxlen=self._stream_maxlen)

    def _build_depth_command(self, event: OrderBookDepthEvent) -> RedisCommand:
        prefix = {
            1: "last:l1",
            5: "last:top5",
            20: "last:top20",
            10: "last:top10",
            50: "last:top50",
            100: "last:top100",
        }[event.depth]
        key = self._key(prefix, event.exchange, event.instrument)
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
        return RedisCommand("hset", key, payload)

    def _build_mark_command(self, event: MarkPriceEvent) -> RedisCommand:
        key = self._key("last:mark", event.exchange, event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
            "mark_px": _str(event.mark_price),
        }
        if event.index_price is not None:
            payload["index_px"] = _str(event.index_price)
        return RedisCommand("hset", key, payload)

    def _build_funding_command(self, event: FundingEvent) -> RedisCommand:
        key = self._key("last:funding", event.exchange, event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
            "funding_rate": _str(event.funding_rate),
            "next_funding_ts_ns": str(event.next_funding_ts_ns),
        }
        return RedisCommand("hset", key, payload)

    def _build_metrics_command(self, event: AdvancedMetricsEvent) -> RedisCommand:
        key = self._key("last:adv", event.exchange, event.instrument)
        payload = {
            "ts_event_ns": str(event.ts_event_ns),
            "ts_recv_ns": str(event.ts_recv_ns),
        }
        for name, value in event.metrics.items():
            payload[name] = _str(value)
        return RedisCommand("hset", key, payload)

    def _build_liquidation_command(self, event: LiquidationEvent) -> RedisCommand:
        key = self._key("stream", "liquidations", event.exchange, event.instrument)
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
        return RedisCommand("xadd", key, payload, maxlen=self._stream_maxlen)

    def _key(self, *parts: str) -> str:
        return ":".join([self._namespace, *parts])


def _str(value: Decimal) -> str:
    return str(to_decimal(value))

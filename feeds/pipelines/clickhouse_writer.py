from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import httpx

from ..core.events import (
    AdvancedMetricsEvent,
    BaseEvent,
    Channel,
    FundingEvent,
    KlineEvent,
    LiquidationEvent,
    MarkPriceEvent,
    OrderBookDepthEvent,
    OrderBookDiffEvent,
    TradeEvent,
)
from ..core.pipeline import EventWriter
from ..utils.decimal import to_decimal


class ClickHouseWriter(EventWriter):
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        database: str,
        batch_rows: int,
        flush_interval_ms: int,
        compression: Optional[str],
    ) -> None:
        super().__init__("clickhouse", flush_interval_ms)
        self._client = client
        self._database = database
        self._batch_rows = batch_rows
        self._compression = compression
        self._buffer: Dict[str, List[Dict[str, object]]] = {}
        self._buffer_lock = asyncio.Lock()
        self._rows_by_table: Dict[str, int] = {}
        self._flushed_by_table: Dict[str, int] = {}
        self._flush_errors = 0
        self._pending_tasks: set[asyncio.Task] = set()
        self._flush_sem = asyncio.Semaphore(4)

    async def enqueue(self, event: BaseEvent) -> None:
        row = self._event_to_row(event)
        if row is None:
            return
        table, data = row
        self._record_event(1)
        self._rows_by_table[table] = self._rows_by_table.get(table, 0) + 1
        rows_to_flush: Optional[List[Dict[str, object]]] = None
        async with self._buffer_lock:
            bucket = self._buffer.setdefault(table, [])
            bucket.append(data)
            if len(bucket) >= self._batch_rows:
                rows_to_flush = bucket[:]
                self._buffer[table] = []
        if rows_to_flush:
            self._schedule_flush(table, rows_to_flush)

    async def flush(self) -> None:
        to_flush: List[Tuple[str, List[Dict[str, object]]]] = []
        async with self._buffer_lock:
            for table in list(self._buffer.keys()):
                if self._buffer[table]:
                    rows = self._buffer[table][:]
                    self._buffer[table] = []
                    to_flush.append((table, rows))
        for table, rows in to_flush:
            self._schedule_flush(table, rows)

    def _schedule_flush(self, table: str, rows: List[Dict[str, object]]) -> None:
        task = asyncio.create_task(self._flush_rows(table, rows))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _flush_rows(self, table: str, rows: List[Dict[str, object]]) -> None:
        if not rows:
            return
        async with self._flush_sem:
            row_count = len(rows)
            payload = "\n".join(json.dumps(row, default=_json_default) for row in rows)
            query = f"INSERT INTO {self._database}.{table} FORMAT JSONEachRow"
            headers = {}
            if self._compression:
                headers["X-ClickHouse-Format"] = self._compression
            try:
                response = await self._client.post(
                    "/",
                    params={"query": query},
                    content=payload.encode("utf-8"),
                    headers=headers,
                )
                response.raise_for_status()
            except Exception as exc:
                self._flush_errors += 1
                logging.warning("CH flush failed table=%s rows=%s error=%s", table, row_count, exc)
                async with self._buffer_lock:
                    self._buffer.setdefault(table, []).extend(rows)
            else:
                self._record_flush(row_count)
                self._flushed_by_table[table] = self._flushed_by_table.get(table, 0) + row_count

    async def stop(self) -> None:
        await super().stop()
        if self._pending_tasks:
            await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)

    def stats(self) -> dict:
        base = super().stats()
        base["rows_by_table"] = dict(self._rows_by_table)
        base["flushed_by_table"] = dict(self._flushed_by_table)
        base["flush_errors"] = self._flush_errors
        return base

    def _event_to_row(self, event: BaseEvent) -> Optional[Tuple[str, Dict[str, object]]]:
        common = {
            "exchange": event.exchange,
            "market_type": event.market_type,
            "instrument": event.instrument,
            "ts_event_ns": event.ts_event_ns,
            "ts_recv_ns": event.ts_recv_ns,
        }
        if isinstance(event, TradeEvent):
            data = {
                **common,
                "price": to_decimal(event.price),
                "qty": to_decimal(event.qty),
                "side": event.side,
                "trade_id": event.trade_id,
                "is_aggressor": event.is_aggressor,
            }
            return "trades", data
        if isinstance(event, OrderBookDepthEvent):
            data = {**common, "depth": event.depth}
            data["bid_prices"] = [to_decimal(px) for px in event.bid_prices]
            data["bid_qtys"] = [to_decimal(qty) for qty in event.bid_qtys]
            data["ask_prices"] = [to_decimal(px) for px in event.ask_prices]
            data["ask_qtys"] = [to_decimal(qty) for qty in event.ask_qtys]
            return self._depth_table(event.channel), data
        if isinstance(event, OrderBookDiffEvent):
            data = {
                **common,
                "sequence": event.sequence,
                "prev_sequence": event.prev_sequence,
                "bids": {str(to_decimal(px)): str(to_decimal(qty)) for px, qty in event.bids.items()},
                "asks": {str(to_decimal(px)): str(to_decimal(qty)) for px, qty in event.asks.items()},
            }
            return "order_book_diffs", data
        if isinstance(event, LiquidationEvent):
            data = {
                **common,
                "side": event.side,
                "price": to_decimal(event.price),
                "qty": to_decimal(event.qty),
                "order_id": event.order_id,
                "reason": event.reason,
            }
            return "liquidations", data
        if isinstance(event, MarkPriceEvent):
            data = {
                **common,
                "mark_price": to_decimal(event.mark_price),
                "index_price": to_decimal(event.index_price) if event.index_price is not None else None,
            }
            return "mark_price", data
        if isinstance(event, FundingEvent):
            data = {
                **common,
                "funding_rate": to_decimal(event.funding_rate),
                "next_funding_ts_ns": event.next_funding_ts_ns,
            }
            return "funding", data
        if isinstance(event, AdvancedMetricsEvent):
            data = {
                **common,
                "metrics": {name: to_decimal(value) for name, value in event.metrics.items()},
            }
            return "advanced_metrics", data
        if isinstance(event, KlineEvent):
            data = {
                **common,
                "interval": event.interval,
                "open": to_decimal(event.open),
                "high": to_decimal(event.high),
                "low": to_decimal(event.low),
                "close": to_decimal(event.close),
                "volume": to_decimal(event.volume),
                "trade_count": event.trade_count,
                "is_closed": event.is_closed,
            }
            return "klines", data
        return None

    def _depth_table(self, channel: Channel) -> str:
        if channel == Channel.l1:
            return "l1"
        if channel == Channel.ob_top5:
            return "ob_top5"
        if channel == Channel.ob_top20:
            return "ob_top20"
        return "order_book_depth"


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bool):
        return value
    return value

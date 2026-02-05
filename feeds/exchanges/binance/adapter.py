from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict
from collections import defaultdict
from decimal import Decimal
from typing import Dict, Iterable, List, Optional

import websockets
from pydantic import ValidationError

from ...config import ChannelConfig, ExchangeConfig
from ...core.events import AggTrade5sEvent, Channel, OrderBookDepthEvent
from ...core.router import PipelineRouter
from ...utils import now_ns
from ...utils.decimal import to_decimal
from ..base import ExchangeFeed
from .capabilities import BASE_URLS, SUPPORTED_CHANNELS
from . import transforms


class _AggTradeBucket:
    def __init__(
        self,
        *,
        window_start_ns: int,
        price: Decimal,
        qty: Decimal,
        notional: Decimal,
        trade_id: Optional[str],
        is_sell: bool,
        ts_recv_ns: int,
    ) -> None:
        self.window_start_ns = window_start_ns
        self.open = price
        self.high = price
        self.low = price
        self.close = price
        self.volume = qty
        self.notional = notional
        self.trade_count = 1
        self.buy_qty = Decimal("0")
        self.sell_qty = Decimal("0")
        self.buy_notional = Decimal("0")
        self.sell_notional = Decimal("0")
        if is_sell:
            self.sell_qty = qty
            self.sell_notional = notional
        else:
            self.buy_qty = qty
            self.buy_notional = notional
        self.first_trade_id = trade_id
        self.last_trade_id = trade_id
        self.last_recv_ns = ts_recv_ns

    def update(
        self,
        *,
        price: Decimal,
        qty: Decimal,
        notional: Decimal,
        trade_id: Optional[str],
        is_sell: bool,
        ts_recv_ns: int,
    ) -> None:
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self.volume += qty
        self.notional += notional
        self.trade_count += 1
        if is_sell:
            self.sell_qty += qty
            self.sell_notional += notional
        else:
            self.buy_qty += qty
            self.buy_notional += notional
        if trade_id is not None:
            self.last_trade_id = trade_id
        self.last_recv_ns = ts_recv_ns


class AggTradeAggregator:
    def __init__(
        self,
        interval_s: int,
        symbols: List[str],
        max_catchup_windows: int,
        late_grace_s: int,
    ) -> None:
        self.interval_s = interval_s
        self._interval_ns = int(interval_s * 1_000_000_000)
        self._symbols = symbols
        self._buckets: Dict[str, _AggTradeBucket] = {}
        self._last_emitted: Dict[str, int] = {}
        self._last_flush_window_start: Optional[int] = None
        self._max_catchup_windows = max_catchup_windows
        self._late_grace_ns = max(0, int(late_grace_s) * 1_000_000_000)
        self._catchup_caps = 0
        self._catchup_skipped = 0
        self._late_trades = 0

    def update(self, symbol: str, payload: dict, ts_recv_ns: int) -> List[AggTrade5sEvent]:
        ts_event_ms = int(payload.get("T") or payload.get("E") or 0)
        ts_event_ns = ts_event_ms * 1_000_000
        window_start_ns = (ts_event_ns // self._interval_ns) * self._interval_ns
        last_emitted = self._last_emitted.get(symbol)
        if last_emitted is not None and window_start_ns <= last_emitted:
            self._late_trades += 1
            return []
        price = to_decimal(payload["p"])
        qty = to_decimal(payload["q"])
        notional = price * qty
        is_sell = bool(payload.get("m"))
        trade_id = payload.get("a") or payload.get("t")
        bucket = self._buckets.get(symbol)
        events: List[AggTrade5sEvent] = []
        if bucket and bucket.window_start_ns != window_start_ns:
            events.append(self._emit(symbol, bucket))
            bucket = None
        if bucket is None:
            bucket = _AggTradeBucket(
                window_start_ns=window_start_ns,
                price=price,
                qty=qty,
                notional=notional,
                trade_id=str(trade_id) if trade_id is not None else None,
                is_sell=is_sell,
                ts_recv_ns=ts_recv_ns,
            )
            self._buckets[symbol] = bucket
        else:
            bucket.update(
                price=price,
                qty=qty,
                notional=notional,
                trade_id=str(trade_id) if trade_id is not None else None,
                is_sell=is_sell,
                ts_recv_ns=ts_recv_ns,
            )
        return events

    def flush(self, now_ns: int) -> List[AggTrade5sEvent]:
        watermark_ns = now_ns - self._late_grace_ns
        if watermark_ns <= 0:
            return []
        last_emittable_window = ((watermark_ns // self._interval_ns) - 1) * self._interval_ns
        if last_emittable_window < 0:
            return []
        if (
            self._last_flush_window_start is not None
            and last_emittable_window <= self._last_flush_window_start
        ):
            return []
        self._last_flush_window_start = last_emittable_window
        events: List[AggTrade5sEvent] = []
        for symbol in self._symbols:
            last_emitted = self._last_emitted.get(symbol)
            if last_emitted is None:
                last_emitted = last_emittable_window - self._interval_ns
            next_window = last_emitted + self._interval_ns
            emitted_windows = 0
            while next_window <= last_emittable_window:
                bucket = self._buckets.get(symbol)
                if bucket and bucket.window_start_ns == next_window:
                    events.append(self._emit(symbol, bucket))
                    del self._buckets[symbol]
                else:
                    events.append(self._emit_empty(symbol, next_window, now_ns))
                self._last_emitted[symbol] = next_window
                emitted_windows += 1
                if self._max_catchup_windows and emitted_windows >= self._max_catchup_windows:
                    remaining = int((last_emittable_window - next_window) // self._interval_ns)
                    if remaining > 0:
                        self._catchup_caps += 1
                        self._catchup_skipped += remaining
                    break
                next_window += self._interval_ns
        return events

    def _emit(self, symbol: str, bucket: _AggTradeBucket) -> AggTrade5sEvent:
        window_end_ns = bucket.window_start_ns + self._interval_ns - 1
        return AggTrade5sEvent(
            instrument=symbol,
            channel=Channel.agg_trades_5s,
            ts_event_ns=window_end_ns,
            ts_recv_ns=bucket.last_recv_ns,
            interval_s=self.interval_s,
            window_start_ns=bucket.window_start_ns,
            open=bucket.open,
            high=bucket.high,
            low=bucket.low,
            close=bucket.close,
            volume=bucket.volume,
            notional=bucket.notional,
            trade_count=bucket.trade_count,
            buy_qty=bucket.buy_qty,
            sell_qty=bucket.sell_qty,
            buy_notional=bucket.buy_notional,
            sell_notional=bucket.sell_notional,
            first_trade_id=bucket.first_trade_id,
            last_trade_id=bucket.last_trade_id,
        )

    def _emit_empty(self, symbol: str, window_start_ns: int, now_ns: int) -> AggTrade5sEvent:
        window_end_ns = window_start_ns + self._interval_ns - 1
        zero = Decimal("0")
        return AggTrade5sEvent(
            instrument=symbol,
            channel=Channel.agg_trades_5s,
            ts_event_ns=window_end_ns,
            ts_recv_ns=now_ns,
            interval_s=self.interval_s,
            window_start_ns=window_start_ns,
            open=zero,
            high=zero,
            low=zero,
            close=zero,
            volume=zero,
            notional=zero,
            trade_count=0,
            buy_qty=zero,
            sell_qty=zero,
            buy_notional=zero,
            sell_notional=zero,
            first_trade_id=None,
            last_trade_id=None,
        )

    def pop_catchup_stats(self) -> tuple[int, int]:
        caps = self._catchup_caps
        skipped = self._catchup_skipped
        self._catchup_caps = 0
        self._catchup_skipped = 0
        return caps, skipped

    def pop_late_stats(self) -> int:
        late = self._late_trades
        self._late_trades = 0
        return late


def _parse_interval_seconds(interval: Optional[str]) -> Optional[int]:
    if not interval:
        return None
    text = interval.strip().lower()
    if not text:
        return None
    num = ""
    unit = ""
    for ch in text:
        if ch.isdigit():
            num += ch
        else:
            unit += ch
    if not num or not unit:
        return None
    try:
        value = int(num)
    except ValueError:
        return None
    factor = {"s": 1, "m": 60, "h": 3600}.get(unit.strip())
    if not factor:
        return None
    return value * factor


class BinanceFeed(ExchangeFeed):
    """Referenz-Adapter für Binance Futures/Spot."""

    def __init__(self, config: ExchangeConfig, router: PipelineRouter):
        super().__init__(config, router)
        self._logger = logging.getLogger("feeds.binance")
        self.market_type = config.market_type
        self.exchange_name = config.exchange
        self._ws_log_interval_s = 10
        if config.metadata and isinstance(config.metadata, dict):
            raw = config.metadata.get("ws_log_interval_s")
            if isinstance(raw, int) and raw > 0:
                self._ws_log_interval_s = raw
        self._advanced_channel = config.channels.get("advanced_metrics")
        self._best_bid: Dict[str, Optional[Decimal]] = defaultdict(lambda: None)
        self._best_ask: Dict[str, Optional[Decimal]] = defaultdict(lambda: None)
        self._top5_state: Dict[str, Dict[str, List]] = defaultdict(dict)
        self._mark_task_created = False
        self._agg_trades_task_created = False
        self._agg_trades_agg: Optional[AggTradeAggregator] = None
        self._agg_trades_queue: Optional[asyncio.Queue] = None
        self._agg_trades_enqueued = 0
        self._agg_trades_processed = 0
        self._agg_trades_dropped = 0
        self._agg_trades_emitted = 0
        self._agg_trades_queue_max = int(os.getenv("AGG_TRADE_QUEUE_MAX", "20000"))
        self._agg_trades_max_catchup = int(os.getenv("AGG_TRADE_MAX_CATCHUP_WINDOWS", "120"))
        self._agg_trades_late_grace_s = int(os.getenv("AGG_TRADE_LATE_GRACE_S", "2"))
        self._msg_counts: Dict[str, int] = defaultdict(int)
        self._conn_counts: Dict[str, int] = defaultdict(int)
        self._disc_counts: Dict[str, int] = defaultdict(int)
        self._parse_errors: Dict[str, int] = defaultdict(int)
        self._validation_errors: Dict[str, int] = defaultdict(int)

    async def start(self) -> None:
        for channel_name, channel_conf in self.config.channels.items():
            if not channel_conf.enabled:
                continue
            if channel_name == Channel.advanced_metrics.value:
                continue  # computed intern
            if channel_name in {"mark_price", "funding"}:
                if not self._mark_task_created:
                    combined_conf = ChannelConfig(
                        enabled=True,
                        depth=channel_conf.depth,
                        interval=channel_conf.interval,
                        outputs=channel_conf.outputs,
                        extras=channel_conf.extras,
                    )
                    for symbols in self._symbol_chunks("mark_price"):
                        self.register_task(self._run_mark_and_funding(combined_conf, symbols))
                    self._mark_task_created = True
                continue
            if channel_name == "agg_trades_5s":
                if self._agg_trades_agg is None:
                    interval_s = _parse_interval_seconds(channel_conf.interval) or 5
                    self._agg_trades_agg = AggTradeAggregator(
                        interval_s,
                        symbols=self.config.symbols,
                        max_catchup_windows=self._agg_trades_max_catchup,
                        late_grace_s=self._agg_trades_late_grace_s,
                    )
                    self._agg_trades_queue = asyncio.Queue(maxsize=self._agg_trades_queue_max)
            chunks = self._symbol_chunks(channel_name)
            for symbols in chunks:
                self.register_task(self._run_channel(channel_name, channel_conf, symbols))
        if self._agg_trades_agg and not self._agg_trades_task_created:
            self.register_task(self._run_agg_trades_consumer())
            self.register_task(self._run_agg_trades_flush())
            self._agg_trades_task_created = True
        self.register_task(self._log_stats())

    async def _run_channel(self, channel_name: str, channel_conf: ChannelConfig, symbols: List[str]):
        stream_names = list(self._iter_streams(channel_name, channel_conf, symbols))
        if not stream_names:
            return
        url = self._stream_url(stream_names)
        while not self._stop_event.is_set():
            try:
                self._conn_counts[channel_name] += 1
                self._logger.info(
                    "connect channel=%s streams=%s",
                    channel_name,
                    len(stream_names),
                )
                async with websockets.connect(url, ping_interval=20) as ws:
                    self._logger.info("connected channel=%s", channel_name)
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        ts_recv_ns = now_ns()
                        payload = json.loads(raw)
                        data = payload.get("data", payload)
                        symbol = (data.get("s") or data.get("symbol") or "").upper()
                        if not symbol:
                            continue
                        try:
                            if channel_name == "agg_trades_5s":
                                if self._agg_trades_queue is None:
                                    continue
                                self._agg_trades_enqueued += 1
                                try:
                                    self._agg_trades_queue.put_nowait((symbol, data, ts_recv_ns))
                                except asyncio.QueueFull:
                                    self._agg_trades_dropped += 1
                                continue
                            accepted = await self._handle_message(
                                channel_name,
                                symbol,
                                data,
                                channel_conf,
                                ts_recv_ns,
                            )
                            if channel_name == "klines":
                                if accepted:
                                    self._msg_counts[channel_name] += accepted
                            else:
                                self._msg_counts[channel_name] += 1
                        except ValidationError as exc:
                            self._validation_errors[channel_name] += 1
                            self._logger.warning(
                                "validation_error channel=%s error=%s",
                                channel_name,
                                exc,
                            )
                        except Exception as exc:
                            self._parse_errors[channel_name] += 1
                            self._logger.warning(
                                "parse_error channel=%s error=%s",
                                channel_name,
                                exc,
                            )
            except Exception as exc:
                self._disc_counts[channel_name] += 1
                self._logger.warning("disconnect channel=%s error=%s", channel_name, exc)
                await asyncio.sleep(1.0)

    async def _run_mark_and_funding(self, channel_conf: ChannelConfig, symbols: List[str]):
        stream_names = list(self._iter_streams("mark_price", channel_conf, symbols))
        if not stream_names:
            return
        url = self._stream_url(stream_names)
        funding_conf = self.config.channels.get("funding", ChannelConfig(enabled=False))
        mark_conf = self.config.channels.get("mark_price", ChannelConfig(enabled=False))
        funding_enabled = funding_conf.enabled and self.has_outputs(funding_conf)
        while not self._stop_event.is_set():
            try:
                self._conn_counts["mark_price"] += 1
                if funding_enabled:
                    self._conn_counts["funding"] += 1
                self._logger.info(
                    "connect channel=mark_price streams=%s",
                    len(stream_names),
                )
                async with websockets.connect(url, ping_interval=20) as ws:
                    self._logger.info("connected channel=mark_price")
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        self._msg_counts["mark_price"] += 1
                        if funding_enabled:
                            self._msg_counts["funding"] += 1
                        ts_recv_ns = now_ns()
                        payload = json.loads(raw)
                        data = payload.get("data", payload)
                        symbol = (data.get("s") or data.get("symbol") or "").upper()
                        if not symbol:
                            continue
                        if mark_conf.enabled and self.has_outputs(mark_conf):
                            try:
                                mark_event = transforms.mark_price_from_stream(
                                    symbol,
                                    data,
                                    ts_recv_ns,
                                )
                                await self.router.publish(mark_event)
                            except ValidationError as exc:
                                self._validation_errors["mark_price"] += 1
                                self._logger.warning("validation_error channel=mark_price error=%s", exc)
                            except Exception as exc:
                                self._parse_errors["mark_price"] += 1
                                self._logger.warning("parse_error channel=mark_price error=%s", exc)
                        if funding_enabled:
                            try:
                                funding_event = transforms.funding_from_stream(
                                    symbol,
                                    data,
                                    ts_recv_ns,
                                )
                                await self.router.publish(funding_event)
                            except ValidationError as exc:
                                self._validation_errors["funding"] += 1
                                self._logger.warning("validation_error channel=funding error=%s", exc)
                            except Exception as exc:
                                self._parse_errors["funding"] += 1
                                self._logger.warning("parse_error channel=funding error=%s", exc)
            except Exception as exc:
                self._disc_counts["mark_price"] += 1
                if funding_enabled:
                    self._disc_counts["funding"] += 1
                self._logger.warning("disconnect channel=mark_price error=%s", exc)
                await asyncio.sleep(1.0)

    async def _run_agg_trades_flush(self) -> None:
        if not self._agg_trades_agg:
            return
        channel_name = "agg_trades_5s"
        while not self._stop_event.is_set():
            await asyncio.sleep(1.0)
            events = self._agg_trades_agg.flush(now_ns())
            if events:
                for event in events:
                    await self.router.publish(event)
                self._msg_counts[channel_name] += len(events)
                self._agg_trades_emitted += len(events)
            caps, skipped = self._agg_trades_agg.pop_catchup_stats()
            if caps:
                self._logger.warning(
                    "agg_trades_5s catchup capped symbols=%s skipped_windows=%s",
                    caps,
                    skipped,
                )

    async def _run_agg_trades_consumer(self) -> None:
        if not self._agg_trades_agg or self._agg_trades_queue is None:
            return
        while not self._stop_event.is_set():
            symbol, payload, ts_recv_ns = await self._agg_trades_queue.get()
            try:
                events = self._agg_trades_agg.update(symbol, payload, ts_recv_ns)
                if events:
                    for event in events:
                        await self.router.publish(event)
                    self._msg_counts["agg_trades_5s"] += len(events)
                    self._agg_trades_emitted += len(events)
                self._agg_trades_processed += 1
            finally:
                self._agg_trades_queue.task_done()

    async def _log_stats(self) -> None:
        last_msgs: Dict[str, int] = defaultdict(int)
        last_conns: Dict[str, int] = defaultdict(int)
        last_discs: Dict[str, int] = defaultdict(int)
        last_parse: Dict[str, int] = defaultdict(int)
        last_validation: Dict[str, int] = defaultdict(int)
        last_agg = {
            "enqueued": 0,
            "processed": 0,
            "dropped": 0,
            "emitted": 0,
        }
        interval_s = max(1, int(self._ws_log_interval_s))
        while not self._stop_event.is_set():
            await asyncio.sleep(interval_s)
            lines = []
            all_channels = set(self._msg_counts.keys()) | set(self._conn_counts.keys()) | set(self._disc_counts.keys())
            all_channels |= set(self._parse_errors.keys()) | set(self._validation_errors.keys())
            for channel in sorted(all_channels):
                msg_delta = self._msg_counts[channel] - last_msgs.get(channel, 0)
                conn_delta = self._conn_counts[channel] - last_conns.get(channel, 0)
                disc_delta = self._disc_counts[channel] - last_discs.get(channel, 0)
                parse_delta = self._parse_errors[channel] - last_parse.get(channel, 0)
                val_delta = self._validation_errors[channel] - last_validation.get(channel, 0)
                if msg_delta or conn_delta or disc_delta:
                    lines.append(
                        f"{channel}: msgs+{msg_delta}/{interval_s}s conns+{conn_delta} discs+{disc_delta}"
                    )
            if lines:
                self._logger.info("ws-stats %s", " | ".join(lines))

            if self._agg_trades_queue is not None:
                pending = self._agg_trades_queue.qsize()
                enq_delta = self._agg_trades_enqueued - last_agg["enqueued"]
                proc_delta = self._agg_trades_processed - last_agg["processed"]
                drop_delta = self._agg_trades_dropped - last_agg["dropped"]
                emit_delta = self._agg_trades_emitted - last_agg["emitted"]
                self._logger.info(
                    "agg-trades backlog=%s enq+%s/%ss proc+%s/%ss emit+%s/%ss drop+%s/%ss",
                    pending,
                    enq_delta,
                    interval_s,
                    proc_delta,
                    interval_s,
                    emit_delta,
                    interval_s,
                    drop_delta,
                    interval_s,
                )
                if self._agg_trades_agg:
                    late = self._agg_trades_agg.pop_late_stats()
                    if late:
                        self._logger.warning("agg-trades late_trades+%s/%ss", late, interval_s)
                last_agg["enqueued"] = self._agg_trades_enqueued
                last_agg["processed"] = self._agg_trades_processed
                last_agg["dropped"] = self._agg_trades_dropped
                last_agg["emitted"] = self._agg_trades_emitted

            err_lines = []
            for channel in sorted(all_channels):
                parse_delta = self._parse_errors[channel] - last_parse.get(channel, 0)
                val_delta = self._validation_errors[channel] - last_validation.get(channel, 0)
                if parse_delta or val_delta:
                    err_lines.append(
                        f"{channel}: parse_error+{parse_delta}/10s validation_error+{val_delta}/10s"
                    )
            if err_lines:
                self._logger.warning("ws-errors %s", " | ".join(err_lines))

            for channel in sorted(all_channels):
                last_msgs[channel] = self._msg_counts[channel]
                last_conns[channel] = self._conn_counts[channel]
                last_discs[channel] = self._disc_counts[channel]
                last_parse[channel] = self._parse_errors[channel]
                last_validation[channel] = self._validation_errors[channel]

    def stats(self) -> dict:
        return {
            "exchange": self.exchange_name,
            "ws_msgs": dict(self._msg_counts),
            "ws_conns": dict(self._conn_counts),
            "ws_discs": dict(self._disc_counts),
            "parse_errors": dict(self._parse_errors),
            "validation_errors": dict(self._validation_errors),
        }

    async def _handle_message(
        self,
        channel_name: str,
        symbol: str,
        payload: dict,
        channel_conf: ChannelConfig,
        ts_recv_ns: int,
    ) -> int:
        if channel_name == "trades":
            event = transforms.trade_from_stream(symbol, payload, ts_recv_ns)
            await self.router.publish(event)
            return 1
        elif channel_name == "l1":
            event = transforms.l1_from_stream(symbol, payload, ts_recv_ns)
            await self._handle_depth_event(symbol, event)
            return 1
        elif channel_name in {"ob_top5", "ob_top20"}:
            depth = channel_conf.depth or (5 if channel_name == "ob_top5" else 20)
            channel_enum = Channel.ob_top5 if depth == 5 else Channel.ob_top20
            event = transforms.depth_from_snapshot(
                symbol,
                payload,
                ts_recv_ns,
                depth,
                channel_enum,
            )
            await self._handle_depth_event(symbol, event)
            return 1
        elif channel_name == "ob_diff":
            event = transforms.diff_from_stream(symbol, payload, ts_recv_ns)
            await self.router.publish(event)
            return 1
        elif channel_name == "liquidations":
            event = transforms.liquidation_from_stream(symbol, payload, ts_recv_ns)
            await self.router.publish(event)
            return 1
        elif channel_name == "klines":
            event = transforms.kline_from_stream(symbol, payload, ts_recv_ns)
            if not event.is_closed:
                return 0
            await self.router.publish(event)
            return 1
        return 0

    async def _handle_depth_event(self, symbol: str, event: OrderBookDepthEvent) -> None:
        await self.router.publish(event)
        bid = event.bid_prices[0] if event.bid_prices else None
        ask = event.ask_prices[0] if event.ask_prices else None
        if bid is not None:
            self._best_bid[symbol] = bid
        if ask is not None:
            self._best_ask[symbol] = ask
        if event.depth == 5:
            self._top5_state[symbol] = {
                "bid_qtys": event.bid_qtys,
                "ask_qtys": event.ask_qtys,
            }
        if self._advanced_channel and self._advanced_channel.enabled and self.has_outputs(self._advanced_channel):
            adv_event = transforms.metrics_from_state(
                symbol,
                event.ts_event_ns,
                event.ts_recv_ns,
                self._best_bid[symbol],
                self._best_ask[symbol],
                self._top5_state.get(symbol),
            )
            if adv_event:
                await self.router.publish(adv_event)

    def _iter_streams(
        self, channel_name: str, channel_conf: ChannelConfig, symbols: List[str]
    ) -> Iterable[str]:
        entry = SUPPORTED_CHANNELS.get(channel_name)
        if not entry:
            return []
        stream_template = entry["stream"]
        interval = channel_conf.interval
        speed = ""
        if "{speed}" in stream_template:
            speed_setting = str(channel_conf.extras.get("speed", "100ms"))
            if speed_setting == "100ms":
                speed = "@100ms"
        for symbol in symbols:
            symbol_l = symbol.lower()
            if "{interval}" in stream_template:
                if not interval:
                    raise ValueError(f"Kline-Stream benötigt interval für {symbol}.")
                yield stream_template.format(symbol=symbol_l, interval=interval, speed=speed)
            else:
                yield stream_template.format(symbol=symbol_l, speed=speed)

    def _stream_url(self, streams: List[str]) -> str:
        base_url = BASE_URLS.get(self.market_type)
        if base_url is None:
            raise ValueError(f"Unbekannter market_type für Binance: {self.market_type}")
        joined = "/".join(streams)
        return f"{base_url}/stream?streams={joined}"

    def _symbol_chunks(self, channel_name: str) -> List[List[str]]:
        settings = self.config.metadata.get("symbols_per_conn", {}) if self.config.metadata else {}
        per_conn = settings.get(channel_name)
        if not per_conn:
            return [self.config.symbols]
        chunks: List[List[str]] = []
        for idx in range(0, len(self.config.symbols), int(per_conn)):
            chunks.append(self.config.symbols[idx : idx + int(per_conn)])
        return chunks

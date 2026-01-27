from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from collections import defaultdict
from decimal import Decimal
from typing import Dict, Iterable, List, Optional

import websockets
from pydantic import ValidationError

from ...config import ChannelConfig, ExchangeConfig
from ...core.events import Channel, OrderBookDepthEvent
from ...core.router import PipelineRouter
from ...utils import now_ns
from ..base import ExchangeFeed
from .capabilities import BASE_URLS, SUPPORTED_CHANNELS
from . import transforms


class BinanceFeed(ExchangeFeed):
    """Referenz-Adapter für Binance Futures/Spot."""

    def __init__(self, config: ExchangeConfig, router: PipelineRouter):
        super().__init__(config, router)
        self._logger = logging.getLogger("feeds.binance")
        self.market_type = config.market_type
        self.exchange_name = config.exchange
        self._advanced_channel = config.channels.get("advanced_metrics")
        self._best_bid: Dict[str, Optional[Decimal]] = defaultdict(lambda: None)
        self._best_ask: Dict[str, Optional[Decimal]] = defaultdict(lambda: None)
        self._top5_state: Dict[str, Dict[str, List]] = defaultdict(dict)
        self._mark_task_created = False
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
            chunks = self._symbol_chunks(channel_name)
            for symbols in chunks:
                self.register_task(self._run_channel(channel_name, channel_conf, symbols))
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
                        self._msg_counts[channel_name] += 1
                        ts_recv_ns = now_ns()
                        payload = json.loads(raw)
                        data = payload.get("data", payload)
                        symbol = (data.get("s") or data.get("symbol") or "").upper()
                        if not symbol:
                            continue
                        try:
                            await self._handle_message(channel_name, symbol, data, channel_conf, ts_recv_ns)
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
        while not self._stop_event.is_set():
            try:
                self._conn_counts["mark_price"] += 1
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
                        ts_recv_ns = now_ns()
                        payload = json.loads(raw)
                        data = payload.get("data", payload)
                        symbol = (data.get("s") or data.get("symbol") or "").upper()
                        if not symbol:
                            continue
                        if mark_conf.enabled and self.has_outputs(mark_conf):
                            try:
                                mark_event = transforms.mark_price_from_stream(
                                    self.exchange_name,
                                    self.market_type,
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
                        if funding_conf.enabled and self.has_outputs(funding_conf):
                            try:
                                funding_event = transforms.funding_from_stream(
                                    self.exchange_name,
                                    self.market_type,
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
                self._logger.warning("disconnect channel=mark_price error=%s", exc)
                await asyncio.sleep(1.0)

    async def _log_stats(self) -> None:
        last_msgs: Dict[str, int] = defaultdict(int)
        last_conns: Dict[str, int] = defaultdict(int)
        last_discs: Dict[str, int] = defaultdict(int)
        last_parse: Dict[str, int] = defaultdict(int)
        last_validation: Dict[str, int] = defaultdict(int)
        while not self._stop_event.is_set():
            await asyncio.sleep(10)
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
                        f"{channel}: msgs+{msg_delta}/10s conns+{conn_delta} discs+{disc_delta}"
                    )
            if lines:
                self._logger.info("ws-stats %s", " | ".join(lines))

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
    ) -> None:
        if channel_name == "trades":
            event = transforms.trade_from_stream(
                self.exchange_name, self.market_type, symbol, payload, ts_recv_ns
            )
            await self.router.publish(event)
        elif channel_name == "l1":
            event = transforms.l1_from_stream(
                self.exchange_name, self.market_type, symbol, payload, ts_recv_ns
            )
            await self._handle_depth_event(symbol, event)
        elif channel_name in {"ob_top5", "ob_top20"}:
            depth = channel_conf.depth or (5 if channel_name == "ob_top5" else 20)
            channel_enum = Channel.ob_top5 if depth == 5 else Channel.ob_top20
            event = transforms.depth_from_snapshot(
                self.exchange_name,
                self.market_type,
                symbol,
                payload,
                ts_recv_ns,
                depth,
                channel_enum,
            )
            await self._handle_depth_event(symbol, event)
        elif channel_name == "ob_diff":
            event = transforms.diff_from_stream(
                self.exchange_name, self.market_type, symbol, payload, ts_recv_ns
            )
            await self.router.publish(event)
        elif channel_name == "liquidations":
            event = transforms.liquidation_from_stream(
                self.exchange_name, self.market_type, symbol, payload, ts_recv_ns
            )
            await self.router.publish(event)
        elif channel_name == "klines":
            event = transforms.kline_from_stream(
                self.exchange_name, self.market_type, symbol, payload, ts_recv_ns
            )
            await self.router.publish(event)

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
                self.exchange_name,
                self.market_type,
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

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from contextlib import suppress
from typing import List

from ..config import ChannelConfig, ExchangeConfig
from ..core.router import PipelineRouter


class ExchangeFeed(ABC):
    """Basisklasse für alle Exchange-spezifischen Feeds."""

    def __init__(self, config: ExchangeConfig, router: PipelineRouter):
        self.config = config
        self.router = router
        self._tasks: List[asyncio.Task[None]] = []
        self._stop_event = asyncio.Event()
        self.redis_enabled = True
        self.clickhouse_enabled = True

    @abstractmethod
    async def start(self) -> None:
        """Startet alle konfigurierten Streams."""

    async def stop(self) -> None:
        """Stoppt laufende Tasks."""
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task

    def register_task(self, coro) -> None:
        """Speichert Tasks zur späteren Terminierung."""
        self._tasks.append(asyncio.create_task(coro))

    def channel_config(self, channel: str) -> ChannelConfig:
        return self.config.channels[channel]

    def set_global_outputs(self, *, redis_enabled: bool, clickhouse_enabled: bool) -> None:
        self.redis_enabled = redis_enabled
        self.clickhouse_enabled = clickhouse_enabled

    def has_outputs(self, channel_conf: ChannelConfig) -> bool:
        return (self.redis_enabled and channel_conf.outputs.redis) or (
            self.clickhouse_enabled and channel_conf.outputs.clickhouse
        )

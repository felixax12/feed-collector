from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from contextlib import suppress
from typing import Optional

from .events import BaseEvent


class EventWriter(ABC):
    """Gemeinsame Schnittstelle für Redis- und ClickHouse-Schreiber."""

    def __init__(self, name: str, flush_interval_ms: int):
        self.name = name
        self._flush_interval = flush_interval_ms / 1000
        self._flush_task: Optional[asyncio.Task[None]] = None
        self._events_received = 0
        self._items_received = 0
        self._items_flushed = 0

    @abstractmethod
    async def enqueue(self, event: BaseEvent) -> None:
        raise NotImplementedError

    @abstractmethod
    async def flush(self) -> None:
        raise NotImplementedError

    async def start(self) -> None:
        if self._flush_task is None and self._flush_interval > 0:
            self._flush_task = asyncio.create_task(self._auto_flush())

    async def stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._flush_task
        await self.flush()

    async def _auto_flush(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            await self.flush()

    def _record_event(self, items: int) -> None:
        self._events_received += 1
        self._items_received += items

    def _record_flush(self, items: int) -> None:
        self._items_flushed += items

    def stats(self) -> dict:
        return {
            "events": self._events_received,
            "items_in": self._items_received,
            "items_flushed": self._items_flushed,
        }

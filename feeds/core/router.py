from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from .events import BaseEvent, Channel
from .pipeline import EventWriter


class PipelineRouter:
    """Leitet normalisierte Events an die konfigurierten Writer weiter."""

    def __init__(self) -> None:
        self._bindings: Dict[Channel, List[EventWriter]] = defaultdict(list)
        self._events_by_channel: Dict[str, int] = defaultdict(int)
        self._last_event_ns: Dict[tuple[str, str], int] = {}
        self._last_recv_ns: Dict[tuple[str, str], int] = {}

    def bind(self, channel: Channel, writer: EventWriter) -> None:
        self._bindings[channel].append(writer)

    def bindings_for(self, channel: Channel) -> Iterable[EventWriter]:
        return self._bindings.get(channel, [])

    async def publish(self, event: BaseEvent) -> None:
        writers = self._bindings.get(event.channel)
        if not writers:
            return
        self._events_by_channel[event.channel.value] += 1
        key = (event.channel.value, event.instrument)
        self._last_event_ns[key] = event.ts_event_ns
        self._last_recv_ns[key] = event.ts_recv_ns
        for writer in writers:
            await writer.enqueue(event)

    async def start(self) -> None:
        for writer in self._all_writers():
            await writer.start()

    async def stop(self) -> None:
        for writer in self._all_writers():
            await writer.stop()

    def _all_writers(self) -> Iterable[EventWriter]:
        seen: set[EventWriter] = set()
        for writers in self._bindings.values():
            for writer in writers:
                if writer not in seen:
                    seen.add(writer)
                    yield writer

    def stats(self) -> dict:
        return {"events_by_channel": dict(self._events_by_channel)}

    def last_event_snapshot(self) -> dict:
        return {
            "event_ns": dict(self._last_event_ns),
            "recv_ns": dict(self._last_recv_ns),
        }

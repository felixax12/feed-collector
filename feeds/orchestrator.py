from __future__ import annotations

from typing import Optional, Set

import httpx

from . import exchanges as _exchanges

try:
    from redis.asyncio import Redis  # type: ignore
except ImportError:  # pragma: no cover
    Redis = None  # type: ignore

from .config import AppConfig
from .core.events import Channel
from .core.router import PipelineRouter
from .pipelines.clickhouse_writer import ClickHouseWriter
from .pipelines.redis_writer import RedisWriter
from .registry import registry


class FeedOrchestrator:
    """Verkabelt Konfiguration, Router, Writer und Exchange-Feeds."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.router = PipelineRouter()
        self._redis_writer: Optional[RedisWriter] = None
        self._clickhouse_writer: Optional[ClickHouseWriter] = None
        self._redis_client: Optional[Redis] = None
        self._clickhouse_client: Optional[httpx.AsyncClient] = None
        self._feeds = []
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        await self._init_writers()
        await self.router.start()
        await self._init_feeds()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        for feed in self._feeds:
            await feed.stop()
        await self.router.stop()
        if self._redis_client is not None:
            await self._redis_client.close()
        if self._clickhouse_client is not None:
            await self._clickhouse_client.aclose()
        self._started = False

    def stats(self) -> dict:
        return {
            "redis": self._redis_writer.stats() if self._redis_writer else None,
            "clickhouse": self._clickhouse_writer.stats() if self._clickhouse_writer else None,
            "router": self.router.stats(),
            "feeds": [feed.stats() for feed in self._feeds if hasattr(feed, "stats")],
        }

    async def _init_writers(self) -> None:
        defaults = self.config.defaults
        required_targets = self._collect_required_targets(defaults)
        if required_targets.get("redis") and defaults.enable_redis:
            if Redis is None:
                raise RuntimeError("redis-py nicht installiert.")
            self._redis_client = Redis.from_url(defaults.redis.dsn)
            self._redis_writer = RedisWriter(
                self._redis_client,
                pipeline_size=defaults.redis.pipeline_size,
                flush_interval_ms=defaults.redis.flush_interval_ms,
                stream_maxlen=defaults.redis.stream_maxlen,
            )
        if required_targets.get("clickhouse") and defaults.enable_clickhouse:
            self._clickhouse_client = httpx.AsyncClient(base_url=str(defaults.clickhouse.dsn))
            self._clickhouse_writer = ClickHouseWriter(
                self._clickhouse_client,
                database=defaults.clickhouse.database,
                batch_rows=defaults.clickhouse.batch_rows,
                flush_interval_ms=defaults.clickhouse.flush_interval_ms,
                compression=defaults.clickhouse.compression,
            )
        for channel_name in required_targets.get("channels_redis", []):
            channel = Channel(channel_name)
            if self._redis_writer:
                self.router.bind(channel, self._redis_writer)
        for channel_name in required_targets.get("channels_clickhouse", []):
            channel = Channel(channel_name)
            if self._clickhouse_writer:
                self.router.bind(channel, self._clickhouse_writer)

    async def _init_feeds(self) -> None:
        defaults = self.config.defaults
        for exchange_cfg in self.config.exchanges:
            feed = registry.create(exchange_cfg.exchange, exchange_cfg, self.router)
            feed.set_global_outputs(
                redis_enabled=defaults.enable_redis,
                clickhouse_enabled=defaults.enable_clickhouse,
            )
            await feed.start()
            self._feeds.append(feed)

    def _collect_required_targets(self, defaults) -> dict:
        redis_enabled = defaults.enable_redis
        clickhouse_enabled = defaults.enable_clickhouse
        redis_needed = False
        clickhouse_needed = False
        redis_channels: Set[str] = set()
        clickhouse_channels: Set[str] = set()
        for exchange_cfg in self.config.exchanges:
            for channel, chan_cfg in exchange_cfg.channels.items():
                if not chan_cfg.enabled:
                    continue
                effective = chan_cfg.effective_outputs(defaults)
                if effective.redis and redis_enabled:
                    redis_needed = True
                    redis_channels.add(channel)
                if effective.clickhouse and clickhouse_enabled:
                    clickhouse_needed = True
                    clickhouse_channels.add(channel)
        return {
            "redis": redis_needed,
            "clickhouse": clickhouse_needed,
            "channels_redis": redis_channels,
            "channels_clickhouse": clickhouse_channels,
        }

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, HttpUrl, validator


class OutputTargets(BaseModel):
    redis: bool = Field(default=True, description="Schreibt Events nach Redis.")
    clickhouse: bool = Field(default=True, description="Schreibt Events nach ClickHouse.")


class RedisSinkConfig(BaseModel):
    dsn: str = Field(..., description="redis:// URI oder Cluster-Konfiguration.")
    pipeline_size: int = Field(default=200, ge=1)
    flush_interval_ms: int = Field(default=50, ge=5)
    stream_maxlen: int = Field(default=1000, ge=10)
    cluster: bool = Field(default=False)


class ClickHouseSinkConfig(BaseModel):
    dsn: HttpUrl
    database: str = Field(default="marketdata")
    batch_rows: int = Field(default=5000, ge=1)
    flush_interval_ms: int = Field(default=250, ge=10)
    compression: Optional[str] = Field(default="lz4")


class ChannelConfig(BaseModel):
    enabled: bool = True
    depth: Optional[int] = None
    interval: Optional[str] = None
    outputs: OutputTargets = Field(default_factory=OutputTargets)
    extras: Dict[str, Any] = Field(default_factory=dict)

    @validator("interval")
    def _validate_interval(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        if value.endswith(("s", "m", "h")):
            return value
        raise ValueError("interval muss ein Suffix s, m oder h besitzen, z. B. '1m'.")

    @validator("depth")
    def _validate_depth(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return value
        if value in {1, 5, 10, 20, 50, 100}:
            return value
        raise ValueError("depth muss einem unterstützten Wert entsprechen (1,5,10,20,50,100).")

    def effective_outputs(self, defaults: "DefaultsConfig") -> OutputTargets:
        """Kombiniert Channel-Outputs mit den globalen Schaltern."""
        return OutputTargets(
            redis=self.outputs.redis and defaults.enable_redis,
            clickhouse=self.outputs.clickhouse and defaults.enable_clickhouse,
        )

    def has_any_output(self, defaults: "DefaultsConfig") -> bool:
        effective = self.effective_outputs(defaults)
        return effective.redis or effective.clickhouse


class ExchangeConfig(BaseModel):
    exchange: str
    market_type: str
    symbols: List[str]
    channels: Dict[str, ChannelConfig]
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @validator("symbols", each_item=True)
    def _upper(cls, value: str) -> str:
        return value.upper()


class DefaultsConfig(BaseModel):
    redis: RedisSinkConfig
    clickhouse: ClickHouseSinkConfig
    enable_redis: bool = Field(default=True, description="Globaler Schalter für Redis-Writer.")
    enable_clickhouse: bool = Field(default=True, description="Globaler Schalter für ClickHouse-Writer.")
    housekeep_interval_s: int = Field(default=30, ge=5)


class AppConfig(BaseModel):
    defaults: DefaultsConfig
    exchanges: List[ExchangeConfig]

    def enabled_channels(self) -> List[tuple[ExchangeConfig, str, ChannelConfig]]:
        result: List[tuple[ExchangeConfig, str, ChannelConfig]] = []
        for exchange_conf in self.exchanges:
            for channel_name, channel_conf in exchange_conf.channels.items():
                if channel_conf.enabled:
                    result.append((exchange_conf, channel_name, channel_conf))
        return result


def load_config(path: str | Path) -> AppConfig:
    """Lädt die YAML-Konfiguration und validiert sie über Pydantic."""
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Konfigdatei nicht gefunden: {file_path}")
    with file_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    return AppConfig.parse_obj(raw)

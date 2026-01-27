from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict

from .config import ExchangeConfig
from .core.router import PipelineRouter

if TYPE_CHECKING:
    from .exchanges.base import ExchangeFeed


Factory = Callable[[ExchangeConfig, PipelineRouter], "ExchangeFeed"]


class ExchangeRegistry:
    def __init__(self) -> None:
        self._factories: Dict[str, Factory] = {}

    def register(self, exchange: str, factory: Factory) -> None:
        key = exchange.lower()
        if key in self._factories:
            raise ValueError(f"Exchange bereits registriert: {exchange}")
        self._factories[key] = factory

    def create(self, exchange: str, config: ExchangeConfig, router: PipelineRouter) -> ExchangeFeed:
        key = exchange.lower()
        if key not in self._factories:
            raise KeyError(f"Keine Factory für {exchange} registriert.")
        return self._factories[key](config, router)


registry = ExchangeRegistry()

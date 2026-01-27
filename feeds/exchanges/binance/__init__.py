from .adapter import BinanceFeed
from ...registry import registry

registry.register("binance", lambda cfg, router: BinanceFeed(cfg, router))

__all__ = ["BinanceFeed"]

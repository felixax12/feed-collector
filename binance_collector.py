"""
Binance Data Collector – erweitert um:
  • REST-Snapshot-Bootstrap & Re-Sync für Orderbücher (sanft gedrosselt)
  • High-Frequency-Sampling:
      – Top-20-Orderbuch-Snapshot @100 ms (aus lokalem Buch)
      – L1/Spread/OFI @200 ms (schlank)
      – Trades RAW (WS 'trade' statt 'aggTrade')
  • ClickHouse-Writer (HTTP) für alle Daten inkl. 1.5s Aggregationen
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from collections import deque
from dataclasses import dataclass, field
from statistics import median
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any, TYPE_CHECKING
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import aiohttp
import websockets
import os
import random
from contextlib import suppress

try:
    import clickhouse_connect
except ImportError:
    clickhouse_connect = None

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client as ClickHouseClient
else:
    ClickHouseClient = Any

# Änderungen (ohne Begründung):
# - OHLC_3s aus Trades; falls keine Trades im Fenster → aus Midquotes (L1-Mid)
# - Flow-Features: OFI, Microprice-Drift, L1-Jump-Rate, Replenishment-Rate
# - Buchform: Slope/Curvature (Proxy) für Bid/Ask (Levels 1–20)
# - Trades: CVD (kumulativ), ITT-Median, Burst-Score, Sweep-Flag
# - Volatilität: RV_3s (Mid-Returns), RV_EWMA (1/5/15min), Parkinson_1m (aus 3s-High/Low)
# - 3s-Log & Zero-Row-Diagnose bleiben
# - Open Interest: REST-Poller (30s Ziel), zeitrichtig in 3s-Fenster schreiben
# - Long/Short Ratio NUR: topLongShortPositionRatio (Periode 5m),
#   nur im passenden 3s-Fenster setzen, sonst NULL lassen; Rate-Limit sicher

# =============================================================================
# NEU: HF-Sampling- und Snapshot-Konfiguration
# =============================================================================
TOP20_SNAPSHOT_MS = int(os.getenv("TOP20_SNAPSHOT_MS", "100"))     # alle 100 ms Top-20-Snapshot
L1_SAMPLE_MS      = int(os.getenv("L1_SAMPLE_MS", "200"))          # alle 200 ms L1/Spread/OFI

# REST Snapshot (Orderbuch) – konservativ gedrosselt
REST_DEPTH_LIMIT   = int(os.getenv("REST_DEPTH_LIMIT", "200"))     # 200 reicht für Top-20 sicher
REST_COOLDOWN_SEC  = int(os.getenv("REST_COOLDOWN_SEC", "30"))     # min. 30s zwischen Snapshots pro Symbol
REST_RETRY_MAX     = int(os.getenv("REST_RETRY_MAX", "3"))
BINANCE_FAPI_DEPTH = "https://fapi.binance.com/fapi/v1/depth"

# Optional: ClickHouse-Writer (HTTP). Aktiv nur, wenn URL gesetzt ist.
CLICKHOUSE_URL          = os.getenv("CLICKHOUSE_URL", "").strip()      # z. B. "http://localhost:8123"
CLICKHOUSE_DB           = os.getenv("CLICKHOUSE_DB", "binance_db").strip() or "binance_db"
CLICKHOUSE_USER         = os.getenv("CLICKHOUSE_USER", "felix").strip()
CLICKHOUSE_PASSWORD     = os.getenv("CLICKHOUSE_PASSWORD", "testpass").strip()
CLICKHOUSE_HOST         = os.getenv("CLICKHOUSE_HOST", "").strip()
CLICKHOUSE_PORT         = os.getenv("CLICKHOUSE_PORT", "").strip()
CLICKHOUSE_SECURE       = os.getenv("CLICKHOUSE_SECURE", "").strip().lower()
CLICKHOUSE_VERIFY       = os.getenv("CLICKHOUSE_VERIFY", "").strip().lower()
CLICKHOUSE_ENABLE       = os.getenv("CLICKHOUSE_ENABLE", "").strip().lower()
CLICKHOUSE_BOOTSTRAP_DB = os.getenv("CLICKHOUSE_BOOTSTRAP_DB", "default").strip() or "default"

TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


# ============================================================================
# ORDERBOOK
# ============================================================================

class LocalOrderbook:
    """Lokales Orderbuch für Top-20 Levels (Abschnitt 3)"""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_u: Optional[int] = None
        self.initialized: bool = False
        self.updates_this_window = 0
        # Ziel: Top-20 stabil
        self.bootstrap_min_levels = 20
        # REST-Snapshot-Guards (Limits schonend)
        self._last_rest_snapshot_s: int = 0
        self._rest_inflight: bool = False

    async def rest_snapshot(self, session: aiohttp.ClientSession, limit: int = REST_DEPTH_LIMIT) -> bool:
        """
        Hole REST-Snapshot (Tiefe = limit) und setze lokales Buch darauf.
        Sanfte Drosselung:
          - Mindestens REST_COOLDOWN_SEC Abstand zwischen zwei Snapshots pro Symbol
          - Jitter + Backoff bei Fehlversuchen
        Gibt True zurück, wenn Buch initialisiert wurde.
        """
        if self._rest_inflight:
            return False
        now_s = int(time.time())
        if now_s - self._last_rest_snapshot_s < REST_COOLDOWN_SEC:
            return False
        self._rest_inflight = True
        try:
            params = {"symbol": self.symbol, "limit": str(limit)}
            delay = 0.2 + random.random() * 0.3  # Jitter, um Shards zu entkoppeln
            await asyncio.sleep(delay)
            for attempt in range(1, REST_RETRY_MAX + 1):
                try:
                    async with session.get(BINANCE_FAPI_DEPTH, params=params) as r:
                        if r.status == 429:
                            # IP-Limit respektieren: kurzen Backoff
                            await asyncio.sleep(min(2 ** attempt, 3))
                            continue
                        r.raise_for_status()
                        data = await r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    self.bids = {float(p): float(q) for p, q in bids if float(q) > 0}
                    self.asks = {float(p): float(q) for p, q in asks if float(q) > 0}
                    self.last_u = int(data.get("lastUpdateId", 0))
                    self.initialized = bool(self.bids and self.asks)
                    self._last_rest_snapshot_s = int(time.time())
                    return self.initialized
                except Exception:
                    await asyncio.sleep(min(2 ** attempt, 3))
            return False
        finally:
            self._rest_inflight = False

    def apply_diff(self, evt: dict) -> bool:
        """Diff-Event anwenden. Returns False bei Gap (nur nach Init)."""
        U = evt.get('U')
        u = evt.get('u')
        bids_raw = evt.get('b', [])
        asks_raw = evt.get('a', [])

        # Sequenz-Check erst NACH Initialisierung
        if self.initialized and self.last_u is not None:
            if U > self.last_u + 1:
                # Gap -> Orderbuch verwerfen, neu bootstrappen
                self.bids.clear()
                self.asks.clear()
                self.initialized = False
                self.last_u = None
                # diff trotzdem anwenden als Teil des neuen Bootstraps
            elif u <= self.last_u:
                # veraltetes Event
                return True

        # Bids anwenden
        for price_str, qty_str in bids_raw:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        # Asks anwenden
        for price_str, qty_str in asks_raw:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_u = u
        self.updates_this_window += 1

        # Bootstrap-Logik: wenn genug Levels vorhanden, als initialisiert markieren
        if not self.initialized:
            if len(self.bids) >= self.bootstrap_min_levels and len(self.asks) >= self.bootstrap_min_levels:
                self.initialized = True

        return True

    # Snapshot-Initialisierung entfällt (nur WS)

    def get_top20(self) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Top-20 Levels zurückgeben"""
        if not self.initialized:
            return [], []
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:20]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:20]
        return sorted_bids, sorted_asks

    def get_l1(self) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """L1 ableiten"""
        if not self.initialized:
            return None, None, None, None
        bid_price = max(self.bids.keys()) if self.bids else None
        ask_price = min(self.asks.keys()) if self.asks else None
        bid_qty = self.bids.get(bid_price) if bid_price else None
        ask_qty = self.asks.get(ask_price) if ask_price else None
        return bid_price, bid_qty, ask_price, ask_qty

    def is_crossed(self) -> bool:
        """Check ob Buch gekreuzt ist"""
        if not self.initialized or not self.bids or not self.asks:
            return False
        return max(self.bids.keys()) >= min(self.asks.keys())

    def reset_window_counters(self):
        """Reset Update-Counter für neues 3s-Fenster"""
        self.updates_this_window = 0
        # kein Reset der Buchdaten hier; nur Zähler


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class TradeAgg:
    """Trade-Aggregat für 3s-Fenster"""
    count: int = 0
    vol_base: float = 0.0
    vol_quote: float = 0.0
    taker_buy_vol: float = 0.0
    taker_sell_vol: float = 0.0
    first_trade_id: int = 0
    last_trade_id: int = 0
    buy_count: int = 0
    sell_count: int = 0
    trade_sizes: List[float] = field(default_factory=list)
    qtys: List[float] = field(default_factory=list)
    sides: List[str] = field(default_factory=list)


@dataclass
class LiqAgg:
    """Liquidation-Aggregat für 3s-Fenster"""
    count: int = 0
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    notional: float = 0.0
    sizes: List[float] = field(default_factory=list)


@dataclass
class MarkSnap:
    """Mark Price Snapshot"""
    mark_price: float = 0.0
    index_price: float = 0.0
    est_settle_price: float = 0.0
    funding_rate: float = 0.0
    next_funding_ms: int = 0
    last_update_ts: int = 0


@dataclass
class BookTickerL1:
    """BookTicker L1 Fallback"""
    bid: float = 0.0
    bid_qty: float = 0.0
    ask: float = 0.0
    ask_qty: float = 0.0
    timestamp_ms: int = 0

# =============================================================================
# NEU: ClickHouse-Writer (optional)
# =============================================================================
class ClickHouseWriter:
    """
    ClickHouse-Writer via clickhouse_connect.
    Aktiv, sobald ein Host/URL konfiguriert wurde oder CLICKHOUSE_ENABLE wahr ist.
    Erwartete Tabellen:
      - binance_trades_raw(ts,symbol,price,qty,is_buyer_maker,trade_id)
      - binance_ob_diffs_raw(ts,symbol,U,u,bids_prices,bids_qtys,asks_prices,asks_qtys)
      - binance_ob_top20_100ms(ts,symbol,best_bid,best_bid_qty,best_ask,best_ask_qty,bid_price,bid_qty,ask_price,ask_qty)
      - binance_l1_200ms(ts,symbol,best_bid,best_ask,bid_qty,ask_qty,spread,mid,microprice,ofi_200ms,trade_count,vol_base,vol_quote)
      - binance_liquidations(ts,symbol,side,price,qty,order_type,time_in_force)
      - binance_mark_price(ts,symbol,mark_price,index_price,est_settle_price,funding_rate,next_funding_time)
      - binance_open_interest(ts,symbol,open_interest,open_interest_value)
      - binance_long_short_ratio(ts,symbol,long_short_ratio,long_account,short_account)
    """

    TABLE_COLUMNS: Dict[str, Tuple[str, ...]] = {
        "binance_trades_raw": ("ts", "symbol", "price", "qty", "is_buyer_maker", "trade_id"),
        "binance_ob_diffs_raw": ("ts", "symbol", "U", "u", "bids_prices", "bids_qtys", "asks_prices", "asks_qtys"),
        "binance_ob_top20_100ms": (
            "ts",
            "symbol",
            "best_bid",
            "best_bid_qty",
            "best_ask",
            "best_ask_qty",
            "bid_price",
            "bid_qty",
            "ask_price",
            "ask_qty",
        ),
        "binance_l1_200ms": (
            "ts",
            "symbol",
            "best_bid",
            "best_ask",
            "bid_qty",
            "ask_qty",
            "spread",
            "mid",
            "microprice",
            "ofi_200ms",
            "trade_count",
            "vol_base",
            "vol_quote",
        ),
        "binance_liquidations": (
            "ts",
            "symbol",
            "side",
            "price",
            "qty",
            "order_type",
            "time_in_force",
        ),
        "binance_mark_price": (
            "ts",
            "symbol",
            "mark_price",
            "index_price",
            "est_settle_price",
            "funding_rate",
            "next_funding_ms",
        ),
        "binance_open_interest": (
            "ts",
            "symbol",
            "open_interest",
            "open_interest_value",
        ),
        "binance_long_short_ratio": (
            "ts",
            "symbol",
            "long_short_ratio",
            "long_account",
            "short_account",
        ),
        "binance_advanced_metrics_1s5": (
            "ts", "symbol", "window_start_ms", "window_end_ms",
            "best_bid", "best_ask", "mid",
            "effective_spread_bps", "realized_spread_5s_bps",
            "kyle_lambda", "amihud_illiq", "vpin",
            "microprice_edge_bps", "qdt_bid_s", "qdt_ask_s",
            "ws_latency_p50_ms", "ws_latency_p95_ms",
            "gap_rate_depth", "dup_rate_depth",
            "ob_entropy_bid", "ob_entropy_ask",
            "variance_ratio",
            "index_basis_bps", "basis_drift_bps", "funding_adj_basis_bps",
        ),
    }

    TABLE_DDL: Dict[str, str] = {
        "binance_trades_raw": """
            CREATE TABLE IF NOT EXISTS {db}.binance_trades_raw (
                ts DateTime64(3),
                symbol String,
                price Float64,
                qty Float64,
                is_buyer_maker UInt8,
                trade_id UInt64
            )
            ENGINE = MergeTree
            ORDER BY (symbol, ts, trade_id)
        """,
        "binance_ob_diffs_raw": """
            CREATE TABLE IF NOT EXISTS {db}.binance_ob_diffs_raw (
                ts DateTime64(3),
                symbol String,
                U UInt64,
                u UInt64,
                bids_prices String,
                bids_qtys String,
                asks_prices String,
                asks_qtys String
            )
            ENGINE = MergeTree
            ORDER BY (symbol, ts, U, u)
        """,
        "binance_ob_top20_100ms": """
            CREATE TABLE IF NOT EXISTS {db}.binance_ob_top20_100ms (
                ts DateTime64(3),
                symbol String,
                best_bid Float64,
                best_bid_qty Float64,
                best_ask Float64,
                best_ask_qty Float64,
                bid_price String,
                bid_qty String,
                ask_price String,
                ask_qty String
            )
            ENGINE = MergeTree
            ORDER BY (symbol, ts)
        """,
        "binance_l1_200ms": """
            CREATE TABLE IF NOT EXISTS {db}.binance_l1_200ms (
                ts DateTime64(3),
                symbol String,
                best_bid Float64,
                best_ask Float64,
                bid_qty Float64,
                ask_qty Float64,
                spread Float64,
                mid Float64,
                microprice Float64,
                ofi_200ms Float64,
                trade_count UInt32,
                vol_base Float64,
                vol_quote Float64
            )
            ENGINE = MergeTree
            ORDER BY (symbol, ts)
        """,
        "binance_liquidations": """
            CREATE TABLE IF NOT EXISTS {db}.binance_liquidations (
                ts DateTime64(3),
                symbol String,
                side String,
                price Float64,
                qty Float64,
                order_type String,
                time_in_force String
            )
            ENGINE = MergeTree
            ORDER BY (symbol, ts)
        """,
        "binance_mark_price": """
            CREATE TABLE IF NOT EXISTS {db}.binance_mark_price (
                ts DateTime64(3),
                symbol String,
                mark_price Float64,
                index_price Float64,
                est_settle_price Float64,
                funding_rate Float64,
                next_funding_ms UInt64
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (symbol, ts)
        """,
        "binance_open_interest": """
            CREATE TABLE IF NOT EXISTS {db}.binance_open_interest (
                ts DateTime64(3),
                symbol String,
                open_interest Float64,
                open_interest_value Float64
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (symbol, ts)
        """,
        "binance_long_short_ratio": """
            CREATE TABLE IF NOT EXISTS {db}.binance_long_short_ratio (
                ts DateTime64(3),
                symbol String,
                long_short_ratio Float64,
                long_account Float64,
                short_account Float64
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (symbol, ts)
        """,
        "binance_advanced_metrics_1s5": """
            CREATE TABLE IF NOT EXISTS {db}.binance_advanced_metrics_1s5 (
                ts DateTime64(3),
                symbol String,
                window_start_ms UInt64,
                window_end_ms UInt64,
                -- Context (minimal)
                best_bid Float64,
                best_ask Float64,
                mid Float64,
                -- Advanced Microstructure Metrics (10 groups)
                effective_spread_bps Float64,
                realized_spread_5s_bps Float64,
                kyle_lambda Float64,
                amihud_illiq Float64,
                vpin Float64,
                microprice_edge_bps Float64,
                qdt_bid_s Float64,
                qdt_ask_s Float64,
                ws_latency_p50_ms Float64,
                ws_latency_p95_ms Float64,
                gap_rate_depth Float64,
                dup_rate_depth Float64,
                ob_entropy_bid Float64,
                ob_entropy_ask Float64,
                variance_ratio Float64,
                index_basis_bps Float64,
                basis_drift_bps Float64,
                funding_adj_basis_bps Float64
            )
            ENGINE = MergeTree
            ORDER BY (symbol, ts)
        """,
    }

    TS_INDEX: Dict[str, int] = {
        "binance_trades_raw": 0,
        "binance_ob_diffs_raw": 0,
        "binance_ob_top20_100ms": 0,
        "binance_l1_200ms": 0,
        "binance_liquidations": 0,
        "binance_mark_price": 0,
        "binance_open_interest": 0,
        "binance_long_short_ratio": 0,
        "binance_advanced_metrics_1s5": 0,  # ts is first field
    }

    def __init__(self):
        self.logger = logging.getLogger("ClickHouseWriter")
        self._disable_reason: Optional[str] = None
        self._config = self._resolve_config()
        self.enabled = self._should_enable()
        self.client: Optional[ClickHouseClient] = None
        self._q_trades: List[tuple] = []
        self._q_diffs: List[tuple] = []
        self._q_top20: List[tuple] = []
        self._q_l1: List[tuple] = []
        self._q_liquidations: List[tuple] = []
        self._q_mark_price: List[tuple] = []
        self._q_open_interest: List[tuple] = []
        self._q_long_short_ratio: List[tuple] = []
        self._q_agg: List[tuple] = []  # 1.5s aggregations
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_interval = 0.25  # alle 250 ms flush
        self._lock = asyncio.Lock()

    def _should_enable(self) -> bool:
        flag = CLICKHOUSE_ENABLE
        if flag:
            if flag in TRUE_VALUES:
                return True
            if flag in FALSE_VALUES:
                self._disable_reason = "CLICKHOUSE_ENABLE=false"
                return False
            self.logger.warning(
                "Unbekannter CLICKHOUSE_ENABLE-Wert '%s' – aktiviere trotzdem.", flag
            )
            return True
        return True

    def _resolve_config(self) -> Dict[str, Any]:
        secure = CLICKHOUSE_SECURE in TRUE_VALUES
        verify = False if CLICKHOUSE_VERIFY in FALSE_VALUES else True
        host = CLICKHOUSE_HOST or ""
        port_raw = CLICKHOUSE_PORT

        if CLICKHOUSE_URL:
            parsed = urlparse(CLICKHOUSE_URL if "://" in CLICKHOUSE_URL else f"http://{CLICKHOUSE_URL}")
            if parsed.hostname:
                host = parsed.hostname
            if parsed.port:
                port_raw = str(parsed.port)
            if parsed.scheme:
                secure = parsed.scheme.lower() == "https"

        if not host:
            host = "localhost"

        default_port = 9440 if secure else 8123
        try:
            port = int(port_raw) if port_raw else default_port
        except ValueError:
            port = default_port

        return {
            "host": host,
            "port": port,
            "secure": secure,
            "verify": verify,
            "username": CLICKHOUSE_USER or "default",
            "password": CLICKHOUSE_PASSWORD or "",
            "database": CLICKHOUSE_DB,
            "bootstrap_db": CLICKHOUSE_BOOTSTRAP_DB or "default",
        }

    async def start(self):
        self.logger.info("=== ClickHouse Writer Start ===")
        self.logger.info(f"Enabled: {self.enabled}")
        self.logger.info(f"Config: {self._config}")

        if not self.enabled:
            reason = self._disable_reason or "deaktiviert"
            self.logger.warning(f"ClickHouseWriter {reason}")
            return
        if clickhouse_connect is None:
            self.logger.error("clickhouse_connect ist nicht installiert. ClickHouseWriter wird deaktiviert.")
            self.enabled = False
            return
        try:
            self.logger.info("Initializing ClickHouse client and creating tables...")
            self.client = await asyncio.to_thread(self._initialize_client)
            self._flush_task = asyncio.create_task(self._autoflush())
            self.logger.info(
                "✅ ClickHouseWriter ACTIVE (host=%s, port=%s, db=%s)",
                self._config["host"],
                self._config["port"],
                self._config["database"],
            )
            self.logger.info("✅ All tables created successfully!")
        except Exception as exc:
            self.logger.error(f"❌ ClickHouse initialisation failed: {exc}", exc_info=True)
            self.enabled = False

    def _initialize_client(self) -> ClickHouseClient:
        cfg = self._config
        kwargs = {
            "host": cfg["host"],
            "port": cfg["port"],
            "username": cfg["username"],
            "password": cfg["password"],
            "secure": cfg["secure"],
            "verify": cfg["verify"],
        }

        self.logger.info(f"Connecting to ClickHouse: {cfg['host']}:{cfg['port']}")
        bootstrap_client = clickhouse_connect.get_client(database=cfg["bootstrap_db"], **kwargs)
        self.logger.info(f"Creating database: {cfg['database']}")
        bootstrap_client.command(f"CREATE DATABASE IF NOT EXISTS {cfg['database']}")
        bootstrap_client.close()

        self.logger.info(f"Connecting to database: {cfg['database']}")
        client = clickhouse_connect.get_client(database=cfg["database"], **kwargs)

        self.logger.info("Creating tables...")
        table_count = 0
        for table_name, ddl in self.TABLE_DDL.items():
            formatted_ddl = ddl.format(db=cfg["database"])
            self.logger.info(f"  - Creating table: {table_name}")
            client.command(formatted_ddl)
            table_count += 1

        self.logger.info(f"✅ Created {table_count} tables successfully!")
        return client

    def _iter_ddl(self, database: str):
        for ddl in self.TABLE_DDL.values():
            yield ddl.format(db=database)

    async def stop(self):
        if self._flush_task:
            self._flush_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._flush_task
        if self.enabled:
            try:
                await self.flush_all()
            except Exception as exc:
                self.logger.debug(f"ClickHouse flush on stop failed: {exc}")
        if self.client is not None:
            try:
                await asyncio.to_thread(self.client.close)
            except Exception as exc:
                self.logger.debug(f"ClickHouse client close failed: {exc}")
            self.client = None

    async def _autoflush(self):
        while self.enabled:
            try:
                await asyncio.sleep(self._flush_interval)
                await self.flush_all()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.logger.debug(f"CH autoflush error: {exc}")

    def add_trade(self, ts_ms: int, symbol: str, price: float, qty: float, is_buyer_maker: int, trade_id: int):
        self._q_trades.append((ts_ms / 1000.0, symbol, price, qty, is_buyer_maker, trade_id))
        if len(self._q_trades) >= 2000:
            asyncio.create_task(self._flush_trades())

    def add_ob_diff(
        self,
        ts_ms: int,
        symbol: str,
        U: int,
        u: int,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
    ):
        bp = [b for b, _ in bids]
        bq = [q for _, q in bids]
        ap = [p for p, _ in asks]
        aq = [q for _, q in asks]
        self._q_diffs.append(
            (
                ts_ms / 1000.0,
                symbol,
                U,
                u,
                json.dumps(bp),
                json.dumps(bq),
                json.dumps(ap),
                json.dumps(aq),
            )
        )
        if len(self._q_diffs) >= 1000:
            asyncio.create_task(self._flush_diffs())

    def add_top20_snapshot(
        self,
        ts_ms: int,
        symbol: str,
        best_bid: float,
        best_bid_qty: float,
        best_ask: float,
        best_ask_qty: float,
        bid_levels: List[Tuple[float, float]],
        ask_levels: List[Tuple[float, float]],
    ):
        bid_price = [b for b, _ in bid_levels]
        bid_qty = [q for _, q in bid_levels]
        ask_price = [p for p, _ in ask_levels]
        ask_qty = [q for _, q in ask_levels]
        self._q_top20.append(
            (
                ts_ms / 1000.0,
                symbol,
                best_bid,
                best_bid_qty,
                best_ask,
                best_ask_qty,
                json.dumps(bid_price),
                json.dumps(bid_qty),
                json.dumps(ask_price),
                json.dumps(ask_qty),
            )
        )
        if len(self._q_top20) >= 1000:
            asyncio.create_task(self._flush_top20())

    def add_l1_200ms(
        self,
        ts_ms: int,
        symbol: str,
        bb: float,
        ba: float,
        bq: float,
        aq: float,
        spread: float,
        mid: float,
        microprice: float,
        ofi_200ms: float,
        trade_count: int,
        vol_base: float,
        vol_quote: float,
    ):
        self._q_l1.append(
            (
                ts_ms / 1000.0,
                symbol,
                bb,
                ba,
                bq,
                aq,
                spread,
                mid,
                microprice,
                ofi_200ms,
                trade_count,
                vol_base,
                vol_quote,
            )
        )
        if len(self._q_l1) >= 2000:
            asyncio.create_task(self._flush_l1())

    def add_liquidation(
        self,
        ts_ms: int,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        order_type: str,
        time_in_force: str,
    ):
        self._q_liquidations.append(
            (
                ts_ms / 1000.0,
                symbol,
                side,
                price,
                qty,
                order_type,
                time_in_force,
            )
        )
        if len(self._q_liquidations) >= 500:
            asyncio.create_task(self._flush_liquidations())

    def add_mark_price(
        self,
        ts_ms: int,
        symbol: str,
        mark_price: float,
        index_price: float,
        est_settle_price: float,
        funding_rate: float,
        next_funding_ms: int,
    ):
        self._q_mark_price.append(
            (
                ts_ms / 1000.0,
                symbol,
                mark_price,
                index_price,
                est_settle_price,
                funding_rate,
                next_funding_ms,  # UInt64 in Millisekunden
            )
        )
        if len(self._q_mark_price) >= 1000:
            asyncio.create_task(self._flush_mark_price())

    def add_open_interest(
        self,
        ts_ms: int,
        symbol: str,
        open_interest: float,
        open_interest_value: float,
    ):
        self._q_open_interest.append(
            (
                ts_ms / 1000.0,
                symbol,
                open_interest,
                open_interest_value,
            )
        )
        if len(self._q_open_interest) >= 500:
            asyncio.create_task(self._flush_open_interest())

    def add_long_short_ratio(
        self,
        ts_ms: int,
        symbol: str,
        long_short_ratio: float,
        long_account: float,
        short_account: float,
    ):
        self._q_long_short_ratio.append(
            (
                ts_ms / 1000.0,
                symbol,
                long_short_ratio,
                long_account,
                short_account,
            )
        )
        if len(self._q_long_short_ratio) >= 500:
            asyncio.create_task(self._flush_long_short_ratio())

    async def _insert_values(self, table: str, rows: List[tuple]):
        if not self.enabled or not rows:
            return
        if self.client is None:
            return
        columns = self.TABLE_COLUMNS[table]
        prepared_rows = self._prepare_rows(table, rows)
        try:
            async with self._lock:
                await asyncio.to_thread(
                    self.client.insert,
                    table,
                    prepared_rows,
                    column_names=list(columns),
                )
        except Exception as exc:
            self.logger.error(f"CH insert {table} failed: {exc}")

    def _prepare_rows(self, table: str, rows: List[tuple]) -> List[tuple]:
        ts_idx = self.TS_INDEX.get(table)
        if ts_idx is None:
            return rows
        prepared: List[tuple] = []
        for row in rows:
            row_list = list(row)
            row_list[ts_idx] = self._ts_to_datetime(row_list[ts_idx])
            prepared.append(tuple(row_list))
        return prepared

    @staticmethod
    def _ts_to_datetime(value: Any) -> datetime:
        if value is None:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            except ValueError:
                try:
                    parsed = datetime.fromisoformat(value)
                    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                except ValueError:
                    return datetime.fromtimestamp(0, tz=timezone.utc)
        return datetime.fromtimestamp(0, tz=timezone.utc)

    async def flush_all(self):
        await asyncio.gather(
            self._flush_trades(),
            self._flush_diffs(),
            self._flush_top20(),
            self._flush_l1(),
            self._flush_liquidations(),
            self._flush_mark_price(),
            self._flush_open_interest(),
            self._flush_long_short_ratio(),
            self._flush_agg(),
        )

    async def _flush_trades(self):
        rows, self._q_trades = self._q_trades, []
        await self._insert_values("binance_trades_raw", rows)

    async def _flush_diffs(self):
        rows, self._q_diffs = self._q_diffs, []
        await self._insert_values("binance_ob_diffs_raw", rows)

    async def _flush_top20(self):
        rows, self._q_top20 = self._q_top20, []
        await self._insert_values("binance_ob_top20_100ms", rows)

    async def _flush_l1(self):
        rows, self._q_l1 = self._q_l1, []
        await self._insert_values("binance_l1_200ms", rows)

    async def _flush_liquidations(self):
        rows, self._q_liquidations = self._q_liquidations, []
        await self._insert_values("binance_liquidations", rows)

    async def _flush_mark_price(self):
        rows, self._q_mark_price = self._q_mark_price, []
        await self._insert_values("binance_mark_price", rows)

    async def _flush_open_interest(self):
        rows, self._q_open_interest = self._q_open_interest, []
        await self._insert_values("binance_open_interest", rows)

    async def _flush_long_short_ratio(self):
        rows, self._q_long_short_ratio = self._q_long_short_ratio, []
        await self._insert_values("binance_long_short_ratio", rows)

    async def _flush_agg(self):
        rows, self._q_agg = self._q_agg, []
        await self._insert_values("binance_advanced_metrics_1s5", rows)

class SymbolState:
    """State für ein Symbol"""
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.ob = LocalOrderbook(symbol)
        self.bt_l1: Optional[BookTickerL1] = None
        self.trades = TradeAgg()
        self.liq = LiqAgg()
        self.mark = MarkSnap()
        # Preisreihen innerhalb 3s
        self.mid_prices: List[float] = []        # Midquotes (aus Depth)
        self.trade_prices: List[float] = []      # echte Trade-Preise
        self.trade_ts: List[int] = []            # Timestamps der Trades (ms)
        # HF-Sampling
        self._last_l1_sample_ms: int = 0
        self.last_close: Optional[float] = None
        # Flow/L1 State innerhalb Fenster
        self.prev_bid_p: Optional[float] = None
        self.prev_ask_p: Optional[float] = None
        self.prev_bid_q: Optional[float] = None
        self.prev_ask_q: Optional[float] = None
        self.ofi_sum: float = 0.0
        self.microprice_first: Optional[float] = None
        self.microprice_last: Optional[float] = None
        self.l1_jumps: int = 0
        self.replenish_events: int = 0
        # Rolling/EWMA über Fenster hinaus
        self.cvd_cum: float = 0.0
        self.rv_ewma_1m: float = 0.0
        self.rv_ewma_5m: float = 0.0
        self.rv_ewma_15m: float = 0.0
        self.rel_spread_mean: float = 0.0
        self.rel_spread_var: float = 0.0  # für z-Score
        self.trade_rate_ewma: float = 0.0
        # Parkinson (1m) Aggregator aus 3s-High/Low
        self.min_bucket_start: int = 0
        self.min_high: float = 0.0
        self.min_low: float = 0.0
        self.parkinson_last: float = 0.0
        # REST-Daten (zeitrichtig ins Fenster zu mappen)
        self.oi_last_ts: int = 0
        self.oi_open_interest: Optional[float] = None
        self.oi_value_usdt: Optional[float] = None
        self.ls_last_period_ms: int = 0
        self.ls_ratio_global: Optional[float] = None  # nicht mehr benutzt (bleibt immer None)
        self.ls_ratio_top_acc: Optional[float] = None # nicht mehr benutzt (bleibt immer None)
        self.ls_ratio_top_pos: Optional[float] = None
        # Advanced microstructure state
        self.last_index_basis_bps: float = 0.0
        self.flags = {
            'has_l1': 0, 'has_trades': 0, 'has_depth': 0,
            'has_liq': 0, 'has_mark': 0, 'crossed_book': 0,
            'resynced_this_window': 0
        }


def floor_to_grid(ts_ms: int, grid_ms: int) -> int:
    """Floor timestamp to grid"""
    return (ts_ms // grid_ms) * grid_ms


# ============================================================================
# GLOBAL STREAMS
# ============================================================================

class GlobalStreams:
    """Globale Streams: !markPrice@arr, !forceOrder@arr, !bookTicker"""

    def __init__(self, ch_writer: Optional[ClickHouseWriter] = None):
        self.logger = logging.getLogger("GlobalStreams")
        self.is_running = True
        self.mark_data: Dict[str, dict] = {}
        self.liq_events: Dict[str, list] = {}
        self.bt_data: Dict[str, dict] = {}
        self.ch_writer = ch_writer

    async def run(self):
        """Start alle globale Streams"""
        tasks = [
            asyncio.create_task(self.mark_price_stream()),
            asyncio.create_task(self.force_order_stream()),
            asyncio.create_task(self.all_book_ticker_stream()),
        ]
        await asyncio.gather(*tasks)

    async def mark_price_stream(self):
        """!markPrice@arr"""
        uri = "wss://fstream.binance.com/ws/!markPrice@arr"
        while self.is_running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10, open_timeout=15) as ws:
                    self.logger.info("Connected to !markPrice@arr")
                    async for msg in ws:
                        if not self.is_running:
                            break
                        try:
                            data = json.loads(msg)
                            if isinstance(data, list):
                                for item in data:
                                    self._handle_mark_price(item)
                            elif isinstance(data, dict):
                                self._handle_mark_price(data)
                        except Exception as e:
                            self.logger.debug(f"Mark parse error: {e}")
            except Exception as e:
                self.logger.error(f"Mark connection error: {e}")
                await asyncio.sleep(3)

    async def force_order_stream(self):
        """!forceOrder@arr"""
        uri = "wss://fstream.binance.com/ws/!forceOrder@arr"
        while self.is_running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10, open_timeout=15) as ws:
                    self.logger.info("Connected to !forceOrder@arr")
                    async for msg in ws:
                        if not self.is_running:
                            break
                        try:
                            payload = json.loads(msg)
                            data = payload.get('data', {})
                            if 'o' in data:
                                self._handle_force_order(data)
                        except Exception as e:
                            self.logger.debug(f"Force parse error: {e}")
            except Exception as e:
                self.logger.error(f"Force connection error: {e}")
                await asyncio.sleep(3)

    def _handle_mark_price(self, item: dict):
        """Handle Mark Price Event"""
        try:
            symbol = item.get('s')
            if not symbol:
                return
            now_ms = int(time.time() * 1000)
            mark_price = float(item.get('p', 0))
            index_price = float(item.get('i', 0))
            est_settle_price = float(item.get('P', 0))
            funding_rate = float(item.get('r', 0))
            next_funding_ms = int(item.get('T', 0))

            self.mark_data[symbol] = {
                'mark_price': mark_price,
                'index_price': index_price,
                'est_settle_price': est_settle_price,
                'funding_rate': funding_rate,
                'next_funding_ms': next_funding_ms,
                'last_update_ts': now_ms
            }

            # ClickHouse Writer
            if self.ch_writer and self.ch_writer.enabled:
                self.ch_writer.add_mark_price(
                    ts_ms=now_ms,
                    symbol=symbol,
                    mark_price=mark_price,
                    index_price=index_price,
                    est_settle_price=est_settle_price,
                    funding_rate=funding_rate,
                    next_funding_ms=next_funding_ms,
                )
        except:
            pass

    # ===================== All Book Tickers =====================
    def _handle_book_ticker(self, item: dict):
        """Handle All-BookTicker Event (L1-Fallback)"""
        try:
            symbol = item.get('s')
            if not symbol:
                return
            now_ms = int(time.time() * 1000)
            # Struktur kompatibel zu choose_l1-Fallback
            self.mark_data.setdefault(symbol, {})  # nichts überschreiben
            self.bt_data[symbol] = {
                'bid': float(item.get('b', 0) or 0),
                'bid_qty': float(item.get('B', 0) or 0),
                'ask': float(item.get('a', 0) or 0),
                'ask_qty': float(item.get('A', 0) or 0),
                'ts': now_ms
            }
        except:
            pass

    async def all_book_ticker_stream(self):
        """!bookTicker – ein Stream für alle Symbole (L1-Fallback, 5s Updates)"""
        uri = "wss://fstream.binance.com/ws/!bookTicker"
        # Hinweis: Ein einzelner Stream; reduziert Start-„0 Zeilen", ohne pro Symbol zu subscriben.
        while self.is_running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10, open_timeout=15) as ws:
                    self.logger.info("Connected to !bookTicker")
                    async for msg in ws:
                        if not self.is_running:
                            break
                        try:
                            data = json.loads(msg)
                            self._handle_book_ticker(data)
                        except Exception as e:
                            self.logger.debug(f"BookTicker parse error: {e}")
            except Exception as e:
                self.logger.error(f"BookTicker connection error: {e}")
                await asyncio.sleep(3)

    def get_book_ticker(self, symbol: str) -> Optional[dict]:
        """Hole letztes L1 aus All-BookTicker (falls vorhanden)"""
        return self.bt_data.get(symbol)

    def _handle_force_order(self, data: dict):
        """Handle Liquidation Event"""
        try:
            order = data.get('o', {})
            symbol = order.get('s')
            if not symbol:
                return
            qty = float(order.get('q', 0) or order.get('z', 0))
            side = order.get('S')
            price_str = order.get('p') or order.get('ap')
            price = float(price_str) if price_str else 0
            order_type = order.get('o', 'UNKNOWN')
            time_in_force = order.get('f', 'UNKNOWN')
            ts_ms = int(order.get('T', time.time() * 1000))

            if qty <= 0 or side not in {'BUY', 'SELL'}:
                return

            liq_event = {
                'qty': qty,
                'is_buy': (side == 'BUY'),
                'price': price,
                'notional': price * qty if price > 0 else 0,
                'timestamp_ms': ts_ms
            }

            if symbol not in self.liq_events:
                self.liq_events[symbol] = []
            self.liq_events[symbol].append(liq_event)

            # ClickHouse Writer
            if self.ch_writer and self.ch_writer.enabled:
                self.ch_writer.add_liquidation(
                    ts_ms=ts_ms,
                    symbol=symbol,
                    side=side,
                    price=price,
                    qty=qty,
                    order_type=order_type,
                    time_in_force=time_in_force,
                )
        except:
            pass

    def get_mark_snap(self, symbol: str) -> dict:
        """Hole Mark Price Snapshot"""
        return self.mark_data.get(symbol, {
            'mark_price': 0, 'index_price': 0, 'est_settle_price': 0,
            'funding_rate': 0, 'next_funding_ms': 0, 'last_update_ts': 0
        })

    def pop_liq_events(self, symbol: str) -> list:
        """Hole und leere Liquidation Events"""
        if symbol not in self.liq_events:
            return []
        events = self.liq_events[symbol]
        self.liq_events[symbol] = []
        return events


# ============================================================================
# SHARD (60 Symbole × 3 Streams)
# ============================================================================

class Shard:
    """Ein Shard verwaltet bis zu 60 Symbole"""

    def __init__(self, symbols: List[str], shard_id: int, global_streams, ch_writer: Optional[ClickHouseWriter] = None):
        self.symbols = symbols
        self.shard_id = shard_id
        self.global_streams = global_streams
        self.state: Dict[str, SymbolState] = {s: SymbolState(s) for s in symbols}
        now_ms = int(time.time() * 1000)
        self.win_start = floor_to_grid(now_ms, 1500)
        self.logger = logging.getLogger(f"Shard-{shard_id}")
        self.is_running = True
        self.timer_task = None
        # Zähler/Diagnose
        self.last_rows_written = 0
        self.last_rows_zero = 0
        self.cum_rows_written = 0
        self.cum_rows_zero = 0
        self.zero_reported: set = set()  # Symbole, für die Grund bereits geloggt wurde
        # REST-Scheduler Referenz (für OI/L/S)
        self.rest_sched = None  # wird im main() gesetzt
        # HTTP-Session für REST-Snapshot & ClickHouse
        self.http_session: Optional[aiohttp.ClientSession] = None
        # ClickHouse-Writer (shared)
        self.ch = ch_writer
        # HF-Timer
        self._top20_next_ms = floor_to_grid(now_ms, TOP20_SNAPSHOT_MS) + TOP20_SNAPSHOT_MS
        self._l1_next_ms    = floor_to_grid(now_ms, L1_SAMPLE_MS)      + L1_SAMPLE_MS
        # Hintergrund-Loops
        self._hf_tasks: List[asyncio.Task] = []

    async def connect(self):
        """WebSocket Connection für diesen Shard"""
        streams = []
        for sym in self.symbols:
            base = sym.lower()
            streams.append(f"{base}@depth20@100ms")
            streams.append(f"{base}@trade")
            # bookTicker entfällt – L1 aus Depth

        uri = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        self.logger.info(f"Connecting {len(self.symbols)} symbols, {len(streams)} streams")

        # HTTP-Session starten
        self.http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        # CH-Writer wird zentral in main() gestartet

        # Start-Bootstrap via REST-Snapshot (Limits schonend, mit Jitter)
        await self._bootstrap_all_symbols()

        while self.is_running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10, open_timeout=15) as ws:
                    self.logger.info(f"WebSocket connected!")

                    if self.timer_task is None:
                        self.timer_task = asyncio.create_task(self.flush_timer())
                    if not self._hf_tasks:
                        self._hf_tasks = [
                            asyncio.create_task(self._top20_snapshot_loop()),
                            asyncio.create_task(self._l1_200ms_loop())
                        ]

                    async for msg in ws:
                        if not self.is_running:
                            break
                        try:
                            payload = json.loads(msg)
                            stream = payload.get('stream', '')
                            data = payload.get('data', {})

                            if '@depth20@100ms' in stream:
                                await self.on_depth(data)
                            elif '@trade' in stream:
                                await self.on_trade_raw(data)
                            # kein bookTicker-Handler
                        except Exception as e:
                            self.logger.debug(f"Handler error: {e}")
            except Exception as e:
                self.logger.error(f"Connection error: {e}")
                await asyncio.sleep(3)
        # Shutdown
        if self.http_session:
            await self.http_session.close()
        await self.ch.stop()

    async def _bootstrap_all_symbols(self):
        """Initialer REST-Snapshot für alle Symbole (sanft, verteilt)."""
        if not self.http_session:
            return
        for i, sym in enumerate(self.symbols):
            st = self.state[sym]
            # kleiner Jitter pro Symbol, um Bursts zu vermeiden
            await asyncio.sleep(0.05 + (i % 10) * 0.01)
            with suppress(Exception):
                await st.ob.rest_snapshot(self.http_session, REST_DEPTH_LIMIT)

    async def _top20_snapshot_loop(self):
        """Alle 100 ms Top-20-Snapshots in ClickHouse schreiben (falls enabled)."""
        while self.is_running:
            now_ms = int(time.time() * 1000)
            if now_ms >= self._top20_next_ms:
                self._top20_next_ms += TOP20_SNAPSHOT_MS
                if self.ch.enabled:
                    for sym, st in self.state.items():
                        if not st.ob.initialized:
                            continue
                        bids, asks = st.ob.get_top20()
                        bb, bq, ba, aq = st.ob.get_l1()
                        if bb and ba:
                            self.ch.add_top20_snapshot(now_ms, sym, bb or 0.0, bq or 0.0, ba or 0.0, aq or 0.0,
                                                       bids, asks)
            await asyncio.sleep(0.01)

    async def _l1_200ms_loop(self):
        """Alle 200 ms L1/Spread/Microprice/OFI & Trade-Rollup in ClickHouse (schlank)."""
        while self.is_running:
            now_ms = int(time.time() * 1000)
            if now_ms >= self._l1_next_ms:
                self._l1_next_ms += L1_SAMPLE_MS
                if self.ch.enabled:
                    for sym, st in self.state.items():
                        bb, bq, ba, aq = st.ob.get_l1()
                        if not (bb and ba):
                            continue
                        spread = ba - bb
                        mid = (bb + ba) / 2.0
                        micro = 0.0
                        if bq and aq and (bq + aq) > 0:
                            micro = (aq * bb + bq * ba) / (bq + aq)
                        # 200ms-OFI: wir nutzen das 3s-OFI nicht, sondern setzen hier 0 (leichtgewichtig)
                        ofi_200 = 0.0
                        # kleine Trade-Rollups (200ms) – aus den letzten 200ms Trades
                        # Wir approximieren: nehmen alle Trades, deren ts in [now_ms-200, now_ms) liegt
                        tc = 0; vb = 0.0; vq = 0.0
                        if st.trade_ts:
                            lo = now_ms - L1_SAMPLE_MS
                            for ts, px, qty in zip(st.trade_ts, st.trade_prices, st.trades.trade_sizes or []):
                                if lo <= ts < now_ms:
                                    tc += 1; vb += qty; vq += qty * px
                        self.ch.add_l1_200ms(now_ms, sym, bb, ba, bq or 0.0, aq or 0.0, spread, mid,
                                             micro, ofi_200, tc, vb, vq)
            await asyncio.sleep(0.01)

    async def on_depth(self, data: dict):
        """Handle @depth20@100ms"""
        symbol = data.get('s')
        if symbol not in self.state:
            return

        st = self.state[symbol]
        evt = {
            'U': data.get('U'), 'u': data.get('u'),
            'b': data.get('b', []), 'a': data.get('a', []),
            'E': data.get('E')
        }

        success = st.ob.apply_diff(evt)
        if not success:
            st.flags['resynced_this_window'] = 1
            # Bei Gap: sanftes REST-Resync anstoßen (mit Cooldown)
            if self.http_session:
                asyncio.create_task(st.ob.rest_snapshot(self.http_session, REST_DEPTH_LIMIT))
        if st.ob.initialized:
            st.flags['has_depth'] = 1
            # L1 + Mid sammeln, Flow-Features updaten
            bid_p, bid_q, ask_p, ask_q = st.ob.get_l1()
            if bid_p and ask_p:
                mid = (bid_p + ask_p) / 2
                st.mid_prices.append(mid)
                # Microprice first/last
                if st.microprice_first is None and bid_q and ask_q and (bid_q + ask_q) > 0:
                    st.microprice_first = (ask_q * bid_p + bid_q * ask_p) / (bid_q + ask_q)
                if bid_q and ask_q and (bid_q + ask_q) > 0:
                    st.microprice_last = (ask_q * bid_p + bid_q * ask_p) / (bid_q + ask_q)
                # L1 jump rate
                if st.prev_bid_p is not None and st.prev_ask_p is not None:
                    if bid_p != st.prev_bid_p or ask_p != st.prev_ask_p:
                        st.l1_jumps += 1
                # OFI (ΔBidQty@L1 − ΔAskQty@L1)
                if st.prev_bid_q is not None and st.prev_ask_q is not None and bid_q is not None and ask_q is not None:
                    st.ofi_sum += (bid_q - st.prev_bid_q) - (ask_q - st.prev_ask_q)
                    # Replenishment: Anstieg der L1-Menge
                    if bid_q > st.prev_bid_q:
                        st.replenish_events += 1
                    if ask_q > st.prev_ask_q:
                        st.replenish_events += 1
                st.prev_bid_p, st.prev_ask_p = bid_p, ask_p
                st.prev_bid_q, st.prev_ask_q = bid_q, ask_q
            # Optional: Diffs raw in ClickHouse schreiben
            if self.ch.enabled:
                self.ch.add_ob_diff(int(time.time()*1000), symbol, evt['U'], evt['u'],
                                    [(float(p), float(q)) for p, q in evt['b']],
                                    [(float(p), float(q)) for p, q in evt['a']])

    async def on_trade_raw(self, data: dict):
        """Handle @trade (jeder Fill einzeln)"""
        symbol = data.get('s')
        if symbol not in self.state:
            return

        st = self.state[symbol]
        price = float(data.get('p', 0))
        qty = float(data.get('q', 0))
        is_buyer_maker = 1 if data.get('m') else 0
        trade_id = int(data.get('t', data.get('a', 0)))

        st.trades.count += 1
        st.trades.vol_base += qty
        st.trades.vol_quote += price * qty

        if not is_buyer_maker:
            st.trades.taker_buy_vol += qty
            st.trades.buy_count += 1
            side = 'buy'
        else:
            st.trades.taker_sell_vol += qty
            st.trades.sell_count += 1
            side = 'sell'

        st.trades.trade_sizes.append(qty)
        st.trades.qtys.append(qty)
        st.trades.sides.append(side)
        st.trade_prices.append(price)
        st.trade_ts.append(int(time.time() * 1000))

        if st.trades.first_trade_id == 0:
            st.trades.first_trade_id = trade_id
        st.trades.last_trade_id = trade_id

        st.flags['has_trades'] = 1
        # RAW in ClickHouse
        if self.ch.enabled:
            self.ch.add_trade(int(time.time()*1000), symbol, price, qty, is_buyer_maker, trade_id)

        # Midquote wird bereits in on_depth gesammelt

    # bookTicker-Handler entfernt

    # REST-Resync wird in on_depth() bei Gaps angestoßen

    async def flush_timer(self):
        """1.5-Sekunden-Timer"""
        while self.is_running:
            now_ms = int(time.time() * 1000)
            next_flush = self.win_start + 1500

            if now_ms >= next_flush:
                await self.flush()
                self.win_start += 1500

            await asyncio.sleep(0.1)

    async def flush(self):
        """Flush alle Symbole - sende Aggregationen an ClickHouse"""
        now_ms = int(time.time() * 1000)
        self.logger.info(f"Flushing window {self.win_start}")

        rows_written = 0
        rows_zero = 0

        for symbol in self.symbols:
            st = self.state[symbol]

            # Sync global data
            self.sync_global_data(st)
            # REST-Snapshot (OI/L-S) zeitrichtig in dieses Fenster mappen
            self.sync_rest_window_data(st, self.win_start, self.win_start + 1500)

            # L1 wählen
            bid_p, bid_q, ask_p, ask_q = self.choose_l1(st)

            # OHLC_3s: Trades bevorzugt, sonst Midquotes
            ohlc = self.derive_ohlc_3s(st, bid_p, ask_p)

            # Metriken berechnen
            trade_metrics = self.calc_trade_metrics(st)
            liq_metrics = self.calc_liq_metrics(st)
            mark_metrics = self.calc_mark_metrics(st, now_ms)
            ob_metrics = self.calc_ob_metrics(st)
            flow_metrics = self.calc_flow_features(st, ob_metrics, now_ms)
            vol_metrics = self.calc_vol_metrics(st, ohlc, now_ms)

            # Top-20 Levels
            bids_20, asks_20 = st.ob.get_top20()

            # Advanced microstructure metrics
            advanced_metrics = self.calc_advanced_microstructure(st, bid_p, ask_p, bid_q, ask_q, bids_20, asks_20)

            # Send to ClickHouse
            self.send_to_clickhouse(symbol, bid_p, bid_q, ask_p, ask_q, ohlc,
                          trade_metrics, liq_metrics, mark_metrics, ob_metrics,
                          flow_metrics, vol_metrics, advanced_metrics, bids_20, asks_20, st)

            rows_written += 1
            # „Leer"-Heuristik: kein L1 ODER alle Kern-Flags 0
            is_zero = (
                (not bid_p or not ask_p) and
                st.trades.count == 0 and
                st.liq.count == 0 and
                st.mark.mark_price == 0
            )
            if is_zero:
                rows_zero += 1
                # Einmalige Ursachendiagnose pro Symbol
                if symbol not in self.zero_reported:
                    reason_bits = []
                    if not st.ob.initialized:
                        reason_bits.append("orderbook_uninitialized")
                    if st.flags.get('has_depth', 0) == 0:
                        reason_bits.append("no_depth_flag")
                    if st.flags.get('has_trades', 0) == 0:
                        reason_bits.append("no_trades")
                    if st.flags.get('has_mark', 0) == 0:
                        reason_bits.append("no_mark")
                    self.logger.warning(f"[ZERO-ROW] {symbol} @ {self.win_start} reasons={','.join(reason_bits)}")
                    self.zero_reported.add(symbol)

            # Parkinson-Minutenaggregator updaten (aus 3s-OHLC)
            self.update_minute_parkinson(st, ohlc, now_ms)

            # Reset fürs nächste 3s-Fenster
            self.reset_window_state(st)

        # 1.5s-Log mit Shard-Zählern
        self.last_rows_written = rows_written
        self.last_rows_zero = rows_zero
        self.cum_rows_written += rows_written
        self.cum_rows_zero += rows_zero
        self.logger.info(
            f"[CH] rows={rows_written} zero={rows_zero} | cum_rows={self.cum_rows_written} cum_zero={self.cum_rows_zero}"
        )

    def sync_global_data(self, st: SymbolState):
        """Sync Mark & Liq von GlobalStreams"""
        mark_snap = self.global_streams.get_mark_snap(st.symbol)
        if mark_snap:
            st.mark.mark_price = mark_snap.get('mark_price', 0)
            st.mark.index_price = mark_snap.get('index_price', 0)
            st.mark.est_settle_price = mark_snap.get('est_settle_price', 0)
            st.mark.funding_rate = mark_snap.get('funding_rate', 0)
            st.mark.next_funding_ms = mark_snap.get('next_funding_ms', 0)
            st.mark.last_update_ts = mark_snap.get('last_update_ts', 0)
            if st.mark.mark_price > 0:
                st.flags['has_mark'] = 1

        liq_events = self.global_streams.pop_liq_events(st.symbol)
        for evt in liq_events:
            st.liq.count += 1
            if evt['is_buy']:
                st.liq.buy_vol += evt['qty']
            else:
                st.liq.sell_vol += evt['qty']
            st.liq.notional += evt['notional']
            st.liq.sizes.append(evt['qty'])

        if st.liq.count > 0:
            st.flags['has_liq'] = 1

    def sync_rest_window_data(self, st: SymbolState, win_start_ms: int, win_end_ms: int):
        """Hole OI/Long-Short-Daten aus REST-Scheduler und schreibe nur, wenn sie ins Fenster fallen"""
        if not self.rest_sched:
            return
        # Open Interest: „momentan" – wir verwenden den Abfragezeitstempel
        # Erweiterte Logik: Wert gilt für die nächsten 12 Sekunden (4 × 3s-Fenster)
        oi = self.rest_sched.get_latest_oi(st.symbol)
        if oi:
            oi_ts = oi['ts']
            oi_value = oi['openInterest']
            oi_notional = oi['openInterestValue']
            # Prüfe ob oi_ts in [win_start_ms - 12000, win_end_ms) liegt
            # Das bedeutet: Wert kann bis zu 12s alt sein und wird trotzdem verwendet
            if win_start_ms - 12000 <= oi_ts < win_end_ms:
                st.oi_last_ts = oi_ts
                st.oi_open_interest = oi_value
                # Berechne OI Value aus OI × Mark Price (falls Mark Price vorhanden)
                if oi_notional is None and st.mark.mark_price > 0 and oi_value > 0:
                    oi_notional = oi_value * st.mark.mark_price
                st.oi_value_usdt = oi_notional
        # Long/Short Ratio (Top Positions): periodisch (5m) – Timestamp ist Periodenende
        # Erweiterte Logik: Wert gilt für die nächsten 12 Sekunden (4 × 3s-Fenster)
        ls = self.rest_sched.get_latest_ls(st.symbol)
        if ls:
            # ls['period_ms'] = Ende der 5m-Periode
            period_ms = ls['period_ms']
            # Prüfe ob period_ms in [win_start_ms - 12000, win_end_ms) liegt
            if win_start_ms - 12000 <= period_ms < win_end_ms:
                st.ls_last_period_ms = period_ms
                # Nur Top-Positions-Ratio befüllen
                st.ls_ratio_top_pos = ls.get('top_pos')
                # explizit leeren, damit nie alte Werte mitschleppen
                st.ls_ratio_global = None
                st.ls_ratio_top_acc = None

    def choose_l1(self, st: SymbolState) -> tuple:
        """L1-Auswahl: Depth zuerst; Fallback: globaler !bookTicker (frisch); sonst last_close"""
        # 1) Depth
        bid_p, bid_q, ask_p, ask_q = st.ob.get_l1()
        if bid_p and ask_p:
            st.flags['has_l1'] = 1
            if st.ob.is_crossed():
                st.flags['crossed_book'] = 1
            return bid_p, bid_q, ask_p, ask_q
        # 2) Globaler BookTicker-Fallback (nur wenn sehr frisch)
        bt = self.global_streams.get_book_ticker(st.symbol)
        if bt:
            now_ms = int(time.time() * 1000)
            if now_ms - bt.get('ts', 0) <= 5000 and bt.get('bid') and bt.get('ask'):
                st.flags['has_l1'] = 1
                return bt['bid'], bt['bid_qty'], bt['ask'], bt['ask_qty']
        # 3) Letzter Close als harmloser Fallback (symmetrisch)
        if st.last_close:
            return st.last_close, 0, st.last_close, 0
        st.flags['has_l1'] = 0
        return 0, 0, 0, 0

    def derive_ohlc_3s(self, st: SymbolState, bid_p: float, ask_p: float) -> dict:
        """3s-OHLC: Trades bevorzugt; wenn keine Trades → Midquotes"""
        if st.trade_prices:
            o = st.trade_prices[0]
            h = max(st.trade_prices)
            l = min(st.trade_prices)
            c = st.trade_prices[-1]
        elif st.mid_prices:
            o = st.mid_prices[0]
            h = max(st.mid_prices)
            l = min(st.mid_prices)
            c = st.mid_prices[-1] if not (bid_p and ask_p) else (bid_p + ask_p) / 2
        else:
            c = st.last_close if st.last_close else ((bid_p + ask_p) / 2 if bid_p and ask_p else 0)
            o = h = l = c
        st.last_close = c
        return {'open': o, 'high': h, 'low': l, 'close': c}

    def calc_trade_metrics(self, st: SymbolState) -> dict:
        """Trade-Metriken"""
        if st.trades.count == 0:
            return {
                'avg_trade_size': 0, 'trade_rate_hz': 0,
                'buy_sell_imbalance_trades': 0, 'buy_sell_imbalance_volume': 0,
                'cvd_cum': st.cvd_cum, 'itt_median_ms': 0,
                'burst_score': 0, 'sweep_flag': 0
            }

        avg_size = st.trades.vol_base / st.trades.count
        rate_hz = st.trades.count / 3.0
        imb_trades = (st.trades.buy_count - st.trades.sell_count) / st.trades.count if st.trades.count > 0 else 0
        imb_vol = (st.trades.taker_buy_vol - st.trades.taker_sell_vol) / st.trades.vol_base if st.trades.vol_base > 0 else 0
        # CVD kumulativ
        st.cvd_cum += (st.trades.taker_buy_vol - st.trades.taker_sell_vol)
        # ITT (ms)
        itt_med = 0
        if len(st.trade_ts) >= 2:
            diffs = [t2 - t1 for t1, t2 in zip(st.trade_ts[:-1], st.trade_ts[1:])]
            itt_med = median(diffs)
        # Burst-Score (Rate vs. EWMA)
        if st.trade_rate_ewma == 0.0:
            st.trade_rate_ewma = rate_hz
        else:
            st.trade_rate_ewma = 0.9 * st.trade_rate_ewma + 0.1 * rate_hz
        burst = rate_hz / st.trade_rate_ewma if st.trade_rate_ewma > 0 else 1.0
        # Sweep-Flag (einfacher Proxy)
        sweep = 1 if abs(st.trades.taker_buy_vol - st.trades.taker_sell_vol) > 0.7 * st.trades.vol_base else 0

        return {
            'avg_trade_size': avg_size,
            'trade_rate_hz': rate_hz,
            'buy_sell_imbalance_trades': imb_trades,
            'buy_sell_imbalance_volume': imb_vol,
            'cvd_cum': st.cvd_cum,
            'itt_median_ms': itt_med,
            'burst_score': burst,
            'sweep_flag': sweep
        }

    def calc_liq_metrics(self, st: SymbolState) -> dict:
        """Liq-Metriken"""
        if st.liq.count == 0:
            return {'avg_liq_size': 0, 'max_liq_size': 0}
        return {
            'avg_liq_size': sum(st.liq.sizes) / len(st.liq.sizes) if st.liq.sizes else 0,
            'max_liq_size': max(st.liq.sizes) if st.liq.sizes else 0
        }

    def calc_mark_metrics(self, st: SymbolState, now_ms: int) -> dict:
        """Mark-Metriken"""
        premium_bps = 0
        if st.mark.index_price > 0:
            premium_bps = (st.mark.mark_price - st.mark.index_price) / st.mark.index_price * 10000

        time_to_funding = 0
        if st.mark.next_funding_ms > now_ms:
            time_to_funding = (st.mark.next_funding_ms - now_ms) / 1000

        mark_age = now_ms - st.mark.last_update_ts if st.mark.last_update_ts > 0 else 0
        stale_flag = 1 if mark_age > 3000 else 0

        return {
            'mark_premium_bps': premium_bps,
            'time_to_next_funding_s': time_to_funding,
            'mark_age_ms': mark_age,
            'mark_stale_flag': stale_flag
        }

    def calc_ob_metrics(self, st: SymbolState) -> dict:
        """Orderbook-Metriken"""
        bid_p, bid_q, ask_p, ask_q = st.ob.get_l1()

        if not bid_p or not ask_p:
            return {
                'spread': 0, 'relative_spread': 0, 'microprice': 0,
                'orderbook_imbalance_l1': 0, 'depth_update_rate_hz': 0,
                'depth_sum_bid_1_5': 0, 'depth_sum_ask_1_5': 0,
                'depth_sum_bid_1_10': 0, 'depth_sum_ask_1_10': 0,
                'depth_sum_bid_1_20': 0, 'depth_sum_ask_1_20': 0,
                'vwap_bid_1_5': 0, 'vwap_ask_1_5': 0,
                'vwap_bid_1_10': 0, 'vwap_ask_1_10': 0,
                'vwap_bid_1_20': 0, 'vwap_ask_1_20': 0
            }

        spread = ask_p - bid_p
        mid = (bid_p + ask_p) / 2
        rel_spread = spread / mid if mid > 0 else 0

        microprice = 0
        if bid_q and ask_q and (bid_q + ask_q) > 0:
            microprice = (ask_q * bid_p + bid_q * ask_p) / (bid_q + ask_q)

        imb_l1 = 0
        if bid_q and ask_q and (bid_q + ask_q) > 0:
            imb_l1 = (bid_q - ask_q) / (bid_q + ask_q)

        bids, asks = st.ob.get_top20()

        def calc_depth_metrics(levels, n):
            if not levels:
                return 0, 0
            levels = levels[:n]
            total_qty = sum(qty for _, qty in levels)
            if total_qty == 0:
                return 0, 0
            vwap = sum(price * qty for price, qty in levels) / total_qty
            return total_qty, vwap

        depth_sum_bid_1_5, vwap_bid_1_5 = calc_depth_metrics(bids, 5)
        depth_sum_ask_1_5, vwap_ask_1_5 = calc_depth_metrics(asks, 5)
        depth_sum_bid_1_10, vwap_bid_1_10 = calc_depth_metrics(bids, 10)
        depth_sum_ask_1_10, vwap_ask_1_10 = calc_depth_metrics(asks, 10)
        depth_sum_bid_1_20, vwap_bid_1_20 = calc_depth_metrics(bids, 20)
        depth_sum_ask_1_20, vwap_ask_1_20 = calc_depth_metrics(asks, 20)

        update_rate_hz = st.ob.updates_this_window / 3.0

        return {
            'spread': spread,
            'relative_spread': rel_spread,
            'microprice': microprice,
            'orderbook_imbalance_l1': imb_l1,
            'depth_update_rate_hz': update_rate_hz,
            'depth_sum_bid_1_5': depth_sum_bid_1_5,
            'depth_sum_ask_1_5': depth_sum_ask_1_5,
            'depth_sum_bid_1_10': depth_sum_bid_1_10,
            'depth_sum_ask_1_10': depth_sum_ask_1_10,
            'depth_sum_bid_1_20': depth_sum_bid_1_20,
            'depth_sum_ask_1_20': depth_sum_ask_1_20,
            'vwap_bid_1_5': vwap_bid_1_5,
            'vwap_ask_1_5': vwap_ask_1_5,
            'vwap_bid_1_10': vwap_bid_1_10,
            'vwap_ask_1_10': vwap_ask_1_10,
            'vwap_bid_1_20': vwap_bid_1_20,
            'vwap_ask_1_20': vwap_ask_1_20
        }

    def calc_flow_features(self, st: SymbolState, ob_metrics: dict, now_ms: int) -> dict:
        """OFI, Microprice-Drift, L1-Jump-Rate, Replenishment, Slope/Curvature, Spread-Regime"""
        # Microprice-Drift
        micro_first = st.microprice_first if st.microprice_first is not None else 0
        micro_last = st.microprice_last if st.microprice_last is not None else 0
        micro_drift = micro_last - micro_first
        # L1-Jump-Rate (pro Sekunde normieren)
        l1_jump_rate = st.l1_jumps / 3.0
        # Replenishment
        replenishment_rate = st.replenish_events / 3.0
        # Slope/Curvature-Proxy aus Top20
        bids, asks = st.ob.get_top20()
        def slope_curvature(levels):
            if not levels:
                return 0.0, 0.0
            cum_qty = 0.0
            xs, ys = [], []
            for i, (p, q) in enumerate(levels, 1):
                cum_qty += q
                xs.append(cum_qty)
                ys.append(p)
            n = len(xs)
            sx = sum(xs); sy = sum(ys)
            sxx = sum(x*x for x in xs); sxy = sum(x*y for x, y in zip(xs, ys))
            denom = (n * sxx - sx * sx)
            slope = (n * sxy - sx * sy) / denom if denom != 0 else 0.0
            # Curvature-Proxy: Unterschied der Mittel-Slopes (erste Hälfte vs. zweite Hälfte)
            half = max(1, n // 2)
            def avg_slope(subx, suby):
                m = len(subx)
                if m < 2:
                    return 0.0
                sx = sum(subx); sy = sum(suby)
                sxx = sum(x*x for x in subx); sxy = sum(x*y for x, y in zip(subx, suby))
                denom = (m * sxx - sx * sx)
                return (m * sxy - sx * sy) / denom if denom != 0 else 0.0
            curv = avg_slope(xs[:half], ys[:half]) - avg_slope(xs[half:], ys[half:])
            return slope, curv
        slope_b, curv_b = slope_curvature(bids)
        slope_a, curv_a = slope_curvature(asks)
        # Spread-Regime via z-Score auf relative_spread (EWMA)
        rel_spread = ob_metrics.get('relative_spread', 0.0)
        if st.rel_spread_mean == 0.0 and st.rel_spread_var == 0.0:
            st.rel_spread_mean = rel_spread
        else:
            alpha = 0.1
            diff = rel_spread - st.rel_spread_mean
            st.rel_spread_mean += alpha * diff
            st.rel_spread_var = (1 - alpha) * (st.rel_spread_var + alpha * diff * diff)
        z = (rel_spread - st.rel_spread_mean) / math.sqrt(st.rel_spread_var) if st.rel_spread_var > 1e-12 else 0.0
        regime = 2 if z > 1.0 else (0 if z < -1.0 else 1)  # 0=tight,1=normal,2=wide
        return {
            'ofi_sum': st.ofi_sum,
            'microprice_drift': micro_drift,
            'l1_jump_rate': l1_jump_rate,
            'replenishment_rate': replenishment_rate,
            'slope_bid': slope_b, 'slope_ask': slope_a,
            'curvature_bid': curv_b, 'curvature_ask': curv_a,
            'spread_regime': regime
        }

    def calc_vol_metrics(self, st: SymbolState, ohlc: dict, now_ms: int) -> dict:
        """RV 3s (Mid-Returns) + EWMA (1/5/15m) + Parkinson 1m (aus 3s-High/Low)"""
        rv_3s = 0.0
        # 3s RV aus Mid-Returns innerhalb Fensters
        if len(st.mid_prices) >= 2:
            rets = []
            for p1, p2 in zip(st.mid_prices[:-1], st.mid_prices[1:]):
                if p1 > 0 and p2 > 0:
                    rets.append(math.log(p2 / p1))
            if rets:
                rv_3s = math.sqrt(sum(r*r for r in rets))  # nicht annualisiert
        # EWMA-Vols
        def ewma(prev, x, alpha):
            return (1 - alpha) * prev + alpha * x
        st.rv_ewma_1m = ewma(st.rv_ewma_1m, rv_3s, 0.1)
        st.rv_ewma_5m = ewma(st.rv_ewma_5m, rv_3s, 0.03)
        st.rv_ewma_15m = ewma(st.rv_ewma_15m, rv_3s, 0.01)
        return {
            'rv_3s': rv_3s,
            'rv_ewma_1m': st.rv_ewma_1m,
            'rv_ewma_5m': st.rv_ewma_5m,
            'rv_ewma_15m': st.rv_ewma_15m,
            'parkinson_1m': st.parkinson_last
        }

    def calc_advanced_microstructure(self, st: SymbolState, bid_p: float, ask_p: float,
                                     bid_q: float, ask_q: float, bids_20: list, asks_20: list) -> dict:
        """Calculate 10 advanced microstructure metrics"""
        mid = (bid_p + ask_p) / 2 if bid_p and ask_p else 0

        # 1 & 2: Effective Spread & Realized Spread (5s look-ahead)
        effective_spread_bps = 0.0
        realized_spread_5s_bps = 0.0
        if st.trades.count > 0 and len(st.trade_prices) > 0:
            # Volume-weighted effective spread
            eff_sum = 0.0
            real_sum = 0.0
            total_vol = 0.0
            for i, (tp, tq, side) in enumerate(zip(st.trade_prices, st.trades.qtys if hasattr(st.trades, 'qtys') else [], st.trades.sides if hasattr(st.trades, 'sides') else [])):
                if mid > 0:
                    side_sign = 1 if side == 'buy' else -1
                    eff = 2 * side_sign * (tp - mid) / mid * 10000
                    eff_sum += eff * tq
                    # Realized spread needs 5s look-ahead - for now use current mid as approximation
                    real = 2 * side_sign * (tp - mid) / mid * 10000
                    real_sum += real * tq
                    total_vol += tq
            if total_vol > 0:
                effective_spread_bps = eff_sum / total_vol
                realized_spread_5s_bps = real_sum / total_vol

        # 3: Kyle Lambda (price impact coefficient)
        kyle_lambda = 0.0
        if st.microprice_first and st.microprice_last and st.trades.vol_quote > 0:
            delta_mid = abs(st.microprice_last - st.microprice_first)
            signed_vol = st.trades.taker_buy_vol - st.trades.taker_sell_vol
            if abs(signed_vol) > 1e-6:
                kyle_lambda = delta_mid / abs(signed_vol)

        # 4: Amihud Illiquidity
        amihud_illiq = 0.0
        if st.microprice_first and st.microprice_last and st.trades.vol_quote > 1e-6:
            delta_mid = abs(st.microprice_last - st.microprice_first)
            amihud_illiq = delta_mid / st.trades.vol_quote

        # 5: VPIN (simplified - volume-synchronized probability of informed trading)
        vpin = 0.0
        if st.trades.vol_base > 0:
            imbalance = abs(st.trades.taker_buy_vol - st.trades.taker_sell_vol)
            vpin = imbalance / st.trades.vol_base

        # 6: Microprice Edge
        microprice_edge_bps = 0.0
        if mid > 0 and st.microprice_last:
            microprice_edge_bps = (st.microprice_last - mid) / mid * 10000

        # 7 & 8: L1 Queue Depletion Time (QDT)
        qdt_bid_s = 0.0
        qdt_ask_s = 0.0
        if bid_q > 0 and st.trades.taker_sell_vol > 0:  # taker sell consumes bid
            consume_rate = st.trades.taker_sell_vol / 1.5  # per second
            qdt_bid_s = bid_q / max(1e-6, consume_rate)
        if ask_q > 0 and st.trades.taker_buy_vol > 0:  # taker buy consumes ask
            consume_rate = st.trades.taker_buy_vol / 1.5  # per second
            qdt_ask_s = ask_q / max(1e-6, consume_rate)

        # 9-12: WebSocket Latency & Gap/Dup Rates (needs infrastructure data)
        ws_latency_p50_ms = 0.0
        ws_latency_p95_ms = 0.0
        gap_rate_depth = 0.0
        dup_rate_depth = 0.0
        # TODO: Implement when we track event timestamps and sequence IDs

        # 13 & 14: OB Entropy (liquidity concentration)
        def calc_entropy(qtys):
            if not qtys or sum(qtys) == 0:
                return 0.0
            total = sum(qtys)
            probs = [q / total for q in qtys if q > 0]
            return -sum(p * math.log(p) for p in probs if p > 0)

        ob_entropy_bid = calc_entropy([q for _, q in bids_20[:20]])
        ob_entropy_ask = calc_entropy([q for _, q in asks_20[:20]])

        # 15: Variance Ratio (200ms -> 1.5s)
        variance_ratio = 1.0  # Default to 1 (random walk)
        # TODO: Needs 200ms returns tracking

        # 16-18: Index Basis & Funding-adjusted Basis
        index_basis_bps = 0.0
        basis_drift_bps = 0.0
        funding_adj_basis_bps = 0.0
        if mid > 0 and st.mark.index_price > 0:
            index_basis_bps = (mid - st.mark.index_price) / st.mark.index_price * 10000
            # Basis drift
            if hasattr(st, 'last_index_basis_bps'):
                basis_drift_bps = index_basis_bps - st.last_index_basis_bps
            st.last_index_basis_bps = index_basis_bps
            # Funding-adjusted basis
            if st.mark.funding_rate and st.mark.next_funding_ms:
                hours_to_funding = (st.mark.next_funding_ms - int(time.time() * 1000)) / 3600000
                funding_adj_basis_bps = index_basis_bps - (st.mark.funding_rate * 10000 * hours_to_funding / 8)

        return {
            'effective_spread_bps': effective_spread_bps,
            'realized_spread_5s_bps': realized_spread_5s_bps,
            'kyle_lambda': kyle_lambda,
            'amihud_illiq': amihud_illiq,
            'vpin': vpin,
            'microprice_edge_bps': microprice_edge_bps,
            'qdt_bid_s': qdt_bid_s,
            'qdt_ask_s': qdt_ask_s,
            'ws_latency_p50_ms': ws_latency_p50_ms,
            'ws_latency_p95_ms': ws_latency_p95_ms,
            'gap_rate_depth': gap_rate_depth,
            'dup_rate_depth': dup_rate_depth,
            'ob_entropy_bid': ob_entropy_bid,
            'ob_entropy_ask': ob_entropy_ask,
            'variance_ratio': variance_ratio,
            'index_basis_bps': index_basis_bps,
            'basis_drift_bps': basis_drift_bps,
            'funding_adj_basis_bps': funding_adj_basis_bps,
        }

    def update_minute_parkinson(self, st: SymbolState, ohlc: dict, now_ms: int):
        """Aggregiere 3s-High/Low zu 1m-High/Low und berechne Parkinson-Vol je Minute"""
        bucket = (now_ms // 60000) * 60000
        high = ohlc['high']; low = ohlc['low']
        if st.min_bucket_start == 0:
            st.min_bucket_start = bucket
            st.min_high = high
            st.min_low = low if low > 0 else high
        if bucket != st.min_bucket_start:
            # Minute geschlossen → Parkinson aus alter Minute
            if st.min_low > 0 and st.min_high > st.min_low:
                ratio = st.min_high / st.min_low
                st.parkinson_last = math.sqrt(max(0.0, (1.0 / (4.0 * math.log(2))) * (math.log(ratio) ** 2)))
            # Neue Minute starten
            st.min_bucket_start = bucket
            st.min_high = high
            st.min_low = low if low > 0 else high
        else:
            st.min_high = max(st.min_high, high)
            st.min_low = min(st.min_low if st.min_low > 0 else high, low if low > 0 else high)

    def send_to_clickhouse(self, symbol: str,
                  bid_p: float, bid_q: float, ask_p: float, ask_q: float,
                  ohlc: dict, trade_metrics: dict, liq_metrics: dict,
                  mark_metrics: dict, ob_metrics: dict, flow_metrics: dict,
                  vol_metrics: dict, advanced_metrics: dict, bids_20: list, asks_20: list, st: SymbolState):
        """Send 1.5s advanced metrics to ClickHouse"""

        if not self.ch.enabled:
            return

        # Calculate mid
        mid = (bid_p + ask_p) / 2 if bid_p and ask_p else 0

        # Build tuple for ClickHouse - only advanced metrics + context
        row = (
            self.win_start / 1000.0,  # ts as seconds (will be converted to DateTime64)
            symbol,
            self.win_start,
            self.win_start + 1500,
            # Context
            bid_p or 0,
            ask_p or 0,
            mid,
            # Advanced metrics only
            advanced_metrics.get('effective_spread_bps', 0),
            advanced_metrics.get('realized_spread_5s_bps', 0),
            advanced_metrics.get('kyle_lambda', 0),
            advanced_metrics.get('amihud_illiq', 0),
            advanced_metrics.get('vpin', 0),
            advanced_metrics.get('microprice_edge_bps', 0),
            advanced_metrics.get('qdt_bid_s', 0),
            advanced_metrics.get('qdt_ask_s', 0),
            advanced_metrics.get('ws_latency_p50_ms', 0),
            advanced_metrics.get('ws_latency_p95_ms', 0),
            advanced_metrics.get('gap_rate_depth', 0),
            advanced_metrics.get('dup_rate_depth', 0),
            advanced_metrics.get('ob_entropy_bid', 0),
            advanced_metrics.get('ob_entropy_ask', 0),
            advanced_metrics.get('variance_ratio', 1.0),
            advanced_metrics.get('index_basis_bps', 0),
            advanced_metrics.get('basis_drift_bps', 0),
            advanced_metrics.get('funding_adj_basis_bps', 0),
        )

        # Add to ClickHouse queue
        self.ch._q_agg.append(row)

    def reset_window_state(self, st: SymbolState):
        """Reset State für neues Fenster"""
        st.trades = TradeAgg()
        st.liq = LiqAgg()
        st.mid_prices.clear()
        st.trade_prices.clear()
        st.trade_ts.clear()
        st.ofi_sum = 0.0
        st.microprice_first = None
        st.microprice_last = None
        st.l1_jumps = 0
        st.replenish_events = 0
        st.ob.reset_window_counters()
        for key in st.flags:
            st.flags[key] = 0
        # REST-Fenster-Mapping zurücksetzen (nur Fenstersicht, nicht die globalen Caches)
        st.oi_last_ts = 0
        st.oi_open_interest = None
        st.oi_value_usdt = None
        st.ls_last_period_ms = 0
        st.ls_ratio_global = None      # bleibt ungenutzt
        st.ls_ratio_top_acc = None     # bleibt ungenutzt
        st.ls_ratio_top_pos = None


# ============================================================================
# REST SCHEDULER (Open Interest & Long/Short Ratios)
# ============================================================================

class RestScheduler:
    """
    Gedrosselte REST-Abfragen:
      - Open Interest: /fapi/v1/openInterest (Weight=1), Ziel ~30s pro Symbol
      - Long/Short Ratio (5m): /futures/data/topLongShortPositionRatio (IP-Limit 1000/5m = 200/min)
    """
    def __init__(self, symbols: List[str], oi_interval_sec: int = 30, enable_ls: bool = True, ch_writer: Optional[ClickHouseWriter] = None):
        self.logger = logging.getLogger("RestScheduler")
        self.symbols = symbols
        self.oi_interval = max(10, oi_interval_sec)
        self.enable_ls = enable_ls
        self.ch_writer = ch_writer
        # aiohttp-Session wird in run() erstellt
        self.session: Optional[aiohttp.ClientSession] = None
        # Caches
        self._oi: Dict[str, dict] = {}   # {symbol: {'ts', 'openInterest', 'openInterestValue'}}
        self._ls: Dict[str, dict] = {}   # {symbol: {'period_ms', 'top_pos'}}
        # Steuerung
        self.is_running = True
        # sanfte Limits
        self._sem_oi = asyncio.Semaphore(50)     # max parallel OI-Calls
        self._sem_ls = asyncio.Semaphore(32)     # max parallel L/S-Calls (konservativ, aber flotter)
        self._ls_budget_per_min = 190            # knapp unter 200/min IP-Limit
        self._ls_tokens = self._ls_budget_per_min
        self._ls_last_refill = time.time()

    async def run(self):
        timeout = aiohttp.ClientTimeout(total=8, connect=3, sock_connect=3, sock_read=5)
        connector = aiohttp.TCPConnector(limit=200, limit_per_host=100, ttl_dns_cache=300)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            self.session = session
            tasks = [asyncio.create_task(self._loop_open_interest())]
            if self.enable_ls:
                tasks.append(asyncio.create_task(self._loop_long_short()))
            await asyncio.gather(*tasks)

    # ---------- Open Interest ----------
    async def _loop_open_interest(self):
        """
        Round-Robin über alle Symbole, Ziel ~30s pro Symbol.
        Pro Tick holen wir so viele Symbole wie budgetiert (ohne aggressiv zu werden).
        """
        idx = 0
        n = len(self.symbols)
        # Zielrate: alle Symbole in oi_interval Sekunden ⇒ ~ n/oi_interval Calls pro Sek.
        # Wir takten in 1s-Schritten.
        while self.is_running:
            start = time.time()
            if n == 0:
                await asyncio.sleep(1.0)
                continue
            per_sec = max(1, n // self.oi_interval)  # grobe Zielrate
            batch = []
            for _ in range(per_sec):
                sym = self.symbols[idx % n]; idx += 1
                batch.append(self._fetch_open_interest_async(sym))
            if batch:
                await asyncio.gather(*batch, return_exceptions=True)
            # Rest der Sekunde schlafen
            elapsed = time.time() - start
            await asyncio.sleep(max(0.0, 1.0 - elapsed))

    async def _fetch_open_interest_async(self, symbol: str):
        async with self._sem_oi:
            try:
                assert self.session is not None
                url = "https://fapi.binance.com/fapi/v1/openInterest"
                async with self.session.get(url, params={"symbol": symbol}) as resp:
                    if resp.status == 429:
                        self.logger.debug("OI 429")
                        return
                    resp.raise_for_status()
                    data = await resp.json()
                now_ms = int(time.time() * 1000)
                oi = float(data.get("openInterest", 0) or 0)
                # Berechne OI Value: Brauchen Mark Price dafür
                # Hole Mark Price aus global_streams Cache (über separate Referenz)
                # ODER: hole es aus dem mark_data des GlobalStreams
                # Problem: Wir haben hier keinen Zugriff auf GlobalStreams
                # Lösung: Wir cachen mark_price separat oder berechnen es später beim Mapping
                # Für jetzt: setzen wir es auf None und berechnen beim sync_rest_window_data
                oi_val = None  # wird später berechnet
                self._oi[symbol] = {"ts": now_ms, "openInterest": oi, "openInterestValue": oi_val}

                # ClickHouse Writer
                if self.ch_writer and self.ch_writer.enabled:
                    self.ch_writer.add_open_interest(
                        ts_ms=now_ms,
                        symbol=symbol,
                        open_interest=oi,
                        open_interest_value=oi_val if oi_val is not None else 0.0,
                    )
            except Exception as e:
                self.logger.debug(f"OI fetch failed {symbol}: {e}")

    def get_latest_oi(self, symbol: str) -> Optional[dict]:
        return self._oi.get(symbol)

    # ---------- Long/Short Ratio (Top Positions, 5m) ----------
    async def _loop_long_short(self):
        """
        Jede 5m-Periode genau 1 Punkt je Symbol (neuester), IP-Limit 200/min beachten.
        Wir verteilen die Symbole gleichmäßig über 5 Minuten (einziger Endpoint: topLongShortPositionRatio).
        """
        n = len(self.symbols)
        minute_buckets = 5
        while self.is_running:
            start_cycle = time.time()
            # erstelle 5 Buckets mit gleichmäßig verteilten Symbolen
            buckets = [[] for _ in range(minute_buckets)]
            for i, sym in enumerate(self.symbols):
                buckets[i % minute_buckets].append(sym)
            # laufe 5 Minuten und bearbeite pro Minute einen Bucket (nur 1 Endpunkt)
            for b in range(minute_buckets):
                tmin = time.time()
                await self._fetch_ls_bucket_async(buckets[b])
                # rest der Minute schlafen
                elapsed = time.time() - tmin
                await asyncio.sleep(max(0.0, 60.0 - elapsed))
            # Zyklusdauer anpassen (sollte ~300s sein)
            drift = time.time() - start_cycle - 300.0
            if drift > 2:
                self.logger.debug(f"L/S cycle drift {drift:.1f}s")

    async def _fetch_ls_bucket_async(self, symbols: List[str]):
        base = "https://fapi.binance.com/futures/data/topLongShortPositionRatio"
        tasks = []
        for sym in symbols:
            tasks.append(self._fetch_ls_one_async(base, sym))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_ls_one_async(self, base: str, sym: str):
        # einfacher Token-Bucket für 200/min
        now = time.time()
        if now - self._ls_last_refill >= 60:
            self._ls_tokens = self._ls_budget_per_min
            self._ls_last_refill = now
        if self._ls_tokens <= 0:
            # Budget weg → diesmal auslassen
            return
        self._ls_tokens -= 1
        async with self._sem_ls:
            try:
                assert self.session is not None
                params = {"symbol": sym, "period": "5m", "limit": 1}
                async with self.session.get(base, params=params) as resp:
                    if resp.status == 429:
                        self.logger.debug("L/S 429 top_pos")
                        return
                    resp.raise_for_status()
                    arr = await resp.json()
                if not arr:
                    return
                item = arr[-1]
                ts = int(item.get("timestamp", 0))
                val = float(item.get("longShortRatio", 0) or 0)
                long_acc = float(item.get("longAccount", 0) or 0)
                short_acc = float(item.get("shortAccount", 0) or 0)
                rec = self._ls.setdefault(sym, {"period_ms": ts, "top_pos": None})
                rec["period_ms"] = ts
                rec["top_pos"] = val

                # ClickHouse Writer
                if self.ch_writer and self.ch_writer.enabled:
                    self.ch_writer.add_long_short_ratio(
                        ts_ms=ts,
                        symbol=sym,
                        long_short_ratio=val,
                        long_account=long_acc,
                        short_account=short_acc,
                    )
            except Exception as e:
                self.logger.debug(f"L/S fetch failed {sym}/top_pos: {e}")

    def get_latest_ls(self, symbol: str) -> Optional[dict]:
        return self._ls.get(symbol)


# ============================================================================
# MAIN
# ============================================================================

def get_all_symbols() -> list:
    """Hole alle USDT Perpetual Symbole"""
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        symbols = []
        for item in data['symbols']:
            if (item['status'] == 'TRADING' and
                item['contractType'] == 'PERPETUAL' and
                item['symbol'].endswith('USDT')):
                symbols.append(item['symbol'])

        logging.info(f"Loaded {len(symbols)} USDT Perpetual symbols")
        return sorted(symbols)

    except Exception as e:
        logging.error(f"Failed to fetch symbols: {e}")
        return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT']


def shard_symbols(symbols: list, symbols_per_shard: int = 30) -> list:
    """Split Symbole in Shards"""
    shards = []
    for i in range(0, len(symbols), symbols_per_shard):
        shard = symbols[i:i + symbols_per_shard]
        shards.append(shard)
    logging.info(f"Created {len(shards)} shards for {len(symbols)} symbols")
    return shards


async def main():
    """Main Entry Point"""
    # WICHTIG: Logging Setup komplett VOR allem anderen
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Alle bestehenden Handler entfernen (falls vorhanden)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File Handler - schreibt in binance_collector.log
    file_handler = logging.FileHandler('binance_collector.log', mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_format)
    root_logger.addHandler(file_handler)

    # Console Handler - schreibt auf Terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)

    logger = logging.getLogger("Main")
    logger.info("="*80)
    logger.info("BINANCE 1.5S COLLECTOR STARTED - ClickHouse Only")
    logger.info("="*80)

    logger.info("Fetching symbols from Binance...")
    symbols = get_all_symbols()

    symbol_shards = shard_symbols(symbols, symbols_per_shard=30)

    # ClickHouse Writer initialisieren
    logger.info("Initializing ClickHouse Writer...")
    ch_writer = ClickHouseWriter()
    await ch_writer.start()

    logger.info("Starting global streams...")
    global_streams = GlobalStreams(ch_writer=ch_writer)
    global_task = asyncio.create_task(global_streams.run())

    logger.info(f"Starting {len(symbol_shards)} shards...")
    shard_tasks = []
    shard_objects = []

    for idx, shard_symbols_list in enumerate(symbol_shards):
        shard = Shard(
            symbols=shard_symbols_list,
            shard_id=idx + 1,
            global_streams=global_streams,
            ch_writer=ch_writer
        )
        shard_objects.append(shard)
        task = asyncio.create_task(shard.connect())
        shard_tasks.append(task)
        await asyncio.sleep(2)

    # REST Scheduler mit kleinem Delay starten, damit WS sich zuerst einwählen
    logger.info("Starting REST scheduler (OI 30s, L/S 5m) in ~8s...")
    await asyncio.sleep(8)
    rest_sched = RestScheduler(symbols, ch_writer=ch_writer)
    # REST-Referenz in alle Shards setzen
    for shard in shard_objects:
        shard.rest_sched = rest_sched
    rest_task = asyncio.create_task(rest_sched.run())

    logger.info(f"System running: {len(symbol_shards)} shards + 2 global streams + REST scheduler")
    logger.info(f"Total WebSocket connections: {len(symbol_shards) + 2}")
    logger.info(f"Expected rows per minute: {20 * len(symbols)}")

    await asyncio.gather(global_task, rest_task, *shard_tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Shutting down...")

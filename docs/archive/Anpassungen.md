diff --git a/binance_3s_collector.py b/binance_3s_collector.py
index 7f0b3c1..4a9d2b8 100644
--- a/binance_3s_collector.py
+++ b/binance_3s_collector.py
@@ -1,28 +1,58 @@
 """
-Binance 3s Data Collector - ALLES IN EINEM FILE
-Gemäß usdm_ws_plan.md - Komplett-System für 3-Sekunden-Datensammlung
+Binance Data Collector – erweitert um:
+  • REST-Snapshot-Bootstrap & Re-Sync für Orderbücher (sanft gedrosselt)
+  • High-Frequency-Sampling:
+      – Top-20-Orderbuch-Snapshot @100 ms (aus lokalem Buch)
+      – L1/Spread/OFI @200 ms (schlank)
+      – Trades RAW (WS 'trade' statt 'aggTrade')
+  • Optionaler ClickHouse-Writer (HTTP), via Umgebungsvariablen aktivierbar
+  • Bestandsschutz: bisherige 3s-Aggregationen (SQLite) bleiben erhalten
 """
 
 import asyncio
 import json
 import logging
 import sqlite3
 import time
 from collections import defaultdict
 from collections import deque
 from dataclasses import dataclass, field
 from statistics import median
 import math
-from typing import Dict, List, Optional, Tuple
+from typing import Dict, List, Optional, Tuple, Any
 
 import requests
 from requests.adapters import HTTPAdapter
 from urllib3.util.retry import Retry
 import aiohttp
 import websockets
+import os
+import random
+from contextlib import suppress
 
 # Änderungen (ohne Begründung):
 # - OHLC_3s aus Trades; falls keine Trades im Fenster → aus Midquotes (L1-Mid)
 # - Flow-Features: OFI, Microprice-Drift, L1-Jump-Rate, Replenishment-Rate
 # - Buchform: Slope/Curvature (Proxy) für Bid/Ask (Levels 1–20)
 # - Trades: CVD (kumulativ), ITT-Median, Burst-Score, Sweep-Flag
 # - Volatilität: RV_3s (Mid-Returns), RV_EWMA (1/5/15min), Parkinson_1m (aus 3s-High/Low)
-# - 3s-Log & Zero-Row-Diagnose bleiben; keine REST-Snapshots
+# - 3s-Log & Zero-Row-Diagnose bleiben
 # - Open Interest: REST-Poller (30s Ziel), zeitrichtig in 3s-Fenster schreiben
 # - Long/Short Ratio NUR: topLongShortPositionRatio (Periode 5m),
 #   nur im passenden 3s-Fenster setzen, sonst NULL lassen; Rate-Limit sicher
+
+# =============================================================================
+# NEU: HF-Sampling- und Snapshot-Konfiguration
+# =============================================================================
+TOP20_SNAPSHOT_MS = int(os.getenv("TOP20_SNAPSHOT_MS", "100"))     # alle 100 ms Top-20-Snapshot
+L1_SAMPLE_MS      = int(os.getenv("L1_SAMPLE_MS", "200"))          # alle 200 ms L1/Spread/OFI
+
+# REST Snapshot (Orderbuch) – konservativ gedrosselt
+REST_DEPTH_LIMIT   = int(os.getenv("REST_DEPTH_LIMIT", "200"))     # 200 reicht für Top-20 sicher
+REST_COOLDOWN_SEC  = int(os.getenv("REST_COOLDOWN_SEC", "30"))     # min. 30s zwischen Snapshots pro Symbol
+REST_RETRY_MAX     = int(os.getenv("REST_RETRY_MAX", "3"))
+BINANCE_FAPI_DEPTH = "https://fapi.binance.com/fapi/v1/depth"
+
+# Optional: ClickHouse-Writer (HTTP). Aktiv nur, wenn URL gesetzt ist.
+CLICKHOUSE_URL      = os.getenv("CLICKHOUSE_URL", "").strip()      # z. B. "http://localhost:8123"
+CLICKHOUSE_DB       = os.getenv("CLICKHOUSE_DB", "default").strip() or "default"
+CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER", "").strip()
+CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "").strip()
 
 # ============================================================================
 # DATABASE SCHEMA
@@ -209,18 +239,86 @@ class LocalOrderbook:
     def __init__(self, symbol: str):
         self.symbol = symbol
         self.bids: Dict[float, float] = {}
         self.asks: Dict[float, float] = {}
-        self.last_u: Optional[int] = None
+        self.last_u: Optional[int] = None
         self.initialized: bool = False
         self.updates_this_window = 0
-        # Bootstrapping nur via WS-Diffs
-        self.bootstrap_min_levels = 20  # Ziel: Top-20 stabil
+        # Ziel: Top-20 stabil
+        self.bootstrap_min_levels = 20
+        # REST-Snapshot-Guards (Limits schonend)
+        self._last_rest_snapshot_s: int = 0
+        self._rest_inflight: bool = False
+
+    async def rest_snapshot(self, session: aiohttp.ClientSession, limit: int = REST_DEPTH_LIMIT) -> bool:
+        """
+        Hole REST-Snapshot (Tiefe = limit) und setze lokales Buch darauf.
+        Sanfte Drosselung:
+          - Mindestens REST_COOLDOWN_SEC Abstand zwischen zwei Snapshots pro Symbol
+          - Jitter + Backoff bei Fehlversuchen
+        Gibt True zurück, wenn Buch initialisiert wurde.
+        """
+        if self._rest_inflight:
+            return False
+        now_s = int(time.time())
+        if now_s - self._last_rest_snapshot_s < REST_COOLDOWN_SEC:
+            return False
+        self._rest_inflight = True
+        try:
+            params = {"symbol": self.symbol, "limit": str(limit)}
+            delay = 0.2 + random.random() * 0.3  # Jitter, um Shards zu entkoppeln
+            await asyncio.sleep(delay)
+            for attempt in range(1, REST_RETRY_MAX + 1):
+                try:
+                    async with session.get(BINANCE_FAPI_DEPTH, params=params) as r:
+                        if r.status == 429:
+                            # IP-Limit respektieren: kurzen Backoff
+                            await asyncio.sleep(min(2 ** attempt, 3))
+                            continue
+                        r.raise_for_status()
+                        data = await r.json()
+                    bids = data.get("bids", [])
+                    asks = data.get("asks", [])
+                    self.bids = {float(p): float(q) for p, q in bids if float(q) > 0}
+                    self.asks = {float(p): float(q) for p, q in asks if float(q) > 0}
+                    self.last_u = int(data.get("lastUpdateId", 0))
+                    self.initialized = bool(self.bids and self.asks)
+                    self._last_rest_snapshot_s = int(time.time())
+                    return self.initialized
+                except Exception:
+                    await asyncio.sleep(min(2 ** attempt, 3))
+            return False
+        finally:
+            self._rest_inflight = False
 
     def apply_diff(self, evt: dict) -> bool:
         """Diff-Event anwenden. Returns False bei Gap (nur nach Init)."""
         U = evt.get('U')
         u = evt.get('u')
         bids_raw = evt.get('b', [])
         asks_raw = evt.get('a', [])
 
-        # Sequenz-Check erst NACH Initialisierung
+        # Sequenz-Check erst NACH Initialisierung
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
@@ -265,7 +363,7 @@ class LocalOrderbook:
 
     def get_top20(self) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
         """Top-20 Levels zurückgeben"""
         if not self.initialized:
-            return [], []
+            return [], []
         sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:20]
         sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:20]
         return sorted_bids, sorted_asks
@@ -339,6 +437,93 @@ class BookTickerL1:
     ask_qty: float = 0.0
     timestamp_ms: int = 0
 
+# =============================================================================
+# NEU: ClickHouse-Writer (optional)
+# =============================================================================
+class ClickHouseWriter:
+    """
+    Schlanker HTTP-Writer für ClickHouse.
+    Aktiv nur, wenn CLICKHOUSE_URL gesetzt ist.
+    Erwartete Tabellen:
+      - trades_raw(ts,symbol,price,qty,is_buyer_maker,trade_id)
+      - ob_diffs_raw(ts,symbol,U,u,bids_prices,bids_qtys,asks_prices,asks_qtys)
+      - ob_top20_100ms(ts,symbol,best_bid,best_bid_qty,best_ask,best_ask_qty,bid_price,bid_qty,ask_price,ask_qty)
+      - l1_200ms(ts,symbol,best_bid,best_ask,bid_qty,ask_qty,spread,mid,microprice,ofi_200ms,trade_count,vol_base,vol_quote)
+    """
+    def __init__(self):
+        self.enabled = bool(CLICKHOUSE_URL)
+        self.session: Optional[aiohttp.ClientSession] = None
+        self._q_trades: List[tuple] = []
+        self._q_diffs: List[tuple] = []
+        self._q_top20: List[tuple] = []
+        self._q_l1: List[tuple] = []
+        self._flush_task: Optional[asyncio.Task] = None
+        self._flush_interval = 0.25  # alle 250 ms flush
+        self.logger = logging.getLogger("ClickHouseWriter")
+
+    async def start(self):
+        if not self.enabled:
+            return
+        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
+        self._flush_task = asyncio.create_task(self._autoflush())
+        self.logger.info("ClickHouseWriter enabled")
+
+    async def stop(self):
+        if self._flush_task:
+            self._flush_task.cancel()
+            with suppress(asyncio.CancelledError):
+                await self._flush_task
+        if self.session:
+            await self.session.close()
+
+    async def _autoflush(self):
+        while True:
+            try:
+                await asyncio.sleep(self._flush_interval)
+                await self.flush_all()
+            except Exception as e:
+                self.logger.debug(f"CH autoflush error: {e}")
+
+    def add_trade(self, ts_ms: int, symbol: str, price: float, qty: float, is_buyer_maker: int, trade_id: int):
+        self._q_trades.append((ts_ms/1000.0, symbol, price, qty, is_buyer_maker, trade_id))
+        if len(self._q_trades) >= 2000:
+            asyncio.create_task(self._flush_trades())
+
+    def add_ob_diff(self, ts_ms: int, symbol: str, U: int, u: int,
+                    bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]):
+        bp = [b for b, _ in bids]; bq = [q for _, q in bids]
+        ap = [p for p, _ in asks]; aq = [q for _, q in asks]
+        self._q_diffs.append((ts_ms/1000.0, symbol, U, u, json.dumps(bp), json.dumps(bq),
+                              json.dumps(ap), json.dumps(aq)))
+        if len(self._q_diffs) >= 1000:
+            asyncio.create_task(self._flush_diffs())
+
+    def add_top20_snapshot(self, ts_ms: int, symbol: str,
+                           best_bid: float, best_bid_qty: float, best_ask: float, best_ask_qty: float,
+                           bid_levels: List[Tuple[float, float]], ask_levels: List[Tuple[float, float]]):
+        bid_price = [b for b, _ in bid_levels]; bid_qty = [q for _, q in bid_levels]
+        ask_price = [p for p, _ in ask_levels]; ask_qty = [q for _, q in ask_levels]
+        self._q_top20.append((ts_ms/1000.0, symbol, best_bid, best_bid_qty, best_ask, best_ask_qty,
+                              json.dumps(bid_price), json.dumps(bid_qty), json.dumps(ask_price), json.dumps(ask_qty)))
+        if len(self._q_top20) >= 1000:
+            asyncio.create_task(self._flush_top20())
+
+    def add_l1_200ms(self, ts_ms: int, symbol: str,
+                     bb: float, ba: float, bq: float, aq: float, spread: float, mid: float,
+                     microprice: float, ofi_200ms: float, trade_count: int, vol_base: float, vol_quote: float):
+        self._q_l1.append((ts_ms/1000.0, symbol, bb, ba, bq, aq, spread, mid,
+                           microprice, ofi_200ms, trade_count, vol_base, vol_quote))
+        if len(self._q_l1) >= 2000:
+            asyncio.create_task(self._flush_l1())
+
+    async def _insert_values(self, table: str, columns: List[str], rows: List[tuple]):
+        if not self.enabled or not rows:
+            return
+        assert self.session is not None
+        cols = ",".join(columns)
+        values_chunks = ",".join(self._tuple_sql(r) for r in rows)
+        data = f"INSERT INTO {CLICKHOUSE_DB}.{table} ({cols}) VALUES {values_chunks}"
+        auth = aiohttp.BasicAuth(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_USER else None
+        async with self.session.post(f"{CLICKHOUSE_URL}", data=data.encode("utf-8"), auth=auth) as resp:
+            if resp.status != 200:
+                txt = await resp.text()
+                self.logger.error(f"CH insert {table} failed: {resp.status} {txt}")
+
+    @staticmethod
+    def _tuple_sql(row: tuple) -> str:
+        def enc(v: Any) -> str:
+            if v is None: return "NULL"
+            if isinstance(v, (int, float)): return str(v)
+            return f"'{str(v).replace(\"'\",\"''\")}'"
+        return "(" + ",".join(enc(v) for v in row) + ")"
+
+    async def flush_all(self):
+        await asyncio.gather(self._flush_trades(), self._flush_diffs(), self._flush_top20(), self._flush_l1())
+    async def _flush_trades(self):
+        rows, self._q_trades = self._q_trades, []
+        await self._insert_values("trades_raw",
+                                  ["ts","symbol","price","qty","is_buyer_maker","trade_id"], rows)
+    async def _flush_diffs(self):
+        rows, self._q_diffs = self._q_diffs, []
+        await self._insert_values("ob_diffs_raw",
+                                  ["ts","symbol","U","u","bids_prices","bids_qtys","asks_prices","asks_qtys"], rows)
+    async def _flush_top20(self):
+        rows, self._q_top20 = self._q_top20, []
+        await self._insert_values("ob_top20_100ms",
+                                  ["ts","symbol","best_bid","best_bid_qty","best_ask","best_ask_qty",
+                                   "bid_price","bid_qty","ask_price","ask_qty"], rows)
+    async def _flush_l1(self):
+        rows, self._q_l1 = self._q_l1, []
+        await self._insert_values("l1_200ms",
+                                  ["ts","symbol","best_bid","best_ask","bid_qty","ask_qty","spread","mid",
+                                   "microprice","ofi_200ms","trade_count","vol_base","vol_quote"], rows)
 
 class SymbolState:
     """State für ein Symbol"""
     def __init__(self, symbol: str):
@@ -359,6 +544,8 @@ class SymbolState:
         self.liq = LiqAgg()
         self.mark = MarkSnap()
         # Preisreihen innerhalb 3s
         self.mid_prices: List[float] = []        # Midquotes (aus Depth)
         self.trade_prices: List[float] = []      # echte Trade-Preise
         self.trade_ts: List[int] = []            # Timestamps der Trades (ms)
+        # HF-Sampling
+        self._last_l1_sample_ms: int = 0
         self.last_close: Optional[float] = None
         # Flow/L1 State innerhalb Fenster
         self.prev_bid_p: Optional[float] = None
@@ -424,16 +611,28 @@ class Shard:
         self.state: Dict[str, SymbolState] = {s: SymbolState(s) for s in symbols}
         now_ms = int(time.time() * 1000)
         self.win_start = floor_to_grid(now_ms, 3000)
         self.logger = logging.getLogger(f"Shard-{shard_id}")
         self.is_running = True
-        self.timer_task = None
+        self.timer_task = None
         # Zähler/Diagnose
         self.last_rows_written = 0
         self.last_rows_zero = 0
         self.cum_rows_written = 0
         self.cum_rows_zero = 0
         self.zero_reported: set = set()  # Symbole, für die Grund bereits geloggt wurde
         # REST-Scheduler Referenz (für OI/L/S)
         self.rest_sched = None  # wird im main() gesetzt
+        # HTTP-Session für REST-Snapshot & ClickHouse
+        self.http_session: Optional[aiohttp.ClientSession] = None
+        # ClickHouse-Writer
+        self.ch = ClickHouseWriter()
+        # HF-Timer
+        self._top20_next_ms = floor_to_grid(now_ms, TOP20_SNAPSHOT_MS) + TOP20_SNAPSHOT_MS
+        self._l1_next_ms    = floor_to_grid(now_ms, L1_SAMPLE_MS)      + L1_SAMPLE_MS
+        # Hintergrund-Loops
+        self._hf_tasks: List[asyncio.Task] = []
 
     async def connect(self):
         """WebSocket Connection für diesen Shard"""
         streams = []
         for sym in self.symbols:
             base = sym.lower()
             streams.append(f"{base}@depth20@100ms")
-            streams.append(f"{base}@aggTrade")
+            streams.append(f"{base}@trade")
             # bookTicker entfällt – L1 aus Depth
 
         uri = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
         self.logger.info(f"Connecting {len(self.symbols)} symbols, {len(streams)} streams")
 
-        # WS starten; Orderbücher bootstrappen rein über Depth-Events
+        # HTTP-Session & CH-Writer starten
+        self.http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
+        await self.ch.start()
+
+        # Start-Bootstrap via REST-Snapshot (Limits schonend, mit Jitter)
+        await self._bootstrap_all_symbols()
 
         while self.is_running:
             try:
                 async with websockets.connect(uri, ping_interval=20, ping_timeout=10, open_timeout=15) as ws:
                     self.logger.info(f"WebSocket connected!")
 
                     if self.timer_task is None:
                         self.timer_task = asyncio.create_task(self.flush_timer())
+                    if not self._hf_tasks:
+                        self._hf_tasks = [
+                            asyncio.create_task(self._top20_snapshot_loop()),
+                            asyncio.create_task(self._l1_200ms_loop())
+                        ]
 
                     async for msg in ws:
                         if not self.is_running:
                             break
                         try:
                             payload = json.loads(msg)
                             stream = payload.get('stream', '')
                             data = payload.get('data', {})
 
                             if '@depth20@100ms' in stream:
                                 await self.on_depth(data)
-                            elif '@aggTrade' in stream:
-                                await self.on_agg_trade(data)
+                            elif '@trade' in stream:
+                                await self.on_trade_raw(data)
                             # kein bookTicker-Handler
                         except Exception as e:
                             self.logger.debug(f"Handler error: {e}")
             except Exception as e:
                 self.logger.error(f"Connection error: {e}")
                 await asyncio.sleep(3)
+        # Shutdown
+        if self.http_session:
+            await self.http_session.close()
+        await self.ch.stop()
+
+    async def _bootstrap_all_symbols(self):
+        """Initialer REST-Snapshot für alle Symbole (sanft, verteilt)."""
+        if not self.http_session:
+            return
+        for i, sym in enumerate(self.symbols):
+            st = self.state[sym]
+            # kleiner Jitter pro Symbol, um Bursts zu vermeiden
+            await asyncio.sleep(0.05 + (i % 10) * 0.01)
+            with suppress(Exception):
+                await st.ob.rest_snapshot(self.http_session, REST_DEPTH_LIMIT)
+
+    async def _top20_snapshot_loop(self):
+        """Alle 100 ms Top-20-Snapshots in ClickHouse schreiben (falls enabled)."""
+        while self.is_running:
+            now_ms = int(time.time() * 1000)
+            if now_ms >= self._top20_next_ms:
+                self._top20_next_ms += TOP20_SNAPSHOT_MS
+                if self.ch.enabled:
+                    for sym, st in self.state.items():
+                        if not st.ob.initialized:
+                            continue
+                        bids, asks = st.ob.get_top20()
+                        bb, bq, ba, aq = st.ob.get_l1()
+                        if bb and ba:
+                            self.ch.add_top20_snapshot(now_ms, sym, bb or 0.0, bq or 0.0, ba or 0.0, aq or 0.0,
+                                                       bids, asks)
+            await asyncio.sleep(0.01)
+
+    async def _l1_200ms_loop(self):
+        """Alle 200 ms L1/Spread/Microprice/OFI & Trade-Rollup in ClickHouse (schlank)."""
+        while self.is_running:
+            now_ms = int(time.time() * 1000)
+            if now_ms >= self._l1_next_ms:
+                self._l1_next_ms += L1_SAMPLE_MS
+                if self.ch.enabled:
+                    for sym, st in self.state.items():
+                        bb, bq, ba, aq = st.ob.get_l1()
+                        if not (bb and ba):
+                            continue
+                        spread = ba - bb
+                        mid = (bb + ba) / 2.0
+                        micro = 0.0
+                        if bq and aq and (bq + aq) > 0:
+                            micro = (aq * bb + bq * ba) / (bq + aq)
+                        # 200ms-OFI: wir nutzen das 3s-OFI nicht, sondern setzen hier 0 (leichtgewichtig)
+                        ofi_200 = 0.0
+                        # kleine Trade-Rollups (200ms) – aus den letzten 200ms Trades
+                        # Wir approximieren: nehmen alle Trades, deren ts in [now_ms-200, now_ms) liegt
+                        tc = 0; vb = 0.0; vq = 0.0
+                        if st.trade_ts:
+                            lo = now_ms - L1_SAMPLE_MS
+                            for ts, px, qty in zip(st.trade_ts, st.trade_prices, st.trades.trade_sizes or []):
+                                if lo <= ts < now_ms:
+                                    tc += 1; vb += qty; vq += qty * px
+                        self.ch.add_l1_200ms(now_ms, sym, bb, ba, bq or 0.0, aq or 0.0, spread, mid,
+                                             micro, ofi_200, tc, vb, vq)
+            await asyncio.sleep(0.01)
 
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
 
-        success = st.ob.apply_diff(evt)
+        success = st.ob.apply_diff(evt)
         if not success:
             st.flags['resynced_this_window'] = 1
+            # Bei Gap: sanftes REST-Resync anstoßen (mit Cooldown)
+            if self.http_session:
+                asyncio.create_task(st.ob.rest_snapshot(self.http_session, REST_DEPTH_LIMIT))
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
+            # Optional: Diffs raw in ClickHouse schreiben
+            if self.ch.enabled:
+                self.ch.add_ob_diff(int(time.time()*1000), symbol, evt['U'], evt['u'],
+                                    [(float(p), float(q)) for p, q in evt['b']],
+                                    [(float(p), float(q)) for p, q in evt['a']])
 
-    async def on_agg_trade(self, data: dict):
-        """Handle @aggTrade"""
+    async def on_trade_raw(self, data: dict):
+        """Handle @trade (jeder Fill einzeln)"""
         symbol = data.get('s')
         if symbol not in self.state:
             return
 
         st = self.state[symbol]
-        price = float(data.get('p', 0))
-        qty = float(data.get('q', 0))
-        is_buyer_maker = data.get('m')
-        trade_id = data.get('a', 0)
+        price = float(data.get('p', 0))
+        qty = float(data.get('q', 0))
+        is_buyer_maker = 1 if data.get('m') else 0
+        trade_id = int(data.get('t', data.get('a', 0)))
 
         st.trades.count += 1
         st.trades.vol_base += qty
         st.trades.vol_quote += price * qty
 
-        if not is_buyer_maker:
+        if not is_buyer_maker:
             st.trades.taker_buy_vol += qty
             st.trades.buy_count += 1
         else:
             st.trades.taker_sell_vol += qty
             st.trades.sell_count += 1
 
         st.trades.trade_sizes.append(qty)
         st.trade_prices.append(price)
         st.trade_ts.append(int(time.time() * 1000))
 
         if st.trades.first_trade_id == 0:
             st.trades.first_trade_id = trade_id
         st.trades.last_trade_id = trade_id
 
         st.flags['has_trades'] = 1
+        # RAW in ClickHouse
+        if self.ch.enabled:
+            self.ch.add_trade(int(time.time()*1000), symbol, price, qty, is_buyer_maker, trade_id)
 
         # Midquote wird bereits in on_depth gesammelt
 
     # bookTicker-Handler entfernt
 
-    # kein REST-Resync – nur WS-Bootstrap
+    # REST-Resync wird in on_depth() bei Gaps angestoßen
 
     async def flush_timer(self):
         """3-Sekunden-Timer"""
         while self.is_running:
             now_ms = int(time.time() * 1000)

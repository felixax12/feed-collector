
==============================
ERGAENZUNG: CLICKHOUSE FUER LIVE ANALYTICS (analytics_runtime)
==============================

Ueberblick
----------
- Dies ist die Live-Analytics-Ingest-Pipeline (Dashboard/Insights), getrennt vom Minimal Recorder.
- Wir nutzen denselben ClickHouse-Server, aber eine eigene Datenbank (Default: analytics).
- Alle Events aus dem Live-System gehen in eine zentrale Tabelle `analytics_events` (JSON-Payload), plus spezialisierte Tabellen fuer Latenzpfade.

Unterschied der beiden Systeme (klar getrennt)
---------------------------------------------
1) Minimal Recorder
   - Datenbank: `arb_db`
   - Tabellen: `arb_events`, `arb_snapshots`
   - Environment: `MR_CLICKHOUSE_*`

2) Live Analytics (analytics_runtime)
   - Datenbank: `analytics`
   - Tabellen: `analytics_events`, `analytics_ingest_health`, `system_latency_windows`, `trade_latency_events`, `latency_timelines`, `trade_latency_metrics`
   - Environment: `ANALYTICS_CH_*`

Vollstaendiges Setup fuer Live Analytics
----------------------------------------
Quickstart (empfohlen):
```bash
# Container starten:
docker-compose up -d

# Schema importieren:
# PowerShell:
Get-Content analytics_runtime\clickhouse_schema.sql | docker exec -i feed_clickhouse clickhouse-client -u feeduser --password feedpass --multiquery

# WSL/Bash:
docker exec -i feed_clickhouse clickhouse-client -u feeduser --password feedpass --multiquery < analytics_runtime/clickhouse_schema.sql
```

**Siehe STARTUP.md fuer vollstaendige Anleitung.**

Environment Variablen (Live Analytics)
--------------------------------------
- `ANALYTICS_CH_URL=http://localhost:8124`
- `ANALYTICS_CH_USER=feeduser`
- `ANALYTICS_CH_PASSWORD=feedpass`
- `ANALYTICS_CH_DB=analytics`
- `ANALYTICS_CH_EVENTS_TABLE=analytics_events`
- `ANALYTICS_CH_INGEST_HEALTH_TABLE=analytics_ingest_health`
- `ANALYTICS_CH_SYSTEM_LATENCY_TABLE=system_latency_windows`
- `ANALYTICS_CH_TRADE_LATENCY_TABLE=trade_latency_events`
- `ANALYTICS_CH_LATENCY_TIMELINES_TABLE=latency_timelines`
- `ANALYTICS_CH_TRADE_LATENCY_METRICS_TABLE=trade_latency_metrics`
- `ANALYTICS_CH_ASYNC_INSERT=1`
- `ANALYTICS_CH_WAIT_FOR_ASYNC_INSERT=0`
- `ANALYTICS_CH_BATCH_MS=500`
- `ANALYTICS_CH_BATCH_SIZE=500`
- `ANALYTICS_CH_MAX_QUEUE=10000`
- `ANALYTICS_WRITER_HEALTH_INTERVAL_S=2.5`

Wichtig: Environment pro Shell
------------------------------
Die Variablen muessen im selben Terminal gesetzt werden, in dem du
`python system_control.py` startest.

PowerShell:
```
$env:ANALYTICS_CH_URL="http://localhost:8124"
$env:ANALYTICS_CH_USER="feeduser"
$env:ANALYTICS_CH_PASSWORD="feedpass"
$env:ANALYTICS_CH_DB="analytics"
```

CMD:
```
set ANALYTICS_CH_URL=http://localhost:8124
set ANALYTICS_CH_USER=feeduser
set ANALYTICS_CH_PASSWORD=feedpass
set ANALYTICS_CH_DB=analytics
```

WSL/bash:
```
export ANALYTICS_CH_URL=http://localhost:8124
export ANALYTICS_CH_USER=feeduser
export ANALYTICS_CH_PASSWORD=feedpass
export ANALYTICS_CH_DB=analytics
```

Hinweis: Wenn `ANALYTICS_CH_URL` nicht gesetzt ist, schreibt die Analytics-Pipeline
weiter in CSV (Fallback) und ClickHouse bleibt leer.

Start (Live System)
-------------------
- Wenn du das Live-System ueber `system_control.py` startest (Multiprocess-Stack), wird die Analytics-Pipeline automatisch mitgestartet.
- Alternativ direkt:
  ```
  python -m analytics_runtime.runner
  ```

Schnelle Checks (Live Analytics)
--------------------------------
- Rows zaehlen:
  ```
  docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT count() FROM analytics.analytics_events"
  ```
- Health-Events sehen:
  ```
  docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT * FROM analytics.analytics_events WHERE event_type='analytics.writer_health' ORDER BY ingest_timestamp_ms DESC LIMIT 5"
  ```
- Ingest-Health Tabelle:
  ```
  docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT * FROM analytics.analytics_ingest_health ORDER BY event_timestamp_ms DESC LIMIT 5"
  ```
- Latenz-Events:
  ```
  docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT count() FROM analytics.trade_latency_events"
  ```

Daten loeschen (Live Analytics - Schema bleibt)
------------------------------------------------
**Alle Analytics-Tabellen auf einmal leeren:**
```bash
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --multiquery "
TRUNCATE TABLE analytics.analytics_events;
TRUNCATE TABLE analytics.analytics_ingest_health;
TRUNCATE TABLE analytics.system_latency_windows;
TRUNCATE TABLE analytics.trade_latency_events;
TRUNCATE TABLE analytics.latency_timelines;
TRUNCATE TABLE analytics.trade_latency_metrics;
"
```

**Oder einzeln:**
```bash
# Nur Events:
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "TRUNCATE TABLE analytics.analytics_events"

# Nur System Latency:
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "TRUNCATE TABLE analytics.system_latency_windows"

# Nur Trade Latency:
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "TRUNCATE TABLE analytics.trade_latency_events"
```

**Verifizieren (sollte 0 zeigen):**
```bash
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT count() FROM analytics.analytics_events"
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT count() FROM analytics.system_latency_windows"
```

Hinweise
--------
- Die zentrale Tabelle `analytics_events` ist CSV-kompatibel via View `analytics.analytics_events_compat`.
- CSV-Ausgaben sind Legacy und wurden in `analytics_runtime/old_system/output` archiviert.
- Redis bleibt fuer Live-Streams, ClickHouse ist fuer Historie/Queries.





































CLICKHOUSE CHEATSHEET (Minimal Recorder)
========================================

Schnellstart (Docker)
---------------------
1. Volume (einmalig):
   ```
   docker volume create clickhouse_data
   ```
2. Server starten:
   ```
   docker run -d -p 8124:8123 -p 9001:9000 -e CLICKHOUSE_USER=feeduser -e CLICKHOUSE_PASSWORD=feedpass -v clickhouse_data:/var/lib/clickhouse --name feed_clickhouse clickhouse/clickhouse-server

   ```
3. Healthcheck:
   - HTTP: `curl http://localhost:8124/`  → Ok
   - Logs: `docker logs --tail 50 feed_clickhouse`

DB und Tabelle für Minimal Recorder
-----------------------------------
In den Container gehen oder via exec:
```
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass
```
Dann:
```
CREATE DATABASE IF NOT EXISTS arb_db;

CREATE TABLE IF NOT EXISTS arb_db.arb_events (
  ts_recv_ms UInt64,
  ts_feed_min_ms UInt64,
  ts_feed_max_ms UInt64,
  symbol String,
  buy_exchange LowCardinality(String),
  sell_exchange LowCardinality(String),
  buy_ask Float64,
  sell_bid Float64,
  spread_abs Float64,
  spread_pct Float64,
  status LowCardinality(String),
  reason LowCardinality(String),
  side LowCardinality(String),
  latency_ms Nullable(Float64),
  seq UInt64
) ENGINE = MergeTree
PARTITION BY toDate(ts_feed_max_ms / 1000)
ORDER BY (symbol, ts_feed_max_ms, buy_exchange, sell_exchange);

CREATE TABLE IF NOT EXISTS arb_db.arb_snapshots (
  ts_recv_ms UInt64,
  symbol String,
  buy_exchange LowCardinality(String),
  sell_exchange LowCardinality(String),
  buy_bid Float64,
  buy_ask Float64,
  buy_bid_qty Float64,
  buy_ask_qty Float64,
  sell_bid Float64,
  sell_ask Float64,
  sell_bid_qty Float64,
  sell_ask_qty Float64,
  spread_abs Float64,
  spread_pct Float64,
  seq UInt64
) ENGINE = MergeTree
PARTITION BY toDate(ts_recv_ms / 1000)
ORDER BY (symbol, ts_recv_ms, buy_exchange, sell_exchange);
```
Optional Top-of-Book Referenz (falls benötigt):
```
CREATE TABLE IF NOT EXISTS arb_db.tob_quotes (
  ts_feed_ms UInt64,
  ts_recv_ms UInt64,
  symbol String,
  exchange LowCardinality(String),
  bid Float64,
  ask Float64,
  latency_ms Float64
) ENGINE = MergeTree
PARTITION BY toDate(ts_feed_ms / 1000)
ORDER BY (symbol, ts_feed_ms, exchange);
```

Schnelle Checks
---------------
- Zeilen zählen:  
  `docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT count() FROM arb_db.arb_events"`  
  `docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT count() FROM arb_db.arb_snapshots"`
- Letzte Events ansehen:  
  `docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT * FROM arb_db.arb_events ORDER BY ts_feed_max_ms DESC LIMIT 20"`  
  `docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SELECT * FROM arb_db.arb_snapshots ORDER BY ts_recv_ms DESC LIMIT 20"`
- Datenbanken/Tabellen:  
  `docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SHOW DATABASES"`  
  `docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "SHOW TABLES FROM arb_db"`

Einfaches Monitoring für Arbs
-----------------------------
- Anzahl enter pro Tag:  
  `SELECT toDate(ts_feed_max_ms/1000) d, countIf(status='enter') FROM arb_db.arb_events GROUP BY d ORDER BY d;`
- Größter Spread je Paar:  
  `SELECT symbol,buy_exchange,sell_exchange,max(spread_pct) AS max_spread FROM arb_db.arb_events WHERE status IN ('enter','update') GROUP BY symbol,buy_exchange,sell_exchange ORDER BY max_spread DESC LIMIT 20;`
- Durchschnittliche Lebensdauer (ms) pro Paar (vereinfachte Enter→Exit):  
  ```
  WITH
    groupArray((ts_feed_max_ms,status)) AS ev,
    arraySort(ev) AS evs,
    arrayFilter(x -> x.2='enter', evs) AS enters,
    arrayFilter(x -> x.2 IN ('exit','missing_book'), evs) AS exits
  SELECT symbol, buy_exchange, sell_exchange,
    avg(exits[i].1 - enters[i].1) AS avg_lifetime_ms
  FROM arb_db.arb_events
  GROUP BY symbol, buy_exchange, sell_exchange;
  ```
- Snapshot-Qualität (Sample):  
  `SELECT symbol, buy_exchange, sell_exchange, max(spread_pct) AS max_spread, count() AS n FROM arb_db.arb_snapshots GROUP BY symbol,buy_exchange,sell_exchange ORDER BY max_spread DESC LIMIT 10;`

Server steuern
--------------
- Stoppen: `docker stop feed_clickhouse`
- Starten: `docker start feed_clickhouse`
- Entfernen (inkl. Volume, falls du komplett neu willst):
  `docker stop feed_clickhouse && docker rm feed_clickhouse && docker volume rm clickhouse_data`

Daten löschen (behält Schema)
------------------------------
Alle Events und Snapshots löschen, ohne Tabellen/Schema zu entfernen:

**Via Docker Exec:**
```bash
docker exec -it feed_clickhouse clickhouse-client --user feeduser --password feedpass --database arb_db --multiquery <<'EOF'
TRUNCATE TABLE arb_events;
TRUNCATE TABLE arb_snapshots;
EOF
```

**Via HTTP (PowerShell/curl):**
```bash
curl -X POST "http://localhost:8124/?user=feeduser&password=feedpass&database=arb_db" --data-binary "TRUNCATE TABLE arb_events; TRUNCATE TABLE arb_snapshots;"
```

**Verifizieren (sollte 0 zeigen):**
```bash
docker exec -it feed_clickhouse clickhouse-client --user feeduser --password feedpass --query "SELECT count() FROM arb_db.arb_events"
docker exec -it feed_clickhouse clickhouse-client --user feeduser --password feedpass --query "SELECT count() FROM arb_db.arb_snapshots"
```

**Via HTTP:**
```bash
curl "http://localhost:8124/?user=feeduser&password=feedpass&query=SELECT%20count()%20FROM%20arb_db.arb_events"
curl "http://localhost:8124/?user=feeduser&password=feedpass&query=SELECT%20count()%20FROM%20arb_db.arb_snapshots"
```

Recorder-Env-Variablen (Reminder)
---------------------------------
- `MR_CLICKHOUSE_URL=http://localhost:8124`
- `MR_CLICKHOUSE_USER=feeduser`
- `MR_CLICKHOUSE_PASSWORD=feedpass`
- `MR_MIN_NOTIONAL=10.0` (nur BBO ≥ 10 USD)
- `MR_SPREAD_THRESHOLD=0.002` (0.2%)

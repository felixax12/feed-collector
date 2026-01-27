# ClickHouse Datenablage & Hardware-Orientierung (Preset 2.1 / markPrice@1s)

Ziel dieses Dokuments: **klar und knapp** beschreiben, wo die Daten liegen, in welchem Format, wie man darauf zugreift, und welche Hardware-Klassen realistisch sind. Kein Health/Monitoring-Text.

## 1) Zugang zu ClickHouse (lokal)
Quelle: `feeds/feeds.yml`
- DSN: `http://feeduser:feedpass@localhost:8124`
- Database: `marketdata`
- User: `feeduser`
- Passwort: `feedpass`

Beispiel (aus `feeds/feeds.yml`):
```
defaults:
  redis:
    dsn: redis://localhost:6380/0
    pipeline_size: 200
    flush_interval_ms: 50
    stream_maxlen: 1000
    cluster: false
  clickhouse:
    dsn: http://feeduser:feedpass@localhost:8124
    database: marketdata
    batch_rows: 5000
    flush_interval_ms: 250
    compression: lz4
  enable_redis: true
  enable_clickhouse: true
  housekeep_interval_s: 30

exchanges: []
```

Hinweis: In anderen Umgebungen werden diese Werte typischerweise via `feeds/feeds.yml` oder ENV ueberschrieben.

## 2) Tabellen & Format (relevant fuer Preset 2.1)
**Tabelle:** `marketdata.mark_price`

Schema (aus `setup_clickhouse_feeds.py`):
- `exchange` String
- `market_type` String
- `instrument` String
- `ts_event_ns` UInt64
- `ts_recv_ns` UInt64
- `mark_price` Decimal(38, 18)
- `index_price` Nullable(Decimal(38, 18))

Zeitstempel-Hinweis:
- Binance liefert `ts_event` in **ms**.
- Im Event wird dieser Wert aktuell als `ts_event_ns` gespeichert (ms-Wert im UInt64-Feld). Das muss im Analyse-Repo klar beruecksichtigt werden.

## 3) Datenzugriff (Orientierung, keine Anleitung)
- Daten liegen **pro Symbol** in der Tabelle `marketdata.mark_price`.
- Abfragen erfolgen typischerweise ueber `instrument` + Zeitfenster (`ts_event_ns`).
- Backtesting liest direkt aus `marketdata.mark_price` (1s Zeitreihe je Symbol).

## 4) Hardware-Orientierung (Praxis-Skala)
**Wichtiger Punkt:** Die reale Last ist `N Symbole * 1 Event/s` fuer markPrice@1s. Die Schreiblast skaliert linear mit N.

Grobe Einschaetzung nach Klasse:
- **Raspberry Pi / Low-Power ARM**: nicht empfehlenswert fuer stabile 24/7 Writes in ClickHouse (Disk/IO Bottleneck).
- **Laptop (Consumer, 4-8 Kerne, 16-32 GB RAM, NVMe)**: ok fuer Preset 2.1 (markPrice only) mit moderater Symbolanzahl. Risiko: Thermal/IO Limits.
- **VPS (4-8 vCPU, 16-32 GB RAM, NVMe)**: solide fuer Preset 2.1, guter 24/7 Betrieb moeglich.
- **Server (>= 8 vCPU, 32-64 GB RAM, NVMe RAID)**: ideal fuer langfristige, stabile Ingest-Last und zusaetzliche Kanale.

### CPU/RAM/IO Messung (direkt aus `run_feeds.py`)
Beim Start loggt `run_feeds.py` alle 10s eine `[sys]` Zeile:\n
- `cpu` in % (Prozess)\n
- `rss` in MB\n
- optional IO-Deltas (`io_read`, `io_write`)\n

Diese Werte sind die Grundlage fuer die Entscheidung: Laptop/VPS/Server.  
Wirklich belastbare Hardware-Entscheidung braucht Messwerte (durchschnittliche Rows/s, CPU/RAM/IO-Profil).

### Gemessene Werte (ab 2026-01-27 13:03:26)
**ClickHouse (Docker, `logs/docker_stats.log`)**
- CPU %: min 5.78, max 31.37, avg 13.88, med 12.65
- RAM (MiB): min 879.7, max 1024.0, avg 956.26, med 958.2
- RAM-Limit: 15.47 GiB (konstant)

**Python Prozess (`logs/feed_runtime.log`)**
- CPU %: min 1.9, max 9.3, avg 4.78, med 4.75
- RAM (MB): min 73.5, max 75.8, avg 74.99, med 75.2

## 5) Kontext fuer Analyse-Repo
- Primärquelle fuer markPrice@1s: `marketdata.mark_price`
- Zeitfensterabfragen laufen ueber `ts_event_ns` + `instrument`
- Der Zugriff erfolgt ueber ClickHouse HTTP (8124) oder native Clients.

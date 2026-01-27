Alles klar. Ich liefere dir jetzt eine saubere, universale Vorlage: ein einheitliches Format (Redis + ClickHouse), ein modulartiges Projekt-Layout, klare Rollen (Starter, Adapter, Writer), plus ganz konkrete Anleitungen, wie ein einzelner Feed/Adapter zu coden ist. Ziel: Du kannst diese Schablone in jeden Ordner kopieren, nur die Exchange-spezifische Logik ergänzen, per Konfig an-/abschalten und sofort stabil laufen lassen.

Ziele (Kurzfassung)

Einheitliches, exchange-übergreifendes Kanonschema für alle Feeds (Trades, L1, OB Top5/Top20, OB-Diffs, Liquidations, Klines, Mark/Funding, Advanced Metrics).

Eine Konfig steuert: welche Exchanges, welche Feeds, wohin (Redis, ClickHouse, beides).

Ein Modul mit Starter, Writer, Adaptern; austauschbar und outsourcbar.

Redis dient als Heiß-Cache (letzter Zustand + kleine Sliding Windows), ClickHouse als Langzeitspeicher (schnelle Abfragen).

1) Ein universales Datenmodell (Kanonschema)
Namens- und Typkonventionen (überall)

exchange als LowCardinality(String)/Enum (z. B. binance_futures).

market_type: spot|perp_linear|perp_inverse|delivery.

instrument: String exakt wie bei der Börse (z. B. ETHUSDT).

Zeit: ts_event (Nanosekunden seit Epoch, Ereigniszeit der Börse), ts_recv (Nanosekunden am Collector).

Preise/Mengen: Decimal (nicht Float). In CH: Decimal(38,18); in Redis als String.

Seiten: side als B|A (Bid/Ask) oder BUY|SELL.

Währungen/Einheiten nicht in den Spaltennamen; einheitlich in Doku/Metadaten.

2) Redis-Formate (Heiß-Cache, schnelles Lesen)

Grundprinzip: pro Instrument/Feed schlanke HASHes für „letzten Zustand“, plus optional Streams für kurze Fenster.

Schlüsselräume (Keyspace)

Last-Value pro Instrument:

last:l1:{exchange}:{instrument} → HASH

last:top5:{exchange}:{instrument} → HASH

last:top20:{exchange}:{instrument} → HASH

last:mark:{exchange}:{instrument} → HASH

last:funding:{exchange}:{instrument} → HASH

last:oi:{exchange}:{instrument} → HASH

last:adv:{exchange}:{instrument} → HASH (Advanced Metrics)

Letzte Trades als schmale Streams (optional, kleines Fenster):

stream:trades:{exchange}:{instrument} → STREAM (XADD mit MAXLEN ~1000)

Hash-Felder bitte flach und stabil benennen. Werte als Strings (Decimal/Integer) für roundtrip-sichere Verwendung.

L1 (Top of Book)

last:l1:binance_futures:ETHUSDT

bid_px, bid_sz, ask_px, ask_sz

last_px, last_sz (optional)

ts_event_ns, ts_recv_ns

Top5 / Top20 (kompakt)

last:top5:binance_futures:ETHUSDT
b1_px…b5_px, b1_sz…b5_sz, a1_px…a5_px, a1_sz…a5_sz, ts_event_ns, ts_recv_ns

last:top20:... analog bis b20_*/a20_*.
Hinweis: Für Redis nur aktuelle Levels. Historie geht nach ClickHouse.

Trades (Stream, kleines Fenster)

stream:trades:binance_futures:ETHUSDT

Fields: px, qty, side, trade_id (falls vorhanden), ts_event_ns, ts_recv_ns

Mark/Funding/OI

last:mark:* → mark_px, index_px, ts_event_ns, ts_recv_ns
last:funding:* → funding_rate, next_funding_ts_ns, ts_event_ns, ts_recv_ns
last:oi:* → open_interest, ts_event_ns, ts_recv_ns

Advanced Metrics (live berechnet)

last:adv:* → z. B. spread_px, spread_bps, imbalance_5, imbalance_20, trade_imb_1s, …

Leistung/Skalierung Redis

Immer Pipelining (100–500 Befehle pro Flush).

Cluster ab ~200k Ops/s: 3–6 Primaries, Replikas optional.

Key-Hash-Tags nutzen, wenn du pro Instrument zusammenhalten willst: last:l1:{binance_futures|ETHUSDT} (nur wenn du bewusst Slot-Steuerung brauchst).

3) ClickHouse-Schemata (Langzeit, Abfragen)

Eine Datenbank, Tabellen pro Event-Typ. Je Tabelle:

exchange LowCardinality(String), market_type LowCardinality(String), instrument String,

ts_event DateTime64(9), ts_recv DateTime64(9) (aus Int64 ns in DateTime64(9) gegossen),

Partition: toDate(ts_event),

Order: (instrument, ts_event) oder (exchange, instrument, ts_event).

DDL-Vorlagen (Kern)
Trades
CREATE TABLE marketdata.trades (
  exchange LowCardinality(String),
  market_type LowCardinality(String),
  instrument String,
  ts_event DateTime64(9),
  ts_recv  DateTime64(9),
  price Decimal(38,18),
  qty   Decimal(38,18),
  side  Enum8('BUY'=1,'SELL'=2),
  trade_id String
)
ENGINE = MergeTree
PARTITION BY toDate(ts_event)
ORDER BY (instrument, ts_event)
SETTINGS index_granularity = 8192;





Orderbuch-Diffs (empfohlen für L2-Historie)
CREATE TABLE marketdata.ob_diffs (
  exchange LowCardinality(String),
  market_type LowCardinality(String),
  instrument String,
  ts_event DateTime64(9),
  ts_recv  DateTime64(9),
  # Diff-Events: mehrere Reihen pro Update
  side Enum8('B'=1,'A'=2),
  level UInt16,                 -- 1..N (optional, wenn vom Feed geliefert)
  price Decimal(38,18),
  delta_qty Decimal(38,18)      -- +/-, 0 = delete
)
ENGINE = MergeTree
PARTITION BY toDate(ts_event)
ORDER BY (instrument, ts_event, side, price);



Orderbuch-Snapshots Top5 / Top20 (aktueller Zustand in Intervallen)
CREATE TABLE marketdata.ob_top5 (
  exchange LowCardinality(String),
  market_type LowCardinality(String),
  instrument String,
  ts_event DateTime64(9),
  ts_recv  DateTime64(9),
  b1_px Decimal(38,18), b1_sz Decimal(38,18), ... b5_px, b5_sz,
  a1_px Decimal(38,18), a1_sz Decimal(38,18), ... a5_px, a5_sz
)
ENGINE = MergeTree
PARTITION BY toDate(ts_event)
ORDER BY (instrument, ts_event);

CREATE TABLE marketdata.ob_top20 ( ... analog bis 20 ... ) ENGINE = MergeTree
PARTITION BY toDate(ts_event)
ORDER BY (instrument, ts_event);




L1 (200 ms)
CREATE TABLE marketdata.l1_200ms (
  exchange LowCardinality(String),
  market_type LowCardinality(String),
  instrument String,
  ts_event DateTime64(9),
  ts_recv  DateTime64(9),
  bid_px Decimal(38,18), bid_sz Decimal(38,18),
  ask_px Decimal(38,18), ask_sz Decimal(38,18),
  last_px Decimal(38,18), last_sz Decimal(38,18)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_event)
ORDER BY (instrument, ts_event);


Liquidations
CREATE TABLE marketdata.liquidations (
  exchange LowCardinality(String),
  instrument String,
  ts_event DateTime64(9),
  ts_recv  DateTime64(9),
  side Enum8('BUY'=1,'SELL'=2),
  price Decimal(38,18),
  qty   Decimal(38,18),
  order_id String
)
ENGINE = MergeTree
PARTITION BY toDate(ts_event)
ORDER BY (instrument, ts_event);


Klines (konfigurierbar)

CREATE TABLE marketdata.klines_agg (
  exchange LowCardinality(String),
  instrument String,
  interval LowCardinality(String), -- '1s','1m','5m',...
  open_time DateTime64(3),
  close_time DateTime64(3),
  open Decimal(38,18),
  high Decimal(38,18),
  low  Decimal(38,18),
  close Decimal(38,18),
  volume Decimal(38,18),
  trades UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(open_time)
ORDER BY (instrument, interval, open_time);



Mark / Funding / OI
CREATE TABLE marketdata.mark_price (... mark_px, index_px ...) ENGINE = MergeTree ...;
CREATE TABLE marketdata.funding    (... funding_rate, next_funding_ts ...) ENGINE = MergeTree ...;
CREATE TABLE marketdata.open_interest (... open_interest ...) ENGINE = MergeTree ...;


Advanced Metrics (live berechnet, optional)
#bisher in advanced metrics 1.5s von binance


Ingest-Empfehlungen

Micro-Batches (5–20k Rows) über HTTP async_insert=1&wait_for_async_insert=0.

Eine Writer-Instanz pro Prozess; Backpressure: bei Insert-Fehlern Batch auf Disk (Spool) und Retry.

4) Projekt-Layout (bulletproof, modular)

feeds/
  __init__.py
  config.yaml                    # zentrale Konfig
  main.py                        # Starter/Orchestrator
  core/
    adapters/
      base_adapter.py            # Basisklasse (Interface)
      binance_futures.py         # Exchange-spezifischer Adapter
      ... weitere_exchanges.py
    models/
      events.py                  # Kanonschemata (Pydantic/attrs)
    outputs/
      redis_writer.py            # Pipelined HSET/XADD
      ch_writer.py               # Micro-batch Insert
      router.py                  # Routing (an/aus nach config)
    utils/
      time.py, decimals.py, ob.py, logging.py
  runners/
    run_exchange.py              # optional einzelner Exchange-Runner
  ddl/
    clickhouse_create.sql        # alle DDLs zentral



Rollen

main.py liest config.yaml, erstellt pro Exchange einen Adapter-Prozess (oder Task) und einen OutputRouter.

router.py besitzt zwei interne Queues: q_redis, q_ch.

redis_writer.py flush’t alle 5–20 ms per Pipeline.

ch_writer.py bündelt 100–300 ms oder N Zeilen und macht Inserts.

events.py definiert ein Kanonschema je Event-Typ (Trade, OB_Diff, OB_Top5, OB_Top20, L1, Mark, Funding, OI, Liquidation, Kline, AdvMetric).

Konfig (Beispiel)
exchanges:
  - name: binance_futures
    enabled: true
    market_type: perp_linear
    symbols: []                   # leer = auto-discover
    feeds:
      trades:        { enabled: true }
      depth_diffs:   { enabled: true, speed: "250ms" }
      ob_top5:       { enabled: true, interval_ms: 100 }   # snapshot cadence
      ob_top20:      { enabled: false }
      l1:            { enabled: true, interval_ms: 200 }
      liquidations:  { enabled: true }
      mark_price:    { enabled: true }
      funding:       { enabled: true }
      klines:        { enabled: true, interval: "1m" }
      advanced:      { enabled: true }
    outputs:
      redis: { enabled: true, host: "127.0.0.1", port: 6379, db: 0, keyspace: "last" }
      clickhouse: { enabled: true, url: "http://ch:8123", database: "marketdata", user: "u", password: "p" }
runtime:
  max_inflight_msgs: 200000
  redis_pipeline_size: 500
  ch_batch_rows: 10000
  ch_batch_ms: 200



5) Wie man einen einzelnen Feed/Adapter codet (Leitfaden)
Adapter-Schnittstelle (Basisklasse)
class BaseAdapter:
    def __init__(self, cfg, router, logger):
        self.cfg = cfg           # exchange-spez. config
        self.router = router     # outputs/router.py
        self.log = logger

    async def run(self):
        await self.discover_symbols()
        await self.connect_streams()   # WS verbinden
        await self.event_loop()        # Nachrichten lesen & normalisieren

    async def discover_symbols(self): ...
    async def connect_streams(self): ...
    async def event_loop(self): ...



Normalisierung (immer gleich)

Roh-Message → Kanonevent aus events.py (z. B. TradeEvent, OBDiffEvent, L1Event …).

Pflichtfelder setzen (exchange, market_type, instrument, ts_event_ns, ts_recv_ns, …).

Numerik sofort in Decimal konvertieren (oder als String + Konverter im Writer, aber einheitlich!).

Danach an router.publish(event) geben.

Routing

router verteilt anhand des Event-Typs und der Output-Flags:

Für Redis: baue Keys und Feldnamen nach obigem Schema und push in q_redis.

Für ClickHouse: mappe Event → Tabellenzeile und push in q_ch.

Writer

Redis-Writer:

Sammle bis redis_pipeline_size oder flush_interval_ms.

HSET/HMSET pro Key; für Trades optional XADD mit MAXLEN.

CH-Writer:

Sammle Zeilen pro Tabelle in separaten Puffern.

Flush, wenn ch_batch_rows erreicht oder ch_batch_ms abgelaufen.

HTTP async_insert (ein Insert je Tabelle/Burst).

Orderbuch-Regeln (wichtig)

Diff-Depth (empfohlen): lokales OB führen (Sequenzen beachten), daraus:

kontinuierlich L1 aktualisieren (für last:l1:*),

periodisch Top5/Top20-Snapshots in Redis & CH schreiben (intervallgesteuert),

OB-Diffs roh nach CH für Historie.

Partial-Snapshots nur, wenn Diff nicht verfügbar ist; dann nicht mischen.

Fehler-/Health-Checks

Heartbeats: „keine neuen Events für Symbol > X s“ → Warnung.

Reconnect mit Backoff/Jitter.

Zähler: msgs/s, drops, parse_errors, resync_count.

6) Performance und Parallelisierung (ohne Overkill)

Prozess/Task-Topologie: 1 Prozess pro 1–3 Exchanges. Innerhalb async/await mit uvloop.

JSON: orjson/rapidjson.

Backpressure: q_redis, q_ch mit maxsize; bei Volllauf sauber drosseln (keine unendliche RAM-Aufnahme).

Batching: konsequent (Redis Pipeline, CH Micro-Batch).

Disk-Spooling: Falls CH down ist, rohen Batch als kompaktes NDJSON/CSV ablegen und später nachschieben.

7) Welche Daten wohin (praktisch)
| Datenart       | Redis (Last)                       | ClickHouse (Historie)               |
| -------------- | ---------------------------------- | ----------------------------------- |
| Trades         | optional Stream (kurzes Fenster)   | `marketdata.trades`                 |
| L1             | `last:l1:*`                        | `marketdata.l1_200ms` (getaktet)    |
| Top5           | `last:top5:*`                      | `marketdata.ob_top5` (getaktet)     |
| Top20          | `last:top20:*`                     | `marketdata.ob_top20` (getaktet)    |
| OB-Diffs       | nicht in Redis                     | `marketdata.ob_diffs` (vollständig) |
| Liquidations   | `last:adv:*` optional nur Counters | `marketdata.liquidations`           |
| Mark/Funding   | `last:mark:*`, `last:funding:*`    | `marketdata.mark_price`, `funding`  |
| OI             | `last:oi:*`                        | `marketdata.open_interest`          |
| Klines         | nicht nötig in Redis               | `marketdata.klines_agg`             |
| Advanced metr. | `last:adv:*`                       | `marketdata.advanced_metrics`       |



8) Wie skalieren wir auf 20–40 Exchanges?

Starter skaliert per Konfig: jede Exchange als eigener Task/Prozess.

Redis: von Standalone → Cluster wachsen; Konfigschalter (cluster: true) vorsehen.

ClickHouse: zunächst 1–3 Nodes; Partitionierung sauber; bei Bedarf Shards hinzufügen (du änderst DDL nicht, nur Distributed-Tabellen ergänzen).

Kein Zwang zu Kafka. Du kannst später nachrüsten, wenn Replays/Reprocess nötig werden.

9) Was du deinem Team zum Outsourcen gibst (Checkliste)

Diese Datei (Schemata + Regeln).

events.py: strikte Pydantic-Modelle mit Beispielen.

base_adapter.py: abstrakte Methoden + Beispiel (Binance).

binance_futures.py: Referenz-Implementierung für 2–3 Feeds (Trades, Diff-Depth, Mark).

redis_writer.py/ch_writer.py: fertig und getestet (nur Konfig nötig).

ddl/clickhouse_create.sql: alle Tabellen; make ddl legt sie an.

config.yaml: fertige Beispiele mit Flags pro Feed.

10) Konkrete Coding-Regeln (damit es wirklich universell bleibt)

Keine exchange-spezifischen Strings in Writer/Router. Nur im Adapter.

Nur Adapter kennt Abo-URLs, Message-Formate und Mapping → Kanonevent.

Zeitstempel: immer in ns einlesen, erst vor CH-Insert in DateTime64(9) casten.

Decimals: sofort string→Decimal; nie Float. Einheitliche Skalierung (keine implizite Multiplikation).

Idempotenz: Writer sollten doppelte Events vertragen (z. B. Reconnect-Replay).

Konfig erzwingt: welche Feeds aktiv, welche Outputs aktiv; Adapter darf keine harten Defaults erzwingen.

Logs strukturiert (JSON): exchange, instrument, event_type, latency_ms, queue_depth.
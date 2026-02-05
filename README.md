# Feed-Modul - Komplettes Handbuch

Dieses Dokument erlaeutert das Feed-Modul ohne weiteren Kontext. Wer es liest, kennt danach:

1. Komponenten und Datenfluss.
2. Das kanonische Eventmodell fuer alle Exchanges.
3. Redis- und ClickHouse-Schemata.
4. Den Aufbau der Konfiguration inklusive globaler Writer-Schalter.
5. Start, Betrieb und Shutdown.
6. Vorgehen zur Implementierung weiterer Exchanges, auch wenn deren Formate stark abweichen.
7. Hilfswerkzeuge (Inspector) zur Verifikation.

Alle Pfadangaben beziehen sich auf das Verzeichnis `feeds/`.

---

## 1. Architektur und Datenfluss

**Ziele**
- Einheitliches Kanonschema fuer Trades, Orderbuch-Tiefen, Mark/Funding, Klines sowie Advanced Metrics.
- Konfigurierbare Ausgabe nach Redis und/oder ClickHouse.
- Austauschbare Exchange-Adapter; nur die Adapter kennen Boersen-Details.
- Schnelles Umschalten per Konfiguration ohne Codeaenderungen.

**Hauptkomponenten**

| Pfad / Datei                      | Aufgabe |
| --------------------------------- | ------- |
| `config.py`                       | Pydantic-Modelle + Loader (`AppConfig`). |
| `orchestrator.py`                 | Startet Writer, verbindet Router, bootet Exchanges. |
| `core/events.py`                  | Kanonische Event-Modelle. |
| `core/router.py`                  | Channel -> Writer Routing. |
| `pipelines/redis_writer.py`       | Abbildung Events -> Redis Hashes/Streams. |
| `pipelines/clickhouse_writer.py`  | Batch-Inserts nach ClickHouse (JSONEachRow). |
| `exchanges/base.py`               | Basisklasse fuer Adapter (Start/Stop/Tasks). |
| `exchanges/binance/`              | Vollstaendige Referenzimplementierung. |
| `utils/`                          | Helfer (`now_ns`, `to_decimal`). |
| `feeds.yml`                       | Defaults fuer Redis/ClickHouse. |
| `presets.json`                    | Vorinstallierte Settings fuer Run-Start. |
| `../run_feeds.py`                 | Interaktiver Starter mit Presets. |
| `main.py`                         | CLI-Einstieg (Low-Level). |
| `../redis_inspector.py`           | Redis-Inspector. |
| `../clickhouse_inspector.py`      | ClickHouse-Inspector. |

**Ablauf**

1. `python run_feeds.py` starten.  
2. Preset waehlen, `presets.json` liefert Channels/Symbole.  
3. `load_config()` liest `feeds.yml` (Defaults) und validiert.  
3. Der Orchestrator erzeugt nur jene Writer, die global aktiviert sind.  
4. Der Router bindet Channels (`Channel.trades`, `Channel.l1`, ...) auf aktive Writer.  
5. Exchange-Adapter verbinden sich (WS/REST), normalisieren Rohdaten zu Events und publizieren sie.  
6. Writer speichern Events in Redis/ClickHouse.  
7. CTRL+C stoppt Feeds, Writer flushen, Verbindungen werden sauber geschlossen.

---

## 2. Eventmodell

Alle Events erben von `BaseEvent` und enthalten Pflichtfelder:

| Feld          | Typ  | Beschreibung |
| ------------- | ---- | ------------ |
| `instrument`  | str  | Symbol exakt wie von der Boerse geliefert (`BTCUSDT`). |
| `channel`     | Enum `Channel` | `trades`, `agg_trades_5s`, `l1`, `ob_top5`, `ob_top20`, `ob_diff`, `liquidations`, `klines`, `mark_price`, `funding`, `advanced_metrics`. |
| `ts_event_ns` | int  | Nanosekunden-Zeitstempel des Exchanges. |
| `ts_recv_ns`  | int  | Nanosekunden-Zeitstempel beim Collector (`now_ns`). |

> **Hinweis:** `exchange` und `market_type` werden nicht mehr im Event gespeichert - diese Informationen sind implizit durch den verwendeten Adapter bekannt und sparen Speicherplatz.

Channel-spezifische Felder (Kurzfassung):

- `TradeEvent`: `price`, `qty`, `side`, optional `trade_id`, `is_aggressor`.  
- `OrderBookDepthEvent`: `depth`, Arrays `bid_prices`, `bid_qtys`, `ask_prices`, `ask_qtys`.  
- `OrderBookDiffEvent`: `sequence`, `prev_sequence`, Dictionaries `bids`, `asks`.  
- `LiquidationEvent`: `side`, `price`, `qty`, optional `order_id`, `reason`.  
- `KlineEvent`: `interval`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `taker_buy_base_volume`, `taker_buy_quote_volume`, `trade_count`, `is_closed`.  
- `MarkPriceEvent`: `mark_price`, optional `index_price`.  
- `FundingEvent`: `funding_rate`, `next_funding_ts_ns`.  
- `AdvancedMetricsEvent`: freie Kennzahlen als Decimal-Mapping (`spread_px`, `spread_bps`, `mid_px`, `imbalance_5`, ...).

Alle numerischen Werte werden als `decimal.Decimal` gehalten, damit Writer verlustfrei arbeiten.

---

## 3. Redis-Schema

Namespace-Pattern: `marketdata:<typ>:<instrument>`.

- `marketdata:last:l1:<instrument>` - Hash mit `bid_price`, `bid_qty`, `ask_price`, `ask_qty`, `ts_event_ns`, `ts_recv_ns`.
- `marketdata:last:top5:<instrument>` / `top20` - Hash mit Feldern `b1_px`, `b1_sz`, ..., `a5_sz` usw.
- `marketdata:last:mark:<instrument>` - letzter Mark Price (TTL 3s).
- `marketdata:last:funding:<instrument>` - Funding Snapshot (ohne TTL).
- `marketdata:last:klines:<interval>:<instrument>` - letzte geschlossene Kline (TTL 120s).
- `marketdata:last:agg_trades_5s:<instrument>` - letzte 5s Agg-Trade-Kerze (TTL 10s).
- `marketdata:last:adv:<instrument>` - Hash fuer Advanced Metrics.
- `marketdata:stream:trades:<instrument>`, `marketdata:stream:liquidations:<instrument>` - Streams (XADD) mit MAXLEN-Begrenzung.

`RedisWriter` pipelinet Kommandos (`pipeline_size`, `flush_interval_ms`). Hashes spiegeln den letzten Zustand, Streams liefern Rollfenster fuer Trades/Liquidationen.

---

## 4. ClickHouse-Schema

Empfohlene Tabellen in Datenbank `marketdata` (siehe auch `DEFAULT_TABLES` im Inspector):

- `trades`: `instrument`, `ts_event_ns`, `ts_recv_ns`, Preise, Mengen, Side, IDs.
- `l1`, `ob_top5`, `ob_top20`: Orderbuch-Snapshots als Arrays plus `depth`.
- `order_book_diffs`: Sequenzen und Delta-Maps (Preis -> Menge).
- `liquidations`, `mark_price`, `funding`, `klines`, `advanced_metrics`: analog zu Eventfeldern.

Alle Tabellen verwenden `ORDER BY (instrument, ts_event_ns)` fuer effiziente Zeitreihenabfragen.

`ClickHouseWriter` sammelt Zeilen je Tabelle (`batch_rows`) und sendet HTTP-Inserts im JSONEachRow-Format (optional LZ4). In der DDL sollten `ts_event_ns`/`ts_recv_ns` als `UInt64` oder `DateTime64(9)` interpretiert werden (z. B. Materialized Column `toDateTime64(ts_event_ns / 1e9, 9)`).

---

## 5. Defaults (`feeds/feeds.yml`)

```yaml
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

**Globale Writer-Schalter**  
- `enable_redis` bzw. `enable_clickhouse` deaktivieren den jeweiligen Writer fuer alle Channels.  
- Adapter koennen ueber `ExchangeFeed.has_outputs(channel_conf)` pruefen, ob es aktive Senken gibt (relevant fuer abgeleitete Channels wie Advanced Metrics).

Channel-Attribute:
- `enabled`: Stream abonnieren ja/nein.  
- `depth`: fuer Snapshot-Kanaele (1, 5, 20, 50, 100).  
- `interval`: fuer Kline-Streams (`1m`, `5s`, ...).  
- `outputs`: sink-spezifische Toggles.  
- `extras`: freier JSON-Block fuer boersenspezifische Einstellungen.

Pydantic validiert Intervalle und Tiefen bereits beim Laden.

---

## 6. Betrieb

Start (Preset-Auswahl):

```bash
python run_feeds.py
```

- `main.py` erkennt sowohl Modulstart als auch direkten Aufruf via Dateipfad.  
- CTRL+C fuehrt einen geordneten Shutdown aus.  
- Writer laufen nur, wenn globale Schalter **und** Channel-Outputs aktiv sind.  
- Fehler bei Redis/ClickHouse werden nicht verschluckt, sondern als Exception propagiert.

---

## 7. Einen neuen Exchange-Adapter bauen

1. **Capabilities definieren** (`exchanges/<exchange>/capabilities.py`)  
   - Mappe Channels auf Stream-Namen/REST-Endpunkte. Beispiel: `"{symbol}@trade"`, `"{symbol}@depth5@100ms"`.  
2. **Transforms schreiben** (`transforms.py`)  
   - Rohnachricht -> Event.  
   - `ts_recv_ns = now_ns()` setzen, numerische Strings mit `to_decimal()` wandeln.  
   - Sequenzen/IDs uebernehmen (Orderbuch-Diffs).  
3. **Adapter implementieren** (`adapter.py`)  
   - Erbt von `ExchangeFeed`.  
   - `start()` iteriert ueber Channels, registriert Tasks (`register_task`).  
   - Fehlerbehandlung via Reconnect und Backoff (`asyncio.sleep(1)`).  
   - Bevor Events produziert werden, optional `self.has_outputs(channel_conf)` pruefen, um Arbeit zu sparen.  
4. **Registry-Eintrag** (`exchanges/<exchange>/__init__.py`)  
   ```python
   from ...registry import registry
   registry.register("okx", lambda cfg, router: OKXFeed(cfg, router))
   ```  
5. **Konfiguration erweitern** (`feeds.yml`), Channels aktivieren, Ausgaben setzen.  
6. **Tests & Verifikation**  
   - Unit-Tests fuer Transforms.  
   - Integration mit Mock-Websocket.  
   - Redis/ClickHouse-Inspector nutzen.

### Unterschiedliche Boersenformate

Jede Abweichung wird im Adapter abgefangen. Ob Rohdaten JSON-Felder `price`, `p`, `Px` oder ganz andere Strukturen enthalten: Entscheidend ist, dass du sie auf das Eventmodell abbildest. Der Rest des Systems (Router, Writer) kennt nur Events. Fuer REST-only-Boersen koennen eigenstaendige Poller-Tasks registriert werden. Falls neue Datentypen benoetigt werden, erweitere `Channel`, die zugehoerige Event-Klasse sowie Writer-Mappings.

---

## 8. Binance-Referenz

`exchanges/binance/` deckt alle geforderten Streams ab:

- Trades, L1, Top5/Top20, Orderbuch-Diffs.  
- Liquidationen, Mark Price, Funding, Klines.  
- Advanced Metrics (Spread, Mid, Spread in Bps, Imbalance auf Basis Top5).  
- Reconnect-Logik und Kombi-Stream fuer Mark/Funding.  
- Advanced Metrics werden nur publiziert, wenn Channel aktiviert **und** mindestens eine Senke aktiv ist.

Dieser Adapter ersetzt `binance_collector.py`. Spezielle Logik aus dem Legacy-Code (z. B. REST-Snapshots) kann als zusaetzlicher Task oder Transform nachgezogen werden.

---

## 9. Inspectoren

### ClickHouse (`clickhouse_inspector.py`)
- Optional `--config feeds/feeds.yml`, sonst Umgebungsvariablen (`CLICKHOUSE_*`).  
- Menue: Tabellen auflisten, Schema anzeigen, Daten ansehen, Zeilen zaehlen, eigene SQLs, Datenbank wechseln/loeschen, Tabelle loeschen, Ping.  
- `DEFAULT_TABLES` spiegelt die empfohlenen Feed-Tabellen wider.

### Redis (`redis_inspector.py`)
- Optional `--config feeds/feeds.yml` oder eigene DSN/Host/Port-Parameter.  
- Menue: Keyspace-Info, DB wechseln, logische Tabellen/Praefixe, Keys pro Tabelle, Tabellen-Sample, Key inspizieren (String/Hash/List/Set/ZSet/Stream), Keys loeschen, Flush DB/all, Ping.

Beide Skripte dienen dazu, Feeds schnell zu validieren oder aufzuraeumen.

---

## 10. FAQ

- **Nur Redis oder nur ClickHouse?**  
  Interaktiv ueber `run_feeds.py`: nach Preset-Auswahl den Output-Modus `ClickHouse`, `Redis` oder `Beides` waehlen. Intern werden `enable_redis`/`enable_clickhouse` und Channel-Outputs passend gesetzt.

- **Advanced Metrics ohne L1 speichern?**  
  Advanced Metrics benoetigen die zugrunde liegenden Rohdaten. Du kannst L1-Ausgaben deaktivieren (`outputs.redis = outputs.clickhouse = false`), der Adapter konsumiert den Stream trotzdem fuer die Berechnung.

- **Exchange liefert stark abweichende Payloads?**  
  Kein Problem: alle Anpassungen passieren im Adapter. Solange du ein valides Event erzeugst, bleibt der Rest unveraendert.

- **Weitere Kennzahlen speichern?**  
  Fuege sie dem `AdvancedMetricsEvent.metrics` hinzu. Redis speichert neue Hash-Felder automatisch, ClickHouse benoetigt passende Spalten (oder JSON/Map-Spalten).

- **Backpressure und Resilience?**  
  Writer arbeiten asynchron; bei Rueckstau blockiert `await router.publish(event)`. Bei laengerem Ausfall kann der ClickHouse-Writer erweitert werden (lokale Puffer oder Spooling). Sequenzen/IDs der Boerse sollten uebernommen werden, um Replays zu erkennen.

---

## 11. Outsourcing-Checkliste

1. Dieses Handbuch und `core/events.py` lesen.  
2. `feeds/feeds.yml` verstehen (globale Schalter, Channel-Outputs).  
3. `exchanges/binance` als Vorlage ansehen.  
4. Eigenen Adapter bauen, registrieren, Konfiguration ergaenzen.  
5. Unit-/Integrationstests, anschliessend Verifikation via Inspector.  
6. Erst nach erfolgreicher Migration den Legacy-Code entfernen.

Damit steht eine modulare, hochperformante Basis, die fuer weitere Exchanges nur noch Adapter-Logik erfordert.

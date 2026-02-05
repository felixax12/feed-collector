# Allround Guide: Setup, 24/7 Betrieb, Remote-Zugriff, Daten-Schema (ClickHouse + Redis)

Stand: 2026-02-05

Dieses Dokument ist die zentrale Referenz fuer:
- Zweitlaptop-Setup und 24/7 Betrieb
- Remote-Zugriff vom Main-Laptop
- Update-Workflow
- Daten-Ingestion fuer Backtesting (ClickHouse) und Live-Trading (Redis)

`STARTUP_COMPOSE_FEEDS.md` bleibt unveraendert als Start-Anleitung bestehen.

## 1) Einmaliges Setup (Zweitlaptop)

### Voraussetzungen
- Docker Desktop (WSL2 aktiv)
- Git
- Python + Abhaengigkeiten (`requirements.txt`)

### Schritte
1. Repo klonen
```bash
git clone https://github.com/DEIN-USERNAME/feed-collector.git
cd feed-collector
```

2. Docker-Stack starten (ClickHouse + Redis)
```bash
docker compose -f docker-compose.feeds.yml up -d
```

3. ClickHouse Schema erstellen/aktualisieren
```bash
python setup_clickhouse_feeds.py
```

4. Feeds starten
```bash
python run_feeds.py
```

Wichtig: Nach der Preset-Auswahl fragt `run_feeds.py` jetzt den Output-Modus ab:
- `ClickHouse`
- `Redis`
- `Beides`

## 2) 24/7 Betrieb (Windows)

### Energieeinstellungen
- Energiesparen/Ruhezustand: `Nie`
- Beim Zuklappen: `Nichts unternehmen`
- Netzteil dauerhaft angeschlossen

### Betrieb
- Docker Desktop dauerhaft aktiv
- `run_feeds.py` dauerhaft aktiv (Konsole offen oder als Task/NSSM Dienst)

## 3) Ports, Services, Firewall

## Container/Ports (`docker-compose.feeds.yml`)
- ClickHouse HTTP: `8124 -> 8123`
- ClickHouse Native: `9001 -> 9000`
- Redis: `6380 -> 6379`

## Firewall (Zweitlaptop) - Pflicht fuer Remote Zugriff
Diese Freigaben sind Pflicht, wenn Main-Laptop remote zugreifen soll:

```powershell
New-NetFirewallRule -DisplayName "ClickHouse 8124" -Direction Inbound -Protocol TCP -LocalPort 8124 -Action Allow
New-NetFirewallRule -DisplayName "Redis 6380" -Direction Inbound -Protocol TCP -LocalPort 6380 -Action Allow
```

## 4) Zugriffsdaten

## ClickHouse (Backtesting)
- URL lokal: `http://feeduser:feedpass@localhost:8124`
- URL remote: `http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124`
- DB: `marketdata`

## Redis (Live-Trading)
- URL lokal: `redis://localhost:6380/0`
- URL remote: `redis://<ZWEITLAPTOP_IP>:6380/0`

## 5) Daten-Ingestion: ClickHouse

Gemeinsame Spalten in allen relevanten Tabellen:
- `instrument`
- `ts_event_ns`
- `ts_recv_ns`
- materialized: `event_time`, `recv_time`

## Preset 2.1 (`mark_price` + `funding`)
- Tabelle `marketdata.mark_price`
  - `mark_price`, `index_price`
- Tabelle `marketdata.funding`
  - `funding_rate`, `next_funding_ts_ns`

## Preset 2.3 (`klines`, 1m)
- Tabelle `marketdata.klines`
  - `interval`
  - `open`, `high`, `low`, `close`
  - `volume`
  - `quote_volume`
  - `taker_buy_base_volume`
  - `taker_buy_quote_volume`
  - `trade_count`
  - `is_closed`

## Preset 2.4 (`agg_trades_5s`)
- Tabelle `marketdata.agg_trades_5s`
  - `interval_s`, `window_start_ns`
  - `open`, `high`, `low`, `close`
  - `volume`, `notional`, `trade_count`
  - `buy_qty`, `sell_qty`
  - `buy_notional`, `sell_notional`
  - `first_trade_id`, `last_trade_id`

## Beispielabfragen ClickHouse

Verbindungstest:
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%201"
```

Liveness Mark Price:
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20max(event_time)%20AS%20last_dt,%20count()%20AS%20rows%20FROM%20marketdata.mark_price"
```

Liveness Funding:
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20max(event_time)%20AS%20last_dt,%20count()%20AS%20rows%20FROM%20marketdata.funding"
```

Letzte Kline:
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20instrument,event_time,open,high,low,close,volume,quote_volume,taker_buy_base_volume,taker_buy_quote_volume%20FROM%20marketdata.klines%20ORDER%20BY%20event_time%20DESC%20LIMIT%205"
```

Letzte AggTrades 5s:
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20instrument,event_time,open,high,low,close,volume,trade_count%20FROM%20marketdata.agg_trades_5s%20ORDER%20BY%20event_time%20DESC%20LIMIT%205"
```

## 6) Daten-Ingestion: Redis

Redis wird als Live-Read-Store genutzt (niedrige Latenz, kurzer Speicherhorizont).

## Key-Schema (wichtig fuer Prod-Reader)
- Mark Price: `marketdata:last:mark:<instrument>`
- Funding: `marketdata:last:funding:<instrument>`
- Klines: `marketdata:last:klines:<interval>:<instrument>`
- AggTrades 5s: `marketdata:last:agg_trades_5s:<instrument>`
- L1: `marketdata:last:l1:<instrument>`
- Top5: `marketdata:last:top5:<instrument>`
- Top20: `marketdata:last:top20:<instrument>`
- Trades Stream: `marketdata:stream:trades:<instrument>`
- Liquidations Stream: `marketdata:stream:liquidations:<instrument>`

## TTL-Policy (Live-Daten)
- `mark_price`: `3s`
- `agg_trades_5s`: `10s`
- `klines`: `120s`

## Redis Feldsets (relevant fuer Presets)
- `last:mark:*`: `ts_event_ns`, `ts_recv_ns`, `mark_px`, optional `index_px`
- `last:funding:*`: `ts_event_ns`, `ts_recv_ns`, `funding_rate`, `next_funding_ts_ns`
- `last:klines:*:*`: `interval`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `taker_buy_base_volume`, `taker_buy_quote_volume`, `trade_count`, `is_closed`, Timestamps
- `last:agg_trades_5s:*`: O/H/L/C + Volumen/Notional + Buy/Sell + TradeCount + IDs + Timestamps

## Beispielabfragen Redis

```bash
redis-cli -h <ZWEITLAPTOP_IP> -p 6380 PING
redis-cli -h <ZWEITLAPTOP_IP> -p 6380 KEYS "marketdata:last:mark:*"
redis-cli -h <ZWEITLAPTOP_IP> -p 6380 HGETALL "marketdata:last:mark:BTCUSDT"
redis-cli -h <ZWEITLAPTOP_IP> -p 6380 HGETALL "marketdata:last:klines:1m:BTCUSDT"
redis-cli -h <ZWEITLAPTOP_IP> -p 6380 HGETALL "marketdata:last:agg_trades_5s:BTCUSDT"
```

## 7) Inspector Tools

## ClickHouse Inspector
```bash
python clickhouse_inspector.py --host <ZWEITLAPTOP_IP> --port 8124 --user feeduser --password feedpass --database marketdata
```

## Redis Inspector
```bash
python redis_inspector.py --host <ZWEITLAPTOP_IP> --port 6380 --db 0
```

Der Redis-Inspector bietet jetzt:
- logische Tabellen/Praefixe
- Keys pro Tabelle
- Tabellen-Samples
- Key-Detailinspektion

## 8) Update-Workflow (Main -> Zweitlaptop)

Main-Laptop:
```bash
git add .
git commit -m "update"
git push
```

Zweitlaptop:
```bash
git pull
docker compose -f docker-compose.feeds.yml up -d
python setup_clickhouse_feeds.py
python run_feeds.py
```

## 9) Betriebskontrollen / Debug

- Containerstatus:
```bash
docker compose -f docker-compose.feeds.yml ps
```

- Feed-Logs: `logs/`
- Bei Remote-Problemen pruefen:
- richtige Zweitlaptop-IP
- Firewall-Regeln `8124` und `6380`
- Port-Mappings aus Compose

## 10) Daten resetten

Kompletter Reset (Volumes loeschen):
```bash
docker compose -f docker-compose.feeds.yml down
docker volume rm feed_clickhouse_data feed_redis_data
```

Nur einzelne ClickHouse-Tabelle leeren:
```bash
curl "http://feeduser:feedpass@localhost:8124/?query=TRUNCATE%20TABLE%20marketdata.mark_price"
```

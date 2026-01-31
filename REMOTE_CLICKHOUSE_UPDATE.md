# Update & Remote ClickHouse Zugriff (Zweitlaptop)

Stand: 2026-01-31

## 1) Relevante Aenderungen (fuer den Zweitlaptop)
- Neue/aktualisierte Presets: 2.3 (1m Klines) und 2.4 (AggTrades 5s) sind live-ready.
- Neue Tabellen in ClickHouse: `marketdata.klines` und `marketdata.agg_trades_5s`.
- Parallelbetrieb: `run_feeds.py` kann mehrere Presets gleichzeitig starten (je Preset eigener Prozess).
- Telegram-Konfig ist getrennt: `TELEGRAM_RAW_*` (Debug) und `TELEGRAM_OVERVIEW_*` (30s Overview).

## 2) Update-Schritte (Zweitlaptop)
1) Repo aktualisieren
   - `git pull`
2) `feed.env` aktualisieren
   - Neue Keys setzen:
     - `TELEGRAM_RAW_BOT_TOKEN`, `TELEGRAM_RAW_CHAT_ID`, `TELEGRAM_RAW_INTERVAL_S`
     - `TELEGRAM_OVERVIEW_BOT_TOKEN`, `TELEGRAM_OVERVIEW_CHAT_ID`, `TELEGRAM_OVERVIEW_INTERVAL_S=30`
3) Docker-Stack starten (ClickHouse + Redis)
   - `docker compose -f docker-compose.feeds.yml up -d`
4) ClickHouse Schema anlegen/aktualisieren (einmalig)
   - `python setup_clickhouse_feeds.py`
5) Feeds starten
   - `python run_feeds.py` (Preset-Auswahl im Menu)

## 3) Remote-Zugriff auf ClickHouse (Main-Laptop -> Zweitlaptop)
Voraussetzungen:
- Zweitlaptop laeuft, `run_feeds.py` schreibt Daten.
- Port `8124` in der Firewall offen.
- IP des Zweitlaptops bekannt.

Firewall (Zweitlaptop, Admin PowerShell):
`New-NetFirewallRule -DisplayName "ClickHouse 8124" -Direction Inbound -Protocol TCP -LocalPort 8124 -Action Allow`

Beispiele (vom Main-Laptop):
`curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20max(event_time)%20AS%20last_dt,%20count()%20AS%20rows%20FROM%20marketdata.mark_price"`

`curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20max(event_time)%20AS%20last_dt,%20count()%20AS%20rows%20FROM%20marketdata.klines"`

`curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20max(event_time)%20AS%20last_dt,%20count()%20AS%20rows%20FROM%20marketdata.agg_trades_5s"`

Kleine Stichprobe:
`curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20instrument,%20event_time,%20mark_price%20FROM%20marketdata.mark_price%20ORDER%20BY%20event_time%20DESC%20LIMIT%205"`

`curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20instrument,%20event_time,%20open,%20high,%20low,%20close%20FROM%20marketdata.klines%20ORDER%20BY%20event_time%20DESC%20LIMIT%205"`

`curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20instrument,%20event_time,%20agg_qty,%20agg_count%20FROM%20marketdata.agg_trades_5s%20ORDER%20BY%20event_time%20DESC%20LIMIT%205"`

## 4) Checks/Debug
- Container-Status: `docker compose -f docker-compose.feeds.yml ps`
- ClickHouse Inspector (remote):
  `python clickhouse_inspector.py --host <ZWEITLAPTOP_IP> --port 8124 --user feeduser --password feedpass --database marketdata`
- Logs: `logs/` oder `logs.md`


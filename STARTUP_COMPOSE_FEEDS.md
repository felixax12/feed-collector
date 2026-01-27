# DOCKER COMPOSE STARTUP (FEED STACK)

Diese Anleitung ist fuer dieses Repo gedacht und nutzt eigene Container-/Volume-Namen,
damit es keine Kollision mit anderen Projekten gibt.

## 1) Container-Stack starten (ClickHouse + Redis)

```bash
docker compose -f docker-compose.feeds.yml up -d
```

Status pruefen:
```bash
docker compose -f docker-compose.feeds.yml ps
```

Erwartung: `feed_clickhouse` und `feed_redis` laufen.

---

## 2) Basis-Checks

ClickHouse erreichbar?
```bash
curl http://localhost:8124/
```
Erwartet: `Ok.`

Redis erreichbar?
```bash
redis-cli -p 6380 ping
```
Erwartet: `PONG`

---

## 2.5) Python Abhaengigkeiten installieren (einmalig)

```bash
pip install -r requirements.txt
```

---

## 3) ClickHouse Schema anlegen (einmalig)

```bash
python setup_clickhouse_feeds.py
```

---

## 4) Feed-Modul starten (neue Compose-Config)

```bash
python run_feeds.py
```

Die Defaults (Redis/ClickHouse-DSN) stehen in `feeds/feeds.yml`.

---

## 5) Verifikation (nach 20-30s Laufzeit)

Redis (Keys fuer L1/Trades pruefen):
```bash
python redis_inspector.py --host localhost --port 6380
```

ClickHouse (nur wenn Tabellen existieren):
```bash
python clickhouse_inspector.py --host localhost --port 8124 --user feeduser --password feedpass --database marketdata
```

---

## 6) Stoppen und Neustarten

Stop (Daten bleiben):
```bash
docker compose -f docker-compose.feeds.yml stop
```

Wieder starten:
```bash
docker compose -f docker-compose.feeds.yml start
```

Stack + Volumes entfernen (alle Daten weg):
```bash
docker compose -f docker-compose.feeds.yml down -v
```

---

## 7) Legacy Collector (optional)

Wenn du weiterhin `binance_collector.py` nutzen willst, setze eigene Ports:
```bash
set CLICKHOUSE_URL=http://localhost:8124
set CLICKHOUSE_USER=feeduser
set CLICKHOUSE_PASSWORD=feedpass
set CLICKHOUSE_DB=binance_db
python binance_collector.py
```

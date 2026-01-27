# SYSTEM STARTUP GUIDE

## 1. CONTAINER-STACK STARTEN (Redis, ClickHouse, Grafana)

### Erstmaliger Start:
```powershell
# Im Projektroot (wo docker-compose.yml liegt):
docker-compose up -d
```

**Was passiert:**
- Redis startet auf Port 6380
- ClickHouse startet auf Port 8124 (HTTP) und 9001 (Native)
- Grafana startet auf Port 3000
- Volumes werden automatisch erstellt (clickhouse_data, redis_data)

### Status prüfen:
```powershell
docker-compose ps
```

**Erwartete Ausgabe:** Alle 3 Container "Up"

---

## 2. CLICKHOUSE SCHEMA IMPORTIEREN (EINMALIG beim ersten Start)

```powershell
Get-Content analytics_runtime\clickhouse_schema.sql | docker exec -i feed_clickhouse clickhouse-client -u feeduser --password feedpass --multiquery
```

**Verifizieren:**
```powershell
# Datenbank existiert?
echo "SHOW DATABASES" | curl.exe -s "http://localhost:8124/?user=feeduser&password=feedpass" --data-binary "@-"

# Tabellen existieren?
echo "SHOW TABLES FROM analytics" | curl.exe -s "http://localhost:8124/?user=feeduser&password=feedpass" --data-binary "@-"
```

**Erwartete Ausgabe:** `analytics`, `analytics_events`, `analytics_ingest_health`, etc.

---

## 3. PYTHON LIVE-SYSTEM STARTEN

**PowerShell (WICHTIG: Gleiche Session für Env-Vars + System-Start):**

```powershell
# Environment setzen:
$env:ANALYTICS_CH_URL="http://localhost:8124"
$env:ANALYTICS_CH_USER="feeduser"
$env:ANALYTICS_CH_PASSWORD="feedpass"
$env:ANALYTICS_CH_DB="analytics"

# System starten:
python system_control.py
# Option 7 wählen (Multiprocess Stack)
```

**Logs prüfen (neue PowerShell):**
```powershell
Get-Content logs/processes/analytics.log -Tail 20
```

**Erwartete Zeile:** `ClickHouse writer enabled (url=http://localhost:8124, db=analytics, ...)`

---

## 4. DATEN VERIFIZIEREN (nach 30 Sekunden Laufzeit)

```powershell
# Event-Types zählen:
'SELECT event_type, count() as cnt FROM analytics.analytics_events GROUP BY event_type ORDER BY event_type' | curl.exe -s "http://localhost:8124/?user=feeduser&password=feedpass" --data-binary "@-"

# Gesamtzahl Events:
'SELECT count() FROM analytics.analytics_events' | curl.exe -s "http://localhost:8124/?user=feeduser&password=feedpass&database=analytics" --data-binary "@-"
```

**Wenn Count > 0:** ✅ System schreibt in ClickHouse

---

## 5. GRAFANA ZUGRIFF

**Browser öffnen:** http://localhost:3000

**Login:**
- User: `admin`
- Password: `admin`

**ClickHouse Datasource prüfen:**
- Configuration → Data Sources → ClickHouse
- Sollte bereits konfiguriert sein (via Provisioning)
- Falls nicht: Manuell hinzufügen (URL: `http://feed_clickhouse:8123`, User: `feeduser`, Password: `feedpass`, DB: `analytics`)

---

## STOPPEN & NEUSTARTEN

### Container stoppen (Daten bleiben):
```powershell
docker-compose stop
```

### Container starten (nach Stopp):
```powershell
docker-compose start
```

### Container + Volumes löschen (WARNUNG: Daten weg!):
```powershell
docker-compose down -v
```

---

## CLICKHOUSE DATEN LÖSCHEN (Schema bleibt)

**Alle Analytics-Tabellen auf einmal leeren:**
```powershell
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --multiquery "
TRUNCATE TABLE analytics.analytics_events;
TRUNCATE TABLE analytics.analytics_ingest_health;
TRUNCATE TABLE analytics.system_latency_windows;
TRUNCATE TABLE analytics.trade_latency_events;
TRUNCATE TABLE analytics.latency_timelines;
TRUNCATE TABLE analytics.trade_latency_metrics;
"
```

**Oder einzeln (falls nur bestimmte Tabellen):**
```powershell
# Nur Events löschen:
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "TRUNCATE TABLE analytics.analytics_events"

# Nur System Latency löschen:
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "TRUNCATE TABLE analytics.system_latency_windows"

# Nur Trade Latency löschen:
docker exec -it feed_clickhouse clickhouse-client -u feeduser --password feedpass --query "TRUNCATE TABLE analytics.trade_latency_events"
```

**Verifizieren:**
```powershell
'SELECT count() FROM analytics.analytics_events' | curl.exe -s "http://localhost:8124/?user=feeduser&password=feedpass&database=analytics" --data-binary "@-"
'SELECT count() FROM analytics.system_latency_windows' | curl.exe -s "http://localhost:8124/?user=feeduser&password=feedpass&database=analytics" --data-binary "@-"
```

**Erwartete Ausgabe:** `0`

---

## TROUBLESHOOTING

### Container läuft nicht:
```powershell
docker-compose logs feed_clickhouse
docker-compose logs feed_redis
docker-compose logs grafana
```

### ClickHouse nicht erreichbar:
```powershell
curl.exe http://localhost:8124/
```
**Erwartete Ausgabe:** `Ok.`

### Python schreibt nicht in ClickHouse:
1. Env-Vars gesetzt? (`echo $env:ANALYTICS_CH_URL`)
2. `logs/processes/analytics.log` prüfen: Steht "ClickHouse writer enabled"?
3. Container läuft? (`docker-compose ps`)

### Alte Container stören:
```powershell
# Alte einzeln gestartete Container stoppen:
docker stop feed_clickhouse feed_redis grafana
docker rm feed_clickhouse feed_redis grafana

# Dann docker-compose neu starten:
docker-compose up -d
```

---

## WICHTIGE URLS

- **ClickHouse HTTP:** http://localhost:8124
- **Grafana:** http://localhost:3000
- **Redis:** localhost:6380

---

## WORKFLOW FÜR NEUEN TAG

1. `docker-compose start` (falls gestoppt)
2. PowerShell öffnen → Env-Vars setzen (siehe Schritt 3)
3. `python system_control.py` → Option 7
4. Grafana öffnen → Dashboards anschauen
5. Bei Bedarf: ClickHouse-Queries für Debugging (siehe Schritt 4)

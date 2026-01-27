# Setup: Zweitlaptop 24/7 + Remote-Abfragen (Windows)

## A) Einmaliges Setup (Zweitlaptop)

### 1. Docker + Git installieren
- Docker Desktop installieren (WSL2 aktivieren)
- Git installieren

### 2. Repo klonen
```bash
git clone https://github.com/DEIN-USERNAME/feed-collector.git
cd feed-collector
```

### 3. Docker Services starten (ClickHouse + Redis)
```bash
docker-compose -f docker-compose.feeds.yml up -d
```

### 4. Tabellen einmalig anlegen
```bash
python setup_clickhouse_feeds.py
```

### 5. Feeds starten (Preset waehlen)
```bash
python run_feeds.py
```

## B) 24/7 Betrieb (Zweitlaptop)
### 1. Windows Energieoptionen
- Systemsteuerung → Energieoptionen → “Energiesparplaneinstellungen ändern”
  - **Bildschirm ausschalten**: egal
  - **Energie sparen/Ruhezustand**: **Nie**

### 2. Deckel-Aktion
- Systemsteuerung → Energieoptionen → “Auswählen, was beim Zuklappen des Computers geschehen soll”
  - **Beim Zuklappen**: **Nichts unternehmen**

### 3. Dauerbetrieb
- Netzteil dauerhaft angeschlossen
- Docker Desktop läuft im Hintergrund
- `run_feeds.py` läuft im Hintergrund (Task Scheduler/NSSM, falls kein Fenster offen bleiben soll)

## C) Remote-Abfragen vom Main-Laptop

### 1. IP vom Zweitlaptop
```bash
ipconfig
```

### 2. Firewall-Port freigeben (Zweitlaptop, Admin PowerShell)
```powershell
New-NetFirewallRule -DisplayName "ClickHouse 8124" -Direction Inbound -Protocol TCP -LocalPort 8124 -Action Allow
```

### 3. Port-Mapping prüfen (Docker Compose)
`docker-compose.feeds.yml` muss enthalten:
```yaml
ports:
  - "8124:8123"
```

### 3. Verbindung testen (Main-Laptop)
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%201"
```

### 4. Letzter Eintrag pruefen
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20max(toDateTime64(ts_event_ns/1000,3))%20AS%20last_dt,%20count()%20AS%20rows%20FROM%20marketdata.mark_price"
```

### 5. Einfache Beispiel-Queries (Analyse-Repo Orientierung)
**Letzte 5 Eintraege (BTCUSDT):**
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20ts_event_ns,mark_price%20FROM%20marketdata.mark_price%20WHERE%20instrument='BTCUSDT'%20ORDER%20BY%20ts_event_ns%20DESC%20LIMIT%205"
```

**Letzte Zeit pro Symbol (Top 10):**
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20instrument,%20max(toDateTime64(ts_event_ns/1000,3))%20AS%20last_dt%20FROM%20marketdata.mark_price%20GROUP%20BY%20instrument%20ORDER%20BY%20last_dt%20DESC%20LIMIT%2010"
```

**1-Minuten-Schnitt (BTCUSDT, letzte 10 Minuten):**
```bash
curl "http://feeduser:feedpass@<ZWEITLAPTOP_IP>:8124/?query=SELECT%20toStartOfMinute(toDateTime64(ts_event_ns/1000,3))%20AS%20minute,%20avg(mark_price)%20AS%20avg_px%20FROM%20marketdata.mark_price%20WHERE%20instrument='BTCUSDT'%20AND%20ts_event_ns%20>=toUnixTimestamp64Milli(now64()-INTERVAL%2010%20MINUTE)%20GROUP%20BY%20minute%20ORDER%20BY%20minute"
```

## D) Daten löschen (gleiches Prinzip wie lokal)
**Alles löschen (Docker Volumes entfernen):**
```bash
docker-compose -f docker-compose.feeds.yml down
docker volume rm feed_clickhouse_data feed_redis_data
```

**Nur Tabelle leeren (ClickHouse):**
```bash
curl "http://feeduser:feedpass@localhost:8124/?query=TRUNCATE%20TABLE%20marketdata.mark_price"
```

## E) Repo Updates
**Hauptlaptop:**
```bash
git add .
git commit -m "Beschreibung der Änderung"
git push
```

**Zweitlaptop:**
```bash
git pull
```

## Typischer Workflow

1. Code auf Hauptlaptop ändern
2. `git add . && git commit -m "xyz" && git push`
3. Auf Zweitlaptop: `git pull`
4. Falls Docker läuft: Container neu starten
   ```bash
   docker-compose -f docker-compose.feeds.yml down
   docker-compose -f docker-compose.feeds.yml up -d
   ```

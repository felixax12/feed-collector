COMMANDS
--------------
Core Workflow (immer verwenden)

# 1. ClickHouse Volume erstellen (einmalig)
docker volume create clickhouse_data

# 2. ClickHouse Server starten (DEFAULT_DB wird ignoriert, nutze binance_db)
docker run -d -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_USER=felix -e CLICKHOUSE_PASSWORD=testpass -v clickhouse_data:/var/lib/clickhouse --name clickhouse-server clickhouse/clickhouse-server

# 3. Exchange-Datenbanken erstellen (einmalig)
docker exec -it clickhouse-server clickhouse-client -u felix --password testpass -q "CREATE DATABASE IF NOT EXISTS binance_db"
docker exec -it clickhouse-server clickhouse-client -u felix --password testpass -q "CREATE DATABASE IF NOT EXISTS okx_db"
docker exec -it clickhouse-server clickhouse-client -u felix --password testpass -q "CREATE DATABASE IF NOT EXISTS bybit_db"

# 4. Alte mydb löschen (optional, mit Inspector oder:)
docker exec -it clickhouse-server clickhouse-client -u felix --password testpass -q "DROP DATABASE IF EXISTS mydb"



docker logs --tail 100 -f clickhouse-server
curl http://localhost:8123/
python clickhouse_test.py
docker stop clickhouse-server
docker start clickhouse-server

--------------
--------------
Collector starten (mit ClickHouse aktiviert)
python binance_collector.py
# ClickHouse ist standardmäßig aktiviert, nutzt binance_db
# Umgebungsvariablen optional überschreiben mit: CLICKHOUSE_DB=binance_db CLICKHOUSE_ENABLE=1 python binance_collector.py

--------------
--------------
Inspector (Tabellen/DBs verwalten)
python clickhouse_inspector.py
# Funktionen:
#   1. Tabellen auflisten
#   2. Tabellenschema anzeigen
#   3. Daten ansehen
#   4. Zeilenanzahl
#   5. SQL ausführen
#   6. DBs auflisten
#   7. DB wechseln
#   8. Tabelle löschen (DROP TABLE)
#   9. Datenbank löschen (DROP DATABASE) <- NEU! Für mydb löschen
#   0. Latency Check

--------------
--------------
Kontrolle und Einsicht (CLI)
docker exec -it clickhouse-server clickhouse-client -u felix --password testpass --query "SELECT count(*) FROM binance_db.binance_trades_raw"
docker exec -it clickhouse-server clickhouse-client -u felix --password testpass --query "SELECT * FROM binance_db.binance_trades_raw ORDER BY ts DESC LIMIT 5"
docker exec -it clickhouse-server clickhouse-client -u felix --password testpass --query "SHOW DATABASES"
docker exec -it clickhouse-server clickhouse-client -u felix --password testpass --query "SHOW TABLES FROM binance_db"
docker volume inspect clickhouse_data
docker run --rm -it -v clickhouse_data:/var/lib/clickhouse alpine ls -R /var/lib/clickhouse | more

--------------
--------------
Weitere Varianten und Wartung
docker run -d -p 8123:8123 -p 9000:9000 --name clickhouse-server clickhouse/clickhouse-server
docker run -d -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_USER=felix -e CLICKHOUSE_PASSWORD=testpass -e CLICKHOUSE_DB=mydb -v "C:\Users\felix_n9pq3vl\clickhouse_data:/var/lib/clickhouse" --name clickhouse-server clickhouse/clickhouse-server
docker run -d -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_USER=felix -e CLICKHOUSE_PASSWORD=testpass -e CLICKHOUSE_DB=mydb --name clickhouse-server clickhouse/clickhouse-server
docker exec -it clickhouse-server clickhouse-client
docker exec -it clickhouse-server clickhouse-client --query "TRUNCATE TABLE mydb.trades"
docker stop clickhouse-server && docker rm clickhouse-server && docker volume rm clickhouse_data

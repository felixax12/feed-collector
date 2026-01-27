COMMANDS
--------------
Core Workflow (immer verwenden)
docker volume create clickhouse_data
docker run -d -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_USER=felix -e CLICKHOUSE_PASSWORD=testpass -e CLICKHOUSE_DB=mydb -v clickhouse_data:/var/lib/clickhouse --name clickhouse-server clickhouse/clickhouse-server
docker logs --tail 100 -f clickhouse-server
curl http://localhost:8123/
python clickhouse_test.py
docker stop clickhouse-server
docker start clickhouse-server

--------------
--------------
Kontrolle und Einsicht
docker exec -it clickhouse-server clickhouse-client --query "SELECT count(*) FROM mydb.trades"
docker exec -it clickhouse-server clickhouse-client --query "SELECT * FROM mydb.trades ORDER BY ts DESC LIMIT 5"
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

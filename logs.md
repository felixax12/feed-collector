PS C:\Users\felix_n9pq3vl\Desktop\Code\AB_33\CC_Data_Collection\AC_Granular\main_collection> python run_feeds.py

Vorinstallierte Settings:
  1 (NICHT bereit fuer Betrieb). 1s Preis + BBO (ClickHouse) - Streams: markPrice@1s (Preis) + bookTicker (BBO). Snapshot only, kein Diff-Rebuild.
  1.1 (NICHT bereit fuer Betrieb). BBO (ClickHouse) - Stream: bookTicker (BBO). Snapshot only, kein Diff-Rebuild. Health-Monitoring aktiv.
  2.1. 1s Preis + Funding (ClickHouse) - Stream: markPrice@1s (Mark+Index+Funding). Snapshot only, kein Diff-Rebuild. Health-Monitoring aktiv.
  2.2 (NICHT bereit fuer Betrieb). 1s Preis + Top5 (ClickHouse) - Streams: markPrice@1s + depth5@1s (sharded). Snapshot only, kein Diff-Rebuild. Health-Monitoring aktiv.
  2.3. 1m Klines (ClickHouse) - Stream: klines@1m. Snapshot only, kein Diff-Rebuild. Health-Monitoring aktiv.
  2.4. AggTrades 5s (ClickHouse) - Stream: aggTrade -> 5s Aggregation. 1 Zeile pro Symbol pro 5 Sekunden. Health-Monitoring aktiv.
  3 (NICHT bereit fuer Betrieb). Jede BBO-Aenderung (ClickHouse) - Stream: bookTicker (BBO). Snapshot only, kein Diff-Rebuild.
  4 (NICHT bereit fuer Betrieb). Alles (ClickHouse) - Streams: trades, bookTicker, depth5@100ms, depth20@100ms, depth@100ms (Diffs), markPrice@1s, klines. Diffs ohne Snapshot-Rebuild.

Nummer waehlen (mehrere mit Komma): 2.1,2.3,2.4

Output-Ziel waehlen:
  1. ClickHouse
  2. Redis
  3. Beides
Auswahl [1/2/3, Default 1]: 2
2026-02-05 08:20:23,323 INFO root: [2.1 1s Preis + Funding (ClickHouse)] CPU affinity set via psutil: core=0
2026-02-05 08:20:23,323 INFO root: [2.4 AggTrades 5s (ClickHouse)] CPU affinity set via psutil: core=2
2026-02-05 08:20:23,323 INFO root: [2.1 1s Preis + Funding (ClickHouse)] Start preset=2.1 1s Preis + Funding (ClickHouse) output_mode=redis
2026-02-05 08:20:23,323 INFO root: [2.4 AggTrades 5s (ClickHouse)] Start preset=2.4 AggTrades 5s (ClickHouse) output_mode=redis
2026-02-05 08:20:23,325 INFO root: [2.3 1m Klines (ClickHouse)] CPU affinity set via psutil: core=1
2026-02-05 08:20:23,326 INFO root: [2.3 1m Klines (ClickHouse)] Start preset=2.3 1m Klines (ClickHouse) output_mode=redis
Geladene Symbole: 544
Geladene Symbole: 544
Geladene Symbole: 544
2026-02-05 08:20:24,872 INFO root: [2.4 AggTrades 5s (ClickHouse)] LOG LEGEND: ws=websocket input, routed=router accepted, written=buffered rows, flushed=rows inserted into ClickHouse. pending=written-flushed (buffer), missing=expected-flushed (per interval), backlog=rolling deficit vs expected, backlog_ws=rolling deficit vs ws (ingest throughput vs input).
2026-02-05 08:20:24,872 INFO root: [2.4 AggTrades 5s (ClickHouse)] Preset config: label=2.4 AggTrades 5s (ClickHouse) log_interval_s=5 health=True output_mode=redis
2026-02-05 08:20:24,873 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,922 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,924 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,925 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,928 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,930 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,932 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,933 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,934 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,935 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=50
2026-02-05 08:20:24,936 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connect channel=agg_trades_5s streams=44
2026-02-05 08:20:25,047 WARNING root: [2.4 AggTrades 5s (ClickHouse)] ClickHouse process not found. Set CLICKHOUSE_PID to force tracking.
2026-02-05 08:20:25,212 INFO root: [2.3 1m Klines (ClickHouse)] LOG LEGEND: ws=websocket input, routed=router accepted, written=buffered rows, flushed=rows inserted into ClickHouse. pending=written-flushed (buffer), missing=expected-flushed (per interval), backlog=rolling deficit vs expected, backlog_ws=rolling deficit vs ws (ingest throughput vs input).
2026-02-05 08:20:25,213 INFO root: [2.3 1m Klines (ClickHouse)] Preset config: label=2.3 1m Klines (ClickHouse) log_interval_s=60 health=True output_mode=redis    
2026-02-05 08:20:25,213 INFO feeds.binance: [2.3 1m Klines (ClickHouse)] connect channel=klines streams=200
2026-02-05 08:20:25,235 INFO root: [2.1 1s Preis + Funding (ClickHouse)] LOG LEGEND: ws=websocket input, routed=router accepted, written=buffered rows, flushed=rows inserted into ClickHouse. pending=written-flushed (buffer), missing=expected-flushed (per interval), backlog=rolling deficit vs expected, backlog_ws=rolling deficit vs ws (ingest throughput vs input).
2026-02-05 08:20:25,238 INFO root: [2.1 1s Preis + Funding (ClickHouse)] Preset config: label=2.1 1s Preis + Funding (ClickHouse) log_interval_s=10 health=True output_mode=redis
2026-02-05 08:20:25,239 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connect channel=mark_price streams=100
2026-02-05 08:20:25,247 INFO feeds.binance: [2.3 1m Klines (ClickHouse)] connect channel=klines streams=200
2026-02-05 08:20:25,249 INFO feeds.binance: [2.3 1m Klines (ClickHouse)] connect channel=klines streams=144
2026-02-05 08:20:25,280 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connect channel=mark_price streams=100
2026-02-05 08:20:25,282 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connect channel=mark_price streams=100
2026-02-05 08:20:25,283 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connect channel=mark_price streams=100
2026-02-05 08:20:25,285 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connect channel=mark_price streams=100
2026-02-05 08:20:25,286 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connect channel=mark_price streams=44
2026-02-05 08:20:25,401 WARNING root: [2.3 1m Klines (ClickHouse)] ClickHouse process not found. Set CLICKHOUSE_PID to force tracking.
2026-02-05 08:20:25,419 WARNING root: [2.1 1s Preis + Funding (ClickHouse)] ClickHouse process not found. Set CLICKHOUSE_PID to force tracking.
2026-02-05 08:20:25,996 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,137 INFO feeds.binance: [2.3 1m Klines (ClickHouse)] connected channel=klines
2026-02-05 08:20:26,150 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connected channel=mark_price
2026-02-05 08:20:26,192 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,193 INFO feeds.binance: [2.3 1m Klines (ClickHouse)] connected channel=klines
2026-02-05 08:20:26,196 INFO feeds.binance: [2.3 1m Klines (ClickHouse)] connected channel=klines
2026-02-05 08:20:26,217 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,217 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,226 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,227 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,235 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,236 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,239 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,246 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,252 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] connected channel=agg_trades_5s
2026-02-05 08:20:26,277 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connected channel=mark_price
2026-02-05 08:20:26,282 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connected channel=mark_price
2026-02-05 08:20:26,289 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connected channel=mark_price
2026-02-05 08:20:26,290 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connected channel=mark_price
2026-02-05 08:20:26,525 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] connected channel=mark_price
2026-02-05 08:20:29,938 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] ws-stats agg_trades_5s: msgs+1099/5s conns+11 discs+0
2026-02-05 08:20:29,939 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] agg-trades backlog=0 enq+1371/5s proc+1371/5s emit+1099/5s drop+0/5s
2026-02-05 08:20:30,052 INFO root: [2.4 AggTrades 5s (ClickHouse)] [ingest] redis: events+1114/5s items+2228/5s flushed+2620/5s
2026-02-05 08:20:30,054 INFO root: [2.4 AggTrades 5s (ClickHouse)] [diff] agg_trades_5s: ws+1114 routed+1114 written+1114 lost+0
2026-02-05 08:20:30,054 INFO root: [2.4 AggTrades 5s (ClickHouse)] [loss] agg_trades_5s: ws->router 0 | router->writer 0 | writer->ch -196
2026-02-05 08:20:30,054 INFO root: [2.4 AggTrades 5s (ClickHouse)] [health] agg_trades_5s: expected=544/5s flushed=1310 pending=0 missing=0 backlog=0 backlog_ws=0 
2026-02-05 08:20:30,067 INFO root: [2.4 AggTrades 5s (ClickHouse)] HEALTH channel=agg_trades_5s lag_ms avg=1905.4 max=2001
2026-02-05 08:20:30,151 INFO root: [2.4 AggTrades 5s (ClickHouse)] [sys] py_cpu=14.4% | py_rss=75.0MB
2026-02-05 08:20:34,941 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] ws-stats agg_trades_5s: msgs+702/5s conns+0 discs+0
2026-02-05 08:20:34,941 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] agg-trades backlog=1 enq+2370/5s proc+2369/5s emit+702/5s drop+0/5s
2026-02-05 08:20:35,069 INFO root: [2.4 AggTrades 5s (ClickHouse)] HEALTH channel=agg_trades_5s lag_ms avg=1724.6 max=2071
2026-02-05 08:20:35,214 INFO root: [2.4 AggTrades 5s (ClickHouse)] [ingest] redis: events+748/5s items+1496/5s flushed+1642/5s
2026-02-05 08:20:35,214 INFO root: [2.4 AggTrades 5s (ClickHouse)] [diff] agg_trades_5s: ws+748 routed+748 written+748 lost+0
2026-02-05 08:20:35,214 INFO root: [2.4 AggTrades 5s (ClickHouse)] [loss] agg_trades_5s: ws->router 0 | router->writer 0 | writer->ch -73
2026-02-05 08:20:35,215 INFO root: [2.4 AggTrades 5s (ClickHouse)] [health] agg_trades_5s: expected=544/5s flushed=821 pending=0 missing=0 backlog=0 backlog_ws=0  
2026-02-05 08:20:35,215 INFO root: [2.4 AggTrades 5s (ClickHouse)] [sys] py_cpu=12.7% | py_rss=75.6MB | py_io_read=0.00MB/5s | py_io_write=0.00MB/5s
2026-02-05 08:20:35,290 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] ws-stats funding: msgs+4896/10s conns+6 discs+0 | mark_price: msgs+4896/10s conns+6 discs+0
2026-02-05 08:20:35,428 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [ingest] redis: events+9792/10s items+14688/10s flushed+21589/10s
2026-02-05 08:20:35,430 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [diff] funding: ws+4896 routed+4896 written+4896 lost+0 | mark_price: ws+4896 routed+4896 written+4896 lost+0
2026-02-05 08:20:35,431 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [loss] funding: ws->router 0 | router->writer 0 | writer->ch -2219 | mark_price: ws->router 0 | router->writer 0 | writer->ch -2341
2026-02-05 08:20:35,432 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [health] mark_price: expected=5440/10s flushed=7237 pending=0 missing=0 backlog=0 backlog_ws=0 | funding: expected=5440/10s flushed=7115 pending=0 missing=0 backlog=0 backlog_ws=0
2026-02-05 08:20:35,442 INFO root: [2.1 1s Preis + Funding (ClickHouse)] HEALTH channel=mark_price lag_ms avg=0.0 max=0
2026-02-05 08:20:35,443 INFO root: [2.1 1s Preis + Funding (ClickHouse)] HEALTH channel=funding lag_ms avg=0.0 max=0
2026-02-05 08:20:35,583 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [sys] py_cpu=8.5% | py_rss=70.2MB
2026-02-05 08:20:39,943 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] ws-stats agg_trades_5s: msgs+704/5s conns+0 discs+0
2026-02-05 08:20:39,943 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] agg-trades backlog=0 enq+2348/5s proc+2349/5s emit+704/5s drop+0/5s
2026-02-05 08:20:40,071 INFO root: [2.4 AggTrades 5s (ClickHouse)] HEALTH channel=agg_trades_5s lag_ms avg=1233.0 max=2143
2026-02-05 08:20:40,217 INFO root: [2.4 AggTrades 5s (ClickHouse)] [ingest] redis: events+732/5s items+1464/5s flushed+1440/5s
2026-02-05 08:20:40,218 INFO root: [2.4 AggTrades 5s (ClickHouse)] [diff] agg_trades_5s: ws+732 routed+732 written+732 lost+0
2026-02-05 08:20:40,219 INFO root: [2.4 AggTrades 5s (ClickHouse)] [loss] agg_trades_5s: ws->router 0 | router->writer 0 | writer->ch 12
2026-02-05 08:20:40,219 INFO root: [2.4 AggTrades 5s (ClickHouse)] [health] agg_trades_5s: expected=544/5s flushed=720 pending=12 missing=0 backlog=0 backlog_ws=12
2026-02-05 08:20:40,221 INFO root: [2.4 AggTrades 5s (ClickHouse)] [sys] py_cpu=14.4% | py_rss=75.9MB | py_io_read=0.00MB/5s | py_io_write=0.00MB/5s
2026-02-05 08:20:44,947 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] ws-stats agg_trades_5s: msgs+790/5s conns+0 discs+0
2026-02-05 08:20:44,947 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] agg-trades backlog=5 enq+2444/5s proc+2439/5s emit+790/5s drop+0/5s
2026-02-05 08:20:45,077 INFO root: [2.4 AggTrades 5s (ClickHouse)] HEALTH channel=agg_trades_5s lag_ms avg=1525.3 max=2201
2026-02-05 08:20:45,225 INFO root: [2.4 AggTrades 5s (ClickHouse)] [ingest] redis: events+730/5s items+1460/5s flushed+1448/5s
2026-02-05 08:20:45,226 INFO root: [2.4 AggTrades 5s (ClickHouse)] [diff] agg_trades_5s: ws+730 routed+730 written+730 lost+0
2026-02-05 08:20:45,227 INFO root: [2.4 AggTrades 5s (ClickHouse)] [loss] agg_trades_5s: ws->router 0 | router->writer 0 | writer->ch 6
2026-02-05 08:20:45,228 INFO root: [2.4 AggTrades 5s (ClickHouse)] [health] agg_trades_5s: expected=544/5s flushed=724 pending=6 missing=0 backlog=0 backlog_ws=6  
2026-02-05 08:20:45,228 INFO root: [2.4 AggTrades 5s (ClickHouse)] [sys] py_cpu=10.9% | py_rss=75.9MB | py_io_read=0.00MB/5s | py_io_write=0.00MB/5s
2026-02-05 08:20:45,307 INFO feeds.binance: [2.1 1s Preis + Funding (ClickHouse)] ws-stats funding: msgs+5440/10s conns+0 discs+0 | mark_price: msgs+5440/10s conns+0 discs+0
2026-02-05 08:20:45,460 INFO root: [2.1 1s Preis + Funding (ClickHouse)] HEALTH channel=mark_price lag_ms avg=0.0 max=0
2026-02-05 08:20:45,462 INFO root: [2.1 1s Preis + Funding (ClickHouse)] HEALTH channel=funding lag_ms avg=0.0 max=0
2026-02-05 08:20:45,705 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [ingest] redis: events+10880/10s items+16320/10s flushed+22394/10s
2026-02-05 08:20:45,706 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [diff] funding: ws+5440 routed+5440 written+5440 lost+0 | mark_price: ws+5440 routed+5440 written+5440 lost+0
2026-02-05 08:20:45,706 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [loss] funding: ws->router 0 | router->writer 0 | writer->ch -1956 | mark_price: ws->router 0 | router->writer 0 | writer->ch -2059
2026-02-05 08:20:45,708 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [health] mark_price: expected=5440/10s flushed=7499 pending=0 missing=0 backlog=0 backlog_ws=0 | funding: expected=5440/10s flushed=7396 pending=0 missing=0 backlog=0 backlog_ws=0
2026-02-05 08:20:45,709 INFO root: [2.1 1s Preis + Funding (ClickHouse)] [sys] py_cpu=6.2% | py_rss=71.0MB | py_io_read=0.00MB/10s | py_io_write=0.00MB/10s        
2026-02-05 08:20:49,956 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] ws-stats agg_trades_5s: msgs+680/5s conns+0 discs+0
2026-02-05 08:20:49,957 INFO feeds.binance: [2.4 AggTrades 5s (ClickHouse)] agg-trades backlog=0 enq+2360/5s proc+2365/5s emit+680/5s drop+0/5s
2026-02-05 08:20:50,082 INFO root: [2.4 AggTrades 5s (ClickHouse)] HEALTH channel=agg_trades_5s lag_ms avg=1123.4 max=2255
2026-02-05 08:20:50,229 INFO root: [2.4 AggTrades 5s (ClickHouse)] [ingest] redis: events+708/5s items+1416/5s flushed+1584/5s
2026-02-05 08:20:50,232 INFO root: [2.4 AggTrades 5s (ClickHouse)] [diff] agg_trades_5s: ws+708 routed+708 written+708 lost+0
2026-02-05 08:20:50,232 INFO root: [2.4 AggTrades 5s (ClickHouse)] [loss] agg_trades_5s: ws->router 0 | router->writer 0 | writer->ch -84
2026-02-05 08:20:50,233 INFO root: [2.4 AggTrades 5s (ClickHouse)] [health] agg_trades_5s: expected=544/5s flushed=792 pending=0 missing=0 backlog=0 backlog_ws=0  
2026-02-05 08:20:50,233 INFO root: [2.4 AggTrades 5s (ClickHouse)] [sys] py_cpu=11.9% | py_rss=76.0MB | py_io_read=0.00MB/5s | py_io_write=0.00MB/5s

lickHouseWriter aktiv (host=localhost, port=8123, db=mydb)
2025-10-12 17:38:50,303 - Main - INFO - Starting global streams...
2025-10-12 17:38:50,303 - Main - INFO - Starting 18 shards...
2025-10-12 17:38:50,303 - Shard-1 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:38:51,012 - GlobalStreams - INFO - Connected to !markPrice@arr
2025-10-12 17:38:51,028 - GlobalStreams - INFO - Connected to !forceOrder@arr
2025-10-12 17:38:51,056 - GlobalStreams - INFO - Connected to !bookTicker
2025-10-12 17:38:52,313 - Shard-2 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:38:53,970 - clickhouse_connect.driver.transform - ERROR - Error serializing column `next_funding_time` into data type `DateTime64(3)`
Traceback (most recent call last):
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\driver\transform.py", line 101, in chunk_gen
    col_type.write_column(data, output, context)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 215, in write_column
    self.write_column_data(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 230, in write_column_data
    self._write_column_binary(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\temporal.py", line 305, in _write_column_binary
    column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
                    ^^^^^^^^^^^
AttributeError: 'float' object has no attribute 'timestamp'
2025-10-12 17:38:54,014 - ClickHouseWriter - ERROR - CH insert binance_mark_price failed: 'float' object has no attribute 'timestamp'
2025-10-12 17:38:54,321 - Shard-3 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:38:56,324 - Shard-4 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:38:56,641 - clickhouse_connect.driver.transform - ERROR - Error serializing column `next_funding_time` into data type `DateTime64(3)`
Traceback (most recent call last):
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\driver\transform.py", line 101, in chunk_gen
    col_type.write_column(data, output, context)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 215, in write_column
    self.write_column_data(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 230, in write_column_data
    self._write_column_binary(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\temporal.py", line 305, in _write_column_binary
    column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
                    ^^^^^^^^^^^
AttributeError: 'float' object has no attribute 'timestamp'
2025-10-12 17:38:56,688 - ClickHouseWriter - ERROR - CH insert binance_mark_price failed: 'float' object has no attribute 'timestamp'
2025-10-12 17:38:58,326 - Shard-5 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:38:59,804 - clickhouse_connect.driver.transform - ERROR - Error serializing column `next_funding_time` into data type `DateTime64(3)`
Traceback (most recent call last):
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\driver\transform.py", line 101, in chunk_gen
    col_type.write_column(data, output, context)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 215, in write_column
    self.write_column_data(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 230, in write_column_data
    self._write_column_binary(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\temporal.py", line 305, in _write_column_binary
    column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
                    ^^^^^^^^^^^
AttributeError: 'float' object has no attribute 'timestamp'
2025-10-12 17:38:59,846 - ClickHouseWriter - ERROR - CH insert binance_mark_price failed: 'float' object has no attribute 'timestamp'
2025-10-12 17:39:00,326 - Shard-6 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:39:02,334 - Shard-7 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:39:02,764 - clickhouse_connect.driver.transform - ERROR - Error serializing column `next_funding_time` into data type `DateTime64(3)`
Traceback (most recent call last):
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\driver\transform.py", line 101, in chunk_gen
    col_type.write_column(data, output, context)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 215, in write_column
    self.write_column_data(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 230, in write_column_data
    self._write_column_binary(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\temporal.py", line 305, in _write_column_binary
    column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
                    ^^^^^^^^^^^
AttributeError: 'float' object has no attribute 'timestamp'
2025-10-12 17:39:02,807 - ClickHouseWriter - ERROR - CH insert binance_mark_price failed: 'float' object has no attribute 'timestamp'
2025-10-12 17:39:04,351 - Shard-8 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:39:05,699 - clickhouse_connect.driver.transform - ERROR - Error serializing column `next_funding_time` into data type `DateTime64(3)`
Traceback (most recent call last):
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\driver\transform.py", line 101, in chunk_gen
    col_type.write_column(data, output, context)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 215, in write_column
    self.write_column_data(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 230, in write_column_data
    self._write_column_binary(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\temporal.py", line 305, in _write_column_binary
    column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
                    ^^^^^^^^^^^
AttributeError: 'float' object has no attribute 'timestamp'
2025-10-12 17:39:05,740 - ClickHouseWriter - ERROR - CH insert binance_mark_price failed: 'float' object has no attribute 'timestamp'
2025-10-12 17:39:06,357 - Shard-9 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:39:08,366 - Shard-10 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:39:08,645 - clickhouse_connect.driver.transform - ERROR - Error serializing column `next_funding_time` into data type `DateTime64(3)`
Traceback (most recent call last):
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\driver\transform.py", line 101, in chunk_gen
    col_type.write_column(data, output, context)
    ~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 215, in write_column
    self.write_column_data(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\base.py", line 230, in write_column_data
    self._write_column_binary(column, dest, ctx)
    ~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^
  File "C:\Users\felix_n9pq3vl\AppData\Local\Programs\Python\Python313\Lib\site-packages\clickhouse_connect\datatypes\temporal.py", line 305, in _write_column_binary
    column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
                    ^^^^^^^^^^^
AttributeError: 'float' object has no attribute 'timestamp'
2025-10-12 17:39:08,688 - ClickHouseWriter - ERROR - CH insert binance_mark_price failed: 'float' object has no attribute 'timestamp'
2025-10-12 17:39:10,369 - Shard-11 - INFO - Connecting 30 symbols, 60 streams
2025-10-12 17:39:11,600 - clickhouse_connect.driver.transform - ERROR - Error serializing column `next_fund
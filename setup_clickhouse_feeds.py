from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

try:
    # Erlaubt das Laden der Feed-Konfiguration ohne Installation als Paket.
    if __package__ in (None, ""):
        import sys

        sys.path.append(str(Path(__file__).resolve().parent))
    from feeds.config import load_config
except Exception:
    load_config = None  # type: ignore


@dataclass
class Settings:
    host: str
    port: int
    database: str
    user: str
    password: str
    secure: bool = False


def resolve_settings() -> Settings:
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    database = os.getenv("CLICKHOUSE_DB", "marketdata")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    secure = bool(os.getenv("CLICKHOUSE_SECURE"))

    config_path = Path("feeds/feeds.yml")
    if config_path.exists() and load_config:
        config = load_config(str(config_path))
        defaults = config.defaults.clickhouse
        parsed = urlparse(str(defaults.dsn))
        if parsed.hostname:
            host = parsed.hostname
        if parsed.port:
            port = parsed.port
        if parsed.username:
            user = parsed.username
        if parsed.password:
            password = parsed.password
        if parsed.scheme == "https":
            secure = True
        if defaults.database:
            database = defaults.database
    elif config_path.exists():
        dsn, db = _load_clickhouse_from_yaml(config_path)
        if dsn:
            parsed = urlparse(dsn)
            if parsed.hostname:
                host = parsed.hostname
            if parsed.port:
                port = parsed.port
            if parsed.username:
                user = parsed.username
            if parsed.password:
                password = parsed.password
            if parsed.scheme == "https":
                secure = True
        if db:
            database = db

    return Settings(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        secure=secure,
    )


def build_schema_sql(database: str) -> Iterable[Tuple[str, str]]:
    common = """
        instrument String,
        ts_event_ns UInt64,
        ts_recv_ns UInt64,
        event_time DateTime64(9) MATERIALIZED toDateTime64(ts_event_ns / 1000000000, 9),
        recv_time DateTime64(9) MATERIALIZED toDateTime64(ts_recv_ns / 1000000000, 9)
    """
    engine = """
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(event_time)
        ORDER BY (instrument, ts_event_ns)
    """

    tables = {
        "trades": f"""
            CREATE TABLE IF NOT EXISTS {database}.trades (
                {common},
                price Decimal(38, 18),
                qty Decimal(38, 18),
                side LowCardinality(String),
                trade_id Nullable(String),
                is_aggressor Nullable(UInt8)
            )
            {engine}
        """,
        "agg_trades_5s": f"""
            CREATE TABLE IF NOT EXISTS {database}.agg_trades_5s (
                {common},
                interval_s UInt16,
                window_start_ns UInt64,
                open Decimal(38, 18),
                high Decimal(38, 18),
                low Decimal(38, 18),
                close Decimal(38, 18),
                volume Decimal(38, 18),
                notional Decimal(38, 18),
                trade_count UInt32,
                buy_qty Decimal(38, 18),
                sell_qty Decimal(38, 18),
                buy_notional Decimal(38, 18),
                sell_notional Decimal(38, 18),
                first_trade_id Nullable(String),
                last_trade_id Nullable(String)
            )
            {engine}
        """,
        "l1": f"""
            CREATE TABLE IF NOT EXISTS {database}.l1 (
                {common},
                depth UInt16,
                bid_prices Array(Decimal(38, 18)),
                bid_qtys Array(Decimal(38, 18)),
                ask_prices Array(Decimal(38, 18)),
                ask_qtys Array(Decimal(38, 18))
            )
            {engine}
        """,
        "ob_top5": f"""
            CREATE TABLE IF NOT EXISTS {database}.ob_top5 (
                {common},
                depth UInt16,
                bid_prices Array(Decimal(38, 18)),
                bid_qtys Array(Decimal(38, 18)),
                ask_prices Array(Decimal(38, 18)),
                ask_qtys Array(Decimal(38, 18))
            )
            {engine}
        """,
        "ob_top20": f"""
            CREATE TABLE IF NOT EXISTS {database}.ob_top20 (
                {common},
                depth UInt16,
                bid_prices Array(Decimal(38, 18)),
                bid_qtys Array(Decimal(38, 18)),
                ask_prices Array(Decimal(38, 18)),
                ask_qtys Array(Decimal(38, 18))
            )
            {engine}
        """,
        "order_book_diffs": f"""
            CREATE TABLE IF NOT EXISTS {database}.order_book_diffs (
                {common},
                sequence UInt64,
                prev_sequence UInt64,
                bids Map(String, Decimal(38, 18)),
                asks Map(String, Decimal(38, 18))
            )
            {engine}
        """,
        "liquidations": f"""
            CREATE TABLE IF NOT EXISTS {database}.liquidations (
                {common},
                side LowCardinality(String),
                price Decimal(38, 18),
                qty Decimal(38, 18),
                order_id Nullable(String),
                reason Nullable(String)
            )
            {engine}
        """,
        "mark_price": f"""
            CREATE TABLE IF NOT EXISTS {database}.mark_price (
                {common},
                mark_price Decimal(38, 18),
                index_price Nullable(Decimal(38, 18))
            )
            {engine}
        """,
        "funding": f"""
            CREATE TABLE IF NOT EXISTS {database}.funding (
                {common},
                funding_rate Decimal(38, 18),
                next_funding_ts_ns UInt64
            )
            {engine}
        """,
        "advanced_metrics": f"""
            CREATE TABLE IF NOT EXISTS {database}.advanced_metrics (
                {common},
                metrics Map(String, Decimal(38, 18))
            )
            {engine}
        """,
        "klines": f"""
            CREATE TABLE IF NOT EXISTS {database}.klines (
                {common},
                interval LowCardinality(String),
                open Decimal(38, 18),
                high Decimal(38, 18),
                low Decimal(38, 18),
                close Decimal(38, 18),
                volume Decimal(38, 18),
                quote_volume Decimal(38, 18),
                taker_buy_base_volume Decimal(38, 18),
                taker_buy_quote_volume Decimal(38, 18),
                trade_count UInt32,
                is_closed UInt8
            )
            {engine}
        """,
    }

    for name, ddl in tables.items():
        yield name, " ".join(ddl.split())


def build_migration_sql(database: str) -> Iterable[Tuple[str, str]]:
    migrations = {
        "funding.funding_rate": (
            f"ALTER TABLE {database}.funding "
            "ADD COLUMN IF NOT EXISTS funding_rate Decimal(38, 18)"
        ),
        "funding.next_funding_ts_ns": (
            f"ALTER TABLE {database}.funding "
            "ADD COLUMN IF NOT EXISTS next_funding_ts_ns UInt64"
        ),
        "klines.quote_volume": (
            f"ALTER TABLE {database}.klines "
            "ADD COLUMN IF NOT EXISTS quote_volume Decimal(38, 18) AFTER volume"
        ),
        "klines.taker_buy_base_volume": (
            f"ALTER TABLE {database}.klines "
            "ADD COLUMN IF NOT EXISTS taker_buy_base_volume Decimal(38, 18) AFTER quote_volume"
        ),
        "klines.taker_buy_quote_volume": (
            f"ALTER TABLE {database}.klines "
            "ADD COLUMN IF NOT EXISTS taker_buy_quote_volume Decimal(38, 18) AFTER taker_buy_base_volume"
        ),
    }
    for name, sql in migrations.items():
        yield name, " ".join(sql.split())


def execute_query(
    base_url: str,
    query: str,
    *,
    user: str,
    password: str,
    database: Optional[str] = None,
) -> None:
    params = {"query": query}
    if user:
        params["user"] = user
    if password:
        params["password"] = password
    if database:
        params["database"] = database
    url = f"{base_url}/?{urlencode(params)}"
    request = Request(url, method="POST")
    with urlopen(request, timeout=10) as response:
        response.read()


def _load_clickhouse_from_yaml(path: Path) -> Tuple[Optional[str], Optional[str]]:
    dsn = None
    database = None
    stack: list[tuple[int, str]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        key_value = line.strip().split(":", 1)
        if len(key_value) != 2:
            continue
        key = key_value[0].strip()
        value = key_value[1].strip().strip('"').strip("'")
        while stack and indent <= stack[-1][0]:
            stack.pop()
        stack.append((indent, key))
        path_keys = [entry[1] for entry in stack]
        if path_keys == ["defaults", "clickhouse", "dsn"] and value:
            dsn = value
        if path_keys == ["defaults", "clickhouse", "database"] and value:
            database = value
    return dsn, database


def main() -> int:
    settings = resolve_settings()
    scheme = "https" if settings.secure else "http"
    base_url = f"{scheme}://{settings.host}:{settings.port}"

    try:
        for attempt in range(1, 11):
            try:
                execute_query(
                    base_url,
                    f"CREATE DATABASE IF NOT EXISTS {settings.database}",
                    user=settings.user,
                    password=settings.password,
                )
                break
            except URLError:
                if attempt == 10:
                    raise
                time.sleep(1.0)
        for table_name, ddl in build_schema_sql(settings.database):
            execute_query(
                base_url,
                ddl,
                user=settings.user,
                password=settings.password,
                database=settings.database,
            )
            print(f"OK: {table_name}")
        for migration_name, migration_sql in build_migration_sql(settings.database):
            execute_query(
                base_url,
                migration_sql,
                user=settings.user,
                password=settings.password,
                database=settings.database,
            )
            print(f"OK: migration {migration_name}")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        print(f"HTTP-Fehler {exc.code}: {body or exc.reason}")
        return 1
    except URLError as exc:
        print(f"Verbindungsfehler: {exc.reason}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

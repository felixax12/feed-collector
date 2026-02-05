from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Sequence, Tuple
from pathlib import Path
from urllib.parse import urlparse

try:
    import clickhouse_connect
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Das Paket 'clickhouse-connect' fehlt. Bitte mit 'pip install clickhouse-connect' installieren."
    ) from exc


try:
    # Erlaubt das Laden der Feed-Konfiguration ohne Installation als Paket.
    if __package__ in (None, ""):
        import sys
        from pathlib import Path

        sys.path.append(str(Path(__file__).resolve().parent))
    from feeds.config import load_config
except Exception:  # pragma: no cover
    load_config = None  # type: ignore


DEFAULT_TABLES = [
    "advanced_metrics",
    "agg_trades_5s",
    "funding",
    "klines",
    "l1",
    "liquidations",
    "mark_price",
    "ob_top20",
    "ob_top5",
    "order_book_diffs",
    "trades",
]

MAX_DISPLAY_ROWS = 50


@dataclass
class Settings:
    host: str
    port: int
    database: str
    user: str
    password: str
    secure: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ClickHouse Inspector")
    parser.add_argument("--config", help="Pfad zu feeds/feeds.yml (optional).")
    parser.add_argument("--host", help="ClickHouse Hostname oder IP.")
    parser.add_argument("--port", type=int, help="ClickHouse Port (HTTP).")
    parser.add_argument("--database", help="Standarddatenbank.")
    parser.add_argument("--user", help="Benutzername.")
    parser.add_argument("--password", help="Passwort.")
    parser.add_argument("--secure", action="store_true", help="HTTPS verwenden.")
    return parser.parse_args()


def resolve_settings(args: argparse.Namespace) -> Settings:
    host = args.host or os.getenv("CLICKHOUSE_HOST", "localhost")
    port = args.port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
    database = args.database or os.getenv("CLICKHOUSE_DB", "marketdata")
    user = args.user or os.getenv("CLICKHOUSE_USER", "default")
    password = args.password or os.getenv("CLICKHOUSE_PASSWORD", "")
    secure = bool(args.secure or os.getenv("CLICKHOUSE_SECURE"))

    config_path = args.config
    if not config_path and Path("feeds/feeds.yml").exists():
        config_path = "feeds/feeds.yml"

    if config_path and load_config:
        config = load_config(config_path)
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
        database = defaults.database or database

    return Settings(host=host, port=port, database=database, user=user, password=password, secure=secure)


def get_client(settings: Settings, database: str | None = None):
    db = database or settings.database
    return clickhouse_connect.get_client(
        host=settings.host,
        port=settings.port,
        username=settings.user,
        password=settings.password,
        secure=settings.secure,
        database=db,
    )


def fetch_databases(client) -> List[str]:
    result = client.query("SELECT name FROM system.databases ORDER BY name")
    return [row[0] for row in result.result_rows]


def list_databases(client, _settings: Settings, current_db: str) -> None:
    dbs = fetch_databases(client)
    print("\nVerfügbare Datenbanken:")
    for name in dbs:
        marker = " <- aktiv" if name == current_db else ""
        print(f"  - {name}{marker}")


def switch_database(client, settings: Settings, current_db: str) -> str:
    dbs = fetch_databases(client)
    if not dbs:
        print("Keine Datenbanken vorhanden.")
        return current_db

    print("\nDatenbanken:")
    for idx, name in enumerate(dbs, 1):
        marker = " <- aktiv" if name == current_db else ""
        print(f"  {idx}. {name}{marker}")

    choice = input("Auswahl (Nummer oder Name): ").strip()
    if choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(dbs):
            current_db = dbs[idx]
    elif choice in dbs:
        current_db = choice
    else:
        print("Ungültige Auswahl.")
        return current_db

    return current_db


def fetch_tables(client, database: str) -> List[str]:
    query = """
        SELECT name
        FROM system.tables
        WHERE database = %(db)s
        ORDER BY name
    """
    result = client.query(query, parameters={"db": database})
    return [row[0] for row in result.result_rows]


def list_tables(client, settings: Settings, current_db: str) -> None:
    tables = fetch_tables(client, current_db)
    if not tables:
        print(f"Keine Tabellen in '{current_db}'.")
        return
    print(f"\nTabellen in '{current_db}':")
    for name in tables:
        marker = " (Standard)" if name in DEFAULT_TABLES else ""
        print(f"  - {name}{marker}")


def describe_table(client, settings: Settings, current_db: str) -> None:
    table = prompt_table_name(client, current_db)
    if not table:
        return
    result = client.query(f"DESCRIBE TABLE {current_db}.{table}")
    print_rows(result.column_names, result.result_rows)


def view_data(client, settings: Settings, current_db: str) -> None:
    table = prompt_table_name(client, current_db)
    if not table:
        return
    limit = input("LIMIT (Standard 20): ").strip()
    limit = limit or "20"
    query = f"SELECT * FROM {current_db}.{table} LIMIT {limit}"
    result = client.query(query)
    print_rows(result.column_names, result.result_rows)


def count_rows(client, settings: Settings, current_db: str) -> None:
    tables = fetch_tables(client, current_db)
    if not tables:
        print("Keine Tabellen.")
        return
    rows = []
    for table in tables:
        result = client.query(
            "SELECT count() FROM {db}.{table}".format(db=current_db, table=table)
        )
        rows.append((table, result.result_rows[0][0]))
    print_rows(["table", "rows"], rows)


def run_custom_sql(client, settings: Settings, current_db: str) -> None:
    print("SQL eingeben (leere Zeile beendet):")
    lines: List[str] = []
    while True:
        line = input()
        if not line:
            break
        lines.append(line)
    if not lines:
        print("Keine Abfrage eingegeben.")
        return
    query = "\n".join(lines)
    try:
        result = client.query(query)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Fehler: {exc}")
        return
    if result.result_rows:
        print_rows(result.column_names, result.result_rows)
    else:
        print("Abfrage erfolgreich, keine Zeilen zurückgegeben.")


def drop_table(client, settings: Settings, current_db: str) -> None:
    table = prompt_table_name(client, current_db)
    if not table:
        return
    confirm = input(f"Tabelle '{current_db}.{table}' wirklich löschen? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Abgebrochen.")
        return
    client.command(f"DROP TABLE {current_db}.{table}")
    print("Tabelle gelöscht.")


def _detect_time_divisor(client, current_db: str, table: str) -> tuple[int, str]:
    result = client.query(f"SELECT max(ts_event_ns) FROM {current_db}.{table}")
    max_ts = result.result_rows[0][0]
    if max_ts is None:
        return 1_000_000_000, "ns"
    if max_ts >= 1_000_000_000_000_000:
        return 1_000_000_000, "ns"
    return 1000, "ms"


def _check_per_second_completeness(
    client,
    current_db: str,
    *,
    table: str,
    title: str,
) -> None:
    print(f"\n{title}")
    minutes_raw = input("Zeitraum in Minuten (Default 10, 'all' fuer alles): ").strip().lower()
    if minutes_raw in {"", "10"}:
        minutes = 10
    elif minutes_raw == "all":
        minutes = None
    elif minutes_raw.isdigit():
        minutes = int(minutes_raw)
    else:
        print("Ungueltige Eingabe.")
        return

    divisor, unit = _detect_time_divisor(client, current_db, table)
    bounds = client.query(
        "SELECT min(toUInt32(ts_event_ns/{div})) AS min_sec, "
        "max(toUInt32(ts_event_ns/{div})) AS max_sec "
        "FROM {db}.{table}".format(db=current_db, table=table, div=divisor)
    )
    min_sec, max_sec = bounds.result_rows[0]
    if min_sec is None or max_sec is None:
        print(f"Keine Daten in {table}.")
        return

    if minutes is None:
        start_sec = int(min_sec)
    else:
        start_sec = max(int(min_sec), int(max_sec) - minutes * 60 + 1)
    end_sec = int(max_sec)

    expected = client.query(
        "SELECT countDistinct(instrument) FROM {db}.{table} "
        "WHERE toUInt32(ts_event_ns/{div}) BETWEEN %(start)s AND %(end)s".format(
            db=current_db, table=table, div=divisor
        ),
        parameters={"start": start_sec, "end": end_sec},
    )
    expected_symbols = expected.result_rows[0][0]
    if not expected_symbols:
        print("Keine Symbole im gewaehlten Zeitraum.")
        return

    summary = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          %(expected)s AS expected_symbols
        SELECT
          sum(missing) AS missing_total,
          countIf(missing > 0) AS seconds_with_missing,
          max(missing) AS max_missing
        FROM (
          SELECT
            sec,
            expected_symbols - ifNull(actual_symbols, 0) AS missing
          FROM
            (SELECT arrayJoin(range(start_sec, end_sec + 1)) AS sec) AS timeline
          LEFT JOIN
            (
              SELECT
                toUInt32(ts_event_ns/{div}) AS sec,
                countDistinct(instrument) AS actual_symbols
              FROM {db}.{table}
              WHERE sec BETWEEN start_sec AND end_sec
              GROUP BY sec
            ) AS actual USING sec
        )
        """.format(db=current_db, table=table, div=divisor),
        parameters={"start": start_sec, "end": end_sec, "expected": expected_symbols},
    )
    missing_total, seconds_with_missing, max_missing = summary.result_rows[0]

    print(
        f"Zeitraum: {start_sec}..{end_sec} (Sekunden, ts_event in {unit}), Symbole: {expected_symbols}\n"
        f"Fehlende Eintraege gesamt: {missing_total}, "
        f"Sekunden mit Luecken: {seconds_with_missing}, "
        f"Max fehlend in Sekunde: {max_missing}"
    )

    rows = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          %(expected)s AS expected_symbols
        SELECT
          toDateTime(sec) AS second,
          ifNull(actual_symbols, 0) AS actual,
          expected_symbols - ifNull(actual_symbols, 0) AS missing
        FROM
          (SELECT arrayJoin(range(start_sec, end_sec + 1)) AS sec) AS timeline
        LEFT JOIN
          (
            SELECT
              toUInt32(ts_event_ns/{div}) AS sec,
              countDistinct(instrument) AS actual_symbols
            FROM {db}.{table}
            WHERE sec BETWEEN start_sec AND end_sec
            GROUP BY sec
          ) AS actual USING sec
        WHERE missing > 0
        ORDER BY sec
        """.format(db=current_db, table=table, div=divisor),
        parameters={"start": start_sec, "end": end_sec, "expected": expected_symbols},
    )
    print("\nLuecken pro Sekunde (erste Zeilen):")
    print_rows(rows.column_names, rows.result_rows)

    per_symbol = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          (end_sec - start_sec + 1) AS expected_secs
        SELECT
          instrument,
          expected_secs - countDistinct(toUInt32(ts_event_ns/{div})) AS missing
        FROM {db}.{table}
        WHERE toUInt32(ts_event_ns/{div}) BETWEEN start_sec AND end_sec
        GROUP BY instrument
        HAVING missing > 0
        ORDER BY missing DESC
        LIMIT 20
        """.format(db=current_db, table=table, div=divisor),
        parameters={"start": start_sec, "end": end_sec},
    )
    print("\nTop 20 Symbole mit Luecken:")
    print_rows(per_symbol.column_names, per_symbol.result_rows)


def check_mark_price_completeness(client, settings: Settings, current_db: str) -> None:
    _check_per_second_completeness(
        client,
        current_db,
        table="mark_price",
        title="Mark-Price Vollstaendigkeit (1 Zeile pro Symbol pro Sekunde)",
    )


def check_funding_completeness(client, settings: Settings, current_db: str) -> None:
    _check_per_second_completeness(
        client,
        current_db,
        table="funding",
        title="Funding Vollstaendigkeit (1 Zeile pro Symbol pro Sekunde)",
    )


def check_ob_top5_completeness(client, settings: Settings, current_db: str) -> None:
    print("\nOB Top5 Vollstaendigkeit (1 Zeile pro Symbol pro Sekunde)")
    minutes_raw = input("Zeitraum in Minuten (Default 10, 'all' fuer alles): ").strip().lower()
    if minutes_raw in {"", "10"}:
        minutes = 10
    elif minutes_raw == "all":
        minutes = None
    elif minutes_raw.isdigit():
        minutes = int(minutes_raw)
    else:
        print("Ungueltige Eingabe.")
        return

    divisor, unit = _detect_time_divisor(client, current_db, "ob_top5")
    bounds = client.query(
        "SELECT min(toUInt32(ts_event_ns/{div})) AS min_sec, "
        "max(toUInt32(ts_event_ns/{div})) AS max_sec "
        "FROM {db}.ob_top5".format(db=current_db, div=divisor)
    )
    min_sec, max_sec = bounds.result_rows[0]
    if min_sec is None or max_sec is None:
        print("Keine Daten in ob_top5.")
        return

    if minutes is None:
        start_sec = int(min_sec)
    else:
        start_sec = max(int(min_sec), int(max_sec) - minutes * 60 + 1)
    end_sec = int(max_sec)

    expected = client.query(
        "SELECT countDistinct(instrument) FROM {db}.ob_top5 "
        "WHERE toUInt32(ts_event_ns/{div}) BETWEEN %(start)s AND %(end)s".format(db=current_db, div=divisor),
        parameters={"start": start_sec, "end": end_sec},
    )
    expected_symbols = expected.result_rows[0][0]
    if not expected_symbols:
        print("Keine Symbole im gewaehlten Zeitraum.")
        return

    summary = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          %(expected)s AS expected_symbols
        SELECT
          sum(missing) AS missing_total,
          countIf(missing > 0) AS seconds_with_missing,
          max(missing) AS max_missing
        FROM (
          SELECT
            sec,
            expected_symbols - ifNull(actual_symbols, 0) AS missing
          FROM
            (SELECT arrayJoin(range(start_sec, end_sec + 1)) AS sec) AS timeline
          LEFT JOIN
            (
              SELECT
                toUInt32(ts_event_ns/{div}) AS sec,
                countDistinct(instrument) AS actual_symbols
              FROM {db}.ob_top5
              WHERE sec BETWEEN start_sec AND end_sec
              GROUP BY sec
            ) AS actual USING sec
        )
        """.format(db=current_db, div=divisor),
        parameters={"start": start_sec, "end": end_sec, "expected": expected_symbols},
    )
    missing_total, seconds_with_missing, max_missing = summary.result_rows[0]

    print(
        f"Zeitraum: {start_sec}..{end_sec} (Sekunden, ts_event in {unit}), Symbole: {expected_symbols}\n"
        f"Fehlende Eintraege gesamt: {missing_total}, "
        f"Sekunden mit Luecken: {seconds_with_missing}, "
        f"Max fehlend in Sekunde: {max_missing}"
    )

    rows = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          %(expected)s AS expected_symbols
        SELECT
          toDateTime(sec) AS second,
          ifNull(actual_symbols, 0) AS actual,
          expected_symbols - ifNull(actual_symbols, 0) AS missing
        FROM
          (SELECT arrayJoin(range(start_sec, end_sec + 1)) AS sec) AS timeline
        LEFT JOIN
          (
            SELECT
              toUInt32(ts_event_ns/{div}) AS sec,
              countDistinct(instrument) AS actual_symbols
            FROM {db}.ob_top5
            WHERE sec BETWEEN start_sec AND end_sec
            GROUP BY sec
          ) AS actual USING sec
        WHERE missing > 0
        ORDER BY sec
        """.format(db=current_db, div=divisor),
        parameters={"start": start_sec, "end": end_sec, "expected": expected_symbols},
    )
    print("\nLuecken pro Sekunde (erste Zeilen):")
    print_rows(rows.column_names, rows.result_rows)

    per_symbol = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          (end_sec - start_sec + 1) AS expected_secs
        SELECT
          instrument,
          expected_secs - countDistinct(toUInt32(ts_event_ns/{div})) AS missing
        FROM {db}.ob_top5
        WHERE toUInt32(ts_event_ns/{div}) BETWEEN start_sec AND end_sec
        GROUP BY instrument
        HAVING missing > 0
        ORDER BY missing DESC
        LIMIT 20
        """.format(db=current_db, div=divisor),
        parameters={"start": start_sec, "end": end_sec},
    )
    print("\nTop 20 Symbole mit Luecken:")
    print_rows(per_symbol.column_names, per_symbol.result_rows)


def check_klines_1m_completeness(client, settings: Settings, current_db: str) -> None:
    print("\nKlines Vollstaendigkeit (1 Zeile pro Symbol pro Minute, interval=1m, is_closed=1)")
    minutes_raw = input("Zeitraum in Minuten (Default 10, 'all' fuer alles): ").strip().lower()
    if minutes_raw in {"", "10"}:
        minutes = 10
    elif minutes_raw == "all":
        minutes = None
    elif minutes_raw.isdigit():
        minutes = int(minutes_raw)
    else:
        print("Ungueltige Eingabe.")
        return

    divisor, unit = _detect_time_divisor(client, current_db, "klines")
    bounds = client.query(
        "SELECT min(toUInt32(ts_event_ns/{div})) AS min_sec, "
        "max(toUInt32(ts_event_ns/{div})) AS max_sec "
        "FROM {db}.klines WHERE interval = '1m' AND is_closed = 1".format(
            db=current_db, div=divisor
        )
    )
    min_sec, max_sec = bounds.result_rows[0]
    if min_sec is None or max_sec is None:
        print("Keine Daten in klines (interval=1m, is_closed=1).")
        return

    if minutes is None:
        start_sec = int(min_sec)
    else:
        start_sec = max(int(min_sec), int(max_sec) - minutes * 60 + 1)
    end_sec = int(max_sec)

    expected = client.query(
        "SELECT countDistinct(instrument) FROM {db}.klines "
        "WHERE interval = '1m' AND is_closed = 1 "
        "AND toUInt32(ts_event_ns/{div}) BETWEEN %(start)s AND %(end)s".format(
            db=current_db, div=divisor
        ),
        parameters={"start": start_sec, "end": end_sec},
    )
    expected_symbols = expected.result_rows[0][0]
    if not expected_symbols:
        print("Keine Symbole im gewaehlten Zeitraum.")
        return

    summary = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          %(expected)s AS expected_symbols
        SELECT
          sum(missing) AS missing_total,
          countIf(missing > 0) AS minutes_with_missing,
          max(missing) AS max_missing
        FROM (
          SELECT
            minute,
            expected_symbols - ifNull(actual_symbols, 0) AS missing
          FROM
            (SELECT arrayJoin(range(intDiv(start_sec, 60), intDiv(end_sec, 60) + 1)) AS minute) AS timeline
          LEFT JOIN
            (
              SELECT
                toUInt32(intDiv(ts_event_ns/{div}, 60)) AS minute,
                countDistinct(instrument) AS actual_symbols
              FROM {db}.klines
              WHERE interval = '1m' AND is_closed = 1
                AND toUInt32(ts_event_ns/{div}) BETWEEN start_sec AND end_sec
              GROUP BY minute
            ) AS actual USING minute
        )
        """.format(db=current_db, div=divisor),
        parameters={"start": start_sec, "end": end_sec, "expected": expected_symbols},
    )
    missing_total, minutes_with_missing, max_missing = summary.result_rows[0]

    print(
        f"Zeitraum: {start_sec}..{end_sec} (Sekunden, ts_event in {unit}), Symbole: {expected_symbols}\n"
        f"Fehlende Eintraege gesamt: {missing_total}, "
        f"Minuten mit Luecken: {minutes_with_missing}, "
        f"Max fehlend pro Minute: {max_missing}"
    )

    rows = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          %(expected)s AS expected_symbols
        SELECT
          minute,
          expected_symbols - ifNull(actual_symbols, 0) AS missing
        FROM
          (SELECT arrayJoin(range(intDiv(start_sec, 60), intDiv(end_sec, 60) + 1)) AS minute) AS timeline
        LEFT JOIN
          (
            SELECT
              toUInt32(intDiv(ts_event_ns/{div}, 60)) AS minute,
              countDistinct(instrument) AS actual_symbols
            FROM {db}.klines
            WHERE interval = '1m' AND is_closed = 1
              AND toUInt32(ts_event_ns/{div}) BETWEEN start_sec AND end_sec
            GROUP BY minute
          ) AS actual USING minute
        WHERE expected_symbols - ifNull(actual_symbols, 0) > 0
        ORDER BY minute DESC
        LIMIT 20
        """.format(db=current_db, div=divisor),
        parameters={"start": start_sec, "end": end_sec, "expected": expected_symbols},
    )
    if rows.result_rows:
        print("\nBeispiele mit Luecken (minute, missing):")
        print_rows(rows.column_names, rows.result_rows)
    else:
        print("\nKeine Luecken im betrachteten Zeitraum gefunden.")


def check_agg_trades_5s_completeness(client, settings: Settings, current_db: str) -> None:
    print("\nAggTrades Vollstaendigkeit (1 Zeile pro Symbol pro 5 Sekunden)")
    minutes_raw = input("Zeitraum in Minuten (Default 10, 'all' fuer alles): ").strip().lower()
    if minutes_raw in {"", "10"}:
        minutes = 10
    elif minutes_raw == "all":
        minutes = None
    elif minutes_raw.isdigit():
        minutes = int(minutes_raw)
    else:
        print("Ungueltige Eingabe.")
        return

    divisor, unit = _detect_time_divisor(client, current_db, "agg_trades_5s")
    bounds = client.query(
        "SELECT min(toUInt32(ts_event_ns/{div})) AS min_sec, "
        "max(toUInt32(ts_event_ns/{div})) AS max_sec "
        "FROM {db}.agg_trades_5s".format(db=current_db, div=divisor)
    )
    min_sec, max_sec = bounds.result_rows[0]
    if min_sec is None or max_sec is None:
        print("Keine Daten in agg_trades_5s.")
        return

    if minutes is None:
        start_sec = int(min_sec)
    else:
        start_sec = max(int(min_sec), int(max_sec) - minutes * 60 + 1)
    end_sec = int(max_sec)

    expected = client.query(
        "SELECT countDistinct(instrument) FROM {db}.agg_trades_5s "
        "WHERE toUInt32(ts_event_ns/{div}) BETWEEN %(start)s AND %(end)s".format(
            db=current_db, div=divisor
        ),
        parameters={"start": start_sec, "end": end_sec},
    )
    expected_symbols = expected.result_rows[0][0]
    if not expected_symbols:
        print("Keine Symbole im gewaehlten Zeitraum.")
        return

    summary = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          %(expected)s AS expected_symbols
        SELECT
          sum(missing) AS missing_total,
          countIf(missing > 0) AS windows_with_missing,
          max(missing) AS max_missing
        FROM (
          SELECT
            toUInt32(intDiv(sec, 5)) AS window,
            expected_symbols - ifNull(actual_symbols, 0) AS missing
          FROM
            (SELECT arrayJoin(range(start_sec, end_sec + 1)) AS sec) AS timeline
          LEFT JOIN
            (
              SELECT
                toUInt32(ts_event_ns/{div}) AS sec,
                countDistinct(instrument) AS actual_symbols
              FROM {db}.agg_trades_5s
              WHERE sec BETWEEN start_sec AND end_sec
              GROUP BY sec
            ) AS actual USING sec
          GROUP BY window
        )
        """.format(db=current_db, div=divisor),
        parameters={"start": start_sec, "end": end_sec, "expected": expected_symbols},
    )
    missing_total, windows_with_missing, max_missing = summary.result_rows[0]

    print(
        f"Zeitraum: {start_sec}..{end_sec} (Sekunden, ts_event in {unit}), Symbole: {expected_symbols}\n"
        f"Fehlende Eintraege gesamt: {missing_total}, "
        f"5s-Fenster mit Luecken: {windows_with_missing}, "
        f"Max fehlend pro 5s: {max_missing}"
    )

    rows = client.query(
        """
        WITH
          %(start)s AS start_sec,
          %(end)s AS end_sec,
          %(expected)s AS expected_symbols
        SELECT
          toUInt32(intDiv(sec, 5)) AS window,
          expected_symbols - countDistinct(instrument) AS missing
        FROM {db}.agg_trades_5s
        WHERE toUInt32(ts_event_ns/{div}) BETWEEN start_sec AND end_sec
        GROUP BY window
        HAVING missing > 0
        ORDER BY window DESC
        LIMIT 20
        """.format(db=current_db, div=divisor),
        parameters={"start": start_sec, "end": end_sec, "expected": expected_symbols},
    )
    if rows.result_rows:
        print("\nBeispiele mit Luecken (window, missing):")
        print_rows(rows.column_names, rows.result_rows)
    else:
        print("\nKeine Luecken im betrachteten Zeitraum gefunden.")


def show_latest_rows(client, settings: Settings, current_db: str) -> None:
    table = prompt_table_name(client, current_db)
    if not table:
        return
    try:
        limit_raw = input("Wie viele Zeilen? (Default 20): ").strip()
        limit = int(limit_raw) if limit_raw else 20
    except ValueError:
        print("Ungueltige Eingabe.")
        return
    query = f"SELECT * FROM {current_db}.{table} ORDER BY ts_event_ns DESC LIMIT {limit}"
    result = client.query(query)
    if result.result_rows:
        print_rows(result.column_names, result.result_rows)
    else:
        print("Keine Daten gefunden.")


def drop_database(client, settings: Settings, current_db: str) -> str:
    confirm = input(f"Datenbank '{current_db}' löschen? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Abgebrochen.")
        return current_db
    client.command(f"DROP DATABASE {current_db}")
    print("Datenbank gelöscht.")
    return settings.database


def check_latency(client, *_args) -> None:
    import time

    start = time.perf_counter()
    client.query("SELECT 1")
    duration_ms = (time.perf_counter() - start) * 1000
    print(f"Roundtrip: {duration_ms:.2f} ms")


def prompt_table_name(client, database: str) -> str:
    tables = fetch_tables(client, database)
    if not tables:
        print("Keine Tabellen vorhanden.")
        return ""
    print("Verfügbare Tabellen:")
    for idx, name in enumerate(tables, 1):
        marker = " (Standard)" if name in DEFAULT_TABLES else ""
        print(f"  {idx}. {name}{marker}")
    choice = input("Tabellenname oder Nummer: ").strip()
    if choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(tables):
            return tables[idx]
    if choice in tables:
        return choice
    print("Ungültige Auswahl.")
    return ""


def print_rows(columns: Sequence[str], rows: Sequence[Sequence[object]]) -> None:
    if not rows:
        print("Keine Daten.")
        return
    display = list(rows[:MAX_DISPLAY_ROWS])
    widths = [len(col) for col in columns]
    formatted: List[List[str]] = []
    for row in display:
        formatted_row = [format_cell(value) for value in row]
        formatted.append(formatted_row)
        for idx, cell in enumerate(formatted_row):
            widths[idx] = max(widths[idx], len(cell))
    separator = "+".join("-" * (w + 2) for w in widths)
    header = "|".join(f" {columns[idx].ljust(widths[idx])} " for idx in range(len(columns)))
    print(separator)
    print(header)
    print(separator)
    for row in formatted:
        print("|".join(f" {row[idx].ljust(widths[idx])} " for idx in range(len(row))))
    print(separator)
    if len(rows) > MAX_DISPLAY_ROWS:
        print(f"... ({len(rows) - MAX_DISPLAY_ROWS} weitere Zeilen unterdrückt)")


def format_cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    return text if len(text) <= 120 else text[:117] + "..."


def main() -> None:
    args = parse_args()
    settings = resolve_settings(args)
    client = get_client(settings)
    current_db = settings.database
    print(f"Verbunden mit ClickHouse @ {settings.host}:{settings.port}, DB='{current_db}'\n")

    actions: Dict[str, Tuple[str, Callable]] = {
        "1": ("Tabellen auflisten", list_tables),
        "2": ("Tabellenschema anzeigen", describe_table),
        "3": ("Tabelleninhalt ansehen", view_data),
        "4": ("Zeilenanzahl pro Tabelle", count_rows),
        "5": ("Eigene SQL-Abfrage ausführen", run_custom_sql),
        "6": ("Datenbanken auflisten", list_databases),
        "7": ("Datenbank wechseln", switch_database),
        "8": ("Tabelle löschen", drop_table),
        "9": ("Datenbank löschen", drop_database),
        "0": ("Latenz prüfen", check_latency),
        "10": ("Mark-Price Vollstaendigkeit (1s)", check_mark_price_completeness),
        "11": ("OB Top5 Vollstaendigkeit (1s)", check_ob_top5_completeness),
        "12": ("Klines Vollstaendigkeit (1m, is_closed)", check_klines_1m_completeness),
        "13": ("AggTrades Vollstaendigkeit (5s)", check_agg_trades_5s_completeness),
        "14": ("Letzte Zeilen anzeigen", show_latest_rows),
        "15": ("Funding Vollstaendigkeit (1s)", check_funding_completeness),
        "q": ("Beenden", lambda *args: None),
    }

    while True:
        print(f"\nAktionen (aktuelle DB: {current_db}):")
        for key, (label, _) in actions.items():
            print(f"  {key} -> {label}")
        choice = input("Auswahl: ").strip().lower()
        if choice in {"q", "quit", "exit"}:
            print("Beende Inspector.")
            break
        action = actions.get(choice)
        if not action:
            print("Ungültige Auswahl.")
            continue
        label, handler = action
        if handler is switch_database:
            current_db = handler(client, settings, current_db)  # type: ignore[arg-type]
            client = get_client(settings, current_db)
        elif handler is drop_database:
            current_db = handler(client, settings, current_db)  # type: ignore[arg-type]
            client = get_client(settings, current_db)
        else:
            handler(client, settings, current_db)  # type: ignore[arg-type]


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAbgebrochen.")

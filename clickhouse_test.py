from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "felix")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "testpass")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "mydb")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "trades")
EXPORT_PATH = Path(
    os.getenv("CLICKHOUSE_EXPORT_PATH", r"C:\clickhouse_exports\trades.parquet")
)


def get_client() -> Client:
    """Return a configured ClickHouse HTTP client."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def ensure_database(client: Client) -> None:
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")


def ensure_table(client: Client) -> None:
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
            symbol String,
            price Float64,
            volume Float64,
            ts DateTime
        )
        ENGINE = MergeTree()
        ORDER BY ts
        """
    )


def insert_rows(
    client: Client,
    rows: pd.DataFrame
    | Iterable[Mapping[str, object]]
    | Iterable[Sequence[object]],
    column_names: Sequence[str] | None = None,
) -> None:
    """
    Insert data into ClickHouse.

    Accepts either a pandas DataFrame, an iterable of mappings (dict-like), or an
    iterable of iterables in the column order defined by column_names.
    """
    if isinstance(rows, pd.DataFrame):
        column_names = list(rows.columns)
        payload = [tuple(row) for row in rows.itertuples(index=False, name=None)]
    else:
        rows_list = list(rows)
        if not rows_list:
            return

        first_row = rows_list[0]
        if isinstance(first_row, Mapping):
            if column_names is None:
                column_names = list(first_row.keys())
            payload = [
                tuple(row[col] for col in column_names) for row in rows_list  # type: ignore[arg-type]
            ]
        else:
            if column_names is None:
                raise ValueError("column_names must be provided for sequence rows")
            payload = rows_list

    if not payload:
        return

    client.insert(
        f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}",
        payload,
        column_names=list(column_names),
    )


def fetch_latest(client: Client, limit: int = 10) -> pd.DataFrame:
    query = f"""
        SELECT *
        FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
        ORDER BY ts DESC
        LIMIT {limit}
    """
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def export_dataframe(df: pd.DataFrame) -> None:
    EXPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(EXPORT_PATH, index=False)


def demo_flow() -> None:
    client = get_client()
    ensure_database(client)
    ensure_table(client)

    demo_rows = [
        ("BTCUSDT", 65000.5, 0.01, datetime(2025, 10, 10, 15, 0, 0)),
        ("ETHUSDT", 2400.3, 1.5, datetime(2025, 10, 10, 15, 0, 1)),
    ]
    insert_rows(
        client,
        demo_rows,
        column_names=["symbol", "price", "volume", "ts"],
    )

    df = fetch_latest(client, limit=10)
    print(df)
    export_dataframe(df)
    print(f"Export gespeichert unter {EXPORT_PATH}")


if __name__ == "__main__":
    demo_flow()

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

try:
    import redis
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Das Paket 'redis' fehlt. Installiere es mit 'pip install redis'.") from exc

try:
    if __package__ in (None, ""):
        import sys
        from pathlib import Path

        sys.path.append(str(Path(__file__).resolve().parent))
    from feeds.config import load_config
except Exception:  # pragma: no cover
    load_config = None  # type: ignore


@dataclass
class RedisSettings:
    host: str
    port: int
    db: int
    password: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Redis Inspector")
    parser.add_argument("--config", help="Pfad zu feeds/feeds.yml (optional).")
    parser.add_argument("--dsn", help="redis:// URI.")
    parser.add_argument("--host", help="Host (überschreibt DSN).")
    parser.add_argument("--port", type=int, help="Port (überschreibt DSN).")
    parser.add_argument("--db", type=int, help="Datenbankindex (0-15).")
    parser.add_argument("--password", help="Passwort.")
    return parser.parse_args()


def resolve_settings(args: argparse.Namespace) -> RedisSettings:
    dsn = args.dsn or os.getenv("REDIS_DSN")
    host = args.host or os.getenv("REDIS_HOST", "localhost")
    port = args.port or int(os.getenv("REDIS_PORT", "6379"))
    db = args.db if args.db is not None else int(os.getenv("REDIS_DB", "0"))
    password = args.password or os.getenv("REDIS_PASSWORD")

    if args.config and load_config:
        config = load_config(args.config)
        defaults = config.defaults.redis
        parsed = urlparse(defaults.dsn)
        if parsed.hostname:
            host = parsed.hostname
        if parsed.port:
            port = parsed.port
        if parsed.path:
            try:
                db = int(parsed.path.strip("/"))
            except ValueError:
                pass
        if parsed.password:
            password = parsed.password
        dsn = defaults.dsn

    if dsn and not args.host:
        parsed = urlparse(dsn)
        if parsed.hostname:
            host = parsed.hostname
        if parsed.port:
            port = parsed.port
        if parsed.path:
            try:
                db = int(parsed.path.strip("/"))
            except ValueError:
                pass
        if parsed.password and not password:
            password = parsed.password

    return RedisSettings(host=host, port=port, db=db, password=password)


def get_client(settings: RedisSettings) -> redis.Redis:
    return redis.Redis(host=settings.host, port=settings.port, db=settings.db, password=settings.password)


def show_info(client: redis.Redis) -> None:
    info = client.info("keyspace")
    print("\nKeyspace:")
    if not info:
        print("  Keine Informationen verfügbar.")
        return
    for db_name, values in info.items():
        keys = values.get("keys", 0)
        expires = values.get("expires", 0)
        avg_ttl = values.get("avg_ttl", 0)
        print(f"  {db_name}: keys={keys}, expires={expires}, avg_ttl={avg_ttl}")


def switch_db(settings: RedisSettings, client: redis.Redis) -> Tuple[RedisSettings, redis.Redis]:
    target = input("Ziel-Datenbank (0-15): ").strip()
    if not target.isdigit():
        print("Ungültige Eingabe.")
        return settings, client
    new_db = int(target)
    if not 0 <= new_db <= 15:
        print("Ungültiger Bereich.")
        return settings, client
    settings.db = new_db
    client = get_client(settings)
    print(f"Auf DB {new_db} gewechselt.")
    return settings, client


def list_keys(client: redis.Redis) -> None:
    pattern = input("Pattern (Standard '*'): ").strip() or "*"
    limit_input = input("Anzahl anzuzeigender Keys (Standard 50): ").strip()
    limit = int(limit_input) if limit_input.isdigit() else 50
    print(f"\nKeys für Pattern '{pattern}':")
    count = 0
    for key in client.scan_iter(match=pattern):
        print(f"  {key.decode('utf-8', errors='replace')}")
        count += 1
        if count >= limit:
            print("  ... weitere Keys ausgelassen.")
            break
    if count == 0:
        print("  Keine Keys gefunden.")


def inspect_key(client: redis.Redis) -> None:
    key = input("Key: ").strip()
    if not key:
        print("Kein Key angegeben.")
        return
    encoded = key.encode("utf-8")
    if not client.exists(encoded):
        print("Key existiert nicht.")
        return
    key_type = client.type(encoded).decode()
    ttl = client.ttl(encoded)
    print(f"Typ: {key_type}, TTL: {ttl}")
    if key_type == "string":
        value = client.get(encoded)
        print(f"Wert: {value.decode('utf-8', errors='replace')}")
    elif key_type == "hash":
        items = client.hgetall(encoded)
        for field, value in list(items.items())[:20]:
            print(f"  {field.decode()}: {value.decode(errors='replace')}")
        if len(items) > 20:
            print("  ... weitere Felder ausgelassen.")
    elif key_type == "list":
        entries = client.lrange(encoded, 0, 19)
        for idx, entry in enumerate(entries):
            print(f"  [{idx}] {entry.decode(errors='replace')}")
        length = client.llen(encoded)
        if length > 20:
            print(f"  ... {length - 20} weitere Einträge.")
    elif key_type == "set":
        members = client.smembers(encoded)
        for idx, member in enumerate(list(members)[:20]):
            print(f"  {idx}: {member.decode(errors='replace')}")
        if len(members) > 20:
            print("  ... weitere Mitglieder ausgelassen.")
    elif key_type == "zset":
        members = client.zrange(encoded, 0, 19, withscores=True)
        for member, score in members:
            print(f"  {member.decode(errors='replace')}: {score}")
        length = client.zcard(encoded)
        if length > 20:
            print(f"  ... {length - 20} weitere Einträge.")
    elif key_type == "stream":
        entries = client.xrange(encoded, count=10)
        for entry_id, values in entries:
            print(f"  {entry_id.decode()}")
            for field, value in values.items():
                print(f"    {field.decode()}: {value.decode(errors='replace')}")
        length = client.xlen(encoded)
        if length > 10:
            print(f"  ... {length - 10} weitere Einträge.")
    else:
        print("Noch kein spezieller Reader für diesen Typ implementiert.")


def delete_key(client: redis.Redis) -> None:
    key = input("Key zum Löschen: ").strip()
    if not key:
        print("Kein Key angegeben.")
        return
    encoded = key.encode("utf-8")
    if not client.exists(encoded):
        print("Key existiert nicht.")
        return
    confirm = input(f"Key '{key}' wirklich löschen? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Abgebrochen.")
        return
    client.delete(encoded)
    print("Key gelöscht.")


def flush_db(client: redis.Redis) -> None:
    confirm = input("Aktuelle DB löschen (FLUSHDB)? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Abgebrochen.")
        return
    client.flushdb()
    print("Datenbank geleert.")


def flush_all(client: redis.Redis) -> None:
    confirm = input("Alle DBs löschen (FLUSHALL)? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Abgebrochen.")
        return
    client.flushall()
    print("Alle Datenbanken geleert.")


def ping(client: redis.Redis) -> None:
    import time

    start = time.perf_counter()
    client.ping()
    latency = (time.perf_counter() - start) * 1000
    print(f"Ping: {latency:.2f} ms")


def main() -> None:
    args = parse_args()
    settings = resolve_settings(args)
    client = get_client(settings)
    print(f"Verbunden mit Redis @ {settings.host}:{settings.port}, DB={settings.db}\n")

    menu: Dict[str, Tuple[str, Callable]] = {
        "1": ("Keyspace Info (INFO keyspace)", lambda c: show_info(c)),
        "2": ("Datenbank wechseln", None),
        "3": ("Keys auflisten (SCAN)", lambda c: list_keys(c)),
        "4": ("Key inspizieren", lambda c: inspect_key(c)),
        "5": ("Key löschen", lambda c: delete_key(c)),
        "6": ("DB flushen (FLUSHDB)", lambda c: flush_db(c)),
        "7": ("Alle DBs flushen (FLUSHALL)", lambda c: flush_all(c)),
        "8": ("Ping/Latenz messen", lambda c: ping(c)),
        "q": ("Beenden", None),
    }

    while True:
        print("\nAktionen:")
        for key, (label, _) in menu.items():
            print(f"  {key} -> {label}")
        choice = input("Auswahl: ").strip().lower()
        if choice in {"q", "quit", "exit"}:
            print("Beende Inspector.")
            break
        action = menu.get(choice)
        if not action:
            print("Ungültige Auswahl.")
            continue
        label, handler = action
        if choice == "2":
            settings, client = switch_db(settings, client)
        elif handler:
            handler(client)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAbgebrochen.")

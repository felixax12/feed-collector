from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import URLError
from urllib.request import urlopen

import httpx

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parent))
    from feeds import FeedOrchestrator, load_config  # type: ignore
    from feeds.config import ChannelConfig, ExchangeConfig, OutputTargets  # type: ignore
    from feeds.exchanges.binance.capabilities import SUPPORTED_CHANNELS  # type: ignore
else:
    from feeds import FeedOrchestrator, load_config
    from feeds.config import ChannelConfig, ExchangeConfig, OutputTargets
    from feeds.exchanges.binance.capabilities import SUPPORTED_CHANNELS


PRESETS_PATH = Path("feeds/presets.json")
DEFAULT_CONFIG_PATH = Path("feeds/feeds.yml")


def load_presets() -> List[Dict[str, object]]:
    if not PRESETS_PATH.exists():
        raise FileNotFoundError(f"Preset-Datei nicht gefunden: {PRESETS_PATH}")
    with PRESETS_PATH.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    presets = data.get("presets")
    if not isinstance(presets, list):
        raise ValueError("Preset-Datei hat kein gueltiges 'presets' Array.")
    return presets


def select_preset(presets: List[Dict[str, object]]) -> Dict[str, object]:
    print("\nVorinstallierte Settings:")
    for preset in presets:
        pid = preset.get("id", "?")
        name = preset.get("name", "Unbenannt")
        desc = preset.get("description", "")
        print(f"  {pid}. {name} - {desc}")
    while True:
        choice = input("\nNummer waehlen: ").strip()
        for preset in presets:
            if str(preset.get("id")) == choice:
                return preset
        print("Unbekannte Auswahl.")


def fetch_binance_symbols(market_type: str) -> List[str]:
    if market_type == "perp_linear":
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        contract_key = "contractType"
        contract_value = "PERPETUAL"
    else:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        contract_key = None
        contract_value = None

    try:
        with urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
    except URLError as exc:
        raise RuntimeError(f"Symbol-Liste konnte nicht geladen werden: {exc}") from exc

    symbols = []
    for entry in data.get("symbols", []):
        if entry.get("status") != "TRADING":
            continue
        if contract_key and entry.get(contract_key) != contract_value:
            continue
        if entry.get("quoteAsset") != "USDT":
            continue
        symbol = entry.get("symbol")
        if symbol:
            symbols.append(symbol)
    if not symbols:
        raise RuntimeError("Keine Symbole gefunden (ExchangeInfo leer).")
    return symbols


def build_channels(preset: Dict[str, object]) -> Dict[str, ChannelConfig]:
    channels: Dict[str, ChannelConfig] = {}
    preset_channels = preset.get("channels", {}) or {}
    for channel_name in SUPPORTED_CHANNELS.keys():
        raw = preset_channels.get(channel_name)
        if isinstance(raw, dict):
            outputs_raw = raw.get("outputs", {})
            outputs = OutputTargets(
                redis=bool(outputs_raw.get("redis", False)),
                clickhouse=bool(outputs_raw.get("clickhouse", True)),
            )
            channels[channel_name] = ChannelConfig(
                enabled=bool(raw.get("enabled", True)),
                depth=raw.get("depth"),
                interval=raw.get("interval"),
                outputs=outputs,
            )
        else:
            channels[channel_name] = ChannelConfig(enabled=False)
    return channels


async def run_feeds() -> None:
    presets = load_presets()
    preset = select_preset(presets)
    preset_id = str(preset.get("id", ""))
    health_enabled = preset_id in {"1.1", "2", "2.1", "2.2"}
    if not health_enabled:
        logging.warning(
            "HEALTH DISABLED: Dieses Preset hat kein spezifisches Health-Monitoring. "
            "Vollstaendigkeit kann nicht garantiert werden."
        )
        confirm = input("Trotzdem fortfahren? Tippe YES: ").strip()
        if confirm.upper() != "YES":
            print("Abbruch.")
            return

    if not DEFAULT_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config-Datei nicht gefunden: {DEFAULT_CONFIG_PATH}")
    base_config = load_config(str(DEFAULT_CONFIG_PATH))

    symbols_setting = preset.get("symbols")
    if symbols_setting == "ALL":
        market_type = str(preset.get("market_type", "perp_linear"))
        symbols = fetch_binance_symbols(market_type)
        print(f"Geladene Symbole: {len(symbols)}")
    else:
        symbols = list(symbols_setting or [])

    metadata = {}
    preset_channels = preset.get("channels", {}) or {}
    preset_metadata = preset.get("metadata")
    if isinstance(preset_metadata, dict):
        metadata = dict(preset_metadata)
    ob_top5 = preset_channels.get("ob_top5")
    if isinstance(ob_top5, dict) and ob_top5.get("enabled"):
        symbols_per_conn = dict(metadata.get("symbols_per_conn", {}))
        symbols_per_conn.setdefault("ob_top5", 50)
        metadata["symbols_per_conn"] = symbols_per_conn

    exchange = ExchangeConfig(
        exchange=str(preset.get("exchange", "binance")),
        market_type=str(preset.get("market_type", "perp_linear")),
        symbols=symbols,
        channels=build_channels(preset),
        metadata=metadata,
    )

    config = base_config.model_copy(update={"exchanges": [exchange]})
    orchestrator = FeedOrchestrator(config)
    await orchestrator.start()
    health_channels: List[str] = []
    if isinstance(preset_channels.get("mark_price"), dict):
        if preset_channels.get("mark_price", {}).get("enabled", True):
            health_channels.append("mark_price")
    if isinstance(preset_channels.get("ob_top5"), dict):
        if preset_channels.get("ob_top5", {}).get("enabled", True):
            health_channels.append("ob_top5")
    if isinstance(preset_channels.get("l1"), dict):
        if preset_channels.get("l1", {}).get("enabled", True):
            health_channels.append("l1")

    notifier = _build_telegram_notifier()
    logging.info(
        "LOG LEGEND: ws=websocket input, routed=router accepted, written=buffered rows, "
        "flushed=rows inserted into ClickHouse. pending=written-flushed (buffer), "
        "missing=expected-flushed (per interval), backlog=rolling deficit vs expected, "
        "backlog_ws=rolling deficit vs ws (ingest throughput vs input)."
    )
    stats_task = asyncio.create_task(_log_stats(orchestrator, symbols, tuple(health_channels), notifier))
    health_task = None
    if health_enabled:
        if health_channels:
            health_task = asyncio.create_task(_health_monitor(orchestrator, symbols, tuple(health_channels)))
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        return
    except KeyboardInterrupt:
        logging.info("Shutdown angefordert.")
    finally:
        stats_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stats_task
        if health_task:
            health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await health_task
        if notifier:
            await notifier.close()
        await orchestrator.stop()


def main() -> None:
    log_dir = Path(os.getenv("FEED_LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "feed_runtime.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=50 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(run_feeds())
    except KeyboardInterrupt:
        pass


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, interval_s: int) -> None:
        self._token = token
        self._chat_id = chat_id
        self._interval_s = max(10, interval_s)
        self._last_sent = 0.0
        self._client = httpx.AsyncClient(timeout=10)

    async def maybe_send(self, message: str) -> None:
        now = time.time()
        if now - self._last_sent < self._interval_s:
            return
        await self._send(message)
        self._last_sent = now

    async def _send(self, message: str) -> None:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        message = f"{timestamp} | {message}"
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = {"chat_id": self._chat_id, "text": message}
        try:
            await self._client.post(url, json=payload)
        except httpx.HTTPError as exc:
            logging.warning("Telegram send failed: %s", exc)

    async def close(self) -> None:
        await self._client.aclose()


def _load_telegram_info_file(path: Path) -> Optional[Tuple[str, str]]:
    if not path.exists():
        return None
    try:
        raw = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        logging.warning("Telegram info file unreadable: %s", exc)
        return None
    cleaned = [line.strip() for line in raw if line.strip()]
    if len(cleaned) < 2:
        logging.warning("Telegram info file format invalid: %s", path)
        return None
    return cleaned[0], cleaned[1]


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        logging.warning("Env file unreadable: %s", exc)
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _build_telegram_notifier() -> Optional[TelegramNotifier]:
    env_path = Path(os.getenv("FEEDS_ENV_FILE", "feed.env"))
    _load_env_file(env_path)
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        info_path = Path(os.getenv("TELEGRAM_INFO_FILE", "telegram info"))
        info = _load_telegram_info_file(info_path)
        if info:
            token, chat_id = info
    if not token or not chat_id:
        return None
    try:
        interval_s = int(os.getenv("TELEGRAM_INTERVAL_S", "60"))
    except ValueError:
        interval_s = 60
    return TelegramNotifier(token, chat_id, interval_s)


async def _log_stats(
    orchestrator: FeedOrchestrator,
    symbols: List[str],
    health_channels: tuple[str, ...],
    notifier: Optional[TelegramNotifier],
) -> None:
    last = {"redis": None, "clickhouse": None}
    last_router: dict[str, int] = {}
    last_ws: dict[str, int] = {}
    last_discs: dict[str, int] = {}
    last_table: dict[str, int] = {}
    last_flushed: dict[str, int] = {}
    last_errs: dict[str, dict[str, int]] = {}
    last_flush_errors = 0
    backlog_by_channel: dict[str, int] = {}
    backlog_ws_by_channel: dict[str, int] = {}
    channel_to_table = {
        "trades": "trades",
        "l1": "l1",
        "ob_top5": "ob_top5",
        "ob_top20": "ob_top20",
        "ob_diff": "order_book_diffs",
        "liquidations": "liquidations",
        "mark_price": "mark_price",
        "funding": "funding",
        "advanced_metrics": "advanced_metrics",
        "klines": "klines",
    }
    interval_s = 10
    proc = None
    last_io = None
    ch_proc = None
    last_ch_io = None
    try:
        import psutil  # type: ignore

        proc = psutil.Process()
        proc.cpu_percent(interval=None)
        ch_pid_env = os.getenv("CLICKHOUSE_PID", "").strip()
        if ch_pid_env.isdigit():
            try:
                ch_proc = psutil.Process(int(ch_pid_env))
                ch_proc.cpu_percent(interval=None)
            except Exception:
                ch_proc = None
        if ch_proc is None:
            for p in psutil.process_iter(["name", "cmdline"]):
                try:
                    name = (p.info.get("name") or "").lower()
                    cmdline = " ".join(p.info.get("cmdline") or []).lower()
                    if "clickhouse" in name or "clickhouse-server" in cmdline:
                        ch_proc = p
                        ch_proc.cpu_percent(interval=None)
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        if ch_proc is None:
            logging.warning(
                "ClickHouse process not found. Set CLICKHOUSE_PID to force tracking."
            )
    except Exception:
        proc = None
    while True:
        await asyncio.sleep(interval_s)
        stats = orchestrator.stats()
        lines = []
        for target in ("clickhouse", "redis"):
            data = stats.get(target)
            if not data:
                continue
            prev = last.get(target) or {}
            delta_events = data["events"] - prev.get("events", 0)
            delta_items = data["items_in"] - prev.get("items_in", 0)
            delta_flushed = data["items_flushed"] - prev.get("items_flushed", 0)
            line = (
                f"{target}: events+{delta_events}/10s "
                f"items+{delta_items}/10s flushed+{delta_flushed}/10s"
            )
            lines.append(line)
            last[target] = data.copy()
        if lines:
            logging.info("[ingest] %s", " | ".join(lines))

        ws_counts: dict[str, int] = {}
        disc_counts: dict[str, int] = {}
        parse_errors: dict[str, int] = {}
        validation_errors: dict[str, int] = {}
        for feed in stats.get("feeds", []):
            for channel, count in feed.get("ws_msgs", {}).items():
                ws_counts[channel] = ws_counts.get(channel, 0) + count
            for channel, count in feed.get("ws_discs", {}).items():
                disc_counts[channel] = disc_counts.get(channel, 0) + count
            for channel, count in feed.get("parse_errors", {}).items():
                parse_errors[channel] = parse_errors.get(channel, 0) + count
            for channel, count in feed.get("validation_errors", {}).items():
                validation_errors[channel] = validation_errors.get(channel, 0) + count

        router_counts = stats.get("router", {}).get("events_by_channel", {})
        clickhouse_stats = stats.get("clickhouse") or {}
        table_counts = clickhouse_stats.get("rows_by_table", {})
        flushed_counts = clickhouse_stats.get("flushed_by_table", {})
        flush_errors = int(clickhouse_stats.get("flush_errors", 0))

        diff_lines = []
        channels = sorted(set(ws_counts) | set(router_counts))
        for channel in channels:
            ws_delta = ws_counts.get(channel, 0) - last_ws.get(channel, 0)
            routed_delta = router_counts.get(channel, 0) - last_router.get(channel, 0)
            table = channel_to_table.get(channel)
            written_delta = 0
            if table:
                written_delta = table_counts.get(table, 0) - last_table.get(table, 0)
            if ws_delta or routed_delta or written_delta:
                lost = ws_delta - written_delta
                diff_lines.append(
                    f"{channel}: ws+{ws_delta} routed+{routed_delta} written+{written_delta} lost+{lost}"
                )

        if diff_lines:
            logging.info("[diff] %s", " | ".join(diff_lines))

        loss_lines = []
        for channel in channels:
            ws_delta = ws_counts.get(channel, 0) - last_ws.get(channel, 0)
            routed_delta = router_counts.get(channel, 0) - last_router.get(channel, 0)
            table = channel_to_table.get(channel)
            writer_delta = 0
            flushed_delta = 0
            if table:
                writer_delta = table_counts.get(table, 0) - last_table.get(table, 0)
                flushed_delta = flushed_counts.get(table, 0) - last_flushed.get(table, 0)
            loss_ws_router = ws_delta - routed_delta
            loss_router_writer = routed_delta - writer_delta
            loss_writer_ch = writer_delta - flushed_delta
            if loss_ws_router or loss_router_writer or loss_writer_ch:
                loss_lines.append(
                    f"{channel}: ws->router {loss_ws_router} | router->writer {loss_router_writer} | writer->ch {loss_writer_ch}"
                )
        if loss_lines:
            logging.info("[loss] %s", " | ".join(loss_lines))

        err_lines = []
        for channel in channels:
            prev = last_errs.get(channel, {"parse": 0, "validation": 0})
            parse_delta = parse_errors.get(channel, 0) - prev.get("parse", 0)
            val_delta = validation_errors.get(channel, 0) - prev.get("validation", 0)
            if parse_delta or val_delta:
                err_lines.append(
                    f"{channel}: parse_error+{parse_delta}/10s validation_error+{val_delta}/10s"
                )
        if err_lines:
            logging.info("[errors] %s", " | ".join(err_lines))
        if flush_errors != last_flush_errors:
            logging.info(
                "[errors] clickhouse_flush_errors+%s/10s",
                flush_errors - last_flush_errors,
            )

        disc_lines = []
        for channel in channels:
            disc_delta = disc_counts.get(channel, 0) - last_discs.get(channel, 0)
            if disc_delta:
                disc_lines.append(f"{channel}: discs+{disc_delta}/10s")
        if disc_lines:
            logging.warning("[discs] %s", " | ".join(disc_lines))

        health_lines = []
        for channel in health_channels:
            if channel != "mark_price":
                continue
            table = channel_to_table.get(channel)
            if not table:
                continue
            expected = len(symbols) * interval_s
            written_delta = table_counts.get(table, 0) - last_table.get(table, 0)
            flushed_delta = flushed_counts.get(table, 0) - last_flushed.get(table, 0)
            pending = max(0, written_delta - flushed_delta)
            interval_missing = max(0, expected - flushed_delta)
            backlog = backlog_by_channel.get(channel, 0) + expected - flushed_delta
            backlog_by_channel[channel] = max(0, backlog)
            ws_delta = ws_counts.get(channel, 0) - last_ws.get(channel, 0)
            backlog_ws = backlog_ws_by_channel.get(channel, 0) + ws_delta - flushed_delta
            backlog_ws_by_channel[channel] = max(0, backlog_ws)
            health_lines.append(
                f"{channel}: expected={expected}/10s flushed={flushed_delta} "
                f"pending={pending} missing={interval_missing} backlog={backlog_by_channel[channel]} "
                f"backlog_ws={backlog_ws_by_channel[channel]}"
            )
        if health_lines:
            logging.info("[health] %s", " | ".join(health_lines))
            if notifier:
                parts = ["[health] " + " | ".join(health_lines)]
                if err_lines:
                    parts.append("[errors] " + " | ".join(err_lines))
                if disc_lines:
                    parts.append("[discs] " + " | ".join(disc_lines))
                if flush_errors != last_flush_errors:
                    parts.append(
                        f"[errors] clickhouse_flush_errors+{flush_errors - last_flush_errors}/10s"
                    )
                await notifier.maybe_send(" | ".join(parts))

        if proc is not None:
            try:
                cpu_pct = proc.cpu_percent(interval=None)
                mem = proc.memory_info()
                rss_mb = mem.rss / (1024 * 1024)
                sys_parts = [f"py_cpu={cpu_pct:.1f}%", f"py_rss={rss_mb:.1f}MB"]
                try:
                    io = proc.io_counters()
                    if last_io is not None:
                        read_mb = (io.read_bytes - last_io.read_bytes) / (1024 * 1024)
                        write_mb = (io.write_bytes - last_io.write_bytes) / (1024 * 1024)
                        sys_parts.append(f"py_io_read={read_mb:.2f}MB/10s")
                        sys_parts.append(f"py_io_write={write_mb:.2f}MB/10s")
                    last_io = io
                except Exception:
                    pass
                if ch_proc is not None:
                    try:
                        ch_cpu = ch_proc.cpu_percent(interval=None)
                        ch_mem = ch_proc.memory_info()
                        ch_rss_mb = ch_mem.rss / (1024 * 1024)
                        sys_parts.append(f"ch_cpu={ch_cpu:.1f}%")
                        sys_parts.append(f"ch_rss={ch_rss_mb:.1f}MB")
                        try:
                            ch_io = ch_proc.io_counters()
                            if last_ch_io is not None:
                                ch_read_mb = (ch_io.read_bytes - last_ch_io.read_bytes) / (1024 * 1024)
                                ch_write_mb = (ch_io.write_bytes - last_ch_io.write_bytes) / (1024 * 1024)
                                sys_parts.append(f"ch_io_read={ch_read_mb:.2f}MB/10s")
                                sys_parts.append(f"ch_io_write={ch_write_mb:.2f}MB/10s")
                            last_ch_io = ch_io
                        except Exception:
                            pass
                    except Exception:
                        ch_proc = None
                sys_line = " | ".join(sys_parts)
                logging.info("[sys] %s", sys_line)
                if notifier:
                    await notifier.maybe_send("[sys] " + sys_line)
            except Exception:
                proc = None

        last_ws = ws_counts
        last_router = dict(router_counts)
        last_discs = dict(disc_counts)
        if table_counts is not None:
            last_table = dict(table_counts)
        if flushed_counts is not None:
            last_flushed = dict(flushed_counts)
        last_errs = {channel: {"parse": parse_errors.get(channel, 0), "validation": validation_errors.get(channel, 0)} for channel in channels}
        last_flush_errors = flush_errors


async def _health_monitor(
    orchestrator: FeedOrchestrator, symbols: List[str], channels: tuple[str, ...]
) -> None:
    max_lag_ms = {"mark_price": 5000, "ob_top5": 5000, "l1": 30000}
    sample_limit = 5
    while True:
        await asyncio.sleep(10)
        snapshot = orchestrator.router.last_event_snapshot()
        event_ns = snapshot.get("event_ns", {})
        recv_ns = snapshot.get("recv_ns", {})
        now_ns = time.time_ns()
        for channel in channels:
            missing = []
            stale = []
            lags_ms: List[int] = []
            for symbol in symbols:
                key = (channel, symbol)
                ts_event = event_ns.get(key)
                ts_recv = recv_ns.get(key)
                if ts_event is None or ts_recv is None:
                    missing.append(symbol)
                    continue
                event_ns_value = int(ts_event)
                recv_ns_value = int(ts_recv)
                # Some streams provide ms timestamps; normalize to ns for lag.
                if event_ns_value < 1_000_000_000_000_000:
                    event_ns_value *= 1_000_000
                lag_ms = max(0, int((recv_ns_value - event_ns_value) / 1_000_000))
                lags_ms.append(lag_ms)
                if now_ns - event_ns_value > max_lag_ms[channel] * 1_000_000:
                    stale.append(symbol)
            if missing or stale:
                logging.warning(
                    "HEALTH channel=%s missing=%s stale=%s sample_missing=%s sample_stale=%s",
                    channel,
                    len(missing),
                    len(stale),
                    ",".join(missing[:sample_limit]),
                    ",".join(stale[:sample_limit]),
                )
            if lags_ms:
                avg_lag = sum(lags_ms) / len(lags_ms)
                max_lag = max(lags_ms)
                logging.info(
                    "HEALTH channel=%s lag_ms avg=%.1f max=%s",
                    channel,
                    avg_lag,
                    max_lag,
                )


if __name__ == "__main__":
    main()

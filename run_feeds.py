from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from logging.handlers import RotatingFileHandler
import multiprocessing as mp
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
LIVE_READY_PRESETS = {"2.1", "2.3", "2.4"}


def load_presets() -> List[Dict[str, object]]:
    if not PRESETS_PATH.exists():
        raise FileNotFoundError(f"Preset-Datei nicht gefunden: {PRESETS_PATH}")
    with PRESETS_PATH.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    presets = data.get("presets")
    if not isinstance(presets, list):
        raise ValueError("Preset-Datei hat kein gueltiges 'presets' Array.")
    return presets


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


def _parse_interval_seconds(interval: str) -> Optional[int]:
    text = interval.strip().lower()
    if not text:
        return None
    num = ""
    unit = ""
    for ch in text:
        if ch.isdigit():
            num += ch
        else:
            unit += ch
    if not num or not unit:
        return None
    try:
        value = int(num)
    except ValueError:
        return None
    unit = unit.strip()
    factor = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit)
    if not factor:
        return None
    return value * factor


def _preset_label(preset: Dict[str, object]) -> str:
    preset_id = str(preset.get("id", "")).strip()
    preset_name = str(preset.get("name", "")).strip()
    return preset_id if not preset_name else f"{preset_id} {preset_name}"


def _safe_label(label: str) -> str:
    cleaned = []
    for ch in label:
        if ch.isalnum():
            cleaned.append(ch.lower())
        else:
            cleaned.append("_")
    return "".join(cleaned).strip("_")


def _configure_logging(preset_label: str) -> None:
    log_dir = Path(os.getenv("FEED_LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    safe_label = _safe_label(preset_label)
    log_name = "feed_runtime.log" if not safe_label else f"feed_runtime_{safe_label}.log"
    log_path = log_dir / log_name
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: [" + preset_label + "] %(message)s"
    )

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


def _set_cpu_affinity(core_idx: Optional[int]) -> None:
    if core_idx is None:
        return
    try:
        os.sched_setaffinity(0, {core_idx})
        logging.info("CPU affinity set: core=%s", core_idx)
        return
    except AttributeError:
        pass
    except Exception as exc:
        logging.warning("CPU affinity (sched_setaffinity) failed: %s", exc)
        return
    try:
        import psutil  # type: ignore

        psutil.Process().cpu_affinity([core_idx])
        logging.info("CPU affinity set via psutil: core=%s", core_idx)
    except Exception as exc:
        logging.warning("CPU affinity (psutil) failed: %s", exc)


def _preset_log_interval_s(preset: Dict[str, object]) -> int:
    metadata = preset.get("metadata")
    if isinstance(metadata, dict):
        raw = metadata.get("log_interval_s")
        if isinstance(raw, int) and raw > 0:
            return raw
    channels = preset.get("channels", {}) or {}
    if isinstance(channels.get("agg_trades_5s"), dict) and channels.get("agg_trades_5s", {}).get("enabled", True):
        return 5
    if isinstance(channels.get("klines"), dict) and channels.get("klines", {}).get("enabled", True):
        interval_raw = channels.get("klines", {}).get("interval")
        interval_s = _parse_interval_seconds(str(interval_raw)) if interval_raw else None
        if interval_s and interval_s >= 60:
            return interval_s
    return 10


def _load_alert_thresholds(preset: Dict[str, object]) -> dict[str, dict[str, float]]:
    defaults = {
        "mark_price": {"yellow_missing_pct": 0.005, "red_missing_pct": 0.02},
        "klines": {"yellow_missing_pct": 0.002, "red_missing_pct": 0.01},
        "agg_trades_5s": {"yellow_missing_pct": 0.002, "red_missing_pct": 0.01},
        "l1": {"yellow_missing_pct": 0.01, "red_missing_pct": 0.05},
        "ob_top5": {"yellow_missing_pct": 0.01, "red_missing_pct": 0.05},
    }
    metadata = preset.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("alert_thresholds"), dict):
        raw = metadata.get("alert_thresholds", {})
        for channel, thresholds in raw.items():
            if not isinstance(thresholds, dict):
                continue
            merged = dict(defaults.get(channel, {}))
            for key, value in thresholds.items():
                if isinstance(value, (int, float)):
                    merged[key] = float(value)
            defaults[channel] = merged
    return defaults


def _resolve_preset_by_id(presets: List[Dict[str, object]], preset_id: str) -> Dict[str, object]:
    for preset in presets:
        if str(preset.get("id")) == preset_id:
            return preset
    raise ValueError(f"Unbekanntes Preset: {preset_id}")


def select_presets(presets: List[Dict[str, object]]) -> List[Dict[str, object]]:
    print("\nVorinstallierte Settings:")
    for preset in presets:
        pid = preset.get("id", "?")
        name = preset.get("name", "Unbenannt")
        desc = preset.get("description", "")
        ready_hint = "" if str(pid) in LIVE_READY_PRESETS else " (NICHT bereit fuer Betrieb)"
        print(f"  {pid}{ready_hint}. {name} - {desc}")
    while True:
        raw = input("\nNummer waehlen (mehrere mit Komma): ").strip()
        if not raw:
            print("Unbekannte Auswahl.")
            continue
        parts = [p.strip() for p in raw.replace(" ", ",").split(",") if p.strip()]
        if not parts:
            print("Unbekannte Auswahl.")
            continue
        try:
            return [_resolve_preset_by_id(presets, part) for part in parts]
        except ValueError:
            print("Unbekannte Auswahl.")


async def run_preset(preset: Dict[str, object]) -> None:
    preset_id = str(preset.get("id", ""))
    preset_label = _preset_label(preset)
    log_interval_s = _preset_log_interval_s(preset)
    alert_thresholds = _load_alert_thresholds(preset)
    health_enabled = preset_id in {"1.1", "2", "2.1", "2.2", "2.3", "2.4"}
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
    health_intervals: dict[str, int] = {}
    if isinstance(preset_channels.get("mark_price"), dict):
        if preset_channels.get("mark_price", {}).get("enabled", True):
            health_channels.append("mark_price")
            health_intervals["mark_price"] = 1
    if isinstance(preset_channels.get("ob_top5"), dict):
        if preset_channels.get("ob_top5", {}).get("enabled", True):
            health_channels.append("ob_top5")
    if isinstance(preset_channels.get("l1"), dict):
        if preset_channels.get("l1", {}).get("enabled", True):
            health_channels.append("l1")
    if isinstance(preset_channels.get("klines"), dict):
        if preset_channels.get("klines", {}).get("enabled", True):
            health_channels.append("klines")
            interval_raw = preset_channels.get("klines", {}).get("interval")
            interval_s = _parse_interval_seconds(str(interval_raw)) if interval_raw else None
            if interval_s:
                health_intervals["klines"] = interval_s
            else:
                logging.warning("Klines Health: interval ungueltig oder fehlt (%s).", interval_raw)
    if isinstance(preset_channels.get("agg_trades_5s"), dict):
        if preset_channels.get("agg_trades_5s", {}).get("enabled", True):
            health_channels.append("agg_trades_5s")
            interval_raw = preset_channels.get("agg_trades_5s", {}).get("interval")
            interval_s = _parse_interval_seconds(str(interval_raw)) if interval_raw else None
            if interval_s:
                health_intervals["agg_trades_5s"] = interval_s
            else:
                health_intervals["agg_trades_5s"] = 5

    notifier, overview_notifier, overview_interval_s = _build_telegram_notifiers()
    if overview_notifier:
        overview_notifier.set_interval(overview_interval_s)
    overview_aggregator = None
    if overview_notifier:
        overview_aggregator = OverviewAggregator(
            preset_label=preset_label,
            channels=health_channels,
            symbols_count=len(symbols),
            health_intervals=health_intervals,
            alert_thresholds=alert_thresholds,
            overview_interval_s=overview_interval_s,
        )
    logging.info(
        "LOG LEGEND: ws=websocket input, routed=router accepted, written=buffered rows, "
        "flushed=rows inserted into ClickHouse. pending=written-flushed (buffer), "
        "missing=expected-flushed (per interval), backlog=rolling deficit vs expected, "
        "backlog_ws=rolling deficit vs ws (ingest throughput vs input)."
    )
    logging.info("Preset config: label=%s log_interval_s=%s health=%s", preset_label, log_interval_s, health_enabled)
    stats_task = asyncio.create_task(
        _log_stats(
            orchestrator,
            symbols,
            tuple(health_channels),
            health_intervals,
            notifier,
            overview_notifier,
            overview_aggregator,
            preset_label,
            log_interval_s,
            alert_thresholds,
        )
    )
    overview_task = None
    if overview_notifier and overview_aggregator:
        overview_task = asyncio.create_task(
            _overview_loop(overview_aggregator, overview_notifier, overview_interval_s)
        )
    health_task = None
    if health_enabled:
        if health_channels:
            health_task = asyncio.create_task(
                _health_monitor(orchestrator, symbols, tuple(health_channels), log_interval_s)
            )
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
        if overview_task:
            overview_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await overview_task
        if health_task:
            health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await health_task
        if notifier:
            await notifier.close()
        await orchestrator.stop()


def _run_preset_process(preset_id: str, core_idx: Optional[int]) -> None:
    presets = load_presets()
    preset = _resolve_preset_by_id(presets, preset_id)
    preset_label = _preset_label(preset)
    _configure_logging(preset_label)
    _set_cpu_affinity(core_idx)
    logging.info("Start preset=%s", preset_label)
    try:
        asyncio.run(run_preset(preset))
    except KeyboardInterrupt:
        pass


def _start_multi_presets(presets: List[Dict[str, object]]) -> None:
    cpu_count = os.cpu_count() or 0
    core_indices = list(range(cpu_count)) if cpu_count > 0 else []
    if cpu_count and len(presets) > cpu_count:
        print(
            f"Warnung: {len(presets)} Presets, aber nur {cpu_count} CPU-Kerne. "
            "Einige Presets teilen sich Kerne."
        )
    ctx = mp.get_context("spawn")
    processes: list[mp.Process] = []
    for idx, preset in enumerate(presets):
        preset_id = str(preset.get("id", "")).strip()
        core_idx = None
        if core_indices:
            core_idx = core_indices[idx % len(core_indices)]
        proc = ctx.Process(
            target=_run_preset_process,
            args=(preset_id, core_idx),
            daemon=False,
        )
        proc.start()
        processes.append(proc)
    try:
        for proc in processes:
            proc.join()
    except KeyboardInterrupt:
        for proc in processes:
            proc.terminate()
        for proc in processes:
            proc.join()


def main() -> None:
    presets = load_presets()
    selected = select_presets(presets)
    if len(selected) == 1:
        preset = selected[0]
        preset_label = _preset_label(preset)
        _configure_logging(preset_label)
        _set_cpu_affinity(None)
        try:
            asyncio.run(run_preset(preset))
        except KeyboardInterrupt:
            pass
        return
    _start_multi_presets(selected)


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, interval_s: int) -> None:
        self._token = token
        self._chat_id = chat_id
        self._interval_s = max(10, interval_s)
        self._last_sent: dict[str, float] = {}
        self._client = httpx.AsyncClient(timeout=10)

    async def maybe_send(self, message: str, key: str = "default") -> None:
        now = time.time()
        last = self._last_sent.get(key, 0.0)
        if now - last < self._interval_s:
            return
        await self._send(message)
        self._last_sent[key] = now

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

    def set_interval(self, interval_s: int) -> None:
        self._interval_s = max(2, int(interval_s))


class OverviewAggregator:
    def __init__(
        self,
        preset_label: str,
        channels: list[str],
        symbols_count: int,
        health_intervals: dict[str, int],
        alert_thresholds: dict[str, dict[str, float]],
        overview_interval_s: int,
    ) -> None:
        self._preset_label = preset_label
        self._channels = channels
        self._symbols_count = symbols_count
        self._health_intervals = dict(health_intervals)
        self._alert_thresholds = alert_thresholds
        self._overview_interval_s = max(5, int(overview_interval_s))
        self._window_s = 0.0
        self._totals: dict[str, dict[str, float]] = {
            channel: {
                "expected": 0.0,
                "flushed": 0.0,
                "missing": 0.0,
                "pending_max": 0.0,
                "backlog_max": 0.0,
                "backlog_ws_max": 0.0,
                "ws": 0.0,
                "routed": 0.0,
                "written": 0.0,
                "discs": 0.0,
                "native_errors": 0.0,
                "internal_errors": 0.0,
            }
            for channel in channels
        }
        self._total_flush_errors = 0
        self._cpu_sum = 0.0
        self._cpu_count = 0
        self._cpu_last_rss = None
        self._ch_cpu_sum = 0.0
        self._ch_cpu_count = 0
        self._ch_cpu_last_rss = None
        self._last_update_ts = time.time()
        self._last_overview_ts = time.time()
        self._last_status: dict[str, str] = {channel: "OK" for channel in channels}
        self._last_status_ts: dict[str, float] = {channel: 0.0 for channel in channels}

    def update(
        self,
        interval_s: int,
        channel_stats: dict[str, dict[str, int]],
        health_snapshot: dict[str, dict[str, float]],
        flush_errors_delta: int,
        sys_snapshot: dict[str, float],
        notifier: Optional[TelegramNotifier],
    ) -> None:
        self._window_s += interval_s
        self._last_update_ts = time.time()
        self._total_flush_errors += max(0, int(flush_errors_delta))

        for channel in self._channels:
            totals = self._totals.setdefault(channel, {})
            stats = channel_stats.get(channel, {})
            for key in ("ws", "routed", "written", "discs"):
                totals[key] = totals.get(key, 0.0) + float(stats.get(key, 0))
            totals["native_errors"] = totals.get("native_errors", 0.0) + float(
                stats.get("parse_errors", 0) + stats.get("validation_errors", 0)
            )
            internal_loss = 0
            for loss_key in ("loss_ws_router", "loss_router_writer", "loss_writer_ch"):
                loss_val = int(stats.get(loss_key, 0))
                if loss_val > 0:
                    internal_loss += loss_val
            totals["internal_errors"] = totals.get("internal_errors", 0.0) + float(internal_loss)

            health = health_snapshot.get(channel, {})
            if health:
                totals["expected"] = totals.get("expected", 0.0) + float(health.get("expected", 0))
                totals["flushed"] = totals.get("flushed", 0.0) + float(health.get("flushed", 0))
                totals["missing"] = totals.get("missing", 0.0) + float(health.get("missing", 0))
                totals["pending_max"] = max(totals.get("pending_max", 0.0), float(health.get("pending", 0)))
                totals["backlog_max"] = max(
                    totals.get("backlog_max", 0.0), float(health.get("backlog", 0))
                )
                totals["backlog_ws_max"] = max(
                    totals.get("backlog_ws_max", 0.0), float(health.get("backlog_ws", 0))
                )

        cpu_pct = sys_snapshot.get("py_cpu")
        if cpu_pct is not None:
            self._cpu_sum += float(cpu_pct)
            self._cpu_count += 1
        rss_mb = sys_snapshot.get("py_rss")
        if rss_mb is not None:
            self._cpu_last_rss = float(rss_mb)
        ch_cpu = sys_snapshot.get("ch_cpu")
        if ch_cpu is not None:
            self._ch_cpu_sum += float(ch_cpu)
            self._ch_cpu_count += 1
        ch_rss = sys_snapshot.get("ch_rss")
        if ch_rss is not None:
            self._ch_cpu_last_rss = float(ch_rss)

        if notifier:
            self._emit_status_changes(channel_stats, health_snapshot, flush_errors_delta, notifier)

    def _emit_status_changes(
        self,
        channel_stats: dict[str, dict[str, int]],
        health_snapshot: dict[str, dict[str, float]],
        flush_errors_delta: int,
        notifier: TelegramNotifier,
    ) -> None:
        now = time.time()
        for channel in self._channels:
            stats = channel_stats.get(channel, {})
            health = health_snapshot.get(channel, {})
            status = "OK"
            reasons: list[str] = []
            expected = float(health.get("expected", 0.0))
            missing = float(health.get("missing", 0.0))
            missing_pct = (missing / expected) if expected else 0.0
            if expected > 0:
                thresholds = self._alert_thresholds.get(channel, {})
                yellow_pct = thresholds.get("yellow_missing_pct", 0.01)
                red_pct = thresholds.get("red_missing_pct", 0.05)
                if missing_pct >= red_pct:
                    status = "RED"
                    reasons.append(f"missing={missing}/{int(expected)} ({missing_pct:.2%})")
                elif missing_pct >= yellow_pct and status != "RED":
                    status = "YELLOW"
                    reasons.append(f"missing={missing}/{int(expected)} ({missing_pct:.2%})")

            parse_errs = int(stats.get("parse_errors", 0))
            val_errs = int(stats.get("validation_errors", 0))
            native_errs = parse_errs + val_errs
            if native_errs:
                reasons.append(f"errors={native_errs}")
                if native_errs >= 10:
                    status = "RED"
                elif status != "RED":
                    status = "YELLOW"

            discs = int(stats.get("discs", 0))
            if discs:
                reasons.append(f"discs={discs}")
                if discs >= 3:
                    status = "RED"
                elif status != "RED":
                    status = "YELLOW"

            if flush_errors_delta > 0:
                reasons.append(f"flush_errors={flush_errors_delta}")
                status = "RED"

            prev = self._last_status.get(channel, "OK")
            if status != prev:
                self._last_status[channel] = status
                self._last_status_ts[channel] = now
                if status == "OK":
                    msg = (
                        f"[{self._preset_label}] [RECOVERED] {channel} back to OK"
                    )
                else:
                    reason = " | ".join(reasons) if reasons else "status change"
                    msg = f"[{self._preset_label}] [ALERT-{status}] {channel} {reason}"
                asyncio.create_task(notifier.maybe_send(msg, key=f"overview_alert_{channel}"))
            elif status == "RED":
                last_ts = self._last_status_ts.get(channel, 0.0)
                if now - last_ts >= 300:
                    self._last_status_ts[channel] = now
                    reason = " | ".join(reasons) if reasons else "still RED"
                    msg = f"[{self._preset_label}] [ALERT-RED] {channel} {reason}"
                    asyncio.create_task(notifier.maybe_send(msg, key=f"overview_alert_{channel}"))

    def build_overview(self) -> str:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        window_s_raw = int(self._window_s)
        window_s = max(1.0, self._window_s)
        header = f"[{self._preset_label}] OVERVIEW window={window_s_raw}s @ {now}"
        lines = [header]
        for channel in self._channels:
            totals = self._totals.get(channel, {})
            expected = totals.get("expected", 0.0)
            flushed = totals.get("flushed", 0.0)
            missing = totals.get("missing", 0.0)
            missing_pct = (missing / expected) if expected else 0.0
            status = self._last_status.get(channel, "OK")
            status_tag = {"OK": "OK", "YELLOW": "WARN", "RED": "CRIT"}.get(status, status)
            avg_rate = flushed / window_s if window_s else 0.0
            lines.append(
                f"{channel} [{status_tag}] miss={missing_pct:.2%} ({int(missing)}/{int(expected)}) "
                f"avg={avg_rate:.1f}/s back={int(totals.get('backlog_max', 0))} "
                f"native_err={int(totals.get('native_errors', 0))} internal_err={int(totals.get('internal_errors', 0))} "
                f"discs={int(totals.get('discs', 0))}"
            )
        cpu_avg = (self._cpu_sum / self._cpu_count) if self._cpu_count else None
        ch_cpu_avg = (self._ch_cpu_sum / self._ch_cpu_count) if self._ch_cpu_count else None
        sys_parts = []
        if cpu_avg is not None:
            sys_parts.append(f"py_cpu_avg={cpu_avg:.1f}%")
        if self._cpu_last_rss is not None:
            sys_parts.append(f"py_rss={self._cpu_last_rss:.1f}MB")
        if ch_cpu_avg is not None:
            sys_parts.append(f"ch_cpu_avg={ch_cpu_avg:.1f}%")
        if self._ch_cpu_last_rss is not None:
            sys_parts.append(f"ch_rss={self._ch_cpu_last_rss:.1f}MB")
        if self._total_flush_errors:
            sys_parts.append(f"flush_errors={self._total_flush_errors}")
        age_s = int(time.time() - self._last_update_ts)
        if age_s > self._overview_interval_s:
            sys_parts.append(f"data_age={age_s}s")
        if sys_parts:
            lines.append("sys: " + " | ".join(sys_parts))
        return "\n".join(lines)

    def reset_window(self) -> None:
        self._window_s = 0.0
        self._totals = {
            channel: {
                "expected": 0.0,
                "flushed": 0.0,
                "missing": 0.0,
                "pending_max": 0.0,
                "backlog_max": 0.0,
                "backlog_ws_max": 0.0,
                "ws": 0.0,
                "routed": 0.0,
                "written": 0.0,
                "discs": 0.0,
                "native_errors": 0.0,
                "internal_errors": 0.0,
            }
            for channel in self._channels
        }
        self._total_flush_errors = 0
        self._cpu_sum = 0.0
        self._cpu_count = 0
        self._cpu_last_rss = None
        self._ch_cpu_sum = 0.0
        self._ch_cpu_count = 0
        self._ch_cpu_last_rss = None
        self._last_overview_ts = time.time()

    def should_emit_overview(self) -> bool:
        return self._window_s >= self._overview_interval_s

    def has_window_data(self) -> bool:
        return self._window_s > 0.0

    @property
    def overview_interval_s(self) -> int:
        return self._overview_interval_s


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


def _build_telegram_notifier(
    prefix: str,
    fallback_legacy: bool = False,
    default_interval_s: int = 60,
) -> Optional[TelegramNotifier]:
    env_path = Path(os.getenv("FEEDS_ENV_FILE", "feed.env"))
    _load_env_file(env_path)
    token = os.getenv(f"{prefix}_BOT_TOKEN", "").strip()
    chat_id = os.getenv(f"{prefix}_CHAT_ID", "").strip()
    if fallback_legacy and (not token or not chat_id):
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if fallback_legacy and (not token or not chat_id):
        info_path = Path(os.getenv("TELEGRAM_INFO_FILE", "telegram info"))
        info = _load_telegram_info_file(info_path)
        if info:
            token, chat_id = info
    if not token or not chat_id:
        return None
    try:
        interval_s = int(os.getenv(f"{prefix}_INTERVAL_S", str(default_interval_s)))
    except ValueError:
        interval_s = default_interval_s
    return TelegramNotifier(token, chat_id, interval_s)


def _build_telegram_notifiers() -> tuple[Optional[TelegramNotifier], Optional[TelegramNotifier], int]:
    env_path = Path(os.getenv("FEEDS_ENV_FILE", "feed.env"))
    _load_env_file(env_path)
    raw_notifier = _build_telegram_notifier("TELEGRAM_RAW", fallback_legacy=True, default_interval_s=60)
    overview_interval_s = 30
    try:
        overview_interval_s = int(os.getenv("TELEGRAM_OVERVIEW_INTERVAL_S", "30"))
    except ValueError:
        overview_interval_s = 30
    overview_notifier = _build_telegram_notifier(
        "TELEGRAM_OVERVIEW",
        fallback_legacy=False,
        default_interval_s=overview_interval_s,
    )
    return raw_notifier, overview_notifier, max(5, overview_interval_s)


async def _log_stats(
    orchestrator: FeedOrchestrator,
    symbols: List[str],
    health_channels: tuple[str, ...],
    health_intervals: dict[str, int],
    notifier: Optional[TelegramNotifier],
    overview_notifier: Optional[TelegramNotifier],
    overview_aggregator: Optional[OverviewAggregator],
    preset_label: str,
    interval_s: int,
    alert_thresholds: dict[str, dict[str, float]],
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
    health_state = {
        channel: {"elapsed": 0.0, "ws": 0, "written": 0, "flushed": 0}
        for channel in health_intervals
    }
    last_alert_levels: dict[str, str] = {}
    channel_to_table = {
        "trades": "trades",
        "agg_trades_5s": "agg_trades_5s",
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
    interval_s = max(1, int(interval_s))
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
                f"{target}: events+{delta_events}/{interval_s}s "
                f"items+{delta_items}/{interval_s}s flushed+{delta_flushed}/{interval_s}s"
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
        channel_stats: dict[str, dict[str, int]] = {}
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
            channel_stats[channel] = {
                "ws": ws_delta,
                "routed": routed_delta,
                "written": written_delta,
                "discs": disc_counts.get(channel, 0) - last_discs.get(channel, 0),
                "parse_errors": parse_errors.get(channel, 0) - last_errs.get(channel, {}).get("parse", 0),
                "validation_errors": validation_errors.get(channel, 0) - last_errs.get(channel, {}).get("validation", 0),
            }

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
            stats_entry = channel_stats.setdefault(channel, {})
            stats_entry["loss_ws_router"] = loss_ws_router
            stats_entry["loss_router_writer"] = loss_router_writer
            stats_entry["loss_writer_ch"] = loss_writer_ch
            stats_entry["flushed"] = flushed_delta
        if loss_lines:
            logging.info("[loss] %s", " | ".join(loss_lines))

        err_lines = []
        alert_items: list[tuple[str, str]] = []
        for channel in channels:
            prev = last_errs.get(channel, {"parse": 0, "validation": 0})
            parse_delta = parse_errors.get(channel, 0) - prev.get("parse", 0)
            val_delta = validation_errors.get(channel, 0) - prev.get("validation", 0)
            if parse_delta or val_delta:
                err_lines.append(
                    f"{channel}: parse_error+{parse_delta}/{interval_s}s validation_error+{val_delta}/{interval_s}s"
                )
                severity = "yellow"
                if parse_delta + val_delta >= 10:
                    severity = "red"
                alert_items.append(
                    (severity, f"parse/validation errors channel={channel} +{parse_delta}/{val_delta} per {interval_s}s")
                )
        if err_lines:
            logging.info("[errors] %s", " | ".join(err_lines))
        if flush_errors != last_flush_errors:
            logging.info(
                "[errors] clickhouse_flush_errors+%s/%ss",
                flush_errors - last_flush_errors,
                interval_s,
            )
            alert_items.append(
                ("red", f"clickhouse_flush_errors +{flush_errors - last_flush_errors} per {interval_s}s")
            )

        disc_lines = []
        for channel in channels:
            disc_delta = disc_counts.get(channel, 0) - last_discs.get(channel, 0)
            if disc_delta:
                disc_lines.append(f"{channel}: discs+{disc_delta}/{interval_s}s")
                severity = "yellow"
                if disc_delta >= 3:
                    severity = "red"
                alert_items.append(
                    (severity, f"ws disconnects channel={channel} +{disc_delta} per {interval_s}s")
                )
        if disc_lines:
            logging.warning("[discs] %s", " | ".join(disc_lines))

        health_lines = []
        health_snapshot: dict[str, dict[str, float]] = {}
        for channel, target_interval_s in health_intervals.items():
            table = channel_to_table.get(channel)
            if not table:
                continue
            ws_delta = ws_counts.get(channel, 0) - last_ws.get(channel, 0)
            written_delta = table_counts.get(table, 0) - last_table.get(table, 0)
            flushed_delta = flushed_counts.get(table, 0) - last_flushed.get(table, 0)
            state = health_state[channel]
            state["elapsed"] += interval_s
            state["ws"] += ws_delta
            state["written"] += written_delta
            state["flushed"] += flushed_delta
            if state["elapsed"] < target_interval_s:
                continue
            window_s = int(round(state["elapsed"]))
            periods = max(1, int(round(state["elapsed"] / target_interval_s)))
            expected = len(symbols) * periods
            pending = max(0, state["written"] - state["flushed"])
            interval_missing = max(0, expected - state["flushed"])
            missing_pct = (interval_missing / expected) if expected else 0.0
            if channel in {"klines", "agg_trades_5s"}:
                backlog_by_channel[channel] = interval_missing
                backlog_ws_by_channel[channel] = max(0, state["ws"] - state["flushed"])
            else:
                backlog = backlog_by_channel.get(channel, 0) + expected - state["flushed"]
                backlog_by_channel[channel] = max(0, backlog)
                backlog_ws = backlog_ws_by_channel.get(channel, 0) + state["ws"] - state["flushed"]
                backlog_ws_by_channel[channel] = max(0, backlog_ws)
            health_lines.append(
                f"{channel}: expected={expected}/{window_s}s flushed={state['flushed']} "
                f"pending={pending} missing={interval_missing} backlog={backlog_by_channel[channel]} "
                f"backlog_ws={backlog_ws_by_channel[channel]}"
            )
            health_snapshot[channel] = {
                "expected": float(expected),
                "flushed": float(state["flushed"]),
                "pending": float(pending),
                "missing": float(interval_missing),
                "backlog": float(backlog_by_channel[channel]),
                "backlog_ws": float(backlog_ws_by_channel[channel]),
            }
            if interval_missing > 0:
                thresholds = alert_thresholds.get(channel, {})
                yellow_pct = thresholds.get("yellow_missing_pct", 0.01)
                red_pct = thresholds.get("red_missing_pct", 0.05)
                severity = "yellow"
                if missing_pct >= red_pct or interval_missing >= max(10, int(expected * red_pct)):
                    severity = "red"
                elif missing_pct < yellow_pct and interval_missing < max(5, int(expected * yellow_pct)):
                    severity = "yellow"
                alert_items.append(
                    (
                        severity,
                        f"missing data channel={channel} missing={interval_missing}/{expected} ({missing_pct:.2%})",
                    )
                )
            state["elapsed"] = 0.0
            state["ws"] = 0
            state["written"] = 0
            state["flushed"] = 0
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
                        f"[errors] clickhouse_flush_errors+{flush_errors - last_flush_errors}/{interval_s}s"
                    )
                await notifier.maybe_send(
                    f"[{preset_label}] " + " | ".join(parts),
                    key="health",
                )

        if alert_items:
            red_items = [msg for sev, msg in alert_items if sev == "red"]
            yellow_items = [msg for sev, msg in alert_items if sev == "yellow"]
            for sev, items in (("red", red_items), ("yellow", yellow_items)):
                if not items:
                    continue
                key = f"alert_{sev}"
                body = " | ".join(items[:4])
                if len(items) > 4:
                    body += f" | +{len(items) - 4} more"
                level = "ALERT-RED" if sev == "red" else "ALERT-YELLOW"
                msg = f"[{preset_label}] [{level}] {body}"
                prev = last_alert_levels.get(sev)
                last_alert_levels[sev] = level
                logging.warning(msg) if sev == "red" else logging.info(msg)
                if notifier:
                    await notifier.maybe_send(msg, key=key)

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
                        sys_parts.append(f"py_io_read={read_mb:.2f}MB/{interval_s}s")
                        sys_parts.append(f"py_io_write={write_mb:.2f}MB/{interval_s}s")
                    last_io = io
                except Exception:
                    pass
                ch_cpu = None
                ch_rss_mb = None
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
                                sys_parts.append(f"ch_io_read={ch_read_mb:.2f}MB/{interval_s}s")
                                sys_parts.append(f"ch_io_write={ch_write_mb:.2f}MB/{interval_s}s")
                            last_ch_io = ch_io
                        except Exception:
                            pass
                    except Exception:
                        ch_proc = None
                sys_line = " | ".join(sys_parts)
                logging.info("[sys] %s", sys_line)
                if notifier:
                    await notifier.maybe_send(
                        f"[{preset_label}] [sys] " + sys_line,
                        key="sys",
                    )
                if overview_aggregator:
                    sys_snapshot = {
                        "py_cpu": cpu_pct,
                        "py_rss": rss_mb,
                    }
                    if ch_cpu is not None:
                        sys_snapshot["ch_cpu"] = ch_cpu
                    if ch_rss_mb is not None:
                        sys_snapshot["ch_rss"] = ch_rss_mb
                    overview_aggregator.update(
                        interval_s,
                        channel_stats,
                        health_snapshot,
                        flush_errors - last_flush_errors,
                        sys_snapshot,
                        overview_notifier,
                    )
            except Exception:
                proc = None
        elif overview_aggregator:
            overview_aggregator.update(
                interval_s,
                channel_stats,
                health_snapshot,
                flush_errors - last_flush_errors,
                {},
                overview_notifier,
            )

        last_ws = ws_counts
        last_router = dict(router_counts)
        last_discs = dict(disc_counts)
        if table_counts is not None:
            last_table = dict(table_counts)
        if flushed_counts is not None:
            last_flushed = dict(flushed_counts)
        last_errs = {channel: {"parse": parse_errors.get(channel, 0), "validation": validation_errors.get(channel, 0)} for channel in channels}
        last_flush_errors = flush_errors


async def _overview_loop(
    overview_aggregator: OverviewAggregator,
    overview_notifier: TelegramNotifier,
    interval_s: int,
) -> None:
    interval_s = max(5, int(interval_s))
    while True:
        await asyncio.sleep(interval_s)
        overview_message = overview_aggregator.build_overview()
        await overview_notifier.maybe_send(overview_message, key="overview")
        overview_aggregator.reset_window()


async def _health_monitor(
    orchestrator: FeedOrchestrator,
    symbols: List[str],
    channels: tuple[str, ...],
    interval_s: int,
) -> None:
    max_lag_ms = {
        "mark_price": 5000,
        "ob_top5": 5000,
        "l1": 30000,
        "klines": 120000,
        "agg_trades_5s": 15000,
    }
    sample_limit = 5
    interval_s = max(1, int(interval_s))
    while True:
        await asyncio.sleep(interval_s)
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

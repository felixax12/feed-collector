#!/usr/bin/env python3
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Iterable, Optional

START_TS = datetime.strptime("2026-01-27 13:03:26", "%Y-%m-%d %H:%M:%S")

DOCKER_LOG = Path("logs/docker_stats.log")
PY_LOG = Path("logs/feed_runtime.log")


def parse_ts(line: str) -> Optional[datetime]:
    line = line.strip()
    if not line:
        return None
    try:
        return datetime.strptime(line, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    return {
        "min": min(values),
        "max": max(values),
        "avg": mean(values),
        "med": median(values),
    }


@dataclass
class DockerSample:
    ts: datetime
    cpu: float
    mem_mib: float
    mem_limit_gib: float


DOCKER_ROW_RE = re.compile(
    r"^(?P<id>[0-9a-f]+)\s+(?P<name>\S+)\s+"
    r"(?P<cpu>[0-9.]+)%\s+"
    r"(?P<mem>[0-9.]+)MiB\s+/\s+(?P<limit>[0-9.]+)GiB"
)


def parse_docker(log_path: Path) -> list[DockerSample]:
    samples: list[DockerSample] = []
    if not log_path.exists():
        return samples
    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    i = 0
    current_ts: Optional[datetime] = None
    while i < len(lines):
        ts = parse_ts(lines[i])
        if ts is not None:
            current_ts = ts
            i += 1
            continue
        if current_ts is None or current_ts < START_TS:
            i += 1
            continue
        m = DOCKER_ROW_RE.match(lines[i].strip())
        if m:
            samples.append(
                DockerSample(
                    ts=current_ts,
                    cpu=float(m.group("cpu")),
                    mem_mib=float(m.group("mem")),
                    mem_limit_gib=float(m.group("limit")),
                )
            )
        i += 1
    return samples


PY_SYS_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ .* \[sys\] (?P<rest>.*)$"
)


def parse_py(log_path: Path) -> tuple[list[float], list[float]]:
    cpus: list[float] = []
    rss: list[float] = []
    if not log_path.exists():
        return cpus, rss
    for line in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = PY_SYS_RE.match(line)
        if not m:
            continue
        ts = datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S")
        if ts < START_TS:
            continue
        rest = m.group("rest")
        # py_cpu=5.1% | py_rss=75.2MB | ...
        parts = [p.strip() for p in rest.split("|")]
        for part in parts:
            if part.startswith("py_cpu="):
                cpus.append(float(part.split("=")[1].replace("%", "")))
            if part.startswith("py_rss="):
                rss.append(float(part.split("=")[1].replace("MB", "")))
    return cpus, rss


if __name__ == "__main__":
    docker_samples = parse_docker(DOCKER_LOG)
    docker_cpu = [s.cpu for s in docker_samples]
    docker_mem = [s.mem_mib for s in docker_samples]
    docker_limits = [s.mem_limit_gib for s in docker_samples]

    py_cpu, py_rss = parse_py(PY_LOG)

    print("Docker ClickHouse stats (from", START_TS, ")")
    if docker_samples:
        print("  samples:", len(docker_samples))
        print("  cpu %:", stats(docker_cpu))
        print("  mem MiB:", stats(docker_mem))
        print("  mem limit GiB (constant):", stats(docker_limits))
    else:
        print("  no samples found")

    print("\nPython process stats (from", START_TS, ")")
    if py_cpu:
        print("  samples:", len(py_cpu))
        print("  py cpu %:", stats(py_cpu))
        print("  py rss MB:", stats(py_rss))
    else:
        print("  no samples found")

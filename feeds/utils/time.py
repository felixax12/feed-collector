from __future__ import annotations

import time


def now_ns() -> int:
    """Monotone Timestamp in Nanosekunden für ts_recv_ns."""
    return time.time_ns()

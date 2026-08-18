import os
import time
import tracemalloc
from contextlib import contextmanager
from functools import wraps
from typing import Optional

import psutil

from cdisc_rules_engine.services import logger

_ENABLED = os.environ.get("CDISC_TRACK_MEMORY", "0") == "1"
_PROCESS = psutil.Process(os.getpid()) if _ENABLED else None


def _format_bytes(num_bytes: float) -> str:
    """Return a human readable byte string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:3.1f}{unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f}PB"


def _snapshot() -> dict:
    """Capture current memory usage."""
    info = _PROCESS.memory_info()
    return {
        "rss": info.rss,
        "vms": info.vms,
        "py_heap": tracemalloc.get_traced_memory()[0] if tracemalloc.is_tracing() else 0,
    }


def _log(label: str, before: dict, after: dict, elapsed_ms: Optional[float] = None):
    """Log memory deltas."""
    rss_delta = after["rss"] - before["rss"]
    heap_delta = after["py_heap"] - before["py_heap"]
    parts = [
        f"[memory] {label}",
        f"rss={_format_bytes(after['rss'])} (delta={_format_bytes(rss_delta)})",
        f"py_heap={_format_bytes(after['py_heap'])} (delta={_format_bytes(heap_delta)})",
    ]
    if elapsed_ms is not None:
        parts.append(f"time={elapsed_ms:.1f}ms")
    logger.info(" | ".join(parts))


@contextmanager
def track_memory(label: str):
    """Context manager that logs memory use around a block of code.

    Enabled when the environment variable ``CDISC_TRACK_MEMORY=1`` is set.
    """
    if not _ENABLED:
        yield
        return

    if not tracemalloc.is_tracing():
        tracemalloc.start()

    before = _snapshot()
    start = time.perf_counter()
    try:
        yield
    finally:
        after = _snapshot()
        elapsed_ms = (time.perf_counter() - start) * 1000
        _log(label, before, after, elapsed_ms)


def track_memory_function(label: Optional[str] = None):
    """Decorator that logs memory use for a function.

    Enabled when the environment variable ``CDISC_TRACK_MEMORY=1`` is set.

    Examples:
        @track_memory_function()
        def my_func(): ...

        @track_memory_function("custom label")
        def my_func(): ...
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with track_memory(label or f"{func.__module__}.{func.__qualname__}"):
                return func(*args, **kwargs)

        return wrapper

    return decorator

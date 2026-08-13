import threading
import time
from typing import Optional

import psutil

from cdisc_rules_engine.exceptions.custom_exceptions import RuleResourceExceededError
from cdisc_rules_engine.services import logger


class RuleExecutionBudget:
    """
    Context manager that enforces per-rule time and memory budgets.

    While active it runs a background monitor thread that polls elapsed wall
    time and process RSS. If either threshold is exceeded it cancels any
    currently-executing PostgreSQL connection and records the reason so that
    the SQL engine can surface a skipped/resource-limit result.
    """

    def __init__(
        self,
        max_time_seconds: Optional[float] = None,
        max_memory_mb: Optional[float] = None,
        check_interval_seconds: float = 0.1,
    ):
        self.max_time_seconds = max_time_seconds
        self.max_memory_mb = max_memory_mb
        self.check_interval_seconds = check_interval_seconds

        self.start_time: Optional[float] = None
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._current_connection = None
        self._exceeded = False
        self._reason: Optional[str] = None
        self._resource_type: Optional[str] = None

    def __enter__(self):
        self.start_time = time.monotonic()
        self._stop_event.clear()
        self._exceeded = False
        self._reason = None
        self._resource_type = None
        self._monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self._monitor_thread.start()
        logger.debug(
            f"Rule execution budget started: max_time={self.max_time_seconds}s, " f"max_memory={self.max_memory_mb}MB"
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=max(self.check_interval_seconds * 2, 0.2))

        with self._lock:
            self._current_connection = None

        if self._exceeded and exc_type is not RuleResourceExceededError:
            raise RuleResourceExceededError(
                reason=self._reason or "Rule exceeded execution budget",
                resource_type=self._resource_type or "unknown",
            )

    @property
    def exceeded(self) -> bool:
        return self._exceeded

    @property
    def reason(self) -> Optional[str]:
        return self._reason

    @property
    def resource_type(self) -> Optional[str]:
        return self._resource_type

    def register_connection(self, connection):
        """Called by the SQL interface before executing on a connection."""
        with self._lock:
            if self._exceeded:
                raise RuleResourceExceededError(
                    reason=self._reason or "Rule exceeded execution budget",
                    resource_type=self._resource_type or "unknown",
                )
            self._current_connection = connection

    def unregister_connection(self):
        """Called by the SQL interface after a query finishes."""
        with self._lock:
            self._current_connection = None

    def _monitor(self):  # noqa
        try:
            process = psutil.Process()
        except Exception:
            logger.warning("Could not monitor process memory; memory limit disabled")
            process = None

        while not self._stop_event.is_set():
            self._stop_event.wait(self.check_interval_seconds)
            if self._stop_event.is_set():
                break

            if self.max_time_seconds is not None:
                elapsed = time.monotonic() - self.start_time
                if elapsed > self.max_time_seconds:
                    self._trigger_cancel(
                        reason=(
                            f"Rule exceeded maximum execution time of "
                            f"{self.max_time_seconds}s (elapsed {elapsed:.1f}s)"
                        ),
                        resource_type="time",
                    )
                    return

            if self.max_memory_mb is not None and process is not None:
                try:
                    rss_mb = process.memory_info().rss / (1024 * 1024)
                except Exception:
                    rss_mb = 0.0
                if rss_mb > self.max_memory_mb:
                    self._trigger_cancel(
                        reason=(
                            f"Rule exceeded maximum memory usage of " f"{self.max_memory_mb}MB (used {rss_mb:.1f}MB)"
                        ),
                        resource_type="memory",
                    )
                    return

    def _trigger_cancel(self, reason: str, resource_type: str):
        connection = None
        with self._lock:
            self._exceeded = True
            self._reason = reason
            self._resource_type = resource_type
            connection = self._current_connection

        logger.warning(f"Rule execution budget exceeded: {reason}")

        if connection is not None:
            try:
                connection.cancel()
                logger.debug("Cancelled active PostgreSQL connection")
            except Exception as e:
                logger.warning(f"Failed to cancel active PostgreSQL connection: {e}")

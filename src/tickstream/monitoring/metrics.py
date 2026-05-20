"""Central metrics registry for the tickstream pipeline.

All instrumented code calls methods on a :class:`MetricsRegistry` instance
rather than touching ``prometheus_client`` objects directly.  This keeps the
instrumentation surface minimal and makes tests easy: pass a fresh
``CollectorRegistry`` via :func:`create_registry` so no global state leaks
between test runs.

Thread-safety: all prometheus_client objects use internal locks; the
:class:`_AgeTracker` custom collector has its own ``threading.Lock``.
"""

from __future__ import annotations

import threading
import time

import prometheus_client
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client import start_http_server as _prom_start_http_server
from prometheus_client.exposition import generate_latest
from prometheus_client.metrics_core import GaugeMetricFamily

# ---------------------------------------------------------------------------
# Write-latency histogram buckets (tuned for sub-second disk I/O)
# ---------------------------------------------------------------------------

_WRITE_LATENCY_BUCKETS: tuple[float, ...] = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)


# ---------------------------------------------------------------------------
# Custom collector: last-message age
# ---------------------------------------------------------------------------


class _AgeTracker:
    """Custom prometheus collector that reports last-message-age gauges.

    The age is computed at *scrape time* inside ``collect()`` rather than at
    message time, so the metric accurately reflects current feed staleness.
    """

    _METRIC_NAME = "tickstream_last_message_age_seconds"
    _METRIC_HELP = "Seconds elapsed since the last message was received for this feed"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last: dict[tuple[str, str], float] = {}

    def update(self, exchange: str, symbol: str) -> None:
        """Record that a message for ``(exchange, symbol)`` arrived right now."""
        with self._lock:
            self._last[(exchange, symbol)] = time.time()

    # prometheus_client collector protocol --------------------------------

    def describe(self) -> list[GaugeMetricFamily]:  # type: ignore[override]
        return [GaugeMetricFamily(self._METRIC_NAME, self._METRIC_HELP)]

    def collect(self) -> list[GaugeMetricFamily]:  # type: ignore[override]
        now = time.time()
        g = GaugeMetricFamily(
            self._METRIC_NAME,
            self._METRIC_HELP,
            labels=["exchange", "symbol"],
        )
        with self._lock:
            for (exchange, symbol), ts in self._last.items():
                g.add_metric([exchange, symbol], now - ts)
        return [g]


# ---------------------------------------------------------------------------
# MetricsRegistry
# ---------------------------------------------------------------------------


class MetricsRegistry:
    """Thread-safe, in-process metrics registry for tickstream.

    Wraps :mod:`prometheus_client` objects and exposes a clean interface for
    instrumented code to record events.  All methods are safe to call from any
    thread or from asyncio coroutines.

    Parameters
    ----------
    registry:
        The :class:`~prometheus_client.CollectorRegistry` to register metrics
        against.  Omit to use the global default registry.  Pass a fresh
        ``CollectorRegistry()`` in tests to avoid polluting global state.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        r: CollectorRegistry = registry if registry is not None else prometheus_client.REGISTRY

        self._msgs_received = Counter(
            "tickstream_msgs_received_total",
            "Total raw WebSocket frames received, by exchange",
            ["exchange"],
            registry=r,
        )
        self._msgs_parsed = Counter(
            "tickstream_msgs_parsed_total",
            "Total trade ticks successfully parsed, by exchange and symbol",
            ["exchange", "symbol"],
            registry=r,
        )
        self._parse_errors = Counter(
            "tickstream_parse_errors_total",
            "Total message parse failures, by exchange",
            ["exchange"],
            registry=r,
        )
        self._queue_depth = Gauge(
            "tickstream_queue_depth",
            "Current number of ticks waiting in the shared queue",
            registry=r,
        )
        self._ticks_written = Counter(
            "tickstream_ticks_written_total",
            "Total ticks successfully persisted to Parquet, by exchange and symbol",
            ["exchange", "symbol"],
            registry=r,
        )
        self._write_latency = Histogram(
            "tickstream_write_latency_seconds",
            "Time from batch-flush start to Parquet fsync completion",
            buckets=_WRITE_LATENCY_BUCKETS,
            registry=r,
        )
        self._reconnects = Counter(
            "tickstream_reconnects_total",
            "Total WebSocket reconnect attempts, by exchange",
            ["exchange"],
            registry=r,
        )

        # Custom age collector — registered against the *same* registry.
        self._age_tracker = _AgeTracker()
        r.register(self._age_tracker)  # type: ignore[arg-type]

        self._registry = r

        # Internal per-exchange message counters for rate computation.
        # Separate from the prometheus Counter so we can snapshot-and-reset
        # for the periodic structured log.
        self._recv_lock = threading.Lock()
        self._recv_counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Instrumentation helpers
    # ------------------------------------------------------------------

    def record_message_received(self, exchange: str) -> None:
        """One raw WebSocket frame arrived from *exchange*."""
        self._msgs_received.labels(exchange=exchange).inc()
        with self._recv_lock:
            self._recv_counts[exchange] = self._recv_counts.get(exchange, 0) + 1

    def record_tick_parsed(self, exchange: str, symbol: str) -> None:
        """One trade tick was successfully parsed for *exchange*/*symbol*."""
        self._msgs_parsed.labels(exchange=exchange, symbol=symbol).inc()
        self._age_tracker.update(exchange, symbol)

    def record_parse_error(self, exchange: str) -> None:
        """One message from *exchange* failed to parse."""
        self._parse_errors.labels(exchange=exchange).inc()

    def set_queue_depth(self, depth: int) -> None:
        """Update the shared-queue depth gauge (call periodically)."""
        self._queue_depth.set(depth)

    def record_ticks_written(self, exchange: str, symbol: str, n: int) -> None:
        """*n* ticks for *exchange*/*symbol* were written to Parquet."""
        self._ticks_written.labels(exchange=exchange, symbol=symbol).inc(n)

    def observe_write_latency(self, seconds: float) -> None:
        """Record one batch-write duration in *seconds*."""
        self._write_latency.observe(seconds)

    def record_reconnect(self, exchange: str) -> None:
        """One reconnect attempt was made for *exchange*."""
        self._reconnects.labels(exchange=exchange).inc()

    # ------------------------------------------------------------------
    # Rate snapshot (used by periodic structured log)
    # ------------------------------------------------------------------

    def snapshot_recv_rates(self) -> dict[str, int]:
        """Return per-exchange raw-message counts since the last snapshot.

        Resets the internal counters so the next call returns the delta since
        *this* call (not since startup).  Use the return value divided by the
        sample interval to get msgs/s.
        """
        with self._recv_lock:
            snapshot = dict(self._recv_counts)
            self._recv_counts.clear()
        return snapshot

    # ------------------------------------------------------------------
    # Exposition
    # ------------------------------------------------------------------

    def generate_latest(self) -> bytes:
        """Return current metrics in Prometheus text exposition format."""
        return generate_latest(self._registry)  # type: ignore[return-value]

    def start_http_server(self, port: int, addr: str = "0.0.0.0") -> None:
        """Start the Prometheus ``/metrics`` HTTP server on *port*."""
        _prom_start_http_server(port, addr=addr, registry=self._registry)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_registry() -> MetricsRegistry:
    """Return a :class:`MetricsRegistry` backed by a fresh ``CollectorRegistry``.

    Use this in tests to avoid touching the global prometheus registry.
    """
    return MetricsRegistry(registry=CollectorRegistry())

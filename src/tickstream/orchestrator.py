"""Orchestrator: run multiple connectors concurrently on a shared queue.

The orchestrator starts every connector as an :class:`asyncio.Task` inside an
:class:`asyncio.TaskGroup`, waits for a stop signal (SIGINT / SIGTERM or
external cancellation), and then shuts all connectors down cleanly.

Shutdown sequence
-----------------
1. Signal handler sets an internal ``asyncio.Event``.
2. The orchestrator cancels each connector task (``task.cancel()``).
3. Each connector's ``run()`` catches ``CancelledError``, closes the WebSocket
   with a proper close frame, and returns ``None``.
4. The :class:`asyncio.TaskGroup` exits once all tasks finish.

If the orchestrator's own task is cancelled externally (not via signal) the
``CancelledError`` propagates through the TaskGroup, which cancels all
children, then re-raises; the caller sees a normal ``CancelledError``.

Periodic structured log
-----------------------
When a :class:`~tickstream.monitoring.metrics.MetricsRegistry` is supplied,
the orchestrator emits a ``"pipeline.summary"`` structlog event every
``log_interval_s`` seconds (default 10 s) that contains:

- ``queue_depth`` – current items in the shared queue
- ``msgs_per_sec`` – per-exchange raw-frame rate since the last summary
- ``files_written`` – total Parquet files written (requires *writer* kwarg)
- ``oldest_batch_age_s`` – age of the oldest in-memory batch (requires *writer*)
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Sequence
from typing import TYPE_CHECKING

import structlog

from tickstream.connectors.base import BaseConnector
from tickstream.models import Tick

if TYPE_CHECKING:
    from tickstream.monitoring.metrics import MetricsRegistry
    from tickstream.storage.parquet_writer import ParquetWriter

log: structlog.BoundLogger = structlog.get_logger(__name__)

_DEFAULT_LOG_INTERVAL_S: float = 10.0


class Orchestrator:
    """Run multiple :class:`~tickstream.connectors.base.BaseConnector` s on a
    shared output queue.

    Parameters
    ----------
    connectors:
        Connectors to run concurrently.  Each will be started as a separate
        :class:`asyncio.Task`.
    queue:
        The shared destination queue.  All connectors must already be
        configured to push to this queue.
    metrics:
        Optional :class:`~tickstream.monitoring.metrics.MetricsRegistry`.
        When provided, ``queue_depth`` is sampled and a periodic structured
        log is emitted every *log_interval_s* seconds.
    writer:
        Optional :class:`~tickstream.storage.parquet_writer.ParquetWriter`
        reference used to include ``files_written`` and ``oldest_batch_age_s``
        in the periodic log.
    log_interval_s:
        How often (in seconds) the periodic summary log fires.

    Example
    -------
    ::

        queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=50_000)
        orch = Orchestrator(
            connectors=[
                BinanceConnector(["btcusdt", "ethusdt"], queue),
                CoinbaseConnector(["BTC-USD", "ETH-USD"], queue),
            ],
            queue=queue,
        )
        asyncio.run(orch.run())   # blocks; Ctrl-C triggers clean shutdown
    """

    def __init__(
        self,
        connectors: Sequence[BaseConnector],
        queue: asyncio.Queue[Tick],
        *,
        metrics: MetricsRegistry | None = None,
        writer: ParquetWriter | None = None,
        log_interval_s: float = _DEFAULT_LOG_INTERVAL_S,
    ) -> None:
        if not connectors:
            raise ValueError("At least one connector must be provided")
        self._connectors = list(connectors)
        self._queue = queue
        self._metrics = metrics
        self._writer = writer
        self._log_interval_s = log_interval_s

    async def run(self) -> None:
        """Start all connectors; block until a stop signal or cancellation.

        Registers SIGINT and SIGTERM handlers for the duration of the call.
        Both signals trigger a clean shutdown — all connector WebSockets are
        closed before this coroutine returns.
        """
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _on_signal() -> None:
            log.info("orchestrator.signal_received")
            stop.set()

        # add_signal_handler is POSIX-only; suppress on Windows.
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, OSError):
                loop.add_signal_handler(sig, _on_signal)

        all_tasks: list[asyncio.Task[None]] = []

        try:
            async with asyncio.TaskGroup() as tg:
                for i, connector in enumerate(self._connectors):
                    t = tg.create_task(
                        connector.run(),
                        name=f"{type(connector).__name__}-{i}",
                    )
                    all_tasks.append(t)

                # Periodic summary log (only when metrics are wired in).
                if self._metrics is not None:
                    pt = tg.create_task(
                        self._periodic_task(), name="orchestrator-periodic"
                    )
                    all_tasks.append(pt)

                log.info(
                    "orchestrator.started",
                    n_connectors=len(self._connectors),
                    connectors=[type(c).__name__ for c in self._connectors],
                )

                # Block until a stop signal arrives; then cancel all tasks.
                # If we're cancelled externally, CancelledError propagates here,
                # exits the TaskGroup body, and the TaskGroup cancels children.
                await stop.wait()

                log.info("orchestrator.stopping")
                for task in all_tasks:
                    task.cancel()
            # TaskGroup.__aexit__ waits for all tasks to complete before here.

        except* Exception as eg:
            # Unexpected exception from a connector (not CancelledError).
            log.error(
                "orchestrator.connector_crashed",
                errors=[f"{type(e).__name__}: {e}" for e in eg.exceptions],
            )

        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError, OSError):
                    loop.remove_signal_handler(sig)

        log.info("orchestrator.stopped")

    # ------------------------------------------------------------------
    # Periodic summary log
    # ------------------------------------------------------------------

    async def _periodic_task(self) -> None:
        """Sample queue depth and emit a structured pipeline summary log."""
        interval = self._log_interval_s
        while True:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return

            depth = self._queue.qsize()

            # Update queue-depth gauge.
            if self._metrics is not None:
                self._metrics.set_queue_depth(depth)
                raw_rates = self._metrics.snapshot_recv_rates()
                msgs_per_sec = {
                    ex: round(cnt / interval, 1) for ex, cnt in raw_rates.items()
                }
            else:
                msgs_per_sec: dict[str, float] = {}

            files_written: int | None = (
                self._writer.files_written if self._writer is not None else None
            )
            oldest_age: float | None = (
                self._writer.oldest_batch_age_s
                if self._writer is not None
                else None
            )

            log.info(
                "pipeline.summary",
                queue_depth=depth,
                msgs_per_sec=msgs_per_sec,
                files_written=files_written,
                oldest_batch_age_s=(
                    round(oldest_age, 2) if oldest_age is not None else None
                ),
            )

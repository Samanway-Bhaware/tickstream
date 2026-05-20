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
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Sequence

import structlog

from tickstream.connectors.base import BaseConnector
from tickstream.models import Tick

log: structlog.BoundLogger = structlog.get_logger(__name__)


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
    ) -> None:
        if not connectors:
            raise ValueError("At least one connector must be provided")
        self._connectors = list(connectors)
        self._queue = queue

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

        connector_tasks: list[asyncio.Task[None]] = []

        try:
            async with asyncio.TaskGroup() as tg:
                for i, connector in enumerate(self._connectors):
                    t = tg.create_task(
                        connector.run(),
                        name=f"{type(connector).__name__}-{i}",
                    )
                    connector_tasks.append(t)

                log.info(
                    "orchestrator.started",
                    n_connectors=len(self._connectors),
                    connectors=[type(c).__name__ for c in self._connectors],
                )

                # Block until a stop signal arrives; then cancel all connectors.
                # If we're cancelled externally, CancelledError propagates here,
                # exits the TaskGroup body, and the TaskGroup cancels children.
                await stop.wait()

                log.info("orchestrator.stopping")
                for task in connector_tasks:
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

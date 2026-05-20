"""Tests for src/tickstream/orchestrator.py.

Integration approach
--------------------
Real connector subclasses are impractical here because their ``run()``
methods connect to the network.  Instead each test uses ``_MockConnector``,
a minimal ``BaseConnector`` subclass whose ``run()`` is overridden to:

1. Put a fixed set of pre-built ticks directly onto the queue.
2. Block on an ``asyncio.Event`` until it is cancelled.

This exercises the orchestrator's TaskGroup, signal-handling wiring, and
shutdown path without any network I/O.
"""

from __future__ import annotations

import asyncio
import decimal
import signal
import time
from collections.abc import Iterable

import pytest

from tickstream.connectors.base import BaseConnector
from tickstream.models import Tick
from tickstream.orchestrator import Orchestrator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW_NS = time.time_ns()
_NOW_MS = _NOW_NS // 1_000_000
_TS_NS = _NOW_MS * 1_000_000


def _make_tick(exchange: str, trade_id: str) -> Tick:
    return Tick(
        exchange=exchange,
        symbol="BTC-USD",
        price=decimal.Decimal("50000"),
        size=decimal.Decimal("0.001"),
        side="buy",
        timestamp_ns=_TS_NS,
        received_ns=_TS_NS,
        trade_id=trade_id,
    )


class _MockConnector(BaseConnector):
    """Test double: puts pre-defined ticks then blocks until cancelled."""

    def __init__(
        self,
        queue: asyncio.Queue[Tick],
        ticks: list[Tick],
        *,
        exchange_name: str = "mock",
    ) -> None:
        super().__init__([f"{exchange_name}-sym"], queue)
        self._ticks = ticks
        self._exchange_name = exchange_name

    # BaseConnector abstract methods — not exercised in these tests.
    def _url(self) -> str:
        return "ws://mock"

    def _subscribe_message(self, symbols: list[str]) -> str | None:
        return None

    def _parse_message(self, raw: str | bytes, received_ns: int) -> Iterable[Tick]:
        return iter([])  # never called; run() is overridden

    # Override run() to avoid real network connections.
    async def run(self) -> None:
        for tick in self._ticks:
            self._queue.put_nowait(tick)
        # Block until the task is cancelled.
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------


class TestOrchestratorInit:
    def test_empty_connectors_raises(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        with pytest.raises(ValueError, match="At least one connector"):
            Orchestrator([], q)


# ---------------------------------------------------------------------------
# Integration: ticks from both connectors arrive on the shared queue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestOrchestratorRun:
    async def test_ticks_from_both_connectors_arrive(self) -> None:
        """Both connectors' ticks must appear on the single shared queue."""
        q: asyncio.Queue[Tick] = asyncio.Queue()

        ticks_a = [_make_tick("exchange-a", f"a-{i}") for i in range(3)]
        ticks_b = [_make_tick("exchange-b", f"b-{i}") for i in range(3)]

        orch = Orchestrator(
            [_MockConnector(q, ticks_a, exchange_name="exchange-a"),
             _MockConnector(q, ticks_b, exchange_name="exchange-b")],
            q,
        )

        task = asyncio.create_task(orch.run())

        # Collect all 6 ticks (timeout guards against hangs in CI).
        received: list[Tick] = []
        while len(received) < 6:
            tick = await asyncio.wait_for(q.get(), timeout=2.0)
            received.append(tick)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(received) == 6
        exchanges = {t.exchange for t in received}
        assert exchanges == {"exchange-a", "exchange-b"}

    async def test_trade_ids_preserved(self) -> None:
        """No ticks are lost or duplicated in transit through the orchestrator."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        all_ticks = [_make_tick("ex", f"id-{i}") for i in range(5)]
        orch = Orchestrator([_MockConnector(q, all_ticks)], q)

        task = asyncio.create_task(orch.run())

        received_ids: list[str] = []
        for _ in range(5):
            tick = await asyncio.wait_for(q.get(), timeout=2.0)
            received_ids.append(tick.trade_id)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert sorted(received_ids) == [f"id-{i}" for i in range(5)]

    async def test_clean_shutdown_on_task_cancellation(self) -> None:
        """Cancelling the orchestrator task must not raise outside the task."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        orch = Orchestrator([_MockConnector(q, [])], q)

        task = asyncio.create_task(orch.run())
        await asyncio.sleep(0)  # let the task start
        await asyncio.sleep(0)  # let connectors start

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_signal_triggers_graceful_shutdown(self) -> None:
        """Sending SIGINT to the current process stops the orchestrator."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ticks = [_make_tick("sig", f"s-{i}") for i in range(2)]
        orch = Orchestrator([_MockConnector(q, ticks, exchange_name="sig")], q)

        task = asyncio.create_task(orch.run())

        # Wait for both ticks so we know the connectors are running.
        for _ in range(2):
            await asyncio.wait_for(q.get(), timeout=2.0)

        # Trigger SIGINT — the orchestrator's signal handler sets the stop event.
        signal.raise_signal(signal.SIGINT)

        # Orchestrator should stop on its own; give it a short timeout.
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except asyncio.CancelledError:
            pass  # also acceptable
        except asyncio.TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            pytest.fail("Orchestrator did not stop after SIGINT within 2 s")

    async def test_multiple_connectors_start_and_run_concurrently(self) -> None:
        """Verify the tasks actually run concurrently (not sequentially)."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        n = 4
        ticks_per_connector = [
            [_make_tick(f"ex{i}", f"t{i}-0")] for i in range(n)
        ]
        connectors = [
            _MockConnector(q, ticks, exchange_name=f"ex{i}")
            for i, ticks in enumerate(ticks_per_connector)
        ]
        orch = Orchestrator(connectors, q)

        task = asyncio.create_task(orch.run())

        received: list[Tick] = []
        for _ in range(n):
            tick = await asyncio.wait_for(q.get(), timeout=2.0)
            received.append(tick)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        exchanges = {t.exchange for t in received}
        assert len(exchanges) == n  # one tick per connector, all distinct

"""Abstract base class for all exchange WebSocket connectors.

Every connector subclass owns three things:
- :meth:`_url` – the WebSocket URI to connect to
- :meth:`_subscribe_message` – an optional frame sent immediately after
  connecting (return ``None`` if the exchange uses URL-based subscription)
- :meth:`_parse_message` – a generator that turns one raw frame into zero
  or more :class:`~tickstream.models.Tick` objects

The base class owns everything else: the reconnect loop with exponential
backoff + jitter, queue pushing with overflow protection, and lifecycle
(graceful shutdown on cancellation).
"""

from __future__ import annotations

import asyncio
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence

import structlog
import websockets
import websockets.exceptions

from tickstream.models import Tick

log: structlog.BoundLogger = structlog.get_logger(__name__)

# Shared backoff defaults — subclasses may override via constructor kwargs.
_INITIAL_BACKOFF: float = 1.0
_MAX_BACKOFF: float = 30.0
_JITTER_FACTOR: float = 0.25


class BaseConnector(ABC):
    """Reconnecting WebSocket connector with pluggable parsing.

    Parameters
    ----------
    symbols:
        Exchange symbols to subscribe to.  Normalisation (case, separators)
        is the subclass's responsibility.
    queue:
        Destination queue.  Ticks are enqueued via ``put_nowait``; a
        :class:`asyncio.QueueFull` is logged and dropped rather than raising.
    initial_backoff:
        Seconds to wait before the *first* reconnect attempt (after a
        disconnect or error).
    max_backoff:
        Hard ceiling on reconnect sleep in seconds.
    jitter_factor:
        Upper bound on random jitter as a fraction of the current delay.
        Set to ``0.0`` for deterministic behaviour in tests.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        queue: asyncio.Queue[Tick],
        *,
        initial_backoff: float = _INITIAL_BACKOFF,
        max_backoff: float = _MAX_BACKOFF,
        jitter_factor: float = _JITTER_FACTOR,
    ) -> None:
        if not symbols:
            raise ValueError("At least one symbol must be provided")
        self._symbols: list[str] = list(symbols)
        self._queue = queue
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._jitter_factor = jitter_factor

    # ------------------------------------------------------------------
    # Abstract interface — subclasses must implement all three
    # ------------------------------------------------------------------

    @abstractmethod
    def _url(self) -> str:
        """Return the WebSocket URI for this connector."""

    @abstractmethod
    def _subscribe_message(self, symbols: list[str]) -> str | None:
        """Return a JSON subscription frame, or ``None`` if not needed.

        When non-``None``, the base class calls ``ws.send(msg)`` immediately
        after the connection is established.
        """

    @abstractmethod
    def _parse_message(
        self,
        raw: str | bytes,
        received_ns: int,
    ) -> Iterable[Tick]:
        """Parse one raw WebSocket frame into zero or more :class:`Tick` s.

        Implement as a generator (``yield`` each tick).  Use a bare ``return``
        for messages that carry no trade data (e.g. subscription confirmations).

        :param raw: The raw text or binary WebSocket frame.
        :param received_ns: Wall-clock receipt time in nanoseconds (already
            stamped by the base class; do not call ``time.time_ns()`` here).
        """

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Connect, consume, and reconnect indefinitely until cancelled.

        On cancellation the active WebSocket is closed with a proper close
        frame before this coroutine returns.
        """
        name = type(self).__name__
        first_attempt = True
        delay = self._initial_backoff

        while True:
            # Sleep (with jitter) before every attempt except the very first.
            if not first_attempt:
                jitter = random.uniform(0.0, delay * self._jitter_factor)
                sleep_for = min(delay + jitter, self._max_backoff)
                log.warning(
                    "connector.reconnecting",
                    connector=name,
                    sleep_s=round(sleep_for, 3),
                    symbols=self._symbols,
                )
                try:
                    await asyncio.sleep(sleep_for)
                except asyncio.CancelledError:
                    log.info("connector.shutdown_during_backoff", connector=name)
                    return

            first_attempt = False

            try:
                await self._consume()
                # Clean server-side close — reset backoff, reconnect quickly.
                log.info("connector.disconnected", connector=name, symbols=self._symbols)
                delay = self._initial_backoff

            except asyncio.CancelledError:
                # Task cancelled while connected; WS close frame already sent
                # by the ``async with websockets.connect`` context manager.
                log.info("connector.shutdown", connector=name, symbols=self._symbols)
                return

            except (
                websockets.exceptions.WebSocketException,
                OSError,
                TimeoutError,
            ) as exc:
                log.error(
                    "connector.connection_error",
                    connector=name,
                    exc_type=type(exc).__name__,
                    exc=str(exc),
                    symbols=self._symbols,
                )
                delay = min(delay * 2, self._max_backoff)

            except Exception as exc:
                # Unexpected error — log, do not crash, reconnect.
                log.exception(
                    "connector.unexpected_error",
                    connector=name,
                    exc_type=type(exc).__name__,
                    exc=str(exc),
                    symbols=self._symbols,
                )
                delay = min(delay * 2, self._max_backoff)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _consume(self) -> None:
        """Open one WebSocket connection and drain frames until it closes."""
        uri = self._url()
        name = type(self).__name__
        log.info("connector.connecting", connector=name, uri=uri, symbols=self._symbols)

        async with websockets.connect(uri) as ws:  # type: ignore[attr-defined]
            log.info("connector.connected", connector=name, uri=uri)

            sub = self._subscribe_message(self._symbols)
            if sub is not None:
                await ws.send(sub)

            async for raw_message in ws:
                # Stamp receipt time as close to the network read as possible.
                received_ns = time.time_ns()
                self._dispatch(raw_message, received_ns)

    def _dispatch(self, raw_message: str | bytes, received_ns: int) -> None:
        """Iterate ``_parse_message``, enqueue results, swallow parse errors."""
        name = type(self).__name__
        try:
            # _parse_message is a generator; body executes lazily inside this loop.
            for tick in self._parse_message(raw_message, received_ns):
                try:
                    self._queue.put_nowait(tick)
                except asyncio.QueueFull:
                    log.warning(
                        "connector.queue_full",
                        connector=name,
                        symbol=tick.symbol,
                        queue_size=self._queue.qsize(),
                    )
        except Exception as exc:
            log.warning(
                "connector.parse_error",
                connector=name,
                exc=str(exc),
                raw=(
                    raw_message[:300]
                    if isinstance(raw_message, str)
                    else raw_message[:300].hex()
                ),
            )

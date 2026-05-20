"""Binance public trade-stream WebSocket connector.

Subscribes to one or more ``<symbol>@trade`` streams, parses each raw message
into a :class:`~tickstream.models.Tick`, and pushes it onto a caller-supplied
:class:`asyncio.Queue`.

Reconnection uses full-jitter exponential backoff (capped at ``max_backoff``
seconds).  The connector runs until its task is cancelled, at which point it
closes the WebSocket with a clean close frame.

Binance stream endpoint reference:
  https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams

Trade message fields used
-------------------------
- ``s``  – symbol (e.g. ``"BTCUSDT"``)
- ``t``  – trade ID (int)
- ``p``  – price (string decimal)
- ``q``  – quantity (string decimal)
- ``T``  – trade time in **milliseconds** since epoch
- ``m``  – is the buyer the market maker?
           ``True``  → seller aggressed → side = "sell"
           ``False`` → buyer aggressed  → side = "buy"
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import Sequence
from typing import Any, Literal

import structlog
import websockets
import websockets.exceptions

from tickstream.models import Tick

log: structlog.BoundLogger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WS_BASE: str = "wss://stream.binance.com:9443"
_INITIAL_BACKOFF: float = 1.0
_MAX_BACKOFF: float = 30.0
# Jitter window: up to ±(delay * factor) added to each sleep
_JITTER_FACTOR: float = 0.25

# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------


def _parse_trade(raw: dict[str, Any], received_ns: int) -> Tick:
    """Convert a Binance ``@trade`` payload dict into a :class:`Tick`.

    ``raw`` may be the top-level message (single-stream endpoint) or the
    ``"data"`` sub-object (combined-stream endpoint) — both have the same
    schema.

    The trade time ``T`` is in **milliseconds**; we convert to nanoseconds.
    """
    side: Literal["buy", "sell"] = "sell" if raw["m"] else "buy"
    timestamp_ns: int = int(raw["T"]) * 1_000_000  # ms → ns
    return Tick(
        exchange="binance",
        symbol=str(raw["s"]),
        price=str(raw["p"]),   # keep as str so Decimal validator handles it
        size=str(raw["q"]),
        side=side,
        timestamp_ns=timestamp_ns,
        received_ns=received_ns,
        trade_id=str(raw["t"]),
    )


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class BinanceConnector:
    """Async connector for Binance public trade streams.

    Parameters
    ----------
    symbols:
        Exchange symbols to subscribe to, e.g. ``["btcusdt", "ethusdt"]``.
        Case-insensitive; normalised to lower-case internally.
    queue:
        Destination queue.  Parsed :class:`Tick` objects are placed here via
        ``put_nowait``; callers should size the queue appropriately.
    ws_base:
        Override the WebSocket base URL (useful in tests).
    initial_backoff:
        Seconds to wait before the first reconnect attempt.
    max_backoff:
        Hard ceiling on reconnect delay in seconds.
    jitter_factor:
        Fraction of current delay added as random jitter.

    Example
    -------
    ::

        queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=10_000)
        connector = BinanceConnector(["btcusdt", "ethusdt"], queue)
        task = asyncio.create_task(connector.run())

        async for tick in queue_iter(queue):
            print(tick)

        task.cancel()
        await task
    """

    def __init__(
        self,
        symbols: Sequence[str],
        queue: asyncio.Queue[Tick],
        *,
        ws_base: str = _WS_BASE,
        initial_backoff: float = _INITIAL_BACKOFF,
        max_backoff: float = _MAX_BACKOFF,
        jitter_factor: float = _JITTER_FACTOR,
    ) -> None:
        if not symbols:
            raise ValueError("At least one symbol must be provided")
        self._symbols: list[str] = [s.lower() for s in symbols]
        self._queue = queue
        self._ws_base = ws_base
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._jitter_factor = jitter_factor

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Connect and consume messages, reconnecting on any error.

        Runs until the enclosing :class:`asyncio.Task` is cancelled.  On
        cancellation the active WebSocket is closed with a proper close frame
        before the coroutine returns.
        """
        first_attempt = True
        delay = self._initial_backoff

        while True:
            # Sleep before every attempt except the very first one.
            if not first_attempt:
                jitter = random.uniform(0.0, delay * self._jitter_factor)
                sleep_for = min(delay + jitter, self._max_backoff)
                log.warning(
                    "binance.reconnecting",
                    sleep_s=round(sleep_for, 3),
                    next_delay_s=round(min(delay * 2, self._max_backoff), 1),
                    symbols=self._symbols,
                )
                try:
                    await asyncio.sleep(sleep_for)
                except asyncio.CancelledError:
                    log.info("binance.shutdown_during_backoff", symbols=self._symbols)
                    return

            first_attempt = False

            try:
                await self._consume()
                # _consume() returned without error → server closed the connection
                # cleanly.  Reset backoff so the next attempt is fast.
                log.info("binance.disconnected", symbols=self._symbols)
                delay = self._initial_backoff

            except asyncio.CancelledError:
                # Task was cancelled while we were connected.  The async-with
                # block inside _consume() has already sent the WS close frame.
                log.info("binance.shutdown", symbols=self._symbols)
                return

            except (
                websockets.exceptions.WebSocketException,
                OSError,
                TimeoutError,
            ) as exc:
                log.error(
                    "binance.connection_error",
                    exc_type=type(exc).__name__,
                    exc=str(exc),
                    symbols=self._symbols,
                )
                delay = min(delay * 2, self._max_backoff)

            except Exception as exc:
                # Unexpected error — log and reconnect rather than crashing.
                log.exception(
                    "binance.unexpected_error",
                    exc_type=type(exc).__name__,
                    exc=str(exc),
                    symbols=self._symbols,
                )
                delay = min(delay * 2, self._max_backoff)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @property
    def _uri(self) -> str:
        """Build the appropriate WebSocket URI.

        Single symbol  → ``/ws/<symbol>@trade``          (no wrapper object)
        Multiple symbols → ``/stream?streams=<s1>@trade/<s2>@trade``
                                                          (wrapped in ``data``)
        """
        streams = "/".join(f"{s}@trade" for s in self._symbols)
        if len(self._symbols) == 1:
            return f"{self._ws_base}/ws/{streams}"
        return f"{self._ws_base}/stream?streams={streams}"

    async def _consume(self) -> None:
        """Open one WebSocket connection and drain messages until it closes."""
        uri = self._uri
        log.info("binance.connecting", uri=uri, symbols=self._symbols)

        async with websockets.connect(uri) as ws:  # type: ignore[attr-defined]
            log.info("binance.connected", uri=uri)
            async for raw_message in ws:
                # Stamp receipt time as close to the network read as possible.
                received_ns = time.time_ns()
                self._dispatch(raw_message, received_ns)

    def _dispatch(self, raw_message: str | bytes, received_ns: int) -> None:
        """Parse one raw WebSocket frame and enqueue the resulting Tick."""
        try:
            payload: dict[str, Any] = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            log.warning("binance.invalid_json", exc=str(exc))
            return

        # Combined-stream endpoint wraps trade data under "data".
        # Single-stream endpoint sends the trade object directly.
        trade_data: dict[str, Any] = payload.get("data", payload)

        if trade_data.get("e") != "trade":
            # Could be a subscription confirmation or ping — silently ignore.
            return

        try:
            tick = _parse_trade(trade_data, received_ns)
        except Exception as exc:
            log.warning(
                "binance.parse_error",
                exc=str(exc),
                raw=(
                    raw_message[:300]
                    if isinstance(raw_message, str)
                    else raw_message[:300].hex()
                ),
            )
            return

        try:
            self._queue.put_nowait(tick)
        except asyncio.QueueFull:
            log.warning(
                "binance.queue_full",
                symbol=tick.symbol,
                queue_size=self._queue.qsize(),
            )

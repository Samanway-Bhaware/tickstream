"""Binance public trade-stream WebSocket connector.

Subscribes to one or more ``<symbol>@trade`` streams, converts each raw
message into a :class:`~tickstream.models.Tick`, and pushes it onto the
shared queue via the base-class machinery.

Endpoint reference:
  https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams

Trade message fields used
-------------------------
- ``s``  – symbol (e.g. ``"BTCUSDT"``)
- ``t``  – trade ID (int)
- ``p``  – price (string decimal)
- ``q``  – quantity (string decimal)
- ``T``  – trade time in **milliseconds** since epoch
- ``m``  – is the buyer the market maker?
           ``True``  → seller aggressed → ``side = "sell"``
           ``False`` → buyer aggressed  → ``side = "buy"``

URI scheme
----------
- Single symbol  → ``/ws/<symbol>@trade``         (bare trade object)
- Multiple symbols → ``/stream?streams=…``         (wrapped under ``"data"``)

Subscription model: URL-based; no post-connect subscription frame is needed.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from typing import TYPE_CHECKING

from tickstream.connectors.base import (
    BaseConnector,
    _INITIAL_BACKOFF,
    _JITTER_FACTOR,
    _MAX_BACKOFF,
)
from tickstream.models import Tick

if TYPE_CHECKING:
    from tickstream.monitoring.metrics import MetricsRegistry

_WS_BASE: str = "wss://stream.binance.com:9443"


# ---------------------------------------------------------------------------
# Parsing helper (module-level so tests can import it directly)
# ---------------------------------------------------------------------------


def _parse_trade(raw: dict[str, Any], received_ns: int) -> Tick:
    """Convert a Binance ``@trade`` payload dict into a :class:`Tick`.

    Works for both single-stream (bare dict) and combined-stream (``"data"``
    sub-object) payloads — both share the same field schema.

    The trade time ``T`` is in **milliseconds**; we convert to nanoseconds.
    """
    side: Literal["buy", "sell"] = "sell" if raw["m"] else "buy"
    timestamp_ns: int = int(raw["T"]) * 1_000_000  # ms → ns
    return Tick(
        exchange="binance",
        symbol=str(raw["s"]),
        price=str(raw["p"]),  # str keeps Decimal precision
        size=str(raw["q"]),
        side=side,
        timestamp_ns=timestamp_ns,
        received_ns=received_ns,
        trade_id=str(raw["t"]),
    )


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class BinanceConnector(BaseConnector):
    """Connector for Binance public ``@trade`` streams.

    Parameters
    ----------
    symbols:
        Trading pairs, e.g. ``["btcusdt", "ethusdt"]``.  Case-insensitive;
        normalised to lower-case internally.
    queue:
        Destination queue (see :class:`~tickstream.connectors.base.BaseConnector`).
    ws_base:
        Override the WebSocket base URL.  Useful in tests.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        queue: "asyncio.Queue[Tick]",
        *,
        ws_base: str = _WS_BASE,
        initial_backoff: float = _INITIAL_BACKOFF,
        max_backoff: float = _MAX_BACKOFF,
        jitter_factor: float = _JITTER_FACTOR,
        metrics: "MetricsRegistry | None" = None,
    ) -> None:
        import asyncio

        super().__init__(
            [s.lower() for s in symbols],
            queue,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            jitter_factor=jitter_factor,
            metrics=metrics,
        )
        self._ws_base = ws_base

    @property
    def exchange(self) -> str:
        return "binance"

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def _url(self) -> str:
        """Build the WebSocket URI.

        Single symbol  → ``/ws/<symbol>@trade``  (no wrapper object in frames)
        Multiple symbols → ``/stream?streams=…``  (frames wrapped under ``"data"``)
        """
        streams = "/".join(f"{s}@trade" for s in self._symbols)
        if len(self._symbols) == 1:
            return f"{self._ws_base}/ws/{streams}"
        return f"{self._ws_base}/stream?streams={streams}"

    def _subscribe_message(self, symbols: list[str]) -> str | None:
        # Binance uses URL-based subscription; no post-connect frame needed.
        return None

    def _parse_message(
        self,
        raw: str | bytes,
        received_ns: int,
    ) -> Iterable[Tick]:
        """Yield one :class:`Tick` for each trade event, skip everything else."""
        payload: dict[str, Any] = json.loads(raw)

        # Combined-stream endpoint wraps the trade object under ``"data"``.
        # Single-stream endpoint sends the trade object at the top level.
        trade_data: dict[str, Any] = payload.get("data", payload)

        if trade_data.get("e") != "trade":
            # Subscription confirmations, pings, etc. — silently ignore.
            return

        yield _parse_trade(trade_data, received_ns)

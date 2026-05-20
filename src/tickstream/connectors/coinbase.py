"""Coinbase Advanced Trade WebSocket connector.

Subscribes to the ``market_trades`` channel for one or more products and
converts each trade event into a :class:`~tickstream.models.Tick`.

Endpoint reference:
  https://docs.cdp.coinbase.com/advanced-trade/docs/ws-overview

Subscription model
------------------
After connecting, the connector sends a single subscribe frame::

    {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channel": "market_trades"
    }

No authentication is required for public market-data channels.

Incoming message schema (``market_trades`` channel)
-----------------------------------------------------
.. code-block:: json

    {
        "channel": "market_trades",
        "timestamp": "2023-02-09T20:19:35.39625135Z",
        "sequence_num": 0,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "12345",
                        "product_id": "BTC-USD",
                        "price": "21720.50",
                        "size": "0.00413",
                        "side": "BUY",
                        "time": "2023-02-09T20:19:35.39625135Z"
                    }
                ]
            }
        ]
    }

The initial snapshot uses ``"type": "snapshot"``; subsequent updates use
``"type": "update"``.  Both are processed identically.

Symbol format
-------------
Coinbase uses hyphen-separated product IDs: ``BTC-USD``, ``ETH-USD``.
The connector accepts symbols in any case (``btc-usd`` → ``BTC-USD``).
"""

from __future__ import annotations

import calendar
import json
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from tickstream.connectors.base import (
    BaseConnector,
    _INITIAL_BACKOFF,
    _JITTER_FACTOR,
    _MAX_BACKOFF,
)
from tickstream.models import Tick

if TYPE_CHECKING:
    from tickstream.monitoring.metrics import MetricsRegistry

_WS_URL: str = "wss://advanced-trade-ws.coinbase.com"
_CHANNEL: str = "market_trades"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_product_id(symbol: str) -> str:
    """Normalise a user-supplied symbol to Coinbase product-ID format.

    Examples::

        "btc-usd"  → "BTC-USD"
        "ETH-USD"  → "ETH-USD"
    """
    return symbol.upper()


def _iso_to_ns(ts: str) -> int:
    """Parse an ISO 8601 UTC timestamp (with up to nanosecond precision) to ns.

    Coinbase sends timestamps like ``"2023-02-09T20:19:35.39625135Z"``
    (8 fractional-second digits = 100 ns resolution).

    :func:`datetime.fromisoformat` only handles up to microseconds in Python
    3.11, so we split the fractional part manually and use
    :func:`calendar.timegm` for integer epoch-second conversion (avoids
    float precision loss).
    """
    s = ts.rstrip("Z")
    frac_ns = 0
    if "." in s:
        base, frac = s.split(".", 1)
        # Pad / truncate to exactly 9 digits.
        frac_ns = int(frac.ljust(9, "0")[:9])
        s = base
    dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")  # noqa: DTZ007
    epoch_s = calendar.timegm(dt.timetuple())
    return epoch_s * 1_000_000_000 + frac_ns


def _parse_trade(raw: dict[str, Any], received_ns: int) -> Tick:
    """Convert one Coinbase trade dict into a :class:`Tick`."""
    side_str = raw["side"].lower()
    if side_str not in ("buy", "sell"):
        raise ValueError(f"Unknown side value from Coinbase: {raw['side']!r}")
    side: Literal["buy", "sell"] = side_str  # type: ignore[assignment]
    return Tick(
        exchange="coinbase",
        symbol=str(raw["product_id"]),
        price=str(raw["price"]),
        size=str(raw["size"]),
        side=side,
        timestamp_ns=_iso_to_ns(str(raw["time"])),
        received_ns=received_ns,
        trade_id=str(raw["trade_id"]),
    )


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class CoinbaseConnector(BaseConnector):
    """Connector for the Coinbase Advanced Trade ``market_trades`` channel.

    Parameters
    ----------
    symbols:
        Product IDs to subscribe to, e.g. ``["BTC-USD", "ETH-USD"]``.
        Case-insensitive; normalised to upper-case Coinbase format internally.
    queue:
        Destination queue (see :class:`~tickstream.connectors.base.BaseConnector`).
    ws_url:
        Override the WebSocket URL.  Useful in tests.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        queue: "asyncio.Queue[Tick]",
        *,
        ws_url: str = _WS_URL,
        initial_backoff: float = _INITIAL_BACKOFF,
        max_backoff: float = _MAX_BACKOFF,
        jitter_factor: float = _JITTER_FACTOR,
        metrics: "MetricsRegistry | None" = None,
    ) -> None:
        import asyncio

        super().__init__(
            [_to_product_id(s) for s in symbols],
            queue,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            jitter_factor=jitter_factor,
            metrics=metrics,
        )
        self._ws_url = ws_url

    @property
    def exchange(self) -> str:
        return "coinbase"

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def _url(self) -> str:
        return self._ws_url

    def _subscribe_message(self, symbols: list[str]) -> str:
        return json.dumps(
            {
                "type": "subscribe",
                "product_ids": symbols,
                "channel": _CHANNEL,
            }
        )

    def _parse_message(
        self,
        raw: str | bytes,
        received_ns: int,
    ) -> Iterable[Tick]:
        """Yield one :class:`Tick` per trade in the event batch."""
        payload: dict[str, Any] = json.loads(raw)

        if payload.get("channel") != _CHANNEL:
            # subscriptions confirmation, heartbeat, etc.
            return

        for event in payload.get("events", []):
            event_type = event.get("type")
            if event_type not in ("update", "snapshot"):
                continue
            for trade in event.get("trades", []):
                yield _parse_trade(trade, received_ns)

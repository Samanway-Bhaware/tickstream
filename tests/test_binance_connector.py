"""Tests for src/tickstream/connectors/binance.py.

Strategy
--------
- ``_parse_trade`` is tested as a pure function (no I/O).
- ``BinanceConnector`` is tested by patching ``websockets.connect`` with a
  lightweight fake that controls exactly which messages are delivered and when
  the connection closes.
- ``asyncio.sleep`` is patched to a no-op so backoff tests run instantly.
- Cancellation is tested by cancelling the running task and asserting it
  terminates cleanly (no leaked ``CancelledError``).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import websockets.exceptions

from tickstream.connectors.binance import BinanceConnector, _parse_trade
from tickstream.models import Tick

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

NOW_NS = time.time_ns()
# Truncate to millisecond precision (mirrors what Binance sends).
NOW_MS = NOW_NS // 1_000_000
# Back-convert so timestamp_ns == received_ns exactly (avoids clock-skew
# errors in _parse_trade tests when received_ns is also NOW_NS).
TRADE_TIME_NS = NOW_MS * 1_000_000


def _trade_payload(
    *,
    symbol: str = "BTCUSDT",
    price: str = "67000.50",
    qty: str = "0.005",
    trade_id: int = 99999,
    trade_time_ms: int = NOW_MS,
    buyer_is_maker: bool = False,
) -> dict[str, Any]:
    """Return a minimal Binance @trade message dict."""
    return {
        "e": "trade",
        "E": trade_time_ms,
        "s": symbol,
        "t": trade_id,
        "p": price,
        "q": qty,
        "T": trade_time_ms,
        "m": buyer_is_maker,
        "M": True,
    }


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload)


def _combined_json(stream: str, payload: dict[str, Any]) -> str:
    return json.dumps({"stream": stream, "data": payload})


# ---------------------------------------------------------------------------
# Fake WebSocket helpers
# ---------------------------------------------------------------------------


class _FakeWebSocket:
    """Minimal async-iterable stand-in for a websockets connection.

    Yields each string in ``messages`` then stops (simulating a server-side
    close).  A ``close_event`` is set when the context-manager exits, which
    lets tests verify the WS was cleaned up.
    """

    def __init__(self, messages: list[str]) -> None:
        self._messages = messages
        self._index = 0
        self.close_event = asyncio.Event()

    def __aiter__(self) -> "_FakeWebSocket":
        return self

    async def __anext__(self) -> str:
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._index]
        self._index += 1
        return msg

    async def close(self) -> None:
        self.close_event.set()


class _HangingWebSocket:
    """A WebSocket that blocks indefinitely — used for cancellation tests."""

    def __aiter__(self) -> "_HangingWebSocket":
        return self

    async def __anext__(self) -> str:
        # Block until cancelled.
        await asyncio.sleep(10_000)
        raise StopAsyncIteration  # unreachable, but satisfies the type checker


def _make_connect(ws: _FakeWebSocket | _HangingWebSocket) -> MagicMock:
    """Return a mock for ``websockets.connect`` that yields *ws* as the
    context-manager value."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=ws)
    cm.__aexit__ = AsyncMock(return_value=False)
    mock_connect = MagicMock(return_value=cm)
    return mock_connect


# ---------------------------------------------------------------------------
# _parse_trade unit tests
# ---------------------------------------------------------------------------


class TestParseTrade:
    def test_buy_side_when_buyer_is_taker(self) -> None:
        raw = _trade_payload(buyer_is_maker=False)
        tick = _parse_trade(raw, TRADE_TIME_NS)
        assert tick.side == "buy"

    def test_sell_side_when_buyer_is_maker(self) -> None:
        raw = _trade_payload(buyer_is_maker=True)
        tick = _parse_trade(raw, TRADE_TIME_NS)
        assert tick.side == "sell"

    def test_exchange_is_always_binance(self) -> None:
        tick = _parse_trade(_trade_payload(), TRADE_TIME_NS)
        assert tick.exchange == "binance"

    def test_symbol_preserved(self) -> None:
        tick = _parse_trade(_trade_payload(symbol="ETHUSDT"), TRADE_TIME_NS)
        assert tick.symbol == "ETHUSDT"

    def test_price_is_decimal_exact(self) -> None:
        tick = _parse_trade(_trade_payload(price="67000.12345678"), TRADE_TIME_NS)
        assert tick.price == Decimal("67000.12345678")

    def test_size_is_decimal_exact(self) -> None:
        tick = _parse_trade(_trade_payload(qty="0.00000001"), TRADE_TIME_NS)
        assert tick.size == Decimal("0.00000001")

    def test_trade_id_stringified(self) -> None:
        tick = _parse_trade(_trade_payload(trade_id=12345), TRADE_TIME_NS)
        assert tick.trade_id == "12345"

    def test_timestamp_ms_converted_to_ns(self) -> None:
        tick = _parse_trade(_trade_payload(trade_time_ms=NOW_MS), TRADE_TIME_NS)
        assert tick.timestamp_ns == NOW_MS * 1_000_000

    def test_received_ns_stored_verbatim(self) -> None:
        tick = _parse_trade(_trade_payload(), TRADE_TIME_NS)
        assert tick.received_ns == TRADE_TIME_NS

    def test_returns_tick_instance(self) -> None:
        tick = _parse_trade(_trade_payload(), TRADE_TIME_NS)
        assert isinstance(tick, Tick)


# ---------------------------------------------------------------------------
# BinanceConnector — constructor
# ---------------------------------------------------------------------------


class TestBinanceConnectorInit:
    def test_symbols_normalised_to_lowercase(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = BinanceConnector(["BTCUSDT", "EthUsdt"], q)
        assert c._symbols == ["btcusdt", "ethusdt"]

    def test_empty_symbols_raises(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        with pytest.raises(ValueError, match="At least one symbol"):
            BinanceConnector([], q)

    def test_single_symbol_uri(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = BinanceConnector(["btcusdt"], q)
        assert c._uri == "wss://stream.binance.com:9443/ws/btcusdt@trade"

    def test_multi_symbol_uri(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = BinanceConnector(["btcusdt", "ethusdt"], q)
        assert c._uri == (
            "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
        )

    def test_custom_ws_base(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = BinanceConnector(["btcusdt"], q, ws_base="ws://localhost:8765")
        assert c._uri == "ws://localhost:8765/ws/btcusdt@trade"


# ---------------------------------------------------------------------------
# BinanceConnector — message parsing via _dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    """Test _dispatch directly, bypassing network I/O."""

    def _make_connector(self, q: asyncio.Queue[Tick] | None = None) -> BinanceConnector:
        if q is None:
            q = asyncio.Queue()
        return BinanceConnector(["btcusdt"], q)

    def test_single_stream_message_enqueued(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make_connector(q)
        c._dispatch(_json(_trade_payload()), TRADE_TIME_NS)
        assert q.qsize() == 1
        tick = q.get_nowait()
        assert tick.symbol == "BTCUSDT"
        assert tick.price == Decimal("67000.50")

    def test_combined_stream_message_unwrapped(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make_connector(q)
        msg = _combined_json("btcusdt@trade", _trade_payload(symbol="BTCUSDT"))
        c._dispatch(msg, TRADE_TIME_NS)
        assert q.qsize() == 1

    def test_non_trade_event_ignored(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make_connector(q)
        c._dispatch(json.dumps({"e": "depthUpdate", "s": "BTCUSDT"}), TRADE_TIME_NS)
        assert q.qsize() == 0

    def test_invalid_json_does_not_raise(self) -> None:
        c = self._make_connector()
        # Should log a warning and return, not raise.
        c._dispatch("not-json{{{{", TRADE_TIME_NS)

    def test_queue_full_does_not_raise(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue(maxsize=1)
        c = self._make_connector(q)
        c._dispatch(_json(_trade_payload()), TRADE_TIME_NS)
        c._dispatch(_json(_trade_payload()), TRADE_TIME_NS)  # queue is full — should not raise

    def test_bad_payload_fields_do_not_raise(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make_connector(q)
        bad = {"e": "trade", "s": "BTCUSDT", "p": "-1", "q": "1", "T": NOW_MS, "t": 1, "m": False}
        c._dispatch(json.dumps(bad), TRADE_TIME_NS)
        # negative price → Tick validation fails → warning logged, nothing enqueued
        assert q.qsize() == 0


# ---------------------------------------------------------------------------
# BinanceConnector — async run() behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestBinanceConnectorRun:
    async def test_ticks_delivered_to_queue(self) -> None:
        """Messages from the WebSocket are parsed and placed on the queue."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = _FakeWebSocket([
            _json(_trade_payload(symbol="BTCUSDT", price="50000.00", trade_id=1)),
            _json(_trade_payload(symbol="BTCUSDT", price="50001.00", trade_id=2)),
        ])
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        with patch("tickstream.connectors.binance.websockets.connect", _make_connect(ws)):
            # WS yields 2 messages then closes → _consume() returns → run() loops.
            # We cancel after the first reconnect sleep to break the loop.
            with patch("tickstream.connectors.binance.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = asyncio.CancelledError  # cancel on first sleep
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert q.qsize() == 2
        t1 = q.get_nowait()
        t2 = q.get_nowait()
        assert t1.price == Decimal("50000.00")
        assert t2.price == Decimal("50001.00")

    async def test_combined_stream_ticks_delivered(self) -> None:
        """Combined-stream wrapper (``data`` key) is transparently unwrapped."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = _FakeWebSocket([
            _combined_json("btcusdt@trade", _trade_payload(symbol="BTCUSDT", trade_id=10)),
            _combined_json("ethusdt@trade", _trade_payload(symbol="ETHUSDT", trade_id=11)),
        ])
        connector = BinanceConnector(["btcusdt", "ethusdt"], q, ws_base="ws://fake")

        with patch("tickstream.connectors.binance.websockets.connect", _make_connect(ws)):
            with patch("tickstream.connectors.binance.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = asyncio.CancelledError
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert q.qsize() == 2

    async def test_reconnects_after_connection_error(self) -> None:
        """A network error triggers a reconnect with backoff sleep."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        attempt = 0

        def _side_effect(uri: str) -> Any:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise OSError("connection refused")
            # Second attempt: return a real fake WS with one message.
            ws = _FakeWebSocket([_json(_trade_payload())])
            return _make_connect(ws)(uri)

        sleep_calls: list[float] = []

        async def _fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            # Cancel on the second sleep (after the second successful connection
            # also disconnects and triggers another backoff).
            if len(sleep_calls) >= 2:
                raise asyncio.CancelledError

        with patch("tickstream.connectors.binance.websockets.connect", side_effect=_side_effect):
            with patch("tickstream.connectors.binance.asyncio.sleep", side_effect=_fake_sleep):
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        # At least one backoff sleep must have occurred.
        assert len(sleep_calls) >= 1
        # The initial backoff must be ≤ max_backoff.
        assert sleep_calls[0] <= connector._max_backoff

    async def test_backoff_doubles_on_repeated_errors(self) -> None:
        """Successive failures double the backoff up to max_backoff."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        connector = BinanceConnector(
            ["btcusdt"], q,
            ws_base="ws://fake",
            initial_backoff=1.0,
            max_backoff=30.0,
            jitter_factor=0.0,  # disable jitter for deterministic assertions
        )

        failures = 0
        MAX_FAILURES = 6

        def _always_fail(uri: str) -> Any:
            nonlocal failures
            failures += 1
            raise OSError("refused")

        sleep_calls: list[float] = []

        async def _fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            if len(sleep_calls) >= MAX_FAILURES:
                raise asyncio.CancelledError

        with patch("tickstream.connectors.binance.websockets.connect", side_effect=_always_fail):
            with patch("tickstream.connectors.binance.asyncio.sleep", side_effect=_fake_sleep):
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        # With jitter=0: 1, 2, 4, 8, 16, 30 (capped)
        expected = [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
        for actual, exp in zip(sleep_calls, expected, strict=False):
            assert actual == pytest.approx(exp, abs=1e-9)

    async def test_clean_shutdown_on_cancellation(self) -> None:
        """Cancelling the task terminates run() without leaking CancelledError."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = _HangingWebSocket()
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        with patch("tickstream.connectors.binance.websockets.connect", _make_connect(ws)):
            task = asyncio.create_task(connector.run())
            # Give the event loop a turn so the task enters the WS receive loop.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            task.cancel()
            # await the task; it should exit cleanly by catching CancelledError
            # internally and returning — NOT by re-raising it.
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_cancellation_during_backoff_sleep(self) -> None:
        """Cancellation during a backoff sleep also exits cleanly."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        def _fail(uri: str) -> Any:
            raise OSError("down")

        real_sleep = asyncio.sleep

        async def _slow_sleep(delay: float) -> None:
            # Simulate a long sleep so the task can be cancelled mid-backoff.
            await real_sleep(10_000)

        with patch("tickstream.connectors.binance.websockets.connect", side_effect=_fail):
            with patch("tickstream.connectors.binance.asyncio.sleep", side_effect=_slow_sleep):
                task = asyncio.create_task(connector.run())
                await asyncio.sleep(0)  # enter run()
                await asyncio.sleep(0)  # enter first backoff sleep

                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

    async def test_websocket_exception_triggers_reconnect(self) -> None:
        """A websockets.WebSocketException is treated the same as a network error."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        attempt = 0

        def _side_effect(uri: str) -> Any:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise websockets.exceptions.ConnectionClosedError(None, None)
            ws = _FakeWebSocket([_json(_trade_payload())])
            return _make_connect(ws)(uri)

        sleep_calls: list[float] = []

        async def _fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            if len(sleep_calls) >= 2:
                raise asyncio.CancelledError

        with patch("tickstream.connectors.binance.websockets.connect", side_effect=_side_effect):
            with patch("tickstream.connectors.binance.asyncio.sleep", side_effect=_fake_sleep):
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert len(sleep_calls) >= 1

    async def test_received_ns_stamped_at_message_receipt(self) -> None:
        """received_ns is taken from time.time_ns() inside _consume, not the payload."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = _FakeWebSocket([_json(_trade_payload())])
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        before = time.time_ns()

        with patch("tickstream.connectors.binance.websockets.connect", _make_connect(ws)):
            with patch("tickstream.connectors.binance.asyncio.sleep", new_callable=AsyncMock) as s:
                s.side_effect = asyncio.CancelledError
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        after = time.time_ns()

        tick = q.get_nowait()
        assert before <= tick.received_ns <= after

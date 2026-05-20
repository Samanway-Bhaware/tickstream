"""Tests for src/tickstream/connectors/binance.py.

Strategy
--------
- ``_parse_trade`` is tested as a pure function (no I/O).
- ``BinanceConnector`` unit tests exercise ``_url()``, ``_parse_message``, and
  ``_dispatch`` directly without network I/O.
- Lifecycle tests (``run()``) patch ``websockets.connect`` and
  ``asyncio.sleep`` in the **base** module (where the run-loop lives after
  the refactor) to control exactly which messages arrive and when errors occur.
- Cancellation tests verify that ``run()`` exits cleanly with no leaked
  ``CancelledError``.
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets.exceptions

from tests._helpers import FakeWebSocket, HangingWebSocket, make_connect
from tickstream.connectors.binance import BinanceConnector, _parse_trade
from tickstream.models import Tick

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

NOW_NS = time.time_ns()
NOW_MS = NOW_NS // 1_000_000
# Back-convert so timestamp_ns == received_ns (no clock-skew error in parse tests).
TRADE_TIME_NS = NOW_MS * 1_000_000

# Patch targets — both live in the base module after the refactor.
_PATCH_WS = "tickstream.connectors.base.websockets.connect"
_PATCH_SLEEP = "tickstream.connectors.base.asyncio.sleep"


def _trade_payload(
    *,
    symbol: str = "BTCUSDT",
    price: str = "67000.50",
    qty: str = "0.005",
    trade_id: int = 99999,
    trade_time_ms: int = NOW_MS,
    buyer_is_maker: bool = False,
) -> dict[str, Any]:
    """Return a minimal Binance ``@trade`` message dict."""
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
# _parse_trade unit tests
# ---------------------------------------------------------------------------


class TestParseTrade:
    def test_buy_side_when_buyer_is_taker(self) -> None:
        tick = _parse_trade(_trade_payload(buyer_is_maker=False), TRADE_TIME_NS)
        assert tick.side == "buy"

    def test_sell_side_when_buyer_is_maker(self) -> None:
        tick = _parse_trade(_trade_payload(buyer_is_maker=True), TRADE_TIME_NS)
        assert tick.side == "sell"

    def test_exchange_is_always_binance(self) -> None:
        assert _parse_trade(_trade_payload(), TRADE_TIME_NS).exchange == "binance"

    def test_symbol_preserved(self) -> None:
        assert _parse_trade(_trade_payload(symbol="ETHUSDT"), TRADE_TIME_NS).symbol == "ETHUSDT"

    def test_price_is_decimal_exact(self) -> None:
        tick = _parse_trade(_trade_payload(price="67000.12345678"), TRADE_TIME_NS)
        assert tick.price == Decimal("67000.12345678")

    def test_size_is_decimal_exact(self) -> None:
        tick = _parse_trade(_trade_payload(qty="0.00000001"), TRADE_TIME_NS)
        assert tick.size == Decimal("0.00000001")

    def test_trade_id_stringified(self) -> None:
        assert _parse_trade(_trade_payload(trade_id=12345), TRADE_TIME_NS).trade_id == "12345"

    def test_timestamp_ms_converted_to_ns(self) -> None:
        tick = _parse_trade(_trade_payload(trade_time_ms=NOW_MS), TRADE_TIME_NS)
        assert tick.timestamp_ns == NOW_MS * 1_000_000

    def test_received_ns_stored_verbatim(self) -> None:
        assert _parse_trade(_trade_payload(), TRADE_TIME_NS).received_ns == TRADE_TIME_NS

    def test_returns_tick_instance(self) -> None:
        assert isinstance(_parse_trade(_trade_payload(), TRADE_TIME_NS), Tick)


# ---------------------------------------------------------------------------
# BinanceConnector — constructor / URL
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

    def test_single_symbol_url(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = BinanceConnector(["btcusdt"], q)
        assert c._url() == "wss://stream.binance.com:9443/ws/btcusdt@trade"

    def test_multi_symbol_url(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = BinanceConnector(["btcusdt", "ethusdt"], q)
        assert c._url() == (
            "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
        )

    def test_custom_ws_base(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = BinanceConnector(["btcusdt"], q, ws_base="ws://localhost:8765")
        assert c._url() == "ws://localhost:8765/ws/btcusdt@trade"

    def test_no_subscribe_message(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = BinanceConnector(["btcusdt"], q)
        assert c._subscribe_message(c._symbols) is None


# ---------------------------------------------------------------------------
# BinanceConnector — message parsing via _dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    """Test ``_dispatch`` (inherited from BaseConnector) directly."""

    def _make(self, q: asyncio.Queue[Tick] | None = None) -> BinanceConnector:
        return BinanceConnector(["btcusdt"], q or asyncio.Queue())

    def test_single_stream_message_enqueued(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make(q)
        c._dispatch(_json(_trade_payload()), TRADE_TIME_NS)
        assert q.qsize() == 1
        tick = q.get_nowait()
        assert tick.symbol == "BTCUSDT"
        assert tick.price == Decimal("67000.50")

    def test_combined_stream_message_unwrapped(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make(q)
        c._dispatch(_combined_json("btcusdt@trade", _trade_payload(symbol="BTCUSDT")), TRADE_TIME_NS)
        assert q.qsize() == 1

    def test_non_trade_event_ignored(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make(q)
        c._dispatch(json.dumps({"e": "depthUpdate", "s": "BTCUSDT"}), TRADE_TIME_NS)
        assert q.qsize() == 0

    def test_invalid_json_does_not_raise(self) -> None:
        c = self._make()
        c._dispatch("not-json{{{{", TRADE_TIME_NS)  # should just log a warning

    def test_queue_full_does_not_raise(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue(maxsize=1)
        c = self._make(q)
        c._dispatch(_json(_trade_payload()), TRADE_TIME_NS)
        c._dispatch(_json(_trade_payload()), TRADE_TIME_NS)  # queue full — no raise

    def test_bad_payload_fields_do_not_raise(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make(q)
        bad = {"e": "trade", "s": "BTCUSDT", "p": "-1", "q": "1", "T": NOW_MS, "t": 1, "m": False}
        c._dispatch(json.dumps(bad), TRADE_TIME_NS)
        assert q.qsize() == 0  # negative price → Tick validation fails → dropped


# ---------------------------------------------------------------------------
# BinanceConnector — async run() behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestBinanceConnectorRun:
    async def test_ticks_delivered_to_queue(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = FakeWebSocket([
            _json(_trade_payload(price="50000.00", trade_id=1)),
            _json(_trade_payload(price="50001.00", trade_id=2)),
        ])
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        with patch(_PATCH_WS, make_connect(ws)):
            with patch(_PATCH_SLEEP, new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = asyncio.CancelledError
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert q.qsize() == 2
        assert q.get_nowait().price == Decimal("50000.00")
        assert q.get_nowait().price == Decimal("50001.00")

    async def test_combined_stream_ticks_delivered(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = FakeWebSocket([
            _combined_json("btcusdt@trade", _trade_payload(symbol="BTCUSDT", trade_id=10)),
            _combined_json("ethusdt@trade", _trade_payload(symbol="ETHUSDT", trade_id=11)),
        ])
        connector = BinanceConnector(["btcusdt", "ethusdt"], q, ws_base="ws://fake")

        with patch(_PATCH_WS, make_connect(ws)):
            with patch(_PATCH_SLEEP, new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = asyncio.CancelledError
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert q.qsize() == 2

    async def test_reconnects_after_connection_error(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        attempt = 0

        def _side_effect(uri: str) -> Any:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise OSError("connection refused")
            return make_connect(FakeWebSocket([_json(_trade_payload())]))(uri)

        sleep_calls: list[float] = []

        async def _fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            if len(sleep_calls) >= 2:
                raise asyncio.CancelledError

        with patch(_PATCH_WS, side_effect=_side_effect):
            with patch(_PATCH_SLEEP, side_effect=_fake_sleep):
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert len(sleep_calls) >= 1
        assert sleep_calls[0] <= connector._max_backoff

    async def test_backoff_doubles_on_repeated_errors(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        connector = BinanceConnector(
            ["btcusdt"], q,
            ws_base="ws://fake",
            initial_backoff=1.0,
            max_backoff=30.0,
            jitter_factor=0.0,
        )

        def _always_fail(uri: str) -> Any:
            raise OSError("refused")

        sleep_calls: list[float] = []

        async def _fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            if len(sleep_calls) >= 6:
                raise asyncio.CancelledError

        with patch(_PATCH_WS, side_effect=_always_fail):
            with patch(_PATCH_SLEEP, side_effect=_fake_sleep):
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        expected = [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
        for actual, exp in zip(sleep_calls, expected, strict=False):
            assert actual == pytest.approx(exp, abs=1e-9)

    async def test_clean_shutdown_on_cancellation(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = HangingWebSocket()
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        with patch(_PATCH_WS, make_connect(ws)):
            task = asyncio.create_task(connector.run())
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_cancellation_during_backoff_sleep(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")
        real_sleep = asyncio.sleep

        async def _slow_sleep(delay: float) -> None:
            await real_sleep(10_000)

        with patch(_PATCH_WS, side_effect=lambda uri: (_ for _ in ()).throw(OSError("down"))):
            with patch(_PATCH_SLEEP, side_effect=_slow_sleep):
                task = asyncio.create_task(connector.run())
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

    async def test_websocket_exception_triggers_reconnect(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        attempt = 0

        def _side_effect(uri: str) -> Any:
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise websockets.exceptions.ConnectionClosedError(None, None)
            return make_connect(FakeWebSocket([_json(_trade_payload())]))(uri)

        sleep_calls: list[float] = []

        async def _fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            if len(sleep_calls) >= 2:
                raise asyncio.CancelledError

        with patch(_PATCH_WS, side_effect=_side_effect):
            with patch(_PATCH_SLEEP, side_effect=_fake_sleep):
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert len(sleep_calls) >= 1

    async def test_received_ns_stamped_at_message_receipt(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = FakeWebSocket([_json(_trade_payload())])
        connector = BinanceConnector(["btcusdt"], q, ws_base="ws://fake")

        before = time.time_ns()
        with patch(_PATCH_WS, make_connect(ws)):
            with patch(_PATCH_SLEEP, new_callable=AsyncMock) as s:
                s.side_effect = asyncio.CancelledError
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()
        after = time.time_ns()

        tick = q.get_nowait()
        assert before <= tick.received_ns <= after

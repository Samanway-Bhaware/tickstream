"""Tests for src/tickstream/connectors/coinbase.py.

Recorded message samples
------------------------
All raw JSON strings in this file are representative samples that match the
exact schema documented in the Coinbase Advanced Trade WebSocket reference.
They are used as-is in parsing tests so that the tests act as a living
contract against the real wire format.
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from tests._helpers import FakeWebSocket, HangingWebSocket, make_connect
from tickstream.connectors.coinbase import (
    CoinbaseConnector,
    _iso_to_ns,
    _parse_trade,
    _to_product_id,
)
from tickstream.models import Tick

# ---------------------------------------------------------------------------
# Time constants
# ---------------------------------------------------------------------------

# A fixed recorded timestamp for deterministic tests.
_RECORDED_TS = "2023-02-09T20:19:35.39625135Z"
# Expected nanosecond value (verified by hand):
#   calendar.timegm(datetime(2023,2,9,20,19,35).timetuple()) = 1675973975
#   fractional: "39625135" → 396251350 ns
_RECORDED_TS_NS = 1_675_973_975 * 1_000_000_000 + 396_251_350

# Use "now" for lifecycle tests where the timestamp must pass Tick validation.
NOW_NS = time.time_ns()
NOW_MS = NOW_NS // 1_000_000
# Round-trip through ms so received_ns ≥ timestamp_ns (no skew error).
_LIVE_TS_NS = NOW_MS * 1_000_000
_LIVE_TS_ISO = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(NOW_MS // 1_000)) + ".000000000Z"

# Patch targets — lifecycle tests go through the base module.
_PATCH_WS = "tickstream.connectors.base.websockets.connect"
_PATCH_SLEEP = "tickstream.connectors.base.asyncio.sleep"

# ---------------------------------------------------------------------------
# Recorded message samples
# ---------------------------------------------------------------------------

# Subscription confirmation sent by Coinbase immediately after subscribe.
_SUBSCRIPTIONS_MSG: str = json.dumps(
    {
        "channel": "subscriptions",
        "client_id": "",
        "timestamp": _RECORDED_TS,
        "sequence_num": 0,
        "events": [
            {
                "subscriptions": {
                    "market_trades": ["BTC-USD", "ETH-USD"],
                }
            }
        ],
    }
)

# Snapshot: initial batch of recent trades sent when the subscription opens.
_SNAPSHOT_MSG: str = json.dumps(
    {
        "channel": "market_trades",
        "client_id": "",
        "timestamp": _LIVE_TS_ISO,
        "sequence_num": 0,
        "events": [
            {
                "type": "snapshot",
                "trades": [
                    {
                        "trade_id": "snap-1",
                        "product_id": "BTC-USD",
                        "price": "21500.00",
                        "size": "0.10000",
                        "side": "BUY",
                        "time": _LIVE_TS_ISO,
                    },
                    {
                        "trade_id": "snap-2",
                        "product_id": "BTC-USD",
                        "price": "21499.50",
                        "size": "0.05000",
                        "side": "SELL",
                        "time": _LIVE_TS_ISO,
                    },
                ],
            }
        ],
    }
)

# Update: single live trade.
_UPDATE_MSG: str = json.dumps(
    {
        "channel": "market_trades",
        "client_id": "",
        "timestamp": _LIVE_TS_ISO,
        "sequence_num": 1,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "upd-1",
                        "product_id": "ETH-USD",
                        "price": "1620.75",
                        "size": "1.50000",
                        "side": "BUY",
                        "time": _LIVE_TS_ISO,
                    }
                ],
            }
        ],
    }
)

# Multi-trade update (two trades in one event batch).
_MULTI_TRADE_MSG: str = json.dumps(
    {
        "channel": "market_trades",
        "client_id": "",
        "timestamp": _LIVE_TS_ISO,
        "sequence_num": 2,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "mt-1",
                        "product_id": "BTC-USD",
                        "price": "21600.00",
                        "size": "0.01",
                        "side": "BUY",
                        "time": _LIVE_TS_ISO,
                    },
                    {
                        "trade_id": "mt-2",
                        "product_id": "BTC-USD",
                        "price": "21599.00",
                        "size": "0.02",
                        "side": "SELL",
                        "time": _LIVE_TS_ISO,
                    },
                ],
            }
        ],
    }
)


# ---------------------------------------------------------------------------
# _to_product_id
# ---------------------------------------------------------------------------


class TestToProductId:
    def test_lowercase_with_hyphen(self) -> None:
        assert _to_product_id("btc-usd") == "BTC-USD"

    def test_already_uppercase(self) -> None:
        assert _to_product_id("ETH-USD") == "ETH-USD"

    def test_mixed_case(self) -> None:
        assert _to_product_id("Sol-Usd") == "SOL-USD"


# ---------------------------------------------------------------------------
# _iso_to_ns
# ---------------------------------------------------------------------------


class TestIsoToNs:
    def test_recorded_sample(self) -> None:
        assert _iso_to_ns(_RECORDED_TS) == _RECORDED_TS_NS

    def test_whole_seconds(self) -> None:
        # "2023-01-01T00:00:00Z" → epoch 1672531200 s
        ns = _iso_to_ns("2023-01-01T00:00:00Z")
        assert ns == 1_672_531_200 * 1_000_000_000

    def test_fractional_truncated_to_9_digits(self) -> None:
        # "…1.1234567890Z" — 10 fractional digits → truncate to 9
        ns = _iso_to_ns("2023-01-01T00:00:01.1234567890Z")
        assert ns == 1_672_531_201 * 1_000_000_000 + 123_456_789

    def test_fractional_padded_to_9_digits(self) -> None:
        # "…1.5Z" — 1 fractional digit → pad to "500000000"
        ns = _iso_to_ns("2023-01-01T00:00:01.5Z")
        assert ns == 1_672_531_201 * 1_000_000_000 + 500_000_000


# ---------------------------------------------------------------------------
# _parse_trade (unit)
# ---------------------------------------------------------------------------


class TestParseTrade:
    def _trade_dict(self, **kw: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "trade_id": "42",
            "product_id": "BTC-USD",
            "price": "21720.50",
            "size": "0.00413",
            "side": "BUY",
            "time": _LIVE_TS_ISO,
        }
        base.update(kw)
        return base

    def test_exchange_is_coinbase(self) -> None:
        assert _parse_trade(self._trade_dict(), _LIVE_TS_NS).exchange == "coinbase"

    def test_symbol_preserved(self) -> None:
        assert _parse_trade(self._trade_dict(product_id="ETH-USD"), _LIVE_TS_NS).symbol == "ETH-USD"

    def test_buy_side(self) -> None:
        assert _parse_trade(self._trade_dict(side="BUY"), _LIVE_TS_NS).side == "buy"

    def test_sell_side(self) -> None:
        assert _parse_trade(self._trade_dict(side="SELL"), _LIVE_TS_NS).side == "sell"

    def test_price_decimal_exact(self) -> None:
        tick = _parse_trade(self._trade_dict(price="21720.12345678"), _LIVE_TS_NS)
        assert tick.price == Decimal("21720.12345678")

    def test_size_decimal_exact(self) -> None:
        tick = _parse_trade(self._trade_dict(size="0.00000001"), _LIVE_TS_NS)
        assert tick.size == Decimal("0.00000001")

    def test_trade_id_stringified(self) -> None:
        assert _parse_trade(self._trade_dict(trade_id="abc-123"), _LIVE_TS_NS).trade_id == "abc-123"

    def test_timestamp_parsed_from_iso(self) -> None:
        tick = _parse_trade(self._trade_dict(time=_LIVE_TS_ISO), _LIVE_TS_NS)
        assert tick.timestamp_ns == _LIVE_TS_NS

    def test_received_ns_stored_verbatim(self) -> None:
        assert _parse_trade(self._trade_dict(), _LIVE_TS_NS).received_ns == _LIVE_TS_NS

    def test_unknown_side_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown side"):
            _parse_trade(self._trade_dict(side="LONG"), _LIVE_TS_NS)

    def test_returns_tick_instance(self) -> None:
        assert isinstance(_parse_trade(self._trade_dict(), _LIVE_TS_NS), Tick)


# ---------------------------------------------------------------------------
# CoinbaseConnector — constructor / URL
# ---------------------------------------------------------------------------


class TestCoinbaseConnectorInit:
    def test_symbols_uppercased(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = CoinbaseConnector(["btc-usd", "ETH-USD"], q)
        assert c._symbols == ["BTC-USD", "ETH-USD"]

    def test_empty_symbols_raises(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        with pytest.raises(ValueError, match="At least one symbol"):
            CoinbaseConnector([], q)

    def test_default_url(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = CoinbaseConnector(["BTC-USD"], q)
        assert c._url() == "wss://advanced-trade-ws.coinbase.com"

    def test_custom_ws_url(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = CoinbaseConnector(["BTC-USD"], q, ws_url="ws://localhost:9999")
        assert c._url() == "ws://localhost:9999"

    def test_subscribe_message_structure(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = CoinbaseConnector(["BTC-USD", "ETH-USD"], q)
        msg = c._subscribe_message(c._symbols)
        assert msg is not None
        parsed = json.loads(msg)
        assert parsed["type"] == "subscribe"
        assert parsed["channel"] == "market_trades"
        assert parsed["product_ids"] == ["BTC-USD", "ETH-USD"]


# ---------------------------------------------------------------------------
# CoinbaseConnector — _dispatch with recorded message samples
# ---------------------------------------------------------------------------


class TestDispatch:
    def _make(self, q: asyncio.Queue[Tick] | None = None) -> CoinbaseConnector:
        return CoinbaseConnector(["BTC-USD", "ETH-USD"], q or asyncio.Queue(),
                                 ws_url="ws://fake")

    def test_subscriptions_confirmation_ignored(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make(q)
        c._dispatch(_SUBSCRIPTIONS_MSG, _LIVE_TS_NS)
        assert q.qsize() == 0

    def test_snapshot_yields_all_trades(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make(q)
        c._dispatch(_SNAPSHOT_MSG, _LIVE_TS_NS)
        assert q.qsize() == 2
        t1 = q.get_nowait()
        t2 = q.get_nowait()
        assert t1.side == "buy"
        assert t2.side == "sell"
        assert t1.price == Decimal("21500.00")

    def test_update_yields_one_trade(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make(q)
        c._dispatch(_UPDATE_MSG, _LIVE_TS_NS)
        assert q.qsize() == 1
        tick = q.get_nowait()
        assert tick.symbol == "ETH-USD"
        assert tick.exchange == "coinbase"

    def test_multi_trade_update_yields_all(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        c = self._make(q)
        c._dispatch(_MULTI_TRADE_MSG, _LIVE_TS_NS)
        assert q.qsize() == 2

    def test_invalid_json_does_not_raise(self) -> None:
        c = self._make()
        c._dispatch("{{{{not-json", _LIVE_TS_NS)

    def test_queue_full_does_not_raise(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue(maxsize=1)
        c = self._make(q)
        c._dispatch(_UPDATE_MSG, _LIVE_TS_NS)
        c._dispatch(_UPDATE_MSG, _LIVE_TS_NS)  # queue full — should not raise


# ---------------------------------------------------------------------------
# CoinbaseConnector — subscribe message sent after connect
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestCoinbaseSubscription:
    async def test_subscribe_frame_sent_on_connect(self) -> None:
        """The connector must send the subscription frame immediately after the
        WebSocket handshake completes — before any messages are received."""
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = FakeWebSocket([_SUBSCRIPTIONS_MSG, _UPDATE_MSG])
        connector = CoinbaseConnector(["BTC-USD", "ETH-USD"], q, ws_url="ws://fake")

        with patch(_PATCH_WS, make_connect(ws)):
            with patch(_PATCH_SLEEP, new_callable=AsyncMock) as s:
                s.side_effect = asyncio.CancelledError
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert len(ws.sent) == 1
        sub = json.loads(ws.sent[0])
        assert sub["type"] == "subscribe"
        assert sub["channel"] == "market_trades"
        assert "BTC-USD" in sub["product_ids"]
        assert "ETH-USD" in sub["product_ids"]

    async def test_ticks_from_update_reach_queue(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = FakeWebSocket([_SUBSCRIPTIONS_MSG, _UPDATE_MSG])
        connector = CoinbaseConnector(["ETH-USD"], q, ws_url="ws://fake")

        with patch(_PATCH_WS, make_connect(ws)):
            with patch(_PATCH_SLEEP, new_callable=AsyncMock) as s:
                s.side_effect = asyncio.CancelledError
                with pytest.raises(asyncio.CancelledError):
                    await connector.run()

        assert q.qsize() == 1
        tick = q.get_nowait()
        assert tick.symbol == "ETH-USD"
        assert tick.price == Decimal("1620.75")

    async def test_clean_shutdown_on_cancellation(self) -> None:
        q: asyncio.Queue[Tick] = asyncio.Queue()
        ws = HangingWebSocket()
        connector = CoinbaseConnector(["BTC-USD"], q, ws_url="ws://fake")

        with patch(_PATCH_WS, make_connect(ws)):
            task = asyncio.create_task(connector.run())
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

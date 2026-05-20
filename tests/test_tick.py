"""Tests for the Tick domain model."""

from __future__ import annotations

import time
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tickstream.models import Tick

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW_NS = time.time_ns()
EXCHANGE_NS = NOW_NS - 1_000_000  # 1 ms before received


def _valid_tick(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "exchange": "binance",
        "symbol": "BTC-USDT",
        "price": "67432.50",
        "size": "0.01234",
        "side": "buy",
        "timestamp_ns": EXCHANGE_NS,
        "received_ns": NOW_NS,
        "trade_id": "abc123",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Happy-path
# ---------------------------------------------------------------------------


def test_valid_tick_construction() -> None:
    tick = Tick(**_valid_tick())  # type: ignore[arg-type]
    assert tick.exchange == "binance"
    assert tick.symbol == "BTC-USDT"
    assert tick.price == Decimal("67432.50")
    assert tick.size == Decimal("0.01234")
    assert tick.side == "buy"
    assert tick.trade_id == "abc123"


def test_decimal_precision_preserved() -> None:
    """Ensure no floating-point rounding occurs."""
    tick = Tick(**_valid_tick(price="0.00000001", size="123456789.123456789"))  # type: ignore[arg-type]
    assert str(tick.price) == "0.00000001"
    assert str(tick.size) == "123456789.123456789"


def test_integer_price_accepted() -> None:
    tick = Tick(**_valid_tick(price=50000, size=1))  # type: ignore[arg-type]
    assert tick.price == Decimal("50000")


def test_sell_side_accepted() -> None:
    tick = Tick(**_valid_tick(side="sell"))  # type: ignore[arg-type]
    assert tick.side == "sell"


def test_tick_is_immutable() -> None:
    tick = Tick(**_valid_tick())  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValidationError)):
        tick.price = Decimal("1")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Price / size validation
# ---------------------------------------------------------------------------


def test_float_price_rejected() -> None:
    with pytest.raises(ValidationError, match="float is not allowed"):
        Tick(**_valid_tick(price=67432.5))  # type: ignore[arg-type]


def test_float_size_rejected() -> None:
    with pytest.raises(ValidationError, match="float is not allowed"):
        Tick(**_valid_tick(size=0.01))  # type: ignore[arg-type]


def test_zero_price_rejected() -> None:
    with pytest.raises(ValidationError, match="price must be > 0"):
        Tick(**_valid_tick(price="0"))  # type: ignore[arg-type]


def test_negative_price_rejected() -> None:
    with pytest.raises(ValidationError, match="price must be > 0"):
        Tick(**_valid_tick(price="-1"))  # type: ignore[arg-type]


def test_zero_size_rejected() -> None:
    with pytest.raises(ValidationError, match="size must be > 0"):
        Tick(**_valid_tick(size="0"))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Side validation
# ---------------------------------------------------------------------------


def test_invalid_side_rejected() -> None:
    with pytest.raises(ValidationError):
        Tick(**_valid_tick(side="long"))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Timestamp validation
# ---------------------------------------------------------------------------


def test_timestamp_before_2010_rejected() -> None:
    ancient_ns = 1_000_000_000 * 1_000_000_000  # year ~2001
    with pytest.raises(ValidationError, match="before 2010"):
        Tick(**_valid_tick(timestamp_ns=ancient_ns, received_ns=NOW_NS))  # type: ignore[arg-type]


def test_timestamp_far_future_rejected() -> None:
    far_future_ns = NOW_NS + 20 * 365 * 24 * 3_600 * 1_000_000_000
    with pytest.raises(ValidationError, match="future"):
        Tick(**_valid_tick(timestamp_ns=far_future_ns, received_ns=far_future_ns))  # type: ignore[arg-type]


def test_received_before_exchange_by_more_than_1s_rejected() -> None:
    with pytest.raises(ValidationError, match="clock sync"):
        Tick(**_valid_tick(timestamp_ns=NOW_NS, received_ns=NOW_NS - 2_000_000_000))  # type: ignore[arg-type]


def test_minor_clock_skew_within_tolerance_accepted() -> None:
    """Up to 1 s of received-before-exchange skew is allowed."""
    skew_ns = 500_000_000  # 0.5 s
    tick = Tick(**_valid_tick(timestamp_ns=NOW_NS, received_ns=NOW_NS - skew_ns))  # type: ignore[arg-type]
    assert tick.received_ns < tick.timestamp_ns


# ---------------------------------------------------------------------------
# Required fields
# ---------------------------------------------------------------------------


def test_missing_exchange_rejected() -> None:
    data = _valid_tick()
    del data["exchange"]
    with pytest.raises(ValidationError):
        Tick(**data)  # type: ignore[arg-type]


def test_empty_trade_id_rejected() -> None:
    with pytest.raises(ValidationError):
        Tick(**_valid_tick(trade_id=""))  # type: ignore[arg-type]

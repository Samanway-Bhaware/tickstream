"""Test factories for generating synthetic Tick objects efficiently."""

from __future__ import annotations

import time
import uuid
from calendar import timegm
from datetime import date as date_type
from decimal import Decimal
from time import strptime

from tickstream.models import Tick


def make_tick(
    *,
    exchange: str = "binance",
    symbol: str = "BTCUSDT",
    price: str | Decimal = "50000.123456789",
    size: str | Decimal = "0.000001234",
    side: str = "buy",
    timestamp_ns: int | None = None,
    received_ns: int | None = None,
    trade_id: str | None = None,
) -> Tick:
    """Create a single Tick model bypassing expensive validations for fast tests."""
    ts = timestamp_ns if timestamp_ns is not None else int(time.time_ns())
    p = Decimal(str(price)) if not isinstance(price, Decimal) else price
    s = Decimal(str(size)) if not isinstance(size, Decimal) else size
    return Tick.model_construct(
        exchange=exchange,
        symbol=symbol,
        price=p,
        size=s,
        side=side,
        timestamp_ns=ts,
        received_ns=received_ns if received_ns is not None else ts,
        trade_id=trade_id or str(uuid.uuid4()),
    )


def _date_to_base_ns(date: str) -> int:
    """Convert a 'YYYY-MM-DD' string to nanoseconds at midnight UTC (deterministic)."""
    t = strptime(date, "%Y-%m-%d")
    epoch_s = timegm(t)  # UTC, no local-timezone conversion
    return epoch_s * 1_000_000_000


def make_ticks(
    n: int,
    *,
    exchange: str = "binance",
    symbol: str = "BTCUSDT",
    date: str = "2025-01-15",
    start_price: str = "50000.00",
    **overrides: object,
) -> list[Tick]:
    """Generate *n* Tick objects extremely quickly using model_construct.

    Timestamps are derived deterministically from *date* so tests are
    reproducible without depending on wall-clock time.
    """
    base_ns = _date_to_base_ns(date)
    base_price = Decimal(start_price)
    # Small deterministic price variation: cycle through 100 offsets.
    return [
        Tick.model_construct(
            exchange=exchange,
            symbol=symbol,
            price=base_price + Decimal(i % 100),
            size=Decimal("0.001"),
            side="buy" if i % 2 == 0 else "sell",
            timestamp_ns=base_ns + i * 1_000_000,
            received_ns=base_ns + i * 1_000_000,
            trade_id=f"t{i}",
            **overrides,
        )
        for i in range(n)
    ]


def make_validated_ticks(n: int, **kwargs: object) -> list[Tick]:
    """Generate *n* Tick objects through the real Tick(...) constructor.

    Slower than :func:`make_ticks` but exercises Pydantic validation.
    Use only when you need to verify validation paths.
    """
    base_ns = _date_to_base_ns(str(kwargs.pop("date", "2025-01-15")))
    exchange = str(kwargs.pop("exchange", "binance"))
    symbol = str(kwargs.pop("symbol", "BTCUSDT"))
    return [
        Tick(
            exchange=exchange,
            symbol=symbol,
            price=str(50000 + i % 100),
            size="0.001",
            side="buy" if i % 2 == 0 else "sell",
            timestamp_ns=base_ns + i * 1_000_000,
            received_ns=base_ns + i * 1_000_000,
            trade_id=f"tv{i}",
            **kwargs,  # type: ignore[arg-type]
        )
        for i in range(n)
    ]

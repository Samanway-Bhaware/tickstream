"""Core domain models for tickstream."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# Earliest plausible exchange timestamp: 2010-01-01 00:00:00 UTC in nanoseconds.
# Bitcoin launched 3 Jan 2009; no major exchange existed before 2010.
_MIN_TS_NS: int = 1_262_304_000 * 1_000_000_000
# Latest plausible timestamp: 10 years in the future from a fixed reference.
# We compare against wall-clock at validation time instead (see validator below).
_MAX_FUTURE_NS: int = 10 * 365 * 24 * 3_600 * 1_000_000_000


class Tick(BaseModel):
    """A single trade tick from a cryptocurrency exchange.

    All monetary values use ``Decimal`` to avoid floating-point precision loss.
    Both timestamps are in nanoseconds:

    - ``timestamp_ns``: the exchange-reported trade time.
    - ``received_ns``: our wall-clock time when the message arrived.
    """

    exchange: str = Field(..., min_length=1, description="Exchange identifier, e.g. 'binance'")
    symbol: str = Field(..., min_length=1, description="Trading pair, e.g. 'BTC-USDT'")
    price: Decimal = Field(..., description="Trade price (exact decimal)")
    size: Decimal = Field(..., description="Trade quantity (exact decimal)")
    side: Literal["buy", "sell"] = Field(..., description="Aggressor side")
    timestamp_ns: int = Field(..., description="Exchange-reported trade time (ns since epoch)")
    received_ns: int = Field(..., description="Local wall-clock receipt time (ns since epoch)")
    trade_id: str = Field(..., min_length=1, description="Exchange-assigned trade identifier")

    model_config = {"frozen": True}

    # ------------------------------------------------------------------
    # Field-level validators
    # ------------------------------------------------------------------

    @field_validator("price", "size", mode="before")
    @classmethod
    def coerce_to_decimal(cls, v: object) -> Decimal:
        """Accept str/int/Decimal; reject float to preserve precision."""
        if isinstance(v, float):
            raise ValueError(
                "float is not allowed for price/size — use a string or Decimal "
                f"(got {v!r})"
            )
        try:
            return Decimal(str(v))  # type: ignore[arg-type]
        except Exception as exc:
            raise ValueError(f"Cannot convert {v!r} to Decimal: {exc}") from exc

    @field_validator("price")
    @classmethod
    def price_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError(f"price must be > 0, got {v}")
        return v

    @field_validator("size")
    @classmethod
    def size_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError(f"size must be > 0, got {v}")
        return v

    @field_validator("timestamp_ns", "received_ns")
    @classmethod
    def timestamp_in_range(cls, v: int) -> int:
        now_ns = time.time_ns()
        if v < _MIN_TS_NS:
            raise ValueError(
                f"timestamp {v} is before 2010-01-01 — likely wrong unit or corrupted data"
            )
        if v > now_ns + _MAX_FUTURE_NS:
            raise ValueError(
                f"timestamp {v} is more than 10 years in the future — likely corrupted data"
            )
        return v

    # ------------------------------------------------------------------
    # Cross-field validator
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def received_not_before_exchange(self) -> "Tick":
        """received_ns should not be earlier than timestamp_ns by more than 1 s
        (to allow for minor clock skew / clock differences between systems)."""
        tolerance_ns = 1_000_000_000  # 1 second
        if self.received_ns < self.timestamp_ns - tolerance_ns:
            raise ValueError(
                f"received_ns ({self.received_ns}) is more than 1 s before "
                f"timestamp_ns ({self.timestamp_ns}) — check clock sync"
            )
        return self

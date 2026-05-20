"""Comprehensive tests for TickStore query API.

Fixture generates ~1 000 002 ticks (333 334 per symbol × 3 symbols) spanning
two UTC days via the Phase 4 ParquetWriter, then exercises every TickStore
method with hand-computed expected values.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from tickstream.models import Tick
from tickstream.query.store import TickStore, _parse_interval, _to_ns
from tickstream.storage.parquet_writer import (
    _partition_dir,
    _partition_key,
    _write_batch_sync,
)

# ---------------------------------------------------------------------------
# Fixture constants
# ---------------------------------------------------------------------------

_EXCHANGE = "binance"
_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# 2024-05-20 00:00:00 UTC in nanoseconds.
# 2024-05-20 is exactly on a 1-minute and 1-hour boundary.
_DAY1_START_NS: int = 1_716_163_200_000_000_000

# 1 day = 86_400 seconds = 86_400_000_000_000 ns
_SPAN_NS: int = 2 * 86_400 * 1_000_000_000  # 2 days in ns
_ONE_HOUR_NS: int = 3_600 * 1_000_000_000   # 1 hour in ns

_N_PER_SYMBOL: int = 333_334               # ~1 M total across 3 symbols
_STEP_NS: int = _SPAN_NS // _N_PER_SYMBOL  # ≈ 518 ms between ticks

# Price base per symbol; price_i = base + (i % 100), size = 0.1 throughout.
_PRICE_BASES: dict[str, Decimal] = {
    "BTCUSDT": Decimal("1000"),
    "ETHUSDT": Decimal("2000"),
    "SOLUSDT": Decimal("50"),
}
_SIZE = Decimal("0.1")

# ---------------------------------------------------------------------------
# Tick generation & writing helpers
# ---------------------------------------------------------------------------


def _generate_ticks() -> list[Tick]:
    """Build 333 334 ticks per symbol × 3 symbols = ~1 000 002 ticks."""
    ticks: list[Tick] = []
    for symbol in _SYMBOLS:
        price_base = _PRICE_BASES[symbol]
        for i in range(_N_PER_SYMBOL):
            ts_ns = _DAY1_START_NS + i * _STEP_NS
            price = price_base + Decimal(i % 100)
            ticks.append(
                Tick.model_construct(
                    exchange=_EXCHANGE,
                    symbol=symbol,
                    price=price,
                    size=_SIZE,
                    side="buy" if i % 2 == 0 else "sell",
                    timestamp_ns=ts_ns,
                    received_ns=ts_ns + 1_000_000,  # 1 ms after exchange ts
                    trade_id=f"{symbol}-{i}",
                )
            )
    return ticks


def _write_ticks(ticks: list[Tick], output_dir: Path) -> None:
    """Write ticks to partitioned Parquet using the Phase 4 writer internals.

    Uses the same ``_partition_key``, ``_partition_dir``, and
    ``_write_batch_sync`` helpers from ``parquet_writer`` so the on-disk
    layout is identical to what the async ``ParquetWriter`` produces.
    Running synchronously avoids the Python 3.11 ``asyncio.wait_for`` /
    task-cancellation interaction that prevents ``_shutdown_flush`` from
    completing its executor awaits.
    """
    from collections import defaultdict

    batches: dict[tuple[str, str, str], list[Tick]] = defaultdict(list)
    for tick in ticks:
        batches[_partition_key(tick)].append(tick)

    for (exchange, symbol, date), batch in batches.items():
        dir_path = _partition_dir(output_dir, exchange, symbol, date)
        _write_batch_sync(batch, dir_path)


# ---------------------------------------------------------------------------
# Shared module-scoped fixture (written once, shared across all tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def store(tmp_path_factory: pytest.TempPathFactory) -> TickStore:  # type: ignore[misc]
    """Module-scoped TickStore backed by ~1 M synthetic ticks."""
    data_dir = tmp_path_factory.mktemp("tick_data")
    _write_ticks(_generate_ticks(), data_dir)
    ts = TickStore(data_dir)
    yield ts  # type: ignore[misc]
    ts.close()


# ---------------------------------------------------------------------------
# _to_ns — time-input normalisation
# ---------------------------------------------------------------------------


class TestToNs:
    def test_int_passthrough(self) -> None:
        assert _to_ns(1_716_163_200_000_000_000) == 1_716_163_200_000_000_000

    def test_datetime_aware_utc(self) -> None:
        dt = datetime(2024, 5, 20, 0, 0, 0, tzinfo=timezone.utc)
        assert _to_ns(dt) == _DAY1_START_NS

    def test_datetime_naive_assumed_utc(self) -> None:
        dt = datetime(2024, 5, 20, 0, 0, 0)  # naive
        assert _to_ns(dt) == _DAY1_START_NS

    def test_iso_string_with_z(self) -> None:
        assert _to_ns("2024-05-20T00:00:00Z") == _DAY1_START_NS

    def test_iso_string_with_plus_offset(self) -> None:
        assert _to_ns("2024-05-20T00:00:00+00:00") == _DAY1_START_NS


# ---------------------------------------------------------------------------
# _parse_interval — interval string → nanoseconds
# ---------------------------------------------------------------------------


class TestParseInterval:
    def test_seconds(self) -> None:
        assert _parse_interval("1s") == 1_000_000_000

    def test_one_minute(self) -> None:
        assert _parse_interval("1m") == 60_000_000_000

    def test_five_minutes(self) -> None:
        assert _parse_interval("5m") == 300_000_000_000

    def test_one_hour(self) -> None:
        assert _parse_interval("1h") == 3_600_000_000_000

    def test_one_day(self) -> None:
        assert _parse_interval("1d") == 86_400_000_000_000

    def test_multi_digit(self) -> None:
        assert _parse_interval("15m") == 15 * 60_000_000_000

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid interval"):
            _parse_interval("invalid")

    def test_no_unit_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid interval"):
            _parse_interval("60")


# ---------------------------------------------------------------------------
# symbols()
# ---------------------------------------------------------------------------


class TestSymbols:
    def test_all_three_symbols_present(self, store: TickStore) -> None:
        syms = store.symbols()
        for sym in _SYMBOLS:
            assert f"{_EXCHANGE}:{sym}" in syms

    def test_returns_sorted_list(self, store: TickStore) -> None:
        syms = store.symbols()
        assert syms == sorted(syms)

    def test_no_duplicates(self, store: TickStore) -> None:
        syms = store.symbols()
        assert len(syms) == len(set(syms))

    def test_count_equals_three(self, store: TickStore) -> None:
        assert len(store.symbols()) == 3


# ---------------------------------------------------------------------------
# trades()
# ---------------------------------------------------------------------------


class TestTrades:
    def test_returns_polars_dataframe(self, store: TickStore) -> None:
        df = store.trades("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _STEP_NS * 10)
        assert isinstance(df, pl.DataFrame)

    def test_ordered_by_timestamp(self, store: TickStore) -> None:
        df = store.trades("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _STEP_NS * 200)
        ts = df["timestamp_ns"].to_list()
        assert ts == sorted(ts)

    def test_exact_count_first_100_ticks(self, store: TickStore) -> None:
        # tick i=0..99 are at [DAY1_START_NS, DAY1_START_NS + 100*STEP_NS)
        end_ns = _DAY1_START_NS + 100 * _STEP_NS
        df = store.trades("BTCUSDT", _DAY1_START_NS, end_ns)
        assert len(df) == 100

    def test_total_ticks_per_symbol(self, store: TickStore) -> None:
        end_ns = _DAY1_START_NS + _SPAN_NS
        df = store.trades("BTCUSDT", _DAY1_START_NS, end_ns)
        assert len(df) == _N_PER_SYMBOL

    def test_range_is_half_open(self, store: TickStore) -> None:
        """Tick AT end_ns must not appear in results."""
        tick_100_ns = _DAY1_START_NS + 100 * _STEP_NS
        df = store.trades("BTCUSDT", _DAY1_START_NS, tick_100_ns)
        assert int(df["timestamp_ns"].max()) < tick_100_ns  # type: ignore[arg-type]

    def test_exchange_filter_keeps_matching(self, store: TickStore) -> None:
        end_ns = _DAY1_START_NS + 100 * _STEP_NS
        df = store.trades("BTCUSDT", _DAY1_START_NS, end_ns, exchange=_EXCHANGE)
        assert len(df) == 100

    def test_exchange_filter_rejects_other_exchange(self, store: TickStore) -> None:
        end_ns = _DAY1_START_NS + 100 * _STEP_NS
        df = store.trades("BTCUSDT", _DAY1_START_NS, end_ns, exchange="coinbase")
        assert len(df) == 0

    def test_empty_range_returns_empty_df(self, store: TickStore) -> None:
        df = store.trades("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS)
        assert len(df) == 0

    def test_required_columns_present(self, store: TickStore) -> None:
        df = store.trades("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _STEP_NS * 5)
        for col in ("exchange", "symbol", "side", "timestamp_ns", "received_ns", "trade_id"):
            assert col in df.columns


# ---------------------------------------------------------------------------
# vwap()
# ---------------------------------------------------------------------------


class TestVwap:
    def test_hand_computed_first_100_ticks(self, store: TickStore) -> None:
        """VWAP(BTCUSDT, first 100 ticks) should be 1049.5.

        Prices are 1000, 1001, ..., 1099 (uniform size=0.1):
            VWAP = mean(prices) = 1000 + mean(0..99) = 1000 + 49.5 = 1049.5
        """
        end_ns = _DAY1_START_NS + 100 * _STEP_NS
        vwap = store.vwap("BTCUSDT", _DAY1_START_NS, end_ns)
        assert abs(vwap - Decimal("1049.5")) < Decimal("0.0001")

    def test_returns_decimal(self, store: TickStore) -> None:
        end_ns = _DAY1_START_NS + 10 * _STEP_NS
        result = store.vwap("BTCUSDT", _DAY1_START_NS, end_ns)
        assert isinstance(result, Decimal)

    def test_exchange_filter_gives_same_result(self, store: TickStore) -> None:
        end_ns = _DAY1_START_NS + 100 * _STEP_NS
        v1 = store.vwap("BTCUSDT", _DAY1_START_NS, end_ns)
        v2 = store.vwap("BTCUSDT", _DAY1_START_NS, end_ns, exchange=_EXCHANGE)
        assert abs(v1 - v2) < Decimal("0.0001")

    def test_eth_vwap_different_from_btc(self, store: TickStore) -> None:
        end_ns = _DAY1_START_NS + 100 * _STEP_NS
        vwap_btc = store.vwap("BTCUSDT", _DAY1_START_NS, end_ns)
        vwap_eth = store.vwap("ETHUSDT", _DAY1_START_NS, end_ns)
        # ETH base=2000, BTC base=1000 → different VWAPs
        assert vwap_eth != vwap_btc

    def test_empty_range_raises_value_error(self, store: TickStore) -> None:
        with pytest.raises(ValueError, match="No trades found"):
            store.vwap("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS)

    def test_unknown_symbol_raises_value_error(self, store: TickStore) -> None:
        with pytest.raises(ValueError, match="No trades found"):
            store.vwap("XYZUSDT", _DAY1_START_NS, _DAY1_START_NS + _STEP_NS * 10)


# ---------------------------------------------------------------------------
# bars()
# ---------------------------------------------------------------------------


class TestBars:
    def test_returns_polars_dataframe(self, store: TickStore) -> None:
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS)
        assert isinstance(df, pl.DataFrame)

    def test_required_columns_present(self, store: TickStore) -> None:
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS)
        for col in ("bar_start_ns", "open", "high", "low", "close", "volume", "count"):
            assert col in df.columns

    def test_1m_bars_60_for_one_hour(self, store: TickStore) -> None:
        """A full 1-hour window aligned to midnight yields exactly 60 1-min bars."""
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS, interval="1m")
        assert len(df) == 60

    def test_5m_bars_12_for_one_hour(self, store: TickStore) -> None:
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS, interval="5m")
        assert len(df) == 12

    def test_1h_bars_48_for_two_days(self, store: TickStore) -> None:
        end_ns = _DAY1_START_NS + _SPAN_NS
        df = store.bars("BTCUSDT", _DAY1_START_NS, end_ns, interval="1h")
        assert len(df) == 48

    def test_bars_ordered_by_bar_start(self, store: TickStore) -> None:
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS)
        starts = df["bar_start_ns"].to_list()
        assert starts == sorted(starts)

    def test_ohlcv_invariants(self, store: TickStore) -> None:
        """high ≥ open, close; low ≤ open, close; high ≥ low."""
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS)
        assert (df["high"] >= df["open"]).all()
        assert (df["high"] >= df["close"]).all()
        assert (df["high"] >= df["low"]).all()
        assert (df["low"] <= df["open"]).all()
        assert (df["low"] <= df["close"]).all()

    def test_volume_positive_every_bar(self, store: TickStore) -> None:
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS)
        assert (df["volume"] > 0).all()

    def test_volume_equals_count_times_size(self, store: TickStore) -> None:
        """With uniform size=0.1, volume = count × 0.1 for every bar."""
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS)
        expected = df["count"].cast(pl.Float64) * 0.1
        diff = (df["volume"] - expected).abs()
        assert (diff < 1e-9).all()

    def test_total_volume_matches_trades(self, store: TickStore) -> None:
        """Sum of bar volumes == number of trades × size."""
        end_ns = _DAY1_START_NS + _SPAN_NS
        bars = store.bars("BTCUSDT", _DAY1_START_NS, end_ns, interval="1d")
        trades = store.trades("BTCUSDT", _DAY1_START_NS, end_ns)
        expected = len(trades) * 0.1
        assert abs(bars["volume"].sum() - expected) < 1e-6  # type: ignore[operator]

    def test_exchange_filter_returns_same_as_unfiltered(self, store: TickStore) -> None:
        df_all = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS)
        df_binance = store.bars(
            "BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS, exchange=_EXCHANGE
        )
        assert len(df_binance) == len(df_all)

    def test_exchange_filter_wrong_exchange_empty(self, store: TickStore) -> None:
        df = store.bars(
            "BTCUSDT", _DAY1_START_NS, _DAY1_START_NS + _ONE_HOUR_NS, exchange="coinbase"
        )
        assert len(df) == 0

    def test_empty_range_returns_empty_df(self, store: TickStore) -> None:
        df = store.bars("BTCUSDT", _DAY1_START_NS, _DAY1_START_NS)
        assert len(df) == 0


# ---------------------------------------------------------------------------
# gaps() — separate small fixture with a known injected gap
# ---------------------------------------------------------------------------


def _make_gap_ticks(tmp_path: Path) -> TickStore:
    """200 ticks for GAPUSDT/test: 100 ticks at 1-s cadence, then a 5-min
    gap, then 100 more ticks at 1-s cadence."""
    step = 1_000_000_000          # 1 second in ns
    gap_ns = 300 * 1_000_000_000  # 5 minutes in ns
    ticks: list[Tick] = []

    # 100 pre-gap ticks: i = 0..99
    for i in range(100):
        ts = _DAY1_START_NS + i * step
        ticks.append(
            Tick(
                exchange="test",
                symbol="GAPUSDT",
                price="100",
                size="1",
                side="buy",
                timestamp_ns=ts,
                received_ns=ts + 1_000_000,
                trade_id=f"g{i}",
            )
        )

    # 100 post-gap ticks: resume after the gap
    # last pre-gap tick is at DAY1_START_NS + 99*step
    resume_ns = _DAY1_START_NS + 99 * step + gap_ns
    for i in range(100):
        ts = resume_ns + i * step
        ticks.append(
            Tick(
                exchange="test",
                symbol="GAPUSDT",
                price="100",
                size="1",
                side="sell",
                timestamp_ns=ts,
                received_ns=ts + 1_000_000,
                trade_id=f"g{100 + i}",
            )
        )

    _write_ticks(ticks, tmp_path)
    return TickStore(tmp_path)


@pytest.fixture
def gap_store(tmp_path: Path) -> TickStore:  # type: ignore[misc]
    ts = _make_gap_ticks(tmp_path)
    yield ts  # type: ignore[misc]
    ts.close()


class TestGaps:
    def test_no_gaps_on_regular_data(self, store: TickStore) -> None:
        # Tick step ≈ 518 ms, far below the 60-s threshold
        df = store.gaps("BTCUSDT", _EXCHANGE, max_gap_seconds=60.0)
        assert len(df) == 0

    def test_gap_detected(self, gap_store: TickStore) -> None:
        df = gap_store.gaps("GAPUSDT", "test", max_gap_seconds=60.0)
        assert len(df) == 1

    def test_gap_duration_is_300_seconds(self, gap_store: TickStore) -> None:
        """The injected gap between tick 99 and the first post-gap tick is 300 s."""
        df = gap_store.gaps("GAPUSDT", "test", max_gap_seconds=60.0)
        assert abs(df["gap_seconds"][0] - 300.0) < 0.001

    def test_gap_columns_present(self, gap_store: TickStore) -> None:
        df = gap_store.gaps("GAPUSDT", "test", max_gap_seconds=1.0)
        for col in ("gap_start_ns", "gap_end_ns", "gap_seconds"):
            assert col in df.columns

    def test_threshold_hides_small_gap(self, gap_store: TickStore) -> None:
        # 300-s gap is below a 600-s threshold → empty result
        df = gap_store.gaps("GAPUSDT", "test", max_gap_seconds=600.0)
        assert len(df) == 0

    def test_gap_start_before_gap_end(self, gap_store: TickStore) -> None:
        df = gap_store.gaps("GAPUSDT", "test", max_gap_seconds=60.0)
        assert (df["gap_start_ns"] < df["gap_end_ns"]).all()

    def test_returns_polars_dataframe(self, gap_store: TickStore) -> None:
        df = gap_store.gaps("GAPUSDT", "test", max_gap_seconds=60.0)
        assert isinstance(df, pl.DataFrame)


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_enter_returns_self(self, tmp_path: Path) -> None:
        ts = TickStore(tmp_path)
        with ts as s:
            assert s is ts

    def test_close_via_context_manager(self, tmp_path: Path) -> None:
        with TickStore(tmp_path) as ts:
            pass  # __exit__ must not raise

    def test_manual_close(self, tmp_path: Path) -> None:
        ts = TickStore(tmp_path)
        ts.close()
        # Second close is allowed (DuckDB ignores it)
        ts.close()

    def test_connection_unusable_after_close(self, tmp_path: Path) -> None:
        ts = TickStore(tmp_path)
        ts.close()
        with pytest.raises(Exception):  # noqa: B017
            ts._con.execute("SELECT 1")

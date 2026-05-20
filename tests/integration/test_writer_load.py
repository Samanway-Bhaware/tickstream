"""Scale/load integration test for the ParquetWriter.

Run with: pytest --runslow
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tickstream.storage.parquet_writer import TICK_SCHEMA, ParquetWriter

_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
_EXCHANGE = "binance"

_NOW_NS = time.time_ns()
_DAY1_NS: int = _NOW_NS - 2 * 86_400 * 1_000_000_000  # 2 days ago
_DAY2_NS: int = _NOW_NS - 1 * 86_400 * 1_000_000_000  # 1 day ago

# Wall-clock regression guard: the whole test must finish in under this many seconds.
_MAX_WALL_S: float = 30.0


def _make_arrow_table(
    n: int,
    exchange: str,
    symbol: str,
    start_timestamp_ns: int,
) -> pa.Table:
    """Generate a PyArrow Table for *n* ticks without Pydantic overhead."""
    import numpy as np

    prices = [Decimal("50000.123456789")] * n
    sizes = [Decimal("0.000001234")] * n
    timestamps = np.arange(
        start_timestamp_ns,
        start_timestamp_ns + n * 1_000_000,
        1_000_000,
        dtype=np.int64,
    )
    trade_ids = [f"t{i}" for i in range(n)]

    return pa.Table.from_arrays(
        [
            pa.array([exchange] * n, type=pa.string()),
            pa.array([symbol] * n, type=pa.string()),
            pa.array(prices, type=pa.decimal128(38, 18)),
            pa.array(sizes, type=pa.decimal128(38, 18)),
            pa.array(["buy"] * n, type=pa.string()),
            pa.array(timestamps, type=pa.int64()),
            pa.array(timestamps, type=pa.int64()),
            pa.array(trade_ids, type=pa.string()),
        ],
        schema=TICK_SCHEMA,
    )


@pytest.mark.slow
@pytest.mark.integration
class TestParquetWriterLoad:
    async def test_100k_ticks_load_pipeline(self, tmp_path: Path) -> None:
        """100 000 ticks across 3 symbols × 2 dates — partition layout, schema, row count.

        Uses direct PyArrow table insertion (no Pydantic) so the bottleneck is
        I/O, not validation.  The whole test must complete in under 30 seconds.
        """
        wall_start = time.monotonic()

        total_ticks = 100_000
        n_partitions = 6
        base_n = total_ticks // n_partitions
        remainder = total_ticks % n_partitions

        queue: asyncio.Queue = asyncio.Queue()  # type: ignore[type-arg]
        writer = ParquetWriter(queue, root_dir=tmp_path, fsync=False)

        day1_str = datetime.fromtimestamp(_DAY1_NS / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")
        day2_str = datetime.fromtimestamp(_DAY2_NS / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")

        partition_configs: list[tuple[str, str, int, int]] = []
        for part_idx, (symbol, (date_str, ts_val)) in enumerate(
            (sym, dt)
            for sym in _SYMBOLS
            for dt in [(day1_str, _DAY1_NS), (day2_str, _DAY2_NS)]
        ):
            n = base_n + (remainder if part_idx == n_partitions - 1 else 0)
            partition_configs.append((symbol, date_str, ts_val, n))

        for symbol, date_str, ts_val, n in partition_configs:
            table = _make_arrow_table(n, _EXCHANGE, symbol, ts_val)
            await writer.write_table(table, exchange=_EXCHANGE, symbol=symbol, date=date_str)

        # --- assertions ---

        # 1. Partition directory count: 3 symbols × 2 dates = 6
        parquet_files = sorted(tmp_path.rglob("*.parquet"))
        assert len(parquet_files) == 6, (
            f"Expected 6 parquet files, found {len(parquet_files)}"
        )
        for symbol in _SYMBOLS:
            for date_str in (day1_str, day2_str):
                part_dir = (
                    tmp_path
                    / f"exchange={_EXCHANGE}"
                    / f"symbol={symbol}"
                    / f"date={date_str}"
                )
                assert part_dir.is_dir(), f"Missing partition directory: {part_dir}"

        # 2. Total row count matches exactly 100 000
        total_recovered = 0
        for f in parquet_files:
            table = pq.read_table(f)
            total_recovered += len(table)
            schema = pq.read_schema(f)
            assert schema.equals(TICK_SCHEMA, check_metadata=False), (
                f"{f}: schema mismatch\n got: {schema}\n expected: {TICK_SCHEMA}"
            )

        assert total_recovered == total_ticks, (
            f"Data loss detected: recovered {total_recovered} instead of {total_ticks}"
        )

        # 3. No leftover .tmp files
        tmp_files = list(tmp_path.rglob("*.tmp"))
        assert tmp_files == [], f"Leftover .tmp files: {tmp_files}"

        # 4. Wall-clock regression guard
        elapsed = time.monotonic() - wall_start
        assert elapsed < _MAX_WALL_S, (
            f"Load test took {elapsed:.1f}s — exceeds {_MAX_WALL_S}s budget. "
            "Performance regression detected."
        )

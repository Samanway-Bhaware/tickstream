"""Tests for src/tickstream/storage/parquet_writer.py.

Coverage
--------
- 100k synthetic ticks across 3 symbols × 2 UTC dates → verify partition
  directories are created, all ticks are recoverable, and no files are missing.
- Schema conformance: every written file must match ``TICK_SCHEMA`` exactly.
- Atomic writes: no ``.tmp`` files remain after a successful (or cancelled) run.
- Size-based flush: a batch is written as soon as it reaches ``max_batch_size``.
- Time-based flush: a batch is written after ``flush_interval_s`` elapses even
  without reaching ``max_batch_size``.
- Shutdown flush: ticks that are still in the queue when the task is cancelled
  are written before the writer exits.
- Decimal precision: ``price`` and ``size`` survive a write/read round-trip
  with no loss of precision.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tickstream.models import Tick
from tickstream.storage.parquet_writer import (
    TICK_SCHEMA,
    ParquetWriter,
    _partition_key,
    _ticks_to_table,
    _write_batch_sync,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Reference epoch in ns — two calendar dates in the past.
_NOW_NS = time.time_ns()
_DAY1_NS: int = _NOW_NS - 2 * 86_400 * 1_000_000_000  # 2 days ago
_DAY2_NS: int = _NOW_NS - 1 * 86_400 * 1_000_000_000  # 1 day ago

_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
_EXCHANGE = "binance"


def _make_tick(
    *,
    exchange: str = _EXCHANGE,
    symbol: str = "BTCUSDT",
    timestamp_ns: int | None = None,
    price: str = "50000.123456789",
    size: str = "0.000001234",
    side: str = "buy",
    trade_id: str | None = None,
) -> Tick:
    ts = timestamp_ns if timestamp_ns is not None else _NOW_NS
    return Tick(
        exchange=exchange,
        symbol=symbol,
        price=price,
        size=size,
        side=side,  # type: ignore[arg-type]
        timestamp_ns=ts,
        received_ns=ts,  # same → no clock-skew error
        trade_id=trade_id or str(uuid.uuid4()),
    )


def _generate_ticks(n: int) -> list[Tick]:
    """Return *n* ticks distributed evenly across 3 symbols × 2 dates."""
    days = [_DAY1_NS, _DAY2_NS]
    ticks = []
    for i in range(n):
        symbol = _SYMBOLS[i % len(_SYMBOLS)]
        ts = days[i % 2]
        ticks.append(_make_tick(symbol=symbol, timestamp_ns=ts, trade_id=str(i)))
    return ticks


async def _run_writer_with_ticks(
    ticks: list[Tick],
    tmp_path: Path,
    *,
    max_batch_size: int = 1_000,
    flush_interval_s: float = 3_600.0,  # effectively disabled during tests
    drain_wait_s: float = 0.5,
) -> ParquetWriter:
    """Feed *ticks* into a fresh writer, wait for the queue to drain, cancel."""
    queue: asyncio.Queue[Tick] = asyncio.Queue()
    writer = ParquetWriter(
        queue,
        root_dir=tmp_path,
        max_batch_size=max_batch_size,
        flush_interval_s=flush_interval_s,
    )
    task = asyncio.create_task(writer.run())

    for tick in ticks:
        await queue.put(tick)

    # Wait until the queue is empty (writer consumed all ticks).
    deadline = time.monotonic() + 10.0
    while not queue.empty():
        if time.monotonic() > deadline:
            pytest.fail("Timed out waiting for writer to drain queue")
        await asyncio.sleep(0.05)

    # Give the writer a moment to finish the final in-flight write.
    await asyncio.sleep(drain_wait_s)

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return writer


# ---------------------------------------------------------------------------
# _partition_key
# ---------------------------------------------------------------------------


class TestPartitionKey:
    def test_date_extracted_from_timestamp_ns(self) -> None:
        # _DAY1_NS is 2 days ago; verify the date string is a valid YYYY-MM-DD.
        tick = _make_tick(timestamp_ns=_DAY1_NS)
        key = _partition_key(tick)
        assert key[0] == _EXCHANGE
        assert key[1] == "BTCUSDT"
        assert len(key[2]) == 10  # "YYYY-MM-DD"
        assert key[2][4] == "-" and key[2][7] == "-"

    def test_different_timestamps_same_day_same_key(self) -> None:
        # Two ticks 1 second apart on the same day → same partition key.
        t1 = _make_tick(timestamp_ns=_DAY1_NS)
        t2 = _make_tick(timestamp_ns=_DAY1_NS + 1_000_000_000)
        assert _partition_key(t1)[2] == _partition_key(t2)[2]

    def test_ticks_on_different_days_have_different_keys(self) -> None:
        t1 = _make_tick(timestamp_ns=_DAY1_NS)
        t2 = _make_tick(timestamp_ns=_DAY2_NS)
        assert _partition_key(t1) != _partition_key(t2)


# ---------------------------------------------------------------------------
# _ticks_to_table
# ---------------------------------------------------------------------------


class TestTicksToTable:
    def test_schema_matches_tick_schema(self) -> None:
        table = _ticks_to_table([_make_tick()])
        assert table.schema.equals(TICK_SCHEMA, check_metadata=False)

    def test_decimal_columns_have_correct_type(self) -> None:
        import pyarrow as pa

        table = _ticks_to_table([_make_tick()])
        assert table.schema.field("price").type == pa.decimal128(38, 18)
        assert table.schema.field("size").type == pa.decimal128(38, 18)

    def test_row_count_matches_input(self) -> None:
        ticks = [_make_tick() for _ in range(50)]
        assert len(_ticks_to_table(ticks)) == 50


# ---------------------------------------------------------------------------
# _write_batch_sync
# ---------------------------------------------------------------------------


class TestWriteBatchSync:
    def test_file_created(self, tmp_path: Path) -> None:
        path = _write_batch_sync([_make_tick()], tmp_path)
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_no_tmp_files_remain(self, tmp_path: Path) -> None:
        _write_batch_sync([_make_tick() for _ in range(10)], tmp_path)
        assert list(tmp_path.glob("*.tmp")) == []

    def test_returned_path_is_readable(self, tmp_path: Path) -> None:
        path = _write_batch_sync([_make_tick()], tmp_path)
        table = pq.read_table(path)
        assert len(table) == 1

    def test_schema_conformance(self, tmp_path: Path) -> None:
        path = _write_batch_sync([_make_tick() for _ in range(5)], tmp_path)
        schema = pq.read_schema(path)
        assert schema.equals(TICK_SCHEMA, check_metadata=False)

    def test_zstd_compression(self, tmp_path: Path) -> None:
        path = _write_batch_sync([_make_tick() for _ in range(100)], tmp_path)
        meta = pq.read_metadata(path)
        for rg in range(meta.num_row_groups):
            for col in range(meta.num_columns):
                cc = meta.row_group(rg).column(col)
                assert cc.compression == "ZSTD", (
                    f"column {col} in row-group {rg} uses {cc.compression}"
                )

    def test_directory_created_if_absent(self, tmp_path: Path) -> None:
        deep = tmp_path / "a" / "b" / "c"
        assert not deep.exists()
        _write_batch_sync([_make_tick()], deep)
        assert deep.exists()

    def test_decimal_round_trip_precision(self, tmp_path: Path) -> None:
        """price and size survive a write/read cycle without loss."""
        price_str = "67432.12345678901234"
        size_str = "0.00000000012345678"
        tick = _make_tick(price=price_str, size=size_str)
        path = _write_batch_sync([tick], tmp_path)
        table = pq.read_table(path)
        # pyarrow reads Decimal128 as Python Decimal by default.
        read_price = table["price"][0].as_py()
        read_size = table["size"][0].as_py()
        assert read_price == Decimal(price_str)
        assert read_size == Decimal(size_str)


# ---------------------------------------------------------------------------
# ParquetWriter — 100k ticks, 3 symbols, 2 days
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestParquetWriterBulk:
    async def test_100k_ticks_partition_structure(self, tmp_path: Path) -> None:
        """6 (symbol × date) partitions are created; every file is well-formed."""
        ticks = _generate_ticks(100_000)
        writer = await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=5_000)

        parquet_files = sorted(tmp_path.rglob("*.parquet"))
        assert len(parquet_files) > 0, "No Parquet files were written"

        # Verify all 6 partition directories exist.
        from datetime import datetime, timezone

        day1_str = datetime.fromtimestamp(_DAY1_NS / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")
        day2_str = datetime.fromtimestamp(_DAY2_NS / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")

        for symbol in _SYMBOLS:
            for date in (day1_str, day2_str):
                part_dir = (
                    tmp_path
                    / f"exchange={_EXCHANGE}"
                    / f"symbol={symbol}"
                    / f"date={date}"
                )
                assert part_dir.is_dir(), f"Missing partition directory: {part_dir}"

    async def test_100k_ticks_no_data_loss(self, tmp_path: Path) -> None:
        """Total row count across all files equals number of input ticks."""
        ticks = _generate_ticks(100_000)
        await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=5_000)

        total = sum(len(pq.read_table(f)) for f in tmp_path.rglob("*.parquet"))
        assert total == 100_000

    async def test_100k_ticks_schema_conformance(self, tmp_path: Path) -> None:
        """Every written file must match TICK_SCHEMA."""
        ticks = _generate_ticks(100_000)
        await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=5_000)

        for f in tmp_path.rglob("*.parquet"):
            schema = pq.read_schema(f)
            assert schema.equals(TICK_SCHEMA, check_metadata=False), (
                f"{f}: schema mismatch\n  got:      {schema}\n  expected: {TICK_SCHEMA}"
            )

    async def test_no_tmp_files_after_run(self, tmp_path: Path) -> None:
        """No ``.tmp`` files survive a normal or cancelled writer run."""
        ticks = _generate_ticks(100_000)
        await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=5_000)

        tmp_files = list(tmp_path.rglob("*.tmp"))
        assert tmp_files == [], f"Leftover .tmp files: {tmp_files}"

    async def test_partition_path_format(self, tmp_path: Path) -> None:
        """Partition directories use the ``exchange=X/symbol=Y/date=Z`` form."""
        ticks = _generate_ticks(10)
        await _run_writer_with_ticks(ticks, tmp_path)

        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert parquet_files, "No files written"

        for f in parquet_files:
            parts = f.relative_to(tmp_path).parts
            # parts = ("exchange=X", "symbol=Y", "date=YYYY-MM-DD", "<uuid>.parquet")
            assert len(parts) == 4
            assert parts[0].startswith("exchange=")
            assert parts[1].startswith("symbol=")
            assert parts[2].startswith("date=")
            date_val = parts[2].split("=", 1)[1]
            assert len(date_val) == 10 and date_val[4] == "-" and date_val[7] == "-"

    async def test_files_written_counter(self, tmp_path: Path) -> None:
        ticks = _generate_ticks(100_000)
        writer = await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=5_000)
        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert writer.files_written == len(parquet_files)


# ---------------------------------------------------------------------------
# Flush triggers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestFlushTriggers:
    async def test_size_based_flush_triggered(self, tmp_path: Path) -> None:
        """A batch of exactly ``max_batch_size`` ticks triggers one flush."""
        batch_size = 50
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue, root_dir=tmp_path, max_batch_size=batch_size, flush_interval_s=3_600.0
        )
        task = asyncio.create_task(writer.run())

        for i in range(batch_size):
            await queue.put(_make_tick(trade_id=str(i)))

        # Allow the writer to process.
        deadline = time.monotonic() + 5.0
        while writer.files_written < 1:
            if time.monotonic() > deadline:
                pytest.fail("Size-based flush did not fire")
            await asyncio.sleep(0.05)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        table = pq.read_table(list(tmp_path.rglob("*.parquet"))[0])
        assert len(table) == batch_size

    async def test_time_based_flush_triggered(self, tmp_path: Path) -> None:
        """A batch is flushed after ``flush_interval_s`` even if not full."""
        n_ticks = 7
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue,
            root_dir=tmp_path,
            max_batch_size=10_000,  # won't trigger on size
            flush_interval_s=0.2,  # fires quickly
        )
        task = asyncio.create_task(writer.run())

        for i in range(n_ticks):
            await queue.put(_make_tick(trade_id=str(i)))

        # Wait for the interval to pass and the writer to flush.
        deadline = time.monotonic() + 5.0
        while writer.files_written < 1:
            if time.monotonic() > deadline:
                pytest.fail("Time-based flush did not fire")
            await asyncio.sleep(0.05)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        table = pq.read_table(list(tmp_path.rglob("*.parquet"))[0])
        assert len(table) == n_ticks

    async def test_shutdown_flush_drains_queue(self, tmp_path: Path) -> None:
        """Ticks still in the queue when the task is cancelled must be written."""
        n_ticks = 30
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        # Use a very large batch size so nothing flushes until shutdown.
        writer = ParquetWriter(
            queue, root_dir=tmp_path, max_batch_size=10_000, flush_interval_s=3_600.0
        )
        task = asyncio.create_task(writer.run())
        await asyncio.sleep(0)  # let the task start

        for i in range(n_ticks):
            queue.put_nowait(_make_tick(trade_id=str(i)))

        # Cancel immediately — ticks may still be in queue.
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        total = sum(len(pq.read_table(f)) for f in tmp_path.rglob("*.parquet"))
        assert total == n_ticks

    async def test_shutdown_flush_drains_in_memory_batches(self, tmp_path: Path) -> None:
        """In-memory batches (already consumed from queue) are flushed on cancel."""
        n_ticks = 40
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue, root_dir=tmp_path, max_batch_size=10_000, flush_interval_s=3_600.0
        )
        task = asyncio.create_task(writer.run())

        for i in range(n_ticks):
            await queue.put(_make_tick(trade_id=str(i)))

        # Wait until the queue is drained into in-memory batches.
        while not queue.empty():
            await asyncio.sleep(0.02)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        total = sum(len(pq.read_table(f)) for f in tmp_path.rglob("*.parquet"))
        assert total == n_ticks


# ---------------------------------------------------------------------------
# Multiple partitions in one run
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestMultiplePartitions:
    async def test_per_exchange_isolation(self, tmp_path: Path) -> None:
        """Ticks from different exchanges land in separate partition trees."""
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue, root_dir=tmp_path, max_batch_size=5, flush_interval_s=3_600.0
        )
        task = asyncio.create_task(writer.run())

        for i in range(5):
            await queue.put(_make_tick(exchange="binance", symbol="BTCUSDT", trade_id=f"b{i}"))
        for i in range(5):
            await queue.put(_make_tick(exchange="coinbase", symbol="BTC-USD", trade_id=f"c{i}"))

        deadline = time.monotonic() + 5.0
        while writer.files_written < 2:
            if time.monotonic() > deadline:
                pytest.fail("Expected 2 files to be written")
            await asyncio.sleep(0.05)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        binance_dir = tmp_path / "exchange=binance"
        coinbase_dir = tmp_path / "exchange=coinbase"
        assert binance_dir.is_dir()
        assert coinbase_dir.is_dir()
        assert len(list(binance_dir.rglob("*.parquet"))) >= 1
        assert len(list(coinbase_dir.rglob("*.parquet"))) >= 1

    async def test_atomic_write_no_tmp_files_on_error(self, tmp_path: Path) -> None:
        """Even in concurrent writes no leftover .tmp files exist afterward."""
        ticks = _generate_ticks(10_000)
        await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=200, drain_wait_s=1.0)
        assert list(tmp_path.rglob("*.tmp")) == []

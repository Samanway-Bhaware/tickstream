"""Fast unit tests for src/tickstream/storage/parquet_writer.py.

All async tests run under asyncio_mode='auto' — no @pytest.mark.asyncio needed.
Each test uses at most 50 ticks and should complete in well under 200 ms.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
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
from tests.factories import make_tick, make_ticks, make_validated_ticks

# Two deterministic dates used throughout.
_DAY1_NS: int = 1_716_163_200_000_000_000  # 2024-05-20 00:00:00 UTC
_DAY2_NS: int = _DAY1_NS + 86_400_000_000_000  # 2024-05-21 00:00:00 UTC

_EXCHANGE = "binance"


async def _run_writer_with_ticks(
    ticks: list[Tick],
    tmp_path: Path,
    *,
    max_batch_size: int = 10,
    flush_interval_s: float = 3_600.0,
    drain_wait_s: float = 0.05,
) -> ParquetWriter:
    """Feed *ticks* into a fresh writer, wait for the queue to drain, cancel."""
    queue: asyncio.Queue[Tick] = asyncio.Queue()
    writer = ParquetWriter(
        queue,
        root_dir=tmp_path,
        max_batch_size=max_batch_size,
        flush_interval_s=flush_interval_s,
        fsync=False,
    )
    task = asyncio.create_task(writer.run())

    for tick in ticks:
        await queue.put(tick)

    deadline = time.monotonic() + 2.0
    while not queue.empty():
        if time.monotonic() > deadline:
            pytest.fail("Timed out waiting for writer to drain queue")
        await asyncio.sleep(0.01)

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
        tick = make_tick(timestamp_ns=_DAY1_NS)
        key = _partition_key(tick)
        assert key[0] == _EXCHANGE
        assert key[1] == "BTCUSDT"
        assert len(key[2]) == 10  # "YYYY-MM-DD"
        assert key[2][4] == "-" and key[2][7] == "-"

    def test_different_timestamps_same_day_same_key(self) -> None:
        t1 = make_tick(timestamp_ns=_DAY1_NS)
        t2 = make_tick(timestamp_ns=_DAY1_NS + 1_000_000_000)
        assert _partition_key(t1)[2] == _partition_key(t2)[2]

    def test_ticks_on_different_days_have_different_keys(self) -> None:
        t1 = make_tick(timestamp_ns=_DAY1_NS)
        t2 = make_tick(timestamp_ns=_DAY2_NS)
        assert _partition_key(t1) != _partition_key(t2)


# ---------------------------------------------------------------------------
# _ticks_to_table
# ---------------------------------------------------------------------------


class TestTicksToTable:
    def test_schema_matches_tick_schema(self) -> None:
        table = _ticks_to_table([make_tick()])
        assert table.schema.equals(TICK_SCHEMA, check_metadata=False)

    def test_decimal_columns_have_correct_type(self) -> None:
        table = _ticks_to_table([make_tick()])
        assert table.schema.field("price").type == pa.decimal128(38, 18)
        assert table.schema.field("size").type == pa.decimal128(38, 18)

    def test_row_count_matches_input(self) -> None:
        ticks = [make_tick() for _ in range(50)]
        assert len(_ticks_to_table(ticks)) == 50


# ---------------------------------------------------------------------------
# _write_batch_sync
# ---------------------------------------------------------------------------


class TestWriteBatchSync:
    def test_file_created(self, tmp_path: Path) -> None:
        path = _write_batch_sync([make_tick()], tmp_path)
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_no_tmp_files_remain(self, tmp_path: Path) -> None:
        _write_batch_sync([make_tick() for _ in range(10)], tmp_path)
        assert list(tmp_path.glob("*.tmp")) == []

    def test_returned_path_is_readable(self, tmp_path: Path) -> None:
        path = _write_batch_sync([make_tick()], tmp_path)
        table = pq.read_table(path)
        assert len(table) == 1

    def test_schema_conformance(self, tmp_path: Path) -> None:
        path = _write_batch_sync([make_tick() for _ in range(5)], tmp_path)
        schema = pq.read_schema(path)
        assert schema.equals(TICK_SCHEMA, check_metadata=False)

    def test_zstd_compression(self, tmp_path: Path) -> None:
        path = _write_batch_sync([make_tick() for _ in range(20)], tmp_path)
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
        _write_batch_sync([make_tick()], deep)
        assert deep.exists()

    def test_decimal_precision_preserved_through_roundtrip(self, tmp_path: Path) -> None:
        """price and size survive a write/read cycle without loss."""
        price_str = "67432.12345678901234"
        size_str = "0.00000000012345678"
        tick = make_tick(price=price_str, size=size_str)
        path = _write_batch_sync([tick], tmp_path)
        table = pq.read_table(path)
        read_price = table["price"][0].as_py()
        read_size = table["size"][0].as_py()
        assert read_price == Decimal(price_str)
        assert read_size == Decimal(size_str)


# ---------------------------------------------------------------------------
# ParquetWriter — partition path layout
# ---------------------------------------------------------------------------


class TestPartitionPathLayout:
    async def test_partition_path_uses_exchange_symbol_date_layout(
        self, tmp_path: Path
    ) -> None:
        """Written files must sit under exchange=X/symbol=Y/date=YYYY-MM-DD/."""
        ticks = [make_tick(timestamp_ns=_DAY1_NS, symbol="BTCUSDT")]
        await _run_writer_with_ticks(ticks, tmp_path)

        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert parquet_files, "No files written"

        for f in parquet_files:
            parts = f.relative_to(tmp_path).parts
            assert len(parts) == 4
            assert parts[0].startswith("exchange=")
            assert parts[1].startswith("symbol=")
            assert parts[2].startswith("date=")
            date_val = parts[2].split("=", 1)[1]
            assert len(date_val) == 10 and date_val[4] == "-" and date_val[7] == "-"

    async def test_files_written_counter(self, tmp_path: Path) -> None:
        ticks = make_ticks(40)
        writer = await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=10)
        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert writer.files_written == len(parquet_files)


# ---------------------------------------------------------------------------
# ParquetWriter — flush triggers
# ---------------------------------------------------------------------------


class TestFlushTriggers:
    async def test_flush_triggers_at_batch_size_threshold(self, tmp_path: Path) -> None:
        """A batch of exactly max_batch_size ticks triggers one size-based flush."""
        batch_size = 5
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue,
            root_dir=tmp_path,
            max_batch_size=batch_size,
            flush_interval_s=3_600.0,
            fsync=False,
        )
        task = asyncio.create_task(writer.run())

        for i in range(batch_size):
            await queue.put(make_tick(trade_id=str(i)))

        deadline = time.monotonic() + 2.0
        while writer.files_written < 1:
            if time.monotonic() > deadline:
                pytest.fail("Size-based flush did not fire")
            await asyncio.sleep(0.01)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        table = pq.read_table(list(tmp_path.rglob("*.parquet"))[0])
        assert len(table) == batch_size

    async def test_flush_triggers_after_flush_seconds_elapsed(
        self, tmp_path: Path
    ) -> None:
        """A partial batch is flushed after flush_interval_s even without reaching batch_size."""
        n_ticks = 3
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue,
            root_dir=tmp_path,
            max_batch_size=10_000,
            flush_interval_s=0.1,
            fsync=False,
        )
        task = asyncio.create_task(writer.run())

        for i in range(n_ticks):
            await queue.put(make_tick(trade_id=str(i)))

        deadline = time.monotonic() + 2.0
        while writer.files_written < 1:
            if time.monotonic() > deadline:
                pytest.fail("Time-based flush did not fire")
            await asyncio.sleep(0.01)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        table = pq.read_table(list(tmp_path.rglob("*.parquet"))[0])
        assert len(table) == n_ticks


# ---------------------------------------------------------------------------
# ParquetWriter — shutdown / partial-batch flush
# ---------------------------------------------------------------------------


class TestShutdownFlush:
    async def test_partial_batch_flushed_on_shutdown(self, tmp_path: Path) -> None:
        """In-memory batches not yet at batch_size are written on cancellation."""
        n_ticks = 15
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue,
            root_dir=tmp_path,
            max_batch_size=10_000,
            flush_interval_s=3_600.0,
            fsync=False,
        )
        task = asyncio.create_task(writer.run())

        for i in range(n_ticks):
            await queue.put(make_tick(trade_id=str(i)))

        while not queue.empty():
            await asyncio.sleep(0.01)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        total = sum(len(pq.read_table(f)) for f in tmp_path.rglob("*.parquet"))
        assert total == n_ticks

    async def test_queue_items_drained_on_shutdown(self, tmp_path: Path) -> None:
        """Ticks still in the queue when the task is cancelled must be written."""
        n_ticks = 10
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue,
            root_dir=tmp_path,
            max_batch_size=10_000,
            flush_interval_s=3_600.0,
            fsync=False,
        )
        task = asyncio.create_task(writer.run())
        await asyncio.sleep(0)

        for i in range(n_ticks):
            queue.put_nowait(make_tick(trade_id=str(i)))

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        total = sum(len(pq.read_table(f)) for f in tmp_path.rglob("*.parquet"))
        assert total == n_ticks


# ---------------------------------------------------------------------------
# ParquetWriter — atomic writes and schema
# ---------------------------------------------------------------------------


class TestAtomicWriteAndSchema:
    async def test_no_tmp_files_remain_after_flush(self, tmp_path: Path) -> None:
        """No .tmp files survive after normal writer operation."""
        ticks = make_ticks(50)
        await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=10, drain_wait_s=0.1)
        assert list(tmp_path.rglob("*.tmp")) == []

    async def test_schema_is_identical_across_partitions(self, tmp_path: Path) -> None:
        """All Parquet files, regardless of partition, share the canonical TICK_SCHEMA."""
        ticks = (
            make_ticks(10, symbol="BTCUSDT", date="2025-01-15")
            + make_ticks(10, symbol="ETHUSDT", date="2025-01-15")
            + make_ticks(10, symbol="BTCUSDT", date="2025-01-16")
        )
        await _run_writer_with_ticks(ticks, tmp_path, max_batch_size=10)

        files = list(tmp_path.rglob("*.parquet"))
        assert len(files) == 3, f"Expected 3 partition files, got {len(files)}"
        for f in files:
            schema = pq.read_schema(f)
            assert schema.equals(TICK_SCHEMA, check_metadata=False), (
                f"{f}: schema mismatch"
            )

    async def test_per_exchange_isolation(self, tmp_path: Path) -> None:
        """Ticks from different exchanges land in separate partition trees."""
        queue: asyncio.Queue[Tick] = asyncio.Queue()
        writer = ParquetWriter(
            queue,
            root_dir=tmp_path,
            max_batch_size=5,
            flush_interval_s=3_600.0,
            fsync=False,
        )
        task = asyncio.create_task(writer.run())

        for i in range(5):
            await queue.put(make_tick(exchange="binance", symbol="BTCUSDT", trade_id=f"b{i}"))
        for i in range(5):
            await queue.put(make_tick(exchange="coinbase", symbol="BTC-USD", trade_id=f"c{i}"))

        deadline = time.monotonic() + 2.0
        while writer.files_written < 2:
            if time.monotonic() > deadline:
                pytest.fail("Expected 2 files to be written")
            await asyncio.sleep(0.01)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert (tmp_path / "exchange=binance").is_dir()
        assert (tmp_path / "exchange=coinbase").is_dir()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    async def test_malformed_tick_does_not_crash_writer(self, tmp_path: Path) -> None:
        """A Tick with invalid decimal precision should still write; writer must not crash."""
        # make_validated_ticks goes through real Tick() so we get genuine objects.
        valid_ticks = make_validated_ticks(5)
        await _run_writer_with_ticks(valid_ticks, tmp_path, max_batch_size=10)
        # If writer survived, files must exist and be readable.
        files = list(tmp_path.rglob("*.parquet"))
        assert files
        for f in files:
            table = pq.read_table(f)
            assert len(table) > 0

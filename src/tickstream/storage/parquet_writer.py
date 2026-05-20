"""Async Parquet writer that consumes ticks from a queue and persists them.

Partition strategy
------------------
Ticks are grouped by ``(exchange, symbol, date_utc)`` and flushed to disk
as individual Parquet files when **either** condition is met:

- **Size trigger**: the in-memory batch reaches ``max_batch_size`` ticks.
- **Time trigger**: ``flush_interval_s`` seconds have elapsed since the
  first tick arrived in the batch (checked every ``_TIMER_TICK_S`` seconds).

On shutdown (task cancellation) remaining queue items are drained first,
then all in-memory batches are written, so **no ticks are lost**.

Output path
-----------
::

    <root>/exchange=<x>/symbol=<y>/date=YYYY-MM-DD/<uuid>.parquet

Atomic writes
-------------
Each file is first written to ``<name>.parquet.tmp`` in the same directory,
then renamed with :func:`os.replace` (atomic on POSIX; replaces on Windows).
No ``.tmp`` file is ever left behind after a successful flush.

Schema
------
Defined once as the module-level constant :data:`TICK_SCHEMA`.  Every file
written by this module is guaranteed to conform to this schema.  Monetary
values are ``Decimal128(38, 18)``; both timestamps are ``int64`` nanoseconds.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from tickstream.models import Tick

if TYPE_CHECKING:
    from tickstream.monitoring.metrics import MetricsRegistry

log: structlog.BoundLogger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Stable schema — never mutate; embed in every Parquet file.
# ---------------------------------------------------------------------------

#: Canonical pyarrow schema for a single :class:`~tickstream.models.Tick`.
#: ``Decimal128(38, 18)`` covers any realistic crypto price/size with 18
#: decimal places of precision and 20 digits before the decimal point.
TICK_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("price", pa.decimal128(38, 18), nullable=False),
        pa.field("size", pa.decimal128(38, 18), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("timestamp_ns", pa.int64(), nullable=False),
        pa.field("received_ns", pa.int64(), nullable=False),
        pa.field("trade_id", pa.string(), nullable=False),
    ]
)

# How often the main loop wakes up to check time-based flush triggers.
_TIMER_TICK_S: float = 1.0

# Characters that are unsafe in directory/file names.
_UNSAFE_RE: re.Pattern[str] = re.compile(r"[^\w\-.]")

# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

#: Partition key: ``(exchange, symbol, date_utc)``
PartitionKey = tuple[str, str, str]


@dataclass
class _Batch:
    rows: list[Tick] = field(default_factory=list)
    #: :func:`time.monotonic` timestamp of the first tick in this batch.
    opened_at: float = field(default_factory=time.monotonic)


# ---------------------------------------------------------------------------
# Pure helpers (thread-safe; no shared state)
# ---------------------------------------------------------------------------


def _safe_component(s: str) -> str:
    """Replace filesystem-unsafe characters with underscores."""
    return _UNSAFE_RE.sub("_", s)


def _partition_key(tick: Tick) -> PartitionKey:
    date_utc = datetime.fromtimestamp(
        tick.timestamp_ns / 1e9, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    return (tick.exchange, tick.symbol, date_utc)


def _partition_dir(root: Path, exchange: str, symbol: str, date: str) -> Path:
    return (
        root
        / f"exchange={_safe_component(exchange)}"
        / f"symbol={_safe_component(symbol)}"
        / f"date={date}"
    )


def _ticks_to_table(ticks: list[Tick]) -> pa.Table:
    """Convert a list of :class:`Tick` objects to a pyarrow Table.

    Column order and types match :data:`TICK_SCHEMA` exactly.
    """
    return pa.Table.from_arrays(
        [
            pa.array([t.exchange for t in ticks], type=pa.string()),
            pa.array([t.symbol for t in ticks], type=pa.string()),
            pa.array([t.price for t in ticks], type=pa.decimal128(38, 18)),
            pa.array([t.size for t in ticks], type=pa.decimal128(38, 18)),
            pa.array([t.side for t in ticks], type=pa.string()),
            pa.array([t.timestamp_ns for t in ticks], type=pa.int64()),
            pa.array([t.received_ns for t in ticks], type=pa.int64()),
            pa.array([t.trade_id for t in ticks], type=pa.string()),
        ],
        schema=TICK_SCHEMA,
    )


def _write_table_sync(table: pa.Table, dir_path: Path) -> Path:
    """Write a PyArrow *table* to a new Parquet file in *dir_path*.

    This is a **blocking** function intended to run in a thread-pool
    executor.

    Returns the final (post-rename) :class:`Path`.
    """
    dir_path.mkdir(parents=True, exist_ok=True)
    filename = f"{uuid.uuid4()}.parquet"
    final_path = dir_path / filename
    tmp_path = final_path.with_name(final_path.name + ".tmp")

    pq.write_table(
        table,
        str(tmp_path),
        compression="zstd",
        write_statistics=True,
    )
    # Atomic rename — on POSIX this is a single syscall; on Windows it replaces.
    tmp_path.replace(final_path)
    return final_path


def _write_batch_sync(ticks: list[Tick], dir_path: Path) -> Path:
    """Write *ticks* to a new Parquet file in *dir_path*.

    This is a **blocking** function intended to run in a thread-pool
    executor.  It has no side-effects on any shared mutable state.

    Returns the final (post-rename) :class:`Path`.
    """
    table = _ticks_to_table(ticks)
    return _write_table_sync(table, dir_path)


# ---------------------------------------------------------------------------
# ParquetWriter
# ---------------------------------------------------------------------------


class ParquetWriter:
    """Async consumer that reads :class:`~tickstream.models.Tick` objects from
    a queue and writes them to partitioned Parquet files.

    Parameters
    ----------
    queue:
        The same :class:`asyncio.Queue` the connectors write to.
    root_dir:
        Root of the on-disk partition tree (created if absent).
    max_batch_size:
        Flush a partition batch after this many ticks accumulate.
    flush_interval_s:
        Flush a partition batch after this many seconds since its first tick.
    executor:
        Thread-pool executor for blocking Parquet writes.  A default
        single-thread pool is created if ``None``.

    Example
    -------
    ::

        queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=100_000)
        writer = ParquetWriter(queue, root_dir=Path("data"))
        writer_task = asyncio.create_task(writer.run())

        # … feed queue from connectors …

        writer_task.cancel()
        await writer_task  # flushes all pending batches before returning
    """

    def __init__(
        self,
        queue: asyncio.Queue[Tick],
        *,
        root_dir: Path | str = Path("data"),
        max_batch_size: int = 10_000,
        flush_interval_s: float = 30.0,
        executor: concurrent.futures.Executor | None = None,
        metrics: MetricsRegistry | None = None,
        fsync: bool = True,
    ) -> None:
        self._queue = queue
        self._root = Path(root_dir)
        self._max_batch_size = max_batch_size
        self._flush_interval_s = flush_interval_s
        self._executor = executor or concurrent.futures.ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="parquet-writer"
        )
        self._metrics = metrics
        self._fsync = fsync

        self._batches: dict[PartitionKey, _Batch] = {}
        self._files_written: int = 0

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def files_written(self) -> int:
        """Total number of Parquet files written since the writer started."""
        return self._files_written

    @property
    def oldest_batch_age_s(self) -> float | None:
        """Age in seconds of the oldest in-memory batch, or ``None`` if no open batches."""
        if not self._batches:
            return None
        now = time.monotonic()
        return now - min(batch.opened_at for batch in self._batches.values())

    async def write_table(
        self,
        table: pa.Table,
        exchange: str,
        symbol: str,
        date: str,
    ) -> Path:
        """Write a PyArrow Table directly to disk asynchronously.

        Useful for bulk data ingestion or loading tests bypassing model
        parsing overhead.
        """
        dir_path = _partition_dir(self._root, exchange, symbol, date)
        loop = asyncio.get_running_loop()
        path = await loop.run_in_executor(
            self._executor, _write_table_sync, table, dir_path
        )
        self._files_written += 1
        return path

    async def run(self) -> None:
        """Consume ticks from the queue and flush batches to disk.

        Runs until the enclosing task is cancelled.  On cancellation:

        1. Remaining items in the queue are drained into in-memory batches.
        2. All in-memory batches are written to disk.
        3. The method returns normally (``CancelledError`` is absorbed).
        """
        log.info(
            "writer.started",
            root=str(self._root),
            max_batch_size=self._max_batch_size,
            flush_interval_s=self._flush_interval_s,
        )
        try:
            while True:
                # Block until a tick arrives, waking at most every _TIMER_TICK_S
                # seconds so we can check time-based flush triggers even during
                # a quiet period.
                try:
                    tick = await asyncio.wait_for(
                        self._queue.get(), timeout=_TIMER_TICK_S
                    )
                    await self._ingest(tick)
                except asyncio.TimeoutError:
                    pass  # no new tick; fall through to time-flush check

                await self._flush_expired()

        except asyncio.CancelledError:
            pass

        finally:
            await self._shutdown_flush()
            log.info(
                "writer.stopped",
                files_written=self._files_written,
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _add_to_batch(self, tick: Tick) -> PartitionKey:
        """Append *tick* to its partition batch; create the batch if needed."""
        key = _partition_key(tick)
        if key not in self._batches:
            self._batches[key] = _Batch()
        self._batches[key].rows.append(tick)
        return key

    async def _ingest(self, tick: Tick) -> None:
        """Add *tick* to its batch and flush immediately if the batch is full."""
        key = self._add_to_batch(tick)
        if len(self._batches[key].rows) >= self._max_batch_size:
            await self._flush(key)

    async def _flush_expired(self) -> None:
        """Flush every batch that has been open longer than ``flush_interval_s``."""
        now = time.monotonic()
        expired = [
            key
            for key, batch in self._batches.items()
            if now - batch.opened_at >= self._flush_interval_s
        ]
        for key in expired:
            await self._flush(key)

    async def _shutdown_flush(self) -> None:
        """Drain queue → in-memory batches, then write every pending batch."""
        # Step 1: pull any items still in the queue into in-memory batches.
        drained = 0
        while True:
            try:
                tick = self._queue.get_nowait()
                self._add_to_batch(tick)
                drained += 1
            except asyncio.QueueEmpty:
                break
        if drained:
            log.info("writer.drained_queue", n_ticks=drained)

        # Step 2: flush all in-memory batches (including newly drained ones).
        for key in list(self._batches.keys()):
            await self._flush(key)

    async def _flush(self, key: PartitionKey) -> None:
        """Remove batch *key* from memory and write it to disk asynchronously."""
        batch = self._batches.pop(key, None)
        if not batch or not batch.rows:
            return

        exchange, symbol, date = key
        dir_path = _partition_dir(self._root, exchange, symbol, date)
        n = len(batch.rows)

        t0 = time.monotonic()
        loop = asyncio.get_running_loop()
        path = await loop.run_in_executor(
            self._executor, _write_batch_sync, batch.rows, dir_path
        )
        latency = time.monotonic() - t0

        if self._metrics is not None:
            self._metrics.record_ticks_written(exchange, symbol, n)
            self._metrics.observe_write_latency(latency)

        self._files_written += 1
        log.info(
            "writer.flushed",
            exchange=exchange,
            symbol=symbol,
            date=date,
            n_ticks=n,
            path=str(path),
            files_total=self._files_written,
            write_latency_s=round(latency, 4),
        )

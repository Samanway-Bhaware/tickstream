"""Jupyter-style query demo for TickStore.

Runs the storage demo for 30 seconds to populate data, then exercises every
TickStore method and prints the results.

Usage
-----
    uv run python examples/query_demo.py
    uv run python examples/query_demo.py --data ./tick_data   # use existing data
    uv run python examples/query_demo.py --no-collect         # skip data collection

Output
------
The script prints section headers and tables for each TickStore method:
symbols(), trades(), vwap(), bars(), and gaps().
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from tickstream.connectors.binance import BinanceConnector
from tickstream.models import Tick
from tickstream.orchestrator import Orchestrator
from tickstream.query.store import TickStore
from tickstream.storage.parquet_writer import ParquetWriter

_DEFAULT_DATA_DIR = Path("tick_data")
_COLLECT_SECONDS = 30  # short run to gather some data


# ---------------------------------------------------------------------------
# Data collection helpers
# ---------------------------------------------------------------------------


async def _collect(data_dir: Path, seconds: int) -> None:
    """Stream Binance ticks for *seconds*, write to *data_dir*."""
    print(f"Collecting data for {seconds} s → {data_dir.resolve()}")
    queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=50_000)

    connector = BinanceConnector(["btcusdt", "ethusdt"], queue)
    writer = ParquetWriter(queue, root_dir=data_dir, max_batch_size=5_000, flush_interval_s=10.0)
    orch = Orchestrator([connector], queue)

    writer_task = asyncio.create_task(writer.run(), name="writer")
    orch_task = asyncio.create_task(orch.run(), name="orch")

    try:
        await asyncio.wait(
            [orch_task, asyncio.create_task(asyncio.sleep(seconds))],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        if not orch_task.done():
            orch_task.cancel()
        await asyncio.gather(orch_task, return_exceptions=True)
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)

    print(f"  Wrote {writer.files_written} Parquet file(s).\n")


# ---------------------------------------------------------------------------
# Demo sections
# ---------------------------------------------------------------------------


def _section(title: str) -> None:
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print("=" * width)


def _run_demo(data_dir: Path) -> None:
    with TickStore(data_dir) as store:

        # ── 1. symbols ────────────────────────────────────────────────────
        _section("1. symbols() — available exchange:symbol pairs")
        syms = store.symbols()
        if not syms:
            print("  (no data found — run without --no-collect to populate)")
            return
        for s in syms:
            print(f"  {s}")

        # Pick the first symbol for subsequent queries
        first_sym_pair = syms[0]
        exchange, symbol = first_sym_pair.split(":", 1)
        print(f"\n  Using {first_sym_pair} for remaining queries.")

        # ── 2. trades ─────────────────────────────────────────────────────
        _section(f"2. trades('{symbol}', last 60 s)")
        import time

        now_ns = time.time_ns()
        start_ns = now_ns - 60 * 1_000_000_000
        df_trades = store.trades(symbol, start_ns, now_ns, exchange=exchange)
        print(f"  {len(df_trades)} trades in the last 60 s")
        if len(df_trades) > 0:
            print(df_trades.head(5).to_pandas().to_string(index=False))

        # ── 3. vwap ───────────────────────────────────────────────────────
        _section(f"3. vwap('{symbol}', last 60 s)")
        try:
            vwap = store.vwap(symbol, start_ns, now_ns, exchange=exchange)
            print(f"  VWAP = {vwap}")
        except ValueError as exc:
            print(f"  (skipped: {exc})")

        # ── 4. bars ───────────────────────────────────────────────────────
        _section(f"4. bars('{symbol}', last 60 s, interval='10s')")
        df_bars = store.bars(symbol, start_ns, now_ns, interval="10s", exchange=exchange)
        print(f"  {len(df_bars)} bars")
        if len(df_bars) > 0:
            # Add a human-readable bar_start column
            import polars as pl

            df_display = df_bars.with_columns(
                (pl.col("bar_start_ns") // 1_000_000_000)
                .cast(pl.Int64)
                .alias("bar_start_s")
            ).select(["bar_start_s", "open", "high", "low", "close", "volume", "count"])
            print(df_display.to_pandas().to_string(index=False))

        # ── 5. gaps ───────────────────────────────────────────────────────
        _section(f"5. gaps('{symbol}', exchange='{exchange}', max_gap_seconds=5)")
        df_gaps = store.gaps(symbol, exchange, max_gap_seconds=5.0)
        if len(df_gaps) == 0:
            print("  No gaps detected (good — data is continuous).")
        else:
            print(f"  {len(df_gaps)} gap(s) found:")
            print(df_gaps.to_pandas().to_string(index=False))

    print(f"\n{'=' * 60}")
    print("  Demo complete.")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TickStore query demo.")
    parser.add_argument(
        "--data",
        type=Path,
        default=_DEFAULT_DATA_DIR,
        metavar="DIR",
        help=f"Parquet data directory (default: {_DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--no-collect",
        action="store_true",
        help="Skip data collection (use existing data in --data)",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=_COLLECT_SECONDS,
        metavar="N",
        help=f"Collection duration (default: {_COLLECT_SECONDS})",
    )
    args = parser.parse_args()

    data_dir: Path = args.data

    if not args.no_collect:
        asyncio.run(_collect(data_dir, args.seconds))
    else:
        if not data_dir.exists():
            print(
                f"Data directory {data_dir} not found.  "
                "Run without --no-collect to populate it.",
                file=sys.stderr,
            )
            sys.exit(1)

    _run_demo(data_dir)

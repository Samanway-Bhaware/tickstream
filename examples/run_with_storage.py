"""End-to-end demo: Binance + Coinbase → ParquetWriter → directory tree.

Runs both exchange connectors via the Orchestrator for a configurable
duration (default 60 s), writes all received ticks as ZSTD-compressed
Parquet files partitioned by (exchange, symbol, date), then prints a
summary of the resulting directory tree.

Usage
-----
    uv run python examples/run_with_storage.py
    uv run python examples/run_with_storage.py --seconds 30 --out ./tick_data
    uv run python examples/run_with_storage.py --binance btcusdt --no-coinbase

Output layout::

    tick_data/
      exchange=binance/
        symbol=BTCUSDT/
          date=2025-05-20/
            3f2e…abcd.parquet   (  84,231 bytes, 1,042 rows)
        symbol=ETHUSDT/
          …
      exchange=coinbase/
        symbol=BTC-USD/
          …
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import pyarrow.parquet as pq
import structlog

from tickstream.config import get_settings
from tickstream.connectors.base import BaseConnector
from tickstream.connectors.binance import BinanceConnector
from tickstream.connectors.coinbase import CoinbaseConnector
from tickstream.logging import configure_logging
from tickstream.models import Tick
from tickstream.monitoring.metrics import MetricsRegistry
from tickstream.orchestrator import Orchestrator
from tickstream.storage.parquet_writer import ParquetWriter

_DEFAULT_OUT = Path("tick_data")
_DEFAULT_SECONDS = 60


# ---------------------------------------------------------------------------
# Directory-tree summary
# ---------------------------------------------------------------------------


def _print_tree(root: Path) -> None:
    """Print a summary of all Parquet files under *root*."""
    files = sorted(root.rglob("*.parquet"))
    if not files:
        print("  (no files written)")
        return

    total_bytes = 0
    total_rows = 0
    for f in files:
        try:
            meta = pq.read_metadata(f)
            rows = sum(meta.row_group(i).num_rows for i in range(meta.num_row_groups))
            size = f.stat().st_size
            total_bytes += size
            total_rows += rows
            rel = f.relative_to(root)
            print(f"  {rel}  ({size:>10,} bytes, {rows:>7,} rows)")
        except Exception as exc:
            print(f"  {f.relative_to(root)}  [unreadable: {exc}]")

    print(f"\n  Total: {len(files)} files · {total_rows:,} rows · {total_bytes:,} bytes")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main(
    binance_symbols: list[str],
    coinbase_symbols: list[str],
    output_dir: Path,
    duration_s: int,
    metrics_port: int | None,
) -> None:
    settings = get_settings()
    configure_logging(settings)
    log = structlog.get_logger(__name__)

    # Start Prometheus metrics server if a port is configured.
    metrics: MetricsRegistry | None = None
    if metrics_port is not None:
        metrics = MetricsRegistry()
        metrics.start_http_server(metrics_port)
        log.info("metrics.started", port=metrics_port)

    queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=200_000)

    connectors: list[BaseConnector] = []
    if binance_symbols:
        connectors.append(BinanceConnector(binance_symbols, queue, metrics=metrics))
    if coinbase_symbols:
        connectors.append(CoinbaseConnector(coinbase_symbols, queue, metrics=metrics))

    if not connectors:
        print("No connectors configured.  Pass --binance and/or --coinbase.", file=sys.stderr)
        return

    writer = ParquetWriter(
        queue,
        root_dir=output_dir,
        max_batch_size=10_000,
        flush_interval_s=30.0,
        metrics=metrics,
    )

    log.info(
        "demo.starting",
        duration_s=duration_s,
        output_dir=str(output_dir),
        binance=binance_symbols,
        coinbase=coinbase_symbols,
        metrics_port=metrics_port,
    )

    orch = Orchestrator(connectors, queue, metrics=metrics, writer=writer)
    writer_task = asyncio.create_task(writer.run(), name="parquet-writer")
    orch_task = asyncio.create_task(orch.run(), name="orchestrator")

    # ----------------------------------------------------------------
    # Run for `duration_s` seconds, then shut down gracefully.
    # SIGINT (Ctrl-C) before the timer expires is also handled by the
    # orchestrator's signal handler — orch_task will finish early.
    # ----------------------------------------------------------------
    try:
        done, _ = await asyncio.wait(
            [orch_task, asyncio.create_task(asyncio.sleep(duration_s))],
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        pass
    finally:
        # Stop the orchestrator (no-op if it already finished).
        if not orch_task.done():
            orch_task.cancel()
        await asyncio.gather(orch_task, return_exceptions=True)

        # Stop the writer — shutdown flush writes all remaining batches.
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)

    log.info("demo.finished", files_written=writer.files_written)

    # ----------------------------------------------------------------
    # Print the directory tree.
    # ----------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Parquet output: {output_dir.resolve()}")
    print("=" * 60)
    _print_tree(output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stream crypto trades and write them as Parquet."
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=_DEFAULT_SECONDS,
        metavar="N",
        help=f"How long to run (default: {_DEFAULT_SECONDS})",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=_DEFAULT_OUT,
        metavar="DIR",
        help=f"Output directory (default: {_DEFAULT_OUT})",
    )
    parser.add_argument(
        "--binance",
        nargs="*",
        default=["btcusdt", "ethusdt"],
        metavar="SYM",
        help="Binance symbols (default: btcusdt ethusdt)",
    )
    parser.add_argument(
        "--coinbase",
        nargs="*",
        default=["BTC-USD", "ETH-USD"],
        metavar="SYM",
        help="Coinbase symbols (default: BTC-USD ETH-USD)",
    )
    parser.add_argument(
        "--no-coinbase",
        action="store_true",
        help="Disable Coinbase connector",
    )
    parser.add_argument(
        "--no-binance",
        action="store_true",
        help="Disable Binance connector",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        metavar="PORT",
        help="Start Prometheus /metrics server on PORT (default: disabled)",
    )
    args = parser.parse_args()

    binance = [] if args.no_binance else [s.lower() for s in (args.binance or [])]
    coinbase = [] if args.no_coinbase else [s.upper() for s in (args.coinbase or [])]

    print(f"Running for {args.seconds} s → {args.out}")
    if binance:
        print(f"  Binance : {', '.join(binance)}")
    if coinbase:
        print(f"  Coinbase: {', '.join(coinbase)}")
    if args.metrics_port:
        print(f"  Metrics : http://localhost:{args.metrics_port}/metrics")
    print("(Ctrl-C stops early)\n")

    asyncio.run(main(binance, coinbase, args.out, args.seconds, args.metrics_port))

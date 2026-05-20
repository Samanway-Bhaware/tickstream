"""Demo: stream live trades from Binance and/or Coinbase to stdout.

Uses the :class:`~tickstream.orchestrator.Orchestrator` to run connectors
concurrently on a single shared queue.  Press Ctrl-C to stop; the
orchestrator closes all WebSockets cleanly.

Usage
-----
    # Binance BTC + ETH (default)
    uv run python examples/run_binance.py

    # Binance only, custom symbols
    uv run python examples/run_binance.py --binance btcusdt ethusdt solusdt

    # Both exchanges
    uv run python examples/run_binance.py \\
        --binance btcusdt ethusdt \\
        --coinbase BTC-USD ETH-USD

Sample output (with colour in a terminal)::

    [binance ] BTCUSDT    BUY    67432.50000000  ×  0.00123400   id=123456789
    [coinbase] BTC-USD    SELL   67430.00000000  ×  0.01000000   id=abc-9876
"""

from __future__ import annotations

import argparse
import asyncio
import sys

import structlog

from tickstream.config import get_settings
from tickstream.connectors.binance import BinanceConnector
from tickstream.connectors.coinbase import CoinbaseConnector
from tickstream.logging import configure_logging
from tickstream.models import Tick
from tickstream.orchestrator import Orchestrator

_SIDE_COLOUR = {"buy": "\033[32m", "sell": "\033[31m"}
_RESET = "\033[0m"


def _fmt_tick(tick: Tick, *, colour: bool = True) -> str:
    side_str = tick.side.upper().ljust(4)
    if colour:
        side_str = f"{_SIDE_COLOUR.get(tick.side, '')}{side_str}{_RESET}"
    exchange = f"[{tick.exchange:<8}]"
    return (
        f"{exchange} {tick.symbol:<10} {side_str} "
        f"{tick.price:>20}  ×  {tick.size:<18}  id={tick.trade_id}"
    )


async def _consume(queue: asyncio.Queue[Tick], *, colour: bool) -> None:
    while True:
        tick = await queue.get()
        print(_fmt_tick(tick, colour=colour), flush=True)
        queue.task_done()


async def main(
    binance_symbols: list[str],
    coinbase_symbols: list[str],
) -> None:
    settings = get_settings()
    configure_logging(settings)
    log = structlog.get_logger(__name__)

    queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=50_000)
    colour = sys.stdout.isatty()

    connectors = []
    if binance_symbols:
        connectors.append(BinanceConnector(binance_symbols, queue))
        log.info("demo.binance", symbols=binance_symbols)
    if coinbase_symbols:
        connectors.append(CoinbaseConnector(coinbase_symbols, queue))
        log.info("demo.coinbase", symbols=coinbase_symbols)

    if not connectors:
        print("No symbols specified.  Pass --binance and/or --coinbase.", file=sys.stderr)
        return

    consumer = asyncio.create_task(_consume(queue, colour=colour), name="tick-consumer")
    orch = Orchestrator(connectors, queue)

    try:
        await orch.run()  # blocks until Ctrl-C / SIGTERM
    finally:
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        log.info("demo.stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream live crypto trades to stdout.")
    parser.add_argument(
        "--binance",
        nargs="*",
        default=["btcusdt", "ethusdt"],
        metavar="SYMBOL",
        help="Binance symbols, e.g. btcusdt ethusdt (default: btcusdt ethusdt)",
    )
    parser.add_argument(
        "--coinbase",
        nargs="*",
        default=[],
        metavar="SYMBOL",
        help="Coinbase product IDs, e.g. BTC-USD ETH-USD (default: none)",
    )
    args = parser.parse_args()

    binance = [s.lower() for s in (args.binance or [])]
    coinbase = [s.upper() for s in (args.coinbase or [])]

    if binance:
        print(f"Binance  : {', '.join(binance)}")
    if coinbase:
        print(f"Coinbase : {', '.join(coinbase)}")
    print("(Ctrl-C to stop)\n")

    asyncio.run(main(binance, coinbase))

"""Demo: stream live Binance trades to stdout.

Usage
-----
    uv run python examples/run_binance.py
    uv run python examples/run_binance.py btcusdt ethusdt solusdt

Press Ctrl-C to stop.  The connector will close the WebSocket gracefully.

Each received tick is printed as a one-liner, e.g.::

    [binance] BTCUSDT  BUY   67432.50000000  ×  0.00123400   id=123456789
"""

from __future__ import annotations

import asyncio
import sys
from decimal import Decimal

import structlog

from tickstream.config import get_settings
from tickstream.connectors.binance import BinanceConnector
from tickstream.logging import configure_logging
from tickstream.models import Tick

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SIDE_COLOUR = {"buy": "\033[32m", "sell": "\033[31m"}
_RESET = "\033[0m"


def _fmt_tick(tick: Tick, *, colour: bool = True) -> str:
    side_str = tick.side.upper().ljust(4)
    if colour:
        c = _SIDE_COLOUR.get(tick.side, "")
        side_str = f"{c}{side_str}{_RESET}"
    return (
        f"[{tick.exchange}] {tick.symbol:<10} {side_str} "
        f"{tick.price:>20}  ×  {tick.size:<18}  id={tick.trade_id}"
    )


async def _consume(queue: asyncio.Queue[Tick], *, colour: bool) -> None:
    """Drain the queue and print each tick."""
    while True:
        tick = await queue.get()
        print(_fmt_tick(tick, colour=colour), flush=True)
        queue.task_done()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main(symbols: list[str]) -> None:
    settings = get_settings()
    configure_logging(settings)

    log = structlog.get_logger(__name__)
    log.info("demo.starting", symbols=symbols)

    queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=10_000)
    colour = sys.stdout.isatty()

    connector = BinanceConnector(symbols, queue)
    connector_task = asyncio.create_task(connector.run(), name="binance-connector")
    consumer_task = asyncio.create_task(_consume(queue, colour=colour), name="tick-consumer")

    try:
        # Run until one of the tasks finishes unexpectedly or we're interrupted.
        done, pending = await asyncio.wait(
            [connector_task, consumer_task],
            return_when=asyncio.FIRST_EXCEPTION,
        )
        for task in done:
            if exc := task.exception():
                raise exc
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("demo.shutting_down")
    finally:
        connector_task.cancel()
        consumer_task.cancel()
        await asyncio.gather(connector_task, consumer_task, return_exceptions=True)
        log.info("demo.stopped")


if __name__ == "__main__":
    # Default to BTC + ETH if no symbols are supplied on the command line.
    raw_symbols = sys.argv[1:] or ["btcusdt", "ethusdt"]
    symbols = [s.lower() for s in raw_symbols]
    print(f"Subscribing to: {', '.join(symbols)}  (Ctrl-C to stop)\n")
    asyncio.run(main(symbols))

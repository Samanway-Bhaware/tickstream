"""Read-only query API over hive-partitioned Parquet tick data.

The :class:`TickStore` wraps a DuckDB in-memory connection pointed at a
directory written by :class:`~tickstream.storage.parquet_writer.ParquetWriter`.
DuckDB's hive-partitioned Parquet scanning makes ``exchange``, ``symbol``,
and ``date`` queryable as columns while pushing filter predicates down to
individual Parquet row-groups (no full file loads).

Time arguments
--------------
Every method that accepts *start* / *end* accepts any of:

- :class:`~datetime.datetime` (naive → assumed UTC, aware → converted)
- ISO 8601 string (``"2024-05-20T00:00:00Z"`` or ``+00:00`` offset)
- ``int`` nanoseconds since epoch (passed through unchanged)

All time ranges are **half-open** ``[start, end)``.

Example
-------
::

    from tickstream.query.store import TickStore

    with TickStore("tick_data/") as store:
        df = store.trades("BTCUSDT", "2024-05-20T00:00:00Z", "2024-05-20T01:00:00Z")
        vwap = store.vwap("BTCUSDT", "2024-05-20", "2024-05-21", exchange="binance")
        bars = store.bars("BTCUSDT", "2024-05-20", "2024-05-21", interval="5m")
        print(store.symbols())
        gaps = store.gaps("BTCUSDT", "binance", max_gap_seconds=60)
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Union

import duckdb
import polars as pl

# ---------------------------------------------------------------------------
# Type alias for flexible time inputs
# ---------------------------------------------------------------------------

#: Accepted forms for a time boundary: datetime, ISO string, or ns int.
TimeInput = Union[datetime, str, int]

# ---------------------------------------------------------------------------
# Interval helpers
# ---------------------------------------------------------------------------

_INTERVAL_RE: re.Pattern[str] = re.compile(r"^(\d+)(s|m|h|d)$")
_UNIT_TO_NS: dict[str, int] = {
    "s": 1_000_000_000,
    "m": 60 * 1_000_000_000,
    "h": 3_600 * 1_000_000_000,
    "d": 86_400 * 1_000_000_000,
}


def _to_ns(t: TimeInput) -> int:
    """Normalise *t* to nanoseconds since the Unix epoch.

    - ``int``      → returned as-is (assumed already in ns)
    - ``str``      → parsed as ISO 8601; trailing ``Z`` is accepted
    - ``datetime`` → naive datetimes are treated as UTC
    """
    if isinstance(t, int):
        return t
    if isinstance(t, str):
        t = datetime.fromisoformat(t.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    # Use integer arithmetic to avoid float precision loss at second boundaries.
    epoch_s = int(t.timestamp())
    sub_s_ns = t.microsecond * 1_000
    return epoch_s * 1_000_000_000 + sub_s_ns


def _parse_interval(interval: str) -> int:
    """Return nanoseconds for a human interval string like ``'1m'`` or ``'1h'``.

    Supported units: ``s`` (seconds), ``m`` (minutes), ``h`` (hours),
    ``d`` (days).

    Raises
    ------
    ValueError
        If the string does not match ``<int><unit>``.
    """
    m = _INTERVAL_RE.match(interval)
    if not m:
        raise ValueError(
            f"Invalid interval {interval!r}; expected e.g. '1s', '1m', '5m', '1h', '1d'"
        )
    n, unit = int(m.group(1)), m.group(2)
    return n * _UNIT_TO_NS[unit]


# ---------------------------------------------------------------------------
# TickStore
# ---------------------------------------------------------------------------


class TickStore:
    """Read-only query interface over a hive-partitioned Parquet directory.

    Parameters
    ----------
    root_dir:
        Root of the on-disk partition tree produced by
        :class:`~tickstream.storage.parquet_writer.ParquetWriter`.

    Notes
    -----
    A single in-memory DuckDB connection is created at construction and
    closed via :meth:`close` (or the context manager).  All queries use
    DuckDB's native Parquet pushdown — no full files are loaded into Python
    memory.

    The ``exchange``, ``symbol``, and ``date`` hive-partition columns are
    read from the directory names and merged with the file contents by
    DuckDB.  Because the Parquet files already contain ``exchange`` and
    ``symbol`` as data columns, the file columns take precedence for those
    two; ``date`` is added purely from the directory structure.
    """

    def __init__(self, root_dir: Path | str) -> None:
        self._root = Path(root_dir).resolve()
        self._con = duckdb.connect()
        glob = str(self._root / "**" / "*.parquet")
        # Create a persistent view so every query can simply say FROM ticks.
        # hive_partitioning=true adds the `date` column from directory names
        # and enables partition pruning.  Fall back to an empty typed view
        # when no Parquet files exist yet (valid for a fresh data directory).
        try:
            self._con.execute(
                f"""
                CREATE OR REPLACE VIEW ticks AS
                SELECT * FROM read_parquet('{glob}', hive_partitioning = true)
                """
            )
        except duckdb.IOException:
            # No Parquet files found — create an empty view with the correct schema.
            self._con.execute(
                """
                CREATE OR REPLACE VIEW ticks AS
                SELECT
                    NULL::VARCHAR      AS exchange,
                    NULL::VARCHAR      AS symbol,
                    NULL::DECIMAL(38,18) AS price,
                    NULL::DECIMAL(38,18) AS size,
                    NULL::VARCHAR      AS side,
                    NULL::BIGINT       AS timestamp_ns,
                    NULL::BIGINT       AS received_ns,
                    NULL::VARCHAR      AS trade_id,
                    NULL::VARCHAR      AS date
                WHERE FALSE
                """
            )

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "TickStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._con.close()

    # ------------------------------------------------------------------
    # Public query methods
    # ------------------------------------------------------------------

    def trades(
        self,
        symbol: str,
        start: TimeInput,
        end: TimeInput,
        exchange: str | None = None,
    ) -> pl.DataFrame:
        """Return every trade for *symbol* in ``[start, end)`` sorted by time.

        Parameters
        ----------
        symbol:
            Trading-pair identifier as stored, e.g. ``'BTCUSDT'``.
        start:
            Inclusive lower bound (see module docstring for accepted forms).
        end:
            Exclusive upper bound.
        exchange:
            If given, restrict to this exchange (e.g. ``'binance'``).

        Returns
        -------
        pl.DataFrame
            Columns match :data:`~tickstream.storage.parquet_writer.TICK_SCHEMA`
            plus ``date`` (the hive partition date string).
        """
        start_ns, end_ns = _to_ns(start), _to_ns(end)
        where, params = _build_where(symbol, start_ns, end_ns, exchange)
        return self._con.execute(
            f"SELECT * FROM ticks WHERE {where} ORDER BY timestamp_ns",
            params,
        ).pl()

    def vwap(
        self,
        symbol: str,
        start: TimeInput,
        end: TimeInput,
        exchange: str | None = None,
    ) -> Decimal:
        """Volume-weighted average price for *symbol* over ``[start, end)``.

        Parameters
        ----------
        symbol, start, end, exchange:
            Same semantics as :meth:`trades`.

        Returns
        -------
        Decimal
            VWAP with full Decimal precision.

        Raises
        ------
        ValueError
            If no trades exist in the specified range.
        """
        start_ns, end_ns = _to_ns(start), _to_ns(end)
        where, params = _build_where(symbol, start_ns, end_ns, exchange)
        row = self._con.execute(
            # Cast to DOUBLE for arithmetic; convert result back to Decimal.
            f"""
            SELECT SUM(CAST(price AS DOUBLE) * CAST(size AS DOUBLE))
                   / NULLIF(SUM(CAST(size AS DOUBLE)), 0)
            FROM ticks
            WHERE {where}
            """,
            params,
        ).fetchone()
        if row is None or row[0] is None:
            raise ValueError(
                f"No trades found for symbol={symbol!r}, "
                f"start={start!r}, end={end!r}"
                + (f", exchange={exchange!r}" if exchange else "")
            )
        return Decimal(str(row[0]))

    def bars(
        self,
        symbol: str,
        start: TimeInput,
        end: TimeInput,
        interval: str = "1m",
        exchange: str | None = None,
    ) -> pl.DataFrame:
        """OHLCV bars for *symbol* bucketed into *interval*-wide windows.

        Parameters
        ----------
        symbol, start, end, exchange:
            Same semantics as :meth:`trades`.
        interval:
            Bar width as a string: ``'1s'``, ``'1m'``, ``'5m'``, ``'1h'``,
            ``'1d'``, etc.  Bars are aligned to multiples of the interval
            from the Unix epoch (same as most exchange bar APIs).

        Returns
        -------
        pl.DataFrame
            Columns:

            - ``bar_start_ns`` — bar open time in nanoseconds (``Int64``)
            - ``open``  — first trade price in the bar
            - ``high``  — highest trade price
            - ``low``   — lowest trade price
            - ``close`` — last trade price
            - ``volume``— sum of trade sizes (base-currency volume)
            - ``count`` — number of trades in the bar
        """
        interval_ns = _parse_interval(interval)
        start_ns, end_ns = _to_ns(start), _to_ns(end)
        where, params = _build_where(symbol, start_ns, end_ns, exchange)
        return self._con.execute(
            f"""
            SELECT
                (timestamp_ns // {interval_ns}) * {interval_ns}  AS bar_start_ns,
                min_by(CAST(price AS DOUBLE), timestamp_ns)       AS open,
                MAX(CAST(price AS DOUBLE))                        AS high,
                MIN(CAST(price AS DOUBLE))                        AS low,
                max_by(CAST(price AS DOUBLE), timestamp_ns)       AS close,
                SUM(CAST(size  AS DOUBLE))                        AS volume,
                COUNT(*)                                          AS count
            FROM ticks
            WHERE {where}
            GROUP BY bar_start_ns
            ORDER BY bar_start_ns
            """,
            params,
        ).pl()

    def symbols(self) -> list[str]:
        """Return a sorted list of all ``'exchange:symbol'`` pairs in the store."""
        rows = self._con.execute(
            "SELECT DISTINCT exchange, symbol FROM ticks ORDER BY exchange, symbol"
        ).fetchall()
        return [f"{exc}:{sym}" for exc, sym in rows]

    def gaps(
        self,
        symbol: str,
        exchange: str,
        max_gap_seconds: float = 60.0,
    ) -> pl.DataFrame:
        """Detect time ranges longer than *max_gap_seconds* with no trades.

        Useful for data-quality checks: any gap longer than the expected
        maximum inter-trade interval indicates missing data.

        Parameters
        ----------
        symbol:
            Trading-pair identifier as stored.
        exchange:
            Exchange identifier as stored.
        max_gap_seconds:
            Report gaps strictly longer than this many seconds.

        Returns
        -------
        pl.DataFrame
            Columns:

            - ``gap_start_ns``  — ns timestamp of the trade just *before* the gap
            - ``gap_end_ns``    — ns timestamp of the trade just *after* the gap
            - ``gap_seconds``   — gap duration in seconds (float)

            Ordered by ``gap_start_ns``.  Empty DataFrame if no gaps found.
        """
        max_gap_ns = int(max_gap_seconds * 1_000_000_000)
        return self._con.execute(
            f"""
            WITH ordered AS (
                SELECT
                    timestamp_ns,
                    LAG(timestamp_ns) OVER (ORDER BY timestamp_ns) AS prev_ns
                FROM ticks
                WHERE symbol = ? AND exchange = ?
            )
            SELECT
                prev_ns                              AS gap_start_ns,
                timestamp_ns                         AS gap_end_ns,
                (timestamp_ns - prev_ns) / 1e9       AS gap_seconds
            FROM ordered
            WHERE prev_ns IS NOT NULL
              AND (timestamp_ns - prev_ns) > {max_gap_ns}
            ORDER BY gap_start_ns
            """,
            [symbol, exchange],
        ).pl()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _build_where(
    symbol: str,
    start_ns: int,
    end_ns: int,
    exchange: str | None,
) -> tuple[str, list[object]]:
    """Build a parameterised WHERE clause for time-range queries."""
    clause = "symbol = ? AND timestamp_ns >= ? AND timestamp_ns < ?"
    params: list[object] = [symbol, start_ns, end_ns]
    if exchange is not None:
        clause += " AND exchange = ?"
        params.append(exchange)
    return clause, params

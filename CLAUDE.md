# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All commands use `uv`. Install uv from https://github.com/astral-sh/uv if absent.

```bash
# Install all dependencies (including dev extras)
uv sync --extra dev

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_binance_connector.py

# Run a single test by name
uv run pytest tests/test_binance_connector.py::TestParseTrade::test_buy_side_when_buyer_is_taker

# Lint (with auto-fix)
uv run ruff check --fix src tests

# Format
uv run ruff format src tests

# Type-check (strict)
uv run mypy

# Install pre-commit hooks
uv run pre-commit install

# Run the connector demo (prints ticks to stdout)
uv run python examples/run_binance.py
uv run python examples/run_binance.py --binance btcusdt solusdt --coinbase BTC-USD ETH-USD

# Run the storage demo (60 s, writes Parquet, prints tree)
uv run python examples/run_with_storage.py
uv run python examples/run_with_storage.py --seconds 30 --out ./tick_data --no-coinbase
```

## Architecture

### Connector abstraction (`src/tickstream/connectors/`)

The central pattern is a three-layer design:

**`base.py` — `BaseConnector` (ABC)**
Owns the reconnect loop, queue pushing, and lifecycle. Subclasses plug in three abstract methods: `_url()`, `_subscribe_message(symbols)`, and `_parse_message(raw, received_ns)`. The reconnect loop lives entirely in `BaseConnector.run()` / `_consume()`, which means all lifecycle tests must patch `tickstream.connectors.base.websockets.connect` and `tickstream.connectors.base.asyncio.sleep` — **not** the subclass module.

`_parse_message` must be implemented as a **generator** (`yield` each tick, use bare `return` to skip non-trade frames). The base class calls it inside a `for tick in self._parse_message(...)` loop wrapped in a single `try/except`, so any exception raised during parsing (including `json.loads` errors) is caught and logged without crashing.

**`binance.py` — `BinanceConnector`**
URL-based subscription (no post-connect frame). Single-symbol endpoint (`/ws/<sym>@trade`) produces bare trade objects; multi-symbol (`/stream?streams=…`) wraps each in `{"stream": …, "data": {…}}`. Both are handled by `payload.get("data", payload)`. Symbols are normalised to lowercase.

**`coinbase.py` — `CoinbaseConnector`**
Post-connect JSON subscription frame (sent via `ws.send()`). Messages carry a `channel` field; only `"market_trades"` events are parsed. Each message can contain multiple trades nested under `events[].trades[]`. Timestamp format is ISO 8601 with up to nanosecond precision — `_iso_to_ns()` handles this manually because `datetime.fromisoformat` only handles microseconds. Symbols are normalised to uppercase (`BTC-USD` style).

### `Orchestrator` (`src/tickstream/orchestrator.py`)

Wraps an `asyncio.TaskGroup` over a list of `BaseConnector` instances sharing one queue. SIGINT/SIGTERM are handled by setting an `asyncio.Event` which causes the orchestrator to call `task.cancel()` on each connector task. Because `BaseConnector.run()` catches `CancelledError` internally and returns `None`, the tasks complete normally from the TaskGroup's perspective. External cancellation of the orchestrator task itself propagates through the TaskGroup and raises `CancelledError` to the caller.

### Domain model (`src/tickstream/models.py`)

`Tick` is a frozen Pydantic v2 model. `price` and `size` reject `float` outright — pass strings or `Decimal`. Both timestamps are validated: floor is 2010-01-01 in ns, ceiling is `time.time_ns() + 10 years`. A cross-field validator allows up to 1 s of `received_ns < timestamp_ns` for clock skew.

### Configuration (`src/tickstream/config.py`)

`pydantic-settings` reads from env vars (`TICKSTREAM_*` prefix), `.env`, and an optional `config.toml` in the working directory. Call `get_settings()` once at startup. `configure_logging(settings)` wires structlog + stdlib root logger; use `log_format="json"` in production.

### Storage layer (`src/tickstream/storage/parquet_writer.py`)

`ParquetWriter` is an async consumer that batches ticks by `(exchange, symbol, date_utc)` partition key and flushes via two triggers: **size** (`max_batch_size`, default 10 000) and **time** (`flush_interval_s`, default 30 s, polled every 1 s via `asyncio.wait_for` timeout). The main loop runs `asyncio.wait_for(queue.get(), timeout=1.0)` so the time check fires even during quiet periods.

**Writes are non-blocking**: the actual `pq.write_table()` call is dispatched to a `ThreadPoolExecutor` via `loop.run_in_executor`, keeping the event loop free.

**Atomic writes**: `_write_batch_sync` writes to `<uuid>.parquet.tmp`, then calls `Path.replace()` (wraps `os.replace`, atomic on POSIX). No `.tmp` file is ever left visible after a flush.

**Shutdown guarantee**: `run()` catches `CancelledError`, drains any remaining queue items into in-memory batches in its `finally` block, then flushes all batches before returning. No ticks are lost on clean shutdown.

**Schema**: `TICK_SCHEMA` (module-level `pa.Schema`) uses `decimal128(38, 18)` for `price`/`size` and `int64` for both timestamp fields. Every file must be validated against this schema with `schema.equals(TICK_SCHEMA, check_metadata=False)`.

The `_ticks_to_table()` and `_write_batch_sync()` helpers are module-level functions (not methods) so they can be passed to `run_in_executor` and tested independently.

### Testing conventions

- Shared fake WebSocket classes (`FakeWebSocket`, `HangingWebSocket`, `make_connect`) live in `tests/_helpers.py` — import directly, they are not pytest fixtures.
- Backoff tests use `jitter_factor=0.0` to get deterministic sleep values.
- The correct patch targets for any `BaseConnector` subclass lifecycle test are `tickstream.connectors.base.websockets.connect` and `tickstream.connectors.base.asyncio.sleep`.
- Orchestrator tests override `run()` entirely in `_MockConnector` to avoid network I/O.
- Storage tests use `max_batch_size` and `flush_interval_s` as control knobs; set `flush_interval_s=3600.0` to disable time-based flushes and `flush_interval_s=0.1` to force them quickly. Use `writer.files_written` to poll for flush completion in async tests instead of fixed `asyncio.sleep` delays.

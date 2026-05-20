# tickstream

High-throughput cryptocurrency tick data ingestion and storage.

`tickstream` connects to exchange WebSocket feeds, normalises raw trade messages
into a typed `Tick` record (exact `Decimal` price/size, nanosecond timestamps),
and persists them to a configurable storage backend (e.g. TimescaleDB, Parquet).

## Features

- **Exact arithmetic** — `price` and `size` are always `Decimal`; floats are
  rejected at the model boundary.
- **Nanosecond timestamps** — both exchange-reported and local receipt times,
  with sanity checks.
- **Structured logging** — JSON in production, human-readable in dev, via
  `structlog`.
- **Pydantic v2 config** — all settings loaded from env vars
  (`TICKSTREAM_*`) and an optional `config.toml`.
- **Strict typing** — `mypy --strict` passes on all source files.

## Project layout

```
src/tickstream/
├── models.py        # Tick and related domain models
├── config.py        # Application settings (pydantic-settings)
├── logging.py       # structlog configuration
├── connectors/      # Exchange WebSocket / REST adapters
├── storage/         # Storage backends
├── query/           # Query and aggregation helpers
├── monitoring/      # Metrics and health checks
└── cli/             # Command-line entry points
```

## Quick start

```bash
# Install uv (https://github.com/astral-sh/uv) then:
uv sync --extra dev

# Run tests
uv run pytest

# Lint + format
uv run ruff check src tests
uv run ruff format src tests

# Type-check
uv run mypy

# Install pre-commit hooks (runs ruff + mypy on every commit)
uv run pre-commit install
```

## Configuration

| Env var | Default | Description |
|---|---|---|
| `TICKSTREAM_LOG_LEVEL` | `INFO` | Logging level |
| `TICKSTREAM_LOG_FORMAT` | `console` | `json` or `console` |
| `TICKSTREAM_DB_URL` | `postgresql://localhost:5432/tickstream` | Database URL |
| `TICKSTREAM_INGESTOR_BUFFER_SIZE` | `10000` | Tick buffer before flush |
| `TICKSTREAM_INGESTOR_FLUSH_INTERVAL_MS` | `500` | Max ms between flushes |
| `TICKSTREAM_METRICS_PORT` | `9090` | Prometheus metrics port |

All variables can also be set in a `config.toml` at the working directory root.

## Tick model

```python
from tickstream.models import Tick
from decimal import Decimal
import time

tick = Tick(
    exchange="binance",
    symbol="BTC-USDT",
    price="67432.50",   # str or Decimal — never float
    size="0.01234",
    side="buy",
    timestamp_ns=time.time_ns(),
    received_ns=time.time_ns(),
    trade_id="12345678",
)
```

## Status

Early development — connectors, storage backends, and CLI are stubs awaiting
implementation.

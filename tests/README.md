# Test suite

## Layout

```
tests/
├── conftest.py              # Shared fixtures and --runslow flag
├── factories.py             # make_ticks / make_validated_ticks helpers
├── unit/
│   └── test_parquet_writer.py   # Fast unit tests (≤ 50 ticks each)
├── integration/
│   └── test_writer_load.py      # 100k-tick load test (@pytest.mark.slow)
├── test_binance_connector.py
├── test_coinbase_connector.py
├── test_orchestrator.py
├── test_store.py
└── test_metrics.py
```

## Running tests

### Fast suite (default)

Excludes any test marked `@pytest.mark.slow`. Completes in under 5 seconds.

```bash
uv run pytest
```

### Full suite (includes slow / load tests)

```bash
uv run pytest --runslow
```

### Single file or test

```bash
uv run pytest tests/unit/test_parquet_writer.py
uv run pytest tests/unit/test_parquet_writer.py::TestFlushTriggers::test_flush_triggers_at_batch_size_threshold
```

### Timing breakdown

```bash
uv run pytest --durations=10
```

No individual unit test should appear above 200 ms in this output.

## Markers

| Marker        | Meaning                                      | Included by default |
|---------------|----------------------------------------------|---------------------|
| `slow`        | Long-running tests (100k-tick load, etc.)    | No — use `--runslow` |
| `integration` | End-to-end pipeline tests                    | Yes (unless also `slow`) |

## Factories

`tests/factories.py` exposes:

- **`make_tick(**overrides)`** — single `Tick` via `model_construct` (no validation, fast).
- **`make_ticks(n, *, date, symbol, exchange, start_price, **overrides)`** — list of *n* ticks with deterministic, reproducible timestamps derived from `date`. Primary workhorse for unit tests.
- **`make_validated_ticks(n, **kwargs)`** — list of *n* ticks through the real `Tick(...)` constructor. Use only when you need Pydantic validation to run (e.g. testing rejection of bad inputs).

## Parallelism

Install [pytest-xdist](https://github.com/pytest-dev/pytest-xdist) for parallel test execution:

```bash
uv add --dev pytest-xdist
uv run pytest -n auto
```

Not configured by default to keep CI dependencies minimal.

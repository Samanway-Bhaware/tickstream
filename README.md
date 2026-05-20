# tickstream

High-throughput cryptocurrency tick data ingestion, storage, and analytics pipeline.

`tickstream` connects to exchange WebSocket feeds in real time, normalises every raw trade message into a strongly-typed `Tick` record (exact `Decimal` price/size, dual nanosecond timestamps), persists ticks to ZSTD-compressed partitioned Parquet files, and exposes a full observability stack — structured logs, Prometheus metrics, and Grafana dashboards — with zero external database dependencies.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Methodology](#methodology)
3. [Project Layout](#project-layout)
4. [Quick Start (local)](#quick-start-local)
5. [Running with Docker Compose](#running-with-docker-compose)
6. [Configuration](#configuration)
7. [Output: Parquet Files](#output-parquet-files)
8. [Querying the Data](#querying-the-data)
9. [Metrics](#metrics)
10. [Grafana Dashboards](#grafana-dashboards)
11. [Testing](#testing)

---

## Architecture

```
                          ┌─────────────────────────────────────────┐
                          │              Orchestrator                │
                          │  (asyncio.TaskGroup + SIGINT/SIGTERM)    │
                          └───────────────┬─────────────────────────┘
                                          │ supervises
               ┌──────────────────────────┼──────────────────────────┐
               │                          │                          │
    ┌──────────▼──────────┐   ┌──────────▼──────────┐    (N more…)
    │  BinanceConnector   │   │  CoinbaseConnector  │
    │  websockets client  │   │  websockets client  │
    │  reconnect + backoff│   │  reconnect + backoff│
    └──────────┬──────────┘   └──────────┬──────────┘
               │   Tick objects           │
               └──────────┬───────────────┘
                           │  asyncio.Queue[Tick]  (maxsize 200 000)
                           ▼
               ┌───────────────────────┐
               │     ParquetWriter     │
               │  batches by partition │
               │  flush on size or time│
               │  atomic .tmp → rename │
               └───────────┬───────────┘
                           │
               ┌───────────▼───────────┐
               │   tick_data/ tree     │
               │  exchange=X/          │
               │    symbol=Y/          │
               │      date=YYYY-MM-DD/ │
               │        *.parquet      │
               └───────────────────────┘
                           │
               ┌───────────▼───────────┐
               │       TickStore       │
               │  DuckDB in-memory     │
               │  hive-partition scan  │
               │  Polars DataFrames    │
               └───────────────────────┘

   Metrics plane (parallel, non-blocking):
   ┌──────────────────┐    scrapes    ┌─────────────┐   visualises  ┌─────────┐
   │  /metrics :9090  │ ◄──────────── │  Prometheus │ ◄──────────── │ Grafana │
   │  prometheus_client│              │  :9091      │               │  :3000  │
   └──────────────────┘               └─────────────┘               └─────────┘
```

### Key design decisions

| Concern | Decision | Rationale |
|---------|----------|-----------|
| Decimal arithmetic | `Decimal` everywhere; `float` rejected at model boundary | No precision loss on crypto prices with many decimal places |
| Timestamps | Dual `int64` nanosecond fields (`timestamp_ns`, `received_ns`) | Enables latency measurement and clock-skew detection |
| Concurrency | Single `asyncio` event loop; all I/O is non-blocking | One process saturates a gigabit feed without threads |
| Storage format | Partitioned Parquet (ZSTD) | Columnar pushdown via DuckDB; excellent compression; no server needed |
| Atomic writes | Write to `<uuid>.parquet.tmp`, then `os.replace()` | No partial or corrupt files ever visible on POSIX |
| Reconnection | Exponential backoff (1 s → 30 s) with 25 % jitter | Avoids thundering-herd on exchange-side restarts |
| Observability | Prometheus + Grafana provisioned via Docker Compose | Zero manual setup; dashboards load on first boot |

---

## Methodology

### 1. Ingestion

Each `BaseConnector` subclass implements three abstract methods:

- `_url()` — the WebSocket URI
- `_subscribe_message(symbols)` — an optional post-connect JSON frame (`None` for URL-based subscriptions like Binance)
- `_parse_message(raw, received_ns)` — a **generator** that yields zero or more `Tick` objects per raw frame

The base class owns the reconnect loop, queue pushing, and lifecycle. Any exception during parsing is caught and logged; it never crashes the connector.

### 2. Tick validation

Every `Tick` is a frozen Pydantic v2 model with:

- `price` and `size` — `Decimal`; floats rejected; must be positive
- `timestamp_ns` / `received_ns` — validated to be within `[2010-01-01, now + 10 years]`
- Cross-field check: `received_ns` must not be more than 1 second before `timestamp_ns` (clock skew tolerance)

### 3. Storage

`ParquetWriter` consumes `Tick` objects from the shared queue and flushes them to disk when **either** condition is met:

- **Size trigger**: 10 000 ticks accumulated for a `(exchange, symbol, date_utc)` partition
- **Time trigger**: 30 seconds have elapsed since the first tick in a batch

Writes run in a `ThreadPoolExecutor` via `loop.run_in_executor`, keeping the event loop free. On shutdown (`CancelledError`), the writer drains the queue and flushes all remaining batches before returning — **no ticks are lost**.

### 4. Querying

`TickStore` wraps a DuckDB in-memory connection that opens a persistent view over the entire hive-partitioned Parquet tree. DuckDB pushes filter predicates down to row-group level — queries over a single symbol and date range read only the relevant files and row-groups.

---

## Project Layout

```
.
├── src/tickstream/
│   ├── models.py              # Tick domain model (frozen Pydantic v2)
│   ├── config.py              # Settings (pydantic-settings, env / .env / config.toml)
│   ├── logging.py             # structlog configuration helper
│   ├── orchestrator.py        # asyncio.TaskGroup supervisor + signal handling
│   ├── connectors/
│   │   ├── base.py            # BaseConnector — reconnect loop, backoff, queue push
│   │   ├── binance.py         # BinanceConnector — URL subscription, trade parser
│   │   └── coinbase.py        # CoinbaseConnector — frame subscription, ISO ns parser
│   ├── storage/
│   │   └── parquet_writer.py  # ParquetWriter — batch flush, atomic write, executor
│   ├── query/
│   │   └── store.py           # TickStore — DuckDB/Polars query API
│   └── monitoring/
│       └── metrics.py         # MetricsRegistry — Prometheus counters/gauges/histograms
├── examples/
│   ├── run_with_storage.py    # End-to-end demo (connectors + writer + tree summary)
│   └── query_demo.py          # TickStore query demo (trades, vwap, bars, gaps)
├── tests/
│   ├── conftest.py            # Shared fixtures and --runslow flag
│   ├── factories.py           # make_ticks() / make_validated_ticks() helpers
│   ├── unit/                  # Fast unit tests (< 200 ms each)
│   └── integration/           # Slow load tests (opt-in via --runslow)
├── monitoring/
│   ├── prometheus.yml         # Prometheus scrape config
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/prometheus.yml
│       │   └── dashboards/dashboards.yml
│       └── dashboards/tickstream.json
├── Dockerfile
├── docker-compose.yml
└── pyproject.toml
```

---

## Quick Start (local)

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (`pip install uv` or `brew install uv`)

### Install

```bash
git clone <repo-url>
cd "Tick data ingestion"
uv sync --extra dev
```

### Run the ingestion + storage demo

```bash
# Default: Binance BTC/ETH + Coinbase BTC/ETH, runs 60 s, writes to ./tick_data
uv run python examples/run_with_storage.py

# Custom symbols and duration
uv run python examples/run_with_storage.py \
    --binance btcusdt solusdt ethusdt \
    --coinbase BTC-USD ETH-USD \
    --seconds 120 \
    --out ./my_data

# Binance only, 30 seconds, with Prometheus metrics on port 9090
uv run python examples/run_with_storage.py \
    --no-coinbase --seconds 30 --metrics-port 9090
```

After the run, the script prints a directory tree summary:

```
============================================================
Parquet output: /path/to/tick_data
============================================================
  exchange=binance/symbol=BTCUSDT/date=2025-05-20/3f2e…abcd.parquet  (  84,231 bytes,   1,042 rows)
  exchange=binance/symbol=ETHUSDT/date=2025-05-20/7a1c…ef01.parquet  (  71,048 bytes,     887 rows)
  exchange=coinbase/symbol=BTC-USD/date=2025-05-20/2b3d…cd45.parquet (  22,310 bytes,     201 rows)

  Total: 3 files · 2,130 rows · 177,589 bytes
```

### Run the query demo

```bash
# Collect 30 s of data then run all query methods
uv run python examples/query_demo.py

# Use existing data (skip collection)
uv run python examples/query_demo.py --no-collect --data ./tick_data
```

---

## Running with Docker Compose

Docker Compose starts three services:

| Service | Host port | Description |
|---------|-----------|-------------|
| `tickstream` | 9090 | Ingestion pipeline + Prometheus `/metrics` |
| `prometheus` | 9091 | Prometheus UI + TSDB |
| `grafana` | 3000 | Grafana dashboards (no login) |

### Build and start

```bash
docker compose up --build
```

### Start in background

```bash
docker compose up --build -d
docker compose logs -f tickstream   # follow pipeline logs
```

### Stop (data is preserved in named volumes)

```bash
docker compose down
```

### Wipe all data and volumes

```bash
docker compose down -v
```

### Override defaults via environment

```bash
TICKSTREAM_LOG_LEVEL=DEBUG docker compose up
```

Or edit `docker-compose.yml` to change `TICKSTREAM_LOG_FORMAT`, symbols, or duration. The `CMD` in the `Dockerfile` can be overridden per service:

```yaml
services:
  tickstream:
    command: >
      uv run python examples/run_with_storage.py
      --binance btcusdt solusdt
      --no-coinbase
      --seconds 86400
      --metrics-port 9090
```

---

## Configuration

Settings are resolved in this priority order:

1. Environment variables (prefix `TICKSTREAM_`)
2. `.env` file in the working directory
3. `config.toml` in the working directory
4. Defaults below

| Variable | Default | Description |
|----------|---------|-------------|
| `TICKSTREAM_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `TICKSTREAM_LOG_FORMAT` | `console` | `json` (production) or `console` (development) |
| `TICKSTREAM_METRICS_PORT` | `9090` | Prometheus `/metrics` HTTP port |
| `TICKSTREAM_METRICS_ENABLED` | `true` | Enable/disable metrics entirely |
| `TICKSTREAM_INGESTOR_BUFFER_SIZE` | `10000` | Ticks per partition batch before size-flush |
| `TICKSTREAM_INGESTOR_FLUSH_INTERVAL_MS` | `500` | Max ms between time-based flushes |
| `TICKSTREAM_DB_URL` | `postgresql://localhost:5432/tickstream` | Reserved for future SQL backend |

Example `config.toml`:

```toml
log_level = "DEBUG"
log_format = "json"
metrics_port = 9090
```

---

## Output: Parquet Files

### Directory structure (hive partitioning)

```
tick_data/
  exchange=binance/
    symbol=BTCUSDT/
      date=2025-05-20/
        3f2eabcd-….parquet
        9a7bcdef-….parquet   ← each flush produces a new UUID-named file
    symbol=ETHUSDT/
      date=2025-05-20/
        …
  exchange=coinbase/
    symbol=BTC-USD/
      date=2025-05-20/
        …
```

### Schema

Every file is guaranteed to match this schema (`TICK_SCHEMA`):

| Column | Arrow type | Description |
|--------|-----------|-------------|
| `exchange` | `string` | Exchange identifier, e.g. `binance`, `coinbase` |
| `symbol` | `string` | Trading pair, e.g. `BTCUSDT`, `BTC-USD` |
| `price` | `decimal128(38, 18)` | Exact trade price (18 decimal places) |
| `size` | `decimal128(38, 18)` | Exact trade quantity (18 decimal places) |
| `side` | `string` | `buy` or `sell` (aggressor side) |
| `timestamp_ns` | `int64` | Exchange-reported trade time (nanoseconds since epoch) |
| `received_ns` | `int64` | Local receipt time (nanoseconds since epoch) |
| `trade_id` | `string` | Exchange-assigned trade identifier |

Compression: **ZSTD** (level default). Atomic writes ensure no partial files are ever visible.

---

## Querying the Data

### Using TickStore (Python API)

```python
from tickstream.query.store import TickStore

with TickStore("tick_data/") as store:

    # 1. List all available exchange:symbol pairs
    print(store.symbols())
    # ['binance:BTCUSDT', 'binance:ETHUSDT', 'coinbase:BTC-USD']

    # 2. Raw trades — returns a Polars DataFrame
    df = store.trades(
        "BTCUSDT",
        start="2025-05-20T00:00:00Z",
        end="2025-05-20T01:00:00Z",
        exchange="binance",          # optional; omit to query all exchanges
    )
    print(df.head())

    # 3. Volume-weighted average price — returns Decimal
    vwap = store.vwap("BTCUSDT", "2025-05-20T00:00:00Z", "2025-05-20T01:00:00Z")
    print(f"VWAP: {vwap}")

    # 4. OHLCV bars — interval supports: 1s, 5s, 1m, 5m, 15m, 1h, 1d
    bars = store.bars("BTCUSDT", "2025-05-20T00:00:00Z", "2025-05-20T01:00:00Z", interval="1m")
    print(bars)
    # shape: (60, 7)  columns: bar_start_ns, open, high, low, close, volume, count

    # 5. Gap detection — find periods with no trades longer than N seconds
    gaps = store.gaps("BTCUSDT", exchange="binance", max_gap_seconds=5.0)
    if gaps.is_empty():
        print("No gaps — data is continuous.")
    else:
        print(gaps)   # columns: gap_start_ns, gap_end_ns, gap_seconds
```

### Time input formats

All `start` / `end` arguments accept any of:

```python
# ISO 8601 string (Z or +00:00 suffix)
store.trades("BTCUSDT", "2025-05-20T00:00:00Z", "2025-05-20T01:00:00Z")

# datetime object (naive = UTC, aware = converted)
from datetime import datetime, timezone
store.trades("BTCUSDT", datetime(2025, 5, 20, tzinfo=timezone.utc), ...)

# Raw nanoseconds integer
store.trades("BTCUSDT", 1_747_699_200_000_000_000, ...)
```

All time ranges are **half-open** `[start, end)`.

### Using DuckDB / PyArrow directly

```python
import duckdb

con = duckdb.connect()
con.execute("""
    CREATE VIEW ticks AS
    SELECT * FROM read_parquet('tick_data/**/*.parquet', hive_partitioning=true)
""")

# Aggregate ticks per minute
con.execute("""
    SELECT
        date_trunc('minute', to_timestamp(timestamp_ns / 1e9)) AS minute,
        exchange,
        symbol,
        COUNT(*) AS trade_count,
        AVG(CAST(price AS DOUBLE)) AS avg_price
    FROM ticks
    WHERE symbol = 'BTCUSDT'
    GROUP BY 1, 2, 3
    ORDER BY 1
""").df()
```

### Using PyArrow directly

```python
import pyarrow.parquet as pq

# Read one partition
table = pq.read_table(
    "tick_data/exchange=binance/symbol=BTCUSDT/date=2025-05-20/",
    columns=["timestamp_ns", "price", "size", "side"],
)
print(table.to_pandas())
```

---

## Metrics

The pipeline exposes a Prometheus `/metrics` endpoint (default port **9090**).

### Metric reference

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `tickstream_msgs_received_total` | Counter | `exchange` | Raw WebSocket frames received |
| `tickstream_msgs_parsed_total` | Counter | `exchange`, `symbol` | Trade ticks successfully parsed |
| `tickstream_parse_errors_total` | Counter | `exchange` | Message parse failures |
| `tickstream_queue_depth` | Gauge | — | Ticks currently in the shared in-memory queue |
| `tickstream_ticks_written_total` | Counter | `exchange`, `symbol` | Ticks persisted to Parquet |
| `tickstream_write_latency_seconds` | Histogram | — | Time from batch-flush start to Parquet write completion |
| `tickstream_last_message_age_seconds` | Gauge | `exchange`, `symbol` | Seconds since last tick arrived (feed staleness) |

### Scraping manually

```bash
curl http://localhost:9090/metrics
```

### Useful PromQL queries

```promql
# Tick ingestion rate per exchange (per second, 1-minute window)
rate(tickstream_msgs_parsed_total[1m])

# Parse error rate (should be near zero)
rate(tickstream_parse_errors_total[1m])

# Queue depth (alert if growing — writer can't keep up)
tickstream_queue_depth

# Feed staleness per symbol (alert if > 60 s — feed may be down)
tickstream_last_message_age_seconds

# Write throughput (ticks/sec flushed to disk)
rate(tickstream_ticks_written_total[1m])

# Parquet write latency percentiles
histogram_quantile(0.50, rate(tickstream_write_latency_seconds_bucket[5m]))
histogram_quantile(0.95, rate(tickstream_write_latency_seconds_bucket[5m]))
histogram_quantile(0.99, rate(tickstream_write_latency_seconds_bucket[5m]))

# Reconnect rate (spikes indicate exchange instability)
rate(tickstream_reconnects_total[5m])
```

---

## Grafana Dashboards

Open [http://localhost:3000](http://localhost:3000) — no login required (anonymous admin).

The **Tickstream Pipeline** dashboard is auto-provisioned and contains 12 panels:

| Panel | Type | What it shows |
|-------|------|--------------|
| Messages Received / sec | Time series | Raw WebSocket frame rate by exchange |
| Ticks Parsed / sec | Time series | Parsed trade rate by exchange + symbol |
| Queue Depth | Time series | In-memory queue backpressure (thresholds at 50k / 150k) |
| Parse Errors / sec | Time series | Parsing failure rate (highlighted red) |
| Reconnects / sec | Time series | WebSocket reconnect rate by exchange |
| Ticks Written / sec | Time series | Parquet write throughput by exchange + symbol |
| Write Latency (p50/p95/p99) | Time series | Parquet batch-write duration percentiles |
| Last Message Age | Time series | Feed staleness per symbol (thresholds at 30 s / 120 s) |
| Cumulative Ticks Received | Stat | Total raw frames since start |
| Cumulative Ticks Written | Stat | Total ticks persisted to Parquet since start |
| Total Parse Errors | Stat | Total parse failures since start |
| Total Reconnects | Stat | Total reconnect attempts since start |

The dashboard auto-refreshes every **10 seconds** and defaults to a **15-minute** rolling window.

### Prometheus UI

Open [http://localhost:9091](http://localhost:9091) to run ad-hoc PromQL queries directly against the Prometheus TSDB. Data is retained for **7 days**.

---

## Testing

### Run fast tests (default — sub-second)

```bash
uv run pytest
```

### Run with timing breakdown

```bash
uv run pytest --durations=10
```

### Run the full suite including slow load tests

```bash
uv run pytest --runslow
```

### Run a single file or test

```bash
uv run pytest tests/unit/test_parquet_writer.py
uv run pytest tests/unit/test_parquet_writer.py::test_partition_path_uses_exchange_symbol_date_layout
```

### Lint and type-check

```bash
uv run ruff check --fix src tests
uv run ruff format src tests
uv run mypy
```

### Install pre-commit hooks (runs ruff + mypy on every commit)

```bash
uv run pre-commit install
```

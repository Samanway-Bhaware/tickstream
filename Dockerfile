FROM python:3.11-slim

WORKDIR /app

# Pull the uv binary directly from the official image — faster and more
# reliable than pip install uv inside a container.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Tell uv to use the container's Python instead of downloading its own,
# and pre-compile .pyc files for faster container startup.
ENV UV_SYSTEM_PYTHON=1 \
    UV_COMPILE_BYTECODE=1

# Copy dependency manifests first so Docker can cache the dependency layer
# independently of source-code changes.
COPY pyproject.toml uv.lock README.md ./

# Install all runtime dependencies WITHOUT the local package itself.
# This layer is rebuilt only when pyproject.toml / uv.lock changes.
RUN uv sync --no-dev --frozen --no-install-project

# Copy source and examples.
COPY src/ src/
COPY examples/ examples/

# Now install the tickstream package itself (source is present).
RUN uv sync --no-dev --frozen

# Default: run the storage demo with Prometheus metrics on port 9090.
# Override CMD in docker-compose or at runtime as needed.
CMD ["uv", "run", "python", "examples/run_with_storage.py", \
     "--metrics-port", "9090", \
     "--seconds", "86400"]

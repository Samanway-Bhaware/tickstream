"""Pytest configuration for test suite markers."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Generator

import pytest

from tickstream.storage.parquet_writer import ParquetWriter

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.config.argparsing import Parser
    from _pytest.nodes import Item


def pytest_addoption(parser: Parser) -> None:
    """Add --runslow command-line flag."""
    parser.addoption(
        "--runslow",
        action="store_true",
        default=False,
        help="run slow tests",
    )


def pytest_collection_modifyitems(config: Config, items: list[Item]) -> None:
    """Skip slow tests if --runslow is not passed."""
    if config.getoption("--runslow"):
        return
    skip_slow = pytest.mark.skip(reason="need --runslow to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture
def writer(tmp_path: Path) -> Generator[ParquetWriter, None, None]:
    """A ParquetWriter pointed at tmp_path with small batch_size and no fsync."""
    queue: asyncio.Queue = asyncio.Queue()  # type: ignore[type-arg]
    w = ParquetWriter(
        queue,
        root_dir=tmp_path,
        max_batch_size=10,
        flush_interval_s=60.0,
        fsync=False,
    )
    yield w

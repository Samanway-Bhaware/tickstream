"""Shared test infrastructure for connector and orchestrator tests.

Import directly — these are plain functions/classes, not pytest fixtures.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock


class FakeWebSocket:
    """Minimal async-iterable stand-in for a ``websockets`` connection.

    Yields each string in ``messages`` then raises ``StopAsyncIteration``
    (simulating a server-side close).

    Attributes
    ----------
    sent:
        List of messages passed to :meth:`send` — lets tests assert the
        subscription frame was sent correctly.
    """

    def __init__(self, messages: list[str]) -> None:
        self._messages = messages
        self._index = 0
        self.sent: list[str] = []

    def __aiter__(self) -> "FakeWebSocket":
        return self

    async def __anext__(self) -> str:
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._index]
        self._index += 1
        return msg

    async def send(self, msg: str) -> None:
        self.sent.append(msg)


class HangingWebSocket:
    """A WebSocket that blocks indefinitely — used for cancellation tests."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def __aiter__(self) -> "HangingWebSocket":
        return self

    async def __anext__(self) -> str:
        await asyncio.sleep(10_000)
        raise StopAsyncIteration  # unreachable; satisfies type checker

    async def send(self, msg: str) -> None:
        self.sent.append(msg)


def make_connect(ws: FakeWebSocket | HangingWebSocket) -> MagicMock:
    """Return a mock for ``websockets.connect`` that yields *ws* as the
    context-manager value."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=ws)
    cm.__aexit__ = AsyncMock(return_value=False)
    mock_connect = MagicMock(return_value=cm)
    return mock_connect

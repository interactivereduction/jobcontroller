from typing import Iterator, Any
from unittest.mock import AsyncMock

from asynctest import MagicMock


class AwaitableMock(AsyncMock):
    def __await__(self) -> Iterator[Any]:
        self.await_count += 1
        return iter([])


class AwaitableNonAsyncMagicMock(MagicMock):
    def __await__(self) -> Iterator[Any]:
        return iter([])

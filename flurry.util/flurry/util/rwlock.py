from types import TracebackType
from typing import AsyncGenerator

import asyncio
from collections import deque
from contextlib import asynccontextmanager


__all__ = ("RWLock",)


class RWLock:
    """An read-write lock for asyncio. This class is NOT thread-safe.

    Provides an asyncio-powered read-write lock.

    - Read and write locks are mutually exclusive.
    - Only a single write lock may be held at any given time.
    - Multiple read locks may be held at any given time.
    - Acquiring a read lock blocks until all queued writers have finished.
    - Acquiring a write lock blocks until all active readers have finished.
    - If multiple writers are waiting for the lock, they are given the lock
      in the same order they requested it.
    """

    __slots__ = ("_writers", "_readers", "_reading", "_writing")

    def __init__(self):
        self._reading = 0
        self._writing = 0
        self._writers = deque[asyncio.Future[None]]()
        self._readers = deque[asyncio.Future[None]]()

    class RWLockHandle:
        """A handle to a locked RWLock.

        Allows for upgrading/downgrading of the held lock.
        """

        __slots__ = ("_lock", "_released", "_exclusive")

        def __init__(self, lock: "RWLock", exclusive: bool):
            self._lock = lock
            self._released = False
            self._exclusive = exclusive

        def __enter__(self):
            return self

        def __exit__(self, exc_typ: type, exc_val: Exception, exc_trace: TracebackType):
            self._release()

        async def upgrade(self):
            if self._released:
                raise RuntimeError("cannot upgrade a released lock")
            if self._exclusive:
                raise RuntimeError("cannot upgrade a write lock")
            write_wait = self._lock._can_write()  # pylint: disable=protected-access
            self._release()
            self._released = False
            self._exclusive = True
            await write_wait

        async def downgrade(self):
            if self._released:
                raise RuntimeError("cannot downgrade a released lock")
            if not self._exclusive:
                raise RuntimeError("cannot downgrade a read lock")
            read_wait = self._lock._can_read()  # pylint: disable=protected-access
            self._release()
            await read_wait
            self._released = False
            self._exclusive = False

        def _release(self):
            if not self._released:
                self._released = True
                if self._exclusive:
                    self._lock._done_writing()  # pylint: disable=protected-access
                else:
                    self._lock._done_reading()  # pylint: disable=protected-access

    @property  # type: ignore
    @asynccontextmanager
    async def write(self) -> AsyncGenerator["RWLockHandle", None]:
        await self._can_write()
        with self.RWLockHandle(self, True) as handle:
            yield handle

    @property  # type: ignore
    @asynccontextmanager
    async def read(self) -> AsyncGenerator["RWLockHandle", None]:
        await self._can_read()
        with self.RWLockHandle(self, False) as handle:
            yield handle

    async def _can_read(self):
        if self._writing:
            fut = asyncio.Future[None]()
            self._readers.append(fut)
            await fut
        self._reading += 1

    def _done_reading(self):
        self._reading -= 1
        if self._reading > 0:
            return
        # if this is the last reader, wake up the next writer (if any)
        while self._writers:
            next_writer = self._writers.popleft()
            if not next_writer.done():
                next_writer.set_result(None)
                return

    async def _can_write(self):
        self._writing += 1
        if not self._reading:
            return
        try:
            fut = asyncio.Future[None]()
            self._writers.append(fut)
            await fut
        except:
            self._writing -= 1
            raise

    def _done_writing(self):
        self._writing -= 1
        # if there is a writer in line, wake it up
        while self._writers:
            next_writer = self._writers.popleft()
            if not next_writer.done():
                next_writer.set_result(None)
                return
        # otherwise, wake up the readers (if any)
        while self._readers:
            reader = self._readers.popleft()
            if not reader.done():
                reader.set_result(None)

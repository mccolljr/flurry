from __future__ import annotations

import asyncio
import aiosqlite

from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Protocol, TypeVar
from contextlib import asynccontextmanager
from money.framework.event import EventBase, EventMeta
from money.framework.predicate import Predicate


class Storage(Protocol):
    async def load_events(
        self, query: Optional[Predicate[EventBase]] = None
    ) -> Iterable[EventBase]:
        ...

    async def save_events(self, events: Iterable[EventBase]):
        ...


class MemoryStorage:
    __events: List[EventBase]

    def __init__(self):
        self.__events = []

    async def load_events(
        self, query: Optional[Predicate[EventBase]] = None
    ) -> Iterable[EventBase]:
        return [e for e in self.__events if (not query) or query(e)]

    async def save_events(self, events: Iterable[EventBase]):
        for e in events:
            idx = len(self.__events)
            self.__events.append(e)

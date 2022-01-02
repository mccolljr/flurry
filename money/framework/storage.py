from __future__ import annotations

import asyncio
import aiosqlite

from collections import defaultdict
from typing import Dict, Iterable, List, Protocol, TypeVar
from contextlib import asynccontextmanager
from money.framework.event import EventBase, EventMeta
from money.framework.predicate import Predicate


class Storage(Protocol):
    async def load_events(self, query: Predicate[EventBase]) -> Iterable[EventBase]:
        ...

    async def save_events(self, events: Iterable[EventBase]):
        ...


class MemoryStorage:
    __events: List[EventBase]
    __event_types: Dict[EventMeta, List[int]]

    def __init__(self):
        self.__events = []
        self.__event_types = defaultdict(list)

    async def load_events(self, query: Predicate[EventBase]) -> Iterable[EventBase]:
        return [e for e in self.__events if query(e)]

    async def save_events(self, events: Iterable[EventBase]):
        for e in events:
            idx = len(self.__events)
            self.__events.append(e)
            self.__event_types[type(e)].append(idx)

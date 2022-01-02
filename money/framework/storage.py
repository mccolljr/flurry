from __future__ import annotations


from typing import Dict, Iterable, List, Optional, Protocol
from money.framework.event import EventBase
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

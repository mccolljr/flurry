from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from money.framework.event import EventBase
from money.framework.predicate import Predicate
from money.framework.aggregate import AggregateBase, AggregateMeta


class MemoryStorage:
    """Provides rudimentary in-memory storage. Useful for testing, but should never be used in production."""

    __events: List[EventBase]
    __snapshots: Dict[Tuple[AggregateMeta, Any], AggregateBase]

    def __init__(self):
        self.__events = []
        self.__snapshots = {}

    async def load_events(self, query: Predicate = None) -> Iterable[EventBase]:
        return [e for e in self.__events if (not query) or query(e)]

    async def save_events(self, events: Iterable[EventBase]):
        for evt_to_save in events:
            self.__events.append(evt_to_save)

    async def save_snapshots(self, snaps: Iterable[AggregateBase]):
        for snap in snaps:
            snap_typ = type(snap)
            snap_id = getattr(snap, snap_typ.__agg_id__)
            self.__snapshots[(snap_typ, snap_id)] = snap

    async def load_snapshots(self, query: Predicate = None) -> Iterable[AggregateBase]:
        return [s for s in self.__snapshots.values() if (not query) or query(s)]

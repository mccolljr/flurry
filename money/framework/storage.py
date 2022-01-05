from __future__ import annotations

import json
import logging
import asyncio
import aiosqlite

from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    Type,
)

from money.framework import predicate as P
from money.framework.event import EventBase, EventMeta
from money.framework.predicate import Predicate
from money.framework.aggregate import AggregateBase, AggregateMeta


class Storage(Protocol):
    """A protocol describing the shared capabilities of different storage providers."""

    async def load_events(
        self, query: Optional[Predicate] = None
    ) -> Iterable[EventBase]:
        ...

    async def save_events(self, events: Iterable[EventBase]):
        ...

    async def save_snapshots(self, snaps: Iterable[AggregateBase]):
        ...

    async def load_snapshots(self, query: Predicate = None) -> Iterable[AggregateBase]:
        ...


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


class SqliteStorage:
    """Provides rudimentary sqlite3-based storage. Useful for testing, but not fit for 99% of production uses."""

    class JSONDataEncoder(json.JSONEncoder):
        """Provides JSON encoding for the database layer."""

        def default(self, o):
            import datetime

            if isinstance(o, datetime.datetime):
                return o.astimezone(datetime.timezone.utc).isoformat()
            return super().default(o)

    def __init__(self, db_name: str):
        self.__setup = asyncio.Lock()
        self.__db_name = db_name
        self.__setup_done = False

    def __simplify(
        self, pred: Predicate, field_mapping: Dict[Type[Predicate], str]
    ) -> Tuple[Optional[Predicate], Optional[str], Optional[Tuple[Any, ...]]]:
        if isinstance(pred, P.Is):
            args = tuple(t.__name__ for t in pred.types)
            return (
                None,
                f"{field_mapping[P.Is]} IN ({','.join('?' for _ in range(0, len(args)))})",
                args,
            )
        if isinstance(pred, P.Or):
            or_params: List[Any] = []
            or_clauses: List[str] = []
            new_alts: List[Predicate] = []
            for alt in pred.alts:
                simp, where, params = self.__simplify(alt, field_mapping)
                if simp is not None:
                    new_alts.append(simp)
                if where is not None:
                    or_clauses.append(where)
                    if params is not None:
                        or_params.extend(params)
            return (
                P.Or(*new_alts) if new_alts else None,
                f"({' OR '.join(or_clauses)})" if or_clauses else None,
                tuple(or_params) if or_params else None,
            )
        if isinstance(pred, P.And):
            and_params: List[Any] = []
            and_clauses: List[str] = []
            new_preds: List[Predicate] = []
            for subpred in pred.preds:
                simp, where, params = self.__simplify(subpred, field_mapping)
                if simp is not None:
                    new_preds.append(simp)
                if where is not None:
                    and_clauses.append(where)
                    if params is not None:
                        and_params.extend(params)
            return (
                P.Or(*new_preds) if new_preds else None,
                f"({' AND '.join(and_clauses)})" if and_clauses else None,
                tuple(and_params) if and_params else None,
            )
        return pred, None, None

    def __conn(self) -> aiosqlite.Connection:
        return aiosqlite.connect(self.__db_name)

    async def __ensure_setup(self):
        async with self.__setup:
            if self.__setup_done:
                return
            await self.__do_setup()

    async def __do_setup(self):
        async with self.__conn() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS __events (
                    sequence_num INTEGER PRIMARY KEY,
                    event_type   TEXT NOT NULL,
                    event_data   TEXT NOT NULL DEFAULT '{}'
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS __snapshots (
                    sequence_num   INTEGER PRIMARY KEY,
                    aggregate_id   TEXT NOT NULL UNIQUE,
                    aggregate_type TEXT NOT NULL,
                    aggregate_data TEXT NOT NULL DEFAULT '{}'
                );
                """
            )
            await conn.commit()

    async def load_events(self, query: Predicate = None) -> Iterable[EventBase]:
        await self.__ensure_setup()
        async with self.__conn() as conn:
            events: List[EventBase] = []
            sql_str = "SELECT event_type, event_data from __events"
            params = None
            if query:
                query, where_clause, params = self.__simplify(
                    query, field_mapping={P.Is: "event_type"}
                )
                if where_clause is not None:
                    sql_str += f" WHERE {where_clause}"
            logging.info("SQL QUERY: %s / %s", sql_str, params)
            loaded = await conn.execute(sql_str, params)
            async for row in loaded:
                evt = EventMeta.construct_named(row[0], json.loads(row[1]))
                if query is None or query(evt):
                    events.append(evt)
            return events

    async def save_events(self, events: Iterable[EventBase]):
        await self.__ensure_setup()
        async with self.__conn() as conn:
            params = [
                (
                    evt.__class__.__name__,
                    json.dumps(evt.to_dict(), cls=self.JSONDataEncoder),
                )
                for evt in events
            ]
            await conn.executemany(
                "INSERT INTO __events (event_type, event_data) values (?, ?)", params
            )
            await conn.commit()

    async def save_snapshots(self, snaps: Iterable[AggregateBase]):
        await self.__ensure_setup()
        async with self.__conn() as conn:
            for snap in snaps:
                snap_typ = type(snap)
                snap_id = f"{snap_typ.__name__}:{getattr(snap, snap_typ.__agg_id__)}"
                snap_data = json.dumps(snap.to_dict(), cls=self.JSONDataEncoder)
                await conn.execute(
                    """
                    INSERT INTO __snapshots (aggregate_id, aggregate_type, aggregate_data)
                        VALUES (?,?,?)
                        ON CONFLICT(aggregate_id) DO UPDATE SET
                            aggregate_data=excluded.aggregate_data;
                    """,
                    (snap_id, snap_typ.__name__, snap_data),
                )
            await conn.commit()

    async def load_snapshots(self, query: Predicate = None) -> Iterable[AggregateBase]:
        await self.__ensure_setup()
        async with self.__conn() as conn:
            snaps: List[AggregateBase] = []
            sql_str = "SELECT aggregate_type, aggregate_data from __snapshots"
            params = None
            if query:
                query, where_clause, params = self.__simplify(
                    query, {P.Is: "aggregate_type"}
                )
                if where_clause is not None:
                    sql_str += f" WHERE {where_clause}"
            logging.info("SQL QUERY: %s / %s", sql_str, params)
            loaded = await conn.execute(sql_str, params)
            async for row in loaded:
                snap = AggregateMeta.construct_named(row[0], json.loads(row[1]))
                if query is None or query(snap):
                    snaps.append(snap)
            return snaps

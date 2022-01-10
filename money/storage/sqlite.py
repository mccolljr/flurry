"""Sqlite3 storage solution."""

from __future__ import annotations
from typing import Iterable, List

import json
import logging
import asyncio
import aiosqlite

from money import predicate as P
from money.event import EventBase, EventMeta
from money.predicate import Predicate
from money.aggregate import AggregateBase, AggregateMeta
from money.storage.utils import (
    PredicateSQLSimplifier,
    SimplifiedPredicate,
    cast_simplified_predicate,
    visit_predicate,
)

LOG = logging.getLogger("sqlite")


class _SqliteSimplifier(PredicateSQLSimplifier):
    def __init__(self, type_field: str, data_field: str):
        self.type_field = type_field
        self.data_field = data_field

    def on_is(self, p_is: P.Is) -> SimplifiedPredicate:
        clause = f"({', '.join('?' for _ in p_is.types)})"
        params = [t.__name__ if isinstance(t, type) else str(t) for t in p_is.types]
        return None, clause, params


class SqliteStorage:
    """Provides rudimentary sqlite3-based storage. Useful for testing, but not fit for 99% of production uses."""

    class JSONDataEncoder(json.JSONEncoder):
        """Provides JSON encoding for the database layer."""

        def default(self, o):
            """Properly encode values, including datetime values."""
            import datetime

            if isinstance(o, datetime.datetime):
                return o.astimezone(datetime.timezone.utc).isoformat()
            return super().default(o)

    def __init__(self, db_name: str):
        """Initialize new Sqlite3 storage using the given database file."""
        self.__setup = asyncio.Lock()
        self.__db_name = db_name
        self.__setup_done = False

    def __simplify(self, pred: Predicate, type_field: str, data_field: str):
        return cast_simplified_predicate(
            visit_predicate(_SqliteSimplifier(type_field, data_field), pred)
        )

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
        """Load events that match the predicate."""
        await self.__ensure_setup()
        async with self.__conn() as conn:
            events: List[EventBase] = []
            sql_str = "SELECT event_type, event_data from __events"
            params = None
            if query:
                query, where_clause, params = self.__simplify(
                    query, "event_type", "event_data"
                )
                if where_clause is not None:
                    sql_str += f" WHERE {where_clause}"
            LOG.info("SQL QUERY: %s", sql_str)
            loaded = await conn.execute(sql_str, params)
            async for row in loaded:
                evt = EventMeta.construct_named(row[0], json.loads(row[1]))
                if query is None or query(evt):
                    events.append(evt)
            return events

    async def save_events(self, events: Iterable[EventBase]):
        """Save new events."""
        await self.__ensure_setup()
        async with self.__conn() as conn:

            sql_str = "INSERT INTO __events (event_type, event_data) values (?, ?)"
            params = [
                (
                    evt.__class__.__name__,
                    json.dumps(evt.to_dict(), cls=self.JSONDataEncoder),
                )
                for evt in events
            ]
            LOG.info("SQL EXEC: %s", sql_str)
            await conn.executemany(sql_str, params)
            await conn.commit()

    async def save_snapshots(self, snaps: Iterable[AggregateBase]):
        """Save new snapshots."""
        await self.__ensure_setup()
        async with self.__conn() as conn:
            for snap in snaps:
                snap_typ = type(snap)
                snap_id = f"{snap_typ.__name__}:{getattr(snap, snap_typ.__agg_id__)}"
                snap_data = json.dumps(snap.to_dict(), cls=self.JSONDataEncoder)
                sql_str = """
                INSERT INTO __snapshots (aggregate_id, aggregate_type, aggregate_data)
                    VALUES (?,?,?)
                    ON CONFLICT(aggregate_id) DO UPDATE SET
                        aggregate_data=excluded.aggregate_data;
                """
                LOG.info("SQL EXEC: %s", sql_str)
                await conn.execute(sql_str, (snap_id, snap_typ.__name__, snap_data))
            await conn.commit()

    async def load_snapshots(self, query: Predicate = None) -> Iterable[AggregateBase]:
        """Load snapshots that match the predicate."""
        await self.__ensure_setup()
        async with self.__conn() as conn:
            snaps: List[AggregateBase] = []
            sql_str = "SELECT aggregate_type, aggregate_data from __snapshots"
            params = None
            if query:
                query, where_clause, params = self.__simplify(
                    query, "aggregate_type", "aggregate_data"
                )
                if where_clause is not None:
                    sql_str += f" WHERE {where_clause}"
            LOG.info("SQL QUERY: %s", sql_str)
            loaded = await conn.execute(sql_str, params)
            async for row in loaded:
                snap = AggregateMeta.construct_named(row[0], json.loads(row[1]))
                if query is None or query(snap):
                    snaps.append(snap)
            return snaps

    async def close(self):
        """Do nothing, unnecessary for sqlite."""

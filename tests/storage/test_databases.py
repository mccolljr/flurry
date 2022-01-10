import os
import pytest
import aiopg  # type: ignore
import asyncio
import datetime as dt

from money import schema, predicate as P
from money.event import EventBase, EventMeta, handle_event
from money.storage import SqliteStorage, PostgreSQLStorage
from money.aggregate import AggregateBase, AggregateMeta


@pytest.fixture(autouse=True)
def unregister_names_and_stuff():
    yield
    setattr(EventMeta, "_EventMeta__by_name", {})
    setattr(AggregateMeta, "_AggregateMeta__by_name", {})


@pytest.fixture(autouse=True, scope="session")
def initial_dbsetup():
    dsn = "postgres://postgres:unsafe@localhost:5432/postgres"

    async def do_setup():
        async with aiopg.connect(dsn) as conn:
            async with await conn.cursor() as cur:
                await cur.execute("CREATE DATABASE test;")

    async def do_teardown():
        async with aiopg.connect(dsn) as conn:
            async with await conn.cursor() as cur:
                await cur.execute("DROP DATABASE IF EXISTS test;")
        os.remove("test.db")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(do_setup())
    yield
    loop.run_until_complete(do_teardown())


class _TestEventBase(EventBase):
    def __eq__(self, other) -> bool:
        return isinstance(other, self.__class__) and all(
            getattr(other, name) == getattr(self, name)
            for name in self.__schema__.keys()
        )

    def __repr__(self) -> str:
        name = self.__class__.__name__
        fields = {fname: getattr(self, fname) for fname in self.__schema__.keys()}
        return f"{name}({repr(fields)})"

    def __str__(self) -> str:
        return self.__repr__()


class _TestAggregateBase(AggregateBase):
    __agg_mixin__ = True

    id = schema.Field(schema.Str)

    @classmethod
    async def load_events(cls, storage, ids):
        return {}

    def __eq__(self, other) -> bool:
        return isinstance(other, self.__class__) and all(
            getattr(other, name) == getattr(self, name)
            for name in self.__schema__.keys()
        )

    def __repr__(self) -> str:
        name = self.__class__.__name__
        fields = {fname: getattr(self, fname) for fname in self.__schema__.keys()}
        return f"{name}({repr(fields)})"

    def __str__(self) -> str:
        return self.__repr__()


@pytest.mark.parametrize(
    ["storage"],
    [
        (
            PostgreSQLStorage(
                host="localhost",
                port="5432",
                user="postgres",
                password="unsafe",
                database="test",
            ),
        ),
        (SqliteStorage("test.db"),),
    ],
    ids=["postgres", "sqlite"],
)
@pytest.mark.asyncio
async def test_database_backed_storage(storage):
    class E1(_TestEventBase):
        a = schema.Field(schema.Str, nullable=False)
        b = schema.Field(schema.Str, nullable=False)

    class E2(_TestEventBase):
        x = schema.Field(schema.Int, nullable=False)
        y = schema.Field(schema.Int, nullable=False)

    class A1(_TestAggregateBase):
        __agg_create__ = _TestEventBase

        created_at = schema.Field(
            schema.DateTime,
            default=lambda: dt.datetime.utcnow().as_timezone(dt.timezone.utc),
        )
        foo = schema.Field(schema.Str, nullable=False)

        @handle_event(_TestEventBase)
        def handle_create(self, e: _TestEventBase):
            pass

    class A2(_TestAggregateBase):
        __agg_create__ = _TestEventBase

        created_at = schema.Field(
            schema.DateTime,
            default=lambda: dt.datetime.utcnow().as_timezone(dt.timezone.utc),
        )
        bar = schema.Field(schema.Float, nullable=False)

        @handle_event(_TestEventBase)
        def handle_create(self, e: _TestEventBase):
            pass

    event_1 = E1(a="a", b="p")
    event_2 = E1(a="z", b="q")
    event_3 = E2(x=100, y=200)
    event_4 = E2(x=1000, y=2000)

    try:
        await storage.save_events([event_1, event_2, event_3, event_4])

        async with storage.get_cursor() as conn:
            await conn.execute("SELECT COUNT(*) AS total FROM __events")
            total = await conn.fetchone()
            assert total is not None
            assert total[0] == 4

        got_events = await storage.load_events(P.Is(E1))
        assert got_events == [event_1, event_2]

        got_events = await storage.load_events(P.Is(E2))
        assert got_events == [event_3, event_4]

        got_events = await storage.load_events(P.Or(P.Is(E2), P.Is(E1)))
        assert got_events == [event_1, event_2, event_3, event_4]

        got_events = await storage.load_events(P.Where(a="a"))
        assert got_events == [event_1]

        got_events = await storage.load_events(P.Where(a=P.NotEq("a")))
        assert got_events == [event_2, event_3, event_4]

        got_events = await storage.load_events(P.Where(x=100))
        assert got_events == [event_3]

        got_events = await storage.load_events(P.Where(x=P.OneOf(100, 1000)))
        assert got_events == [event_3, event_4]

        got_events = await storage.load_events(P.Where(y=P.More(200)))
        assert got_events == [event_4]

        got_events = await storage.load_events(P.Where(y=P.MoreEq(200)))
        assert got_events == [event_3, event_4]

        got_events = await storage.load_events(P.Where(y=P.Less(2000)))
        assert got_events == [event_3]

        got_events = await storage.load_events(P.Where(y=P.LessEq(2000)))
        assert got_events == [event_3, event_4]

        got_events = await storage.load_events(P.Where(y=P.Between(199, 2001)))
        assert got_events == [event_3, event_4]

        t1 = dt.datetime(2022, 1, 7, 11, 36, 22).astimezone(dt.timezone.utc)
        t2 = dt.datetime(2022, 1, 8, 11, 36, 22).astimezone(dt.timezone.utc)
        agg_1 = A1(id="a1", created_at=t1, foo="test")
        agg_2 = A2(id="a2", created_at=t2, bar=3.1415)

        await storage.save_snapshots([agg_1, agg_2])

        async with storage.get_cursor() as conn:
            await conn.execute("SELECT COUNT(*) AS total FROM __snapshots")
            total = await conn.fetchone()
            assert total is not None
            assert total[0] == 2

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.Eq(t1)))
        assert got_aggs == [agg_1]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.NotEq(t2)))
        assert got_aggs == [agg_1]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.LessEq(t1)))
        assert got_aggs == [agg_1]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.Less(t2)))
        assert got_aggs == [agg_1]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.Eq(t2)))
        assert got_aggs == [agg_2]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.More(t1)))
        assert got_aggs == [agg_2]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.MoreEq(t2)))
        assert got_aggs == [agg_2]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.NotEq(t1)))
        assert got_aggs == [agg_2]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.Between(t1, t2)))
        assert got_aggs == [agg_1, agg_2]

        got_aggs = await storage.load_snapshots(P.Where(created_at=P.OneOf(t1, t2)))
        assert got_aggs == [agg_1, agg_2]

        got_aggs = await storage.load_snapshots(P.Where(no_such_field=P.NotEq(t1)))
        assert got_aggs == [agg_1, agg_2]

    finally:
        await storage.close()

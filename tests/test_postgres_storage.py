from typing import Any
import pytest

from datetime import datetime
from zoneinfo import ZoneInfo

from flurry.util import JSON
from flurry.core import schema, predicate as P
from flurry.core.event import EventBase, EventMeta
from flurry.postgres import PostgreSQLStorage
from flurry.core.aggregate import AggregateMeta


DATETIME_A = datetime(2022, 1, 27, 13, 6, 47, 799859, tzinfo=ZoneInfo("UTC"))
DATETIME_B = datetime(2022, 1, 27, 13, 6, 47, 799859, tzinfo=ZoneInfo("EST"))


@pytest.fixture(autouse=True, scope="function")
def clear_registered_names():
    yield
    setattr(EventMeta, "_EventMeta__by_name", {})
    setattr(AggregateMeta, "_AggregateMeta__by_name", {})


@pytest.fixture
async def storage():
    _storage = PostgreSQLStorage(
        host="localhost",
        port="31415",
        user="postgres",
        password="unsafe",
        database="postgres",
        plpython3u=False,
    )
    try:
        yield _storage
    except:
        await _storage.close()
        raise


class EventA(EventBase):
    a = schema.Field(schema.Str)
    b = schema.Field(schema.Int)
    c = schema.Field(schema.Bool)
    d = schema.Field(schema.Float)
    e = schema.Field(schema.DateTime)
    f = schema.Field(schema.Bytes)

    def __eq__(self, other: Any) -> bool:
        for field in self.__schema__.keys():
            self_val = getattr(self, field)
            if not hasattr(other, field) or getattr(other, field) != self_val:
                print(f"MISMATCH: {field} {self_val} != {getattr(other, field)}")
                return False
        return True

    def __str__(self) -> str:
        return JSON.dumps(self.to_dict())

    def __repr__(self) -> str:
        return str(self)


class EventB(EventBase):
    x = schema.Field(schema.Str)

    def __eq__(self, other: Any) -> bool:
        for field in self.__schema__.keys():
            self_val = getattr(self, field)
            if not hasattr(other, field) or getattr(other, field) != self_val:
                print(f"MISMATCH: {field} {self_val} != {getattr(other, field)}")
                return False
        return True

    def __str__(self) -> str:
        return JSON.dumps(self.to_dict())

    def __repr__(self) -> str:
        return str(self)


@pytest.mark.asyncio
async def test_event_storage(storage: PostgreSQLStorage):
    events = [
        EventA(a="a", b=1, c=True, d=1.5, e=DATETIME_A, f=b"123"),
        EventA(a="", b=0, c=False, d=0.0, e=DATETIME_B, f=b""),
        EventA(),
    ]
    await storage.save_events(events)

    loaded = await storage.load_events(P.Where(e=P.Less(DATETIME_B)))
    assert loaded == [events[0]]

    loaded = await storage.load_events(P.Where(e=P.More(DATETIME_A)))
    assert loaded == [events[1]]

    loaded = await storage.load_events(P.Where(e=P.Eq(DATETIME_A)))
    assert loaded == [events[0]]

    loaded = await storage.load_events(P.Where(e=P.Eq(DATETIME_B)))
    assert loaded == [events[1]]

    loaded = await storage.load_events(P.Where(e=P.Eq(None)))
    assert loaded == [events[2]]

    loaded = await storage.load_events(P.Where(e=P.NotEq(None)))
    assert loaded == [events[0], events[1]]

    try:
        await storage.load_events(P.Where(e=P.Less(None)))
        assert False, "this should not succeed"
    except RuntimeError:
        pass

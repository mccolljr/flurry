import pytest

import aiosqlite
from money.framework.event import EventBase

import money.framework.schema as schema
from money.framework.predicate import Between, Eq, Less, Or, Where
from money.framework.storage import MemoryStorage, Storage


@pytest.mark.asyncio
async def test_memory_storage():
    storage: Storage = MemoryStorage()

    class GenericEvent(EventBase):
        foo = schema.Field(schema.Str, default="foo")
        bar = schema.Field(schema.Int, default=12)

    events = [
        GenericEvent(),
        GenericEvent(foo="baz"),
        GenericEvent(foo="quux"),
        GenericEvent(bar=1010),
    ]
    for i in range(0, 1000):
        events.append(GenericEvent(foo="fabricated", bar=i))

    await storage.save_events(events)

    loaded = await storage.load_events(
        Or(
            Where(foo=Eq("quux")),
            Where(bar=Between(0, 500)),
        )
    )

    assert len(list(loaded)) == 502

import pytest


from money.framework import schema
from money.framework import predicate as P
from money.framework.event import EventBase
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
        P.Or(
            P.Where(foo=P.Eq("quux")),
            P.Where(bar=P.Between(0, 500)),
        )
    )

    assert len(list(loaded)) == 504

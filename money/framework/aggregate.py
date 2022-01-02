from abc import ABCMeta, abstractmethod
from typing import Any, ClassVar, Dict, Iterable, List, Mapping, Tuple, Type, TypeVar

import money.framework.schema as schema
from money.framework.event import EventBase, EventMeta
from money.framework.storage import Storage

E = TypeVar("E", bound=type)


class AggregateDefinitionError(Exception):
    def __init__(self, agg_name: str, problem: str):
        self.agg_name = agg_name
        self.problem = problem
        super().__init__(f"{self.agg_name}: {self.problem}")


class AggregateMeta(schema.SchemaMeta, ABCMeta):
    __agg_id__: str
    __agg_name__: str
    __agg_mixin__: bool
    __agg_events__: Dict[EventMeta, str]
    __agg_create__: EventMeta

    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        # create aggregate metadata
        if not attrs.setdefault("__agg_mixin__", False):
            attrs.setdefault("__agg_id__", "id")
            attrs.setdefault("__agg_name__", name)
            attrs.setdefault("__agg_events__", {})
        x = super().__new__(cls, name, bases, attrs)
        return x

    def __init__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        if cls.__agg_mixin__:
            return
        if not hasattr(cls, "__agg_create__"):
            raise AggregateDefinitionError(
                name, "must specify a creation event in __agg_create__"
            )
        if cls.__agg_create__ not in cls.__agg_events__:
            raise AggregateDefinitionError(
                name,
                f"must specify a handler for creation event {cls.__agg_create__.__name__}",
            )
        if not (cls.__agg_id__ and cls.__agg_id__ in cls.__schema__):
            raise AggregateDefinitionError(
                name, "must define an id field, or must specify __agg_id__"
            )


TAggSelf = TypeVar("TAggSelf", bound="AggregateBase")


class AggregateBase(schema.SchemaBase, metaclass=AggregateMeta):
    __agg_mixin__ = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def apply_event(self, event: EventBase):
        handler_name = type(self).__agg_events__[type(event)]
        getattr(self, handler_name)(event)

    @classmethod
    @abstractmethod
    async def load_events(
        cls, storage: Storage, ids: List[Any]
    ) -> Mapping[Any, List[EventBase]]:
        ...

    @classmethod
    async def load(cls: Type[TAggSelf], storage: Storage, id: Any) -> TAggSelf:
        events = await cls.load_events(storage, [id])
        events = events.get(id, [])
        if not events:
            raise ValueError(f"no {cls.__name__} with id {id}")
        if not isinstance(events[0], cls.__agg_create__):
            raise ValueError(f"first event must be creation event")
        loaded = cls.__new__(cls)
        for e in events:
            loaded.apply_event(e)
        return loaded

    @classmethod
    async def load_all(
        cls: Type[TAggSelf], storage: Storage, ids: List[Any]
    ) -> List[TAggSelf]:
        events = await cls.load_events(storage, ids)
        return [cls.from_events(evts) for evts in events.values()]

    @classmethod
    def from_events(cls: Type[TAggSelf], events: List[EventBase]) -> TAggSelf:
        if not events:
            raise ValueError(f"no {cls.__name__} with id {id}")
        if not isinstance(events[0], cls.__agg_create__):
            raise ValueError(f"first event must be creation event")
        loaded = cls.__new__(cls)
        for e in events:
            loaded.apply_event(e)
        return loaded

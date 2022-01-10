"""Aggregates, data models derived from a stream of events."""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Tuple,
    Type,
    TypeVar,
)

from money import schema
from money.event import EventBase, EventMeta

if TYPE_CHECKING:
    from money.storage import Storage

EventRoot = TypeVar("EventRoot", bound=EventBase)


class AggregateDefinitionError(Exception):
    """An exception caused by an invalid aggregate class definition."""

    def __init__(self, agg_name: str, problem: str):
        """Initialize a new AggregateDefinitionError."""
        self.agg_name = agg_name
        self.problem = problem
        super().__init__(f"{self.agg_name}: {self.problem}")


class AggregateMeta(schema.SchemaMeta, ABCMeta):
    """The metaclass that all aggregate classes must inherit from."""

    __agg_id__: str
    __agg_name__: str
    __agg_mixin__: bool
    __agg_events__: Dict[EventMeta, str]
    __agg_create__: EventMeta

    __by_name: Dict[str, AggregateMeta] = {}

    def __new__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **extra
    ):
        """Construct a new Aggregate class."""
        if not attrs.setdefault("__agg_mixin__", extra.pop("mixin", False)):
            if name in cls.__by_name:
                raise TypeError(f"duplicate definition for aggregate {name}")
            attrs.setdefault("__agg_id__", extra.pop("id", "id"))
            attrs.setdefault("__agg_name__", name)
            attrs.setdefault("__agg_events__", {})
            attrs.setdefault("__agg_create__", extra.pop("create", None))
        new_class = super().__new__(cls, name, bases, attrs)
        if not new_class.__agg_mixin__:
            cls.__by_name[name] = new_class
        return new_class

    def __init__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **_extra
    ):
        """Initialize and validate a newly created Aggregate class."""
        print(_extra)
        super().__init__(name, bases, attrs)
        if not cls.__agg_mixin__:
            AggregateMeta._validate_aggregate_id(cls)
            AggregateMeta._validate_creation_event(cls)
            AggregateMeta._validate_load_events_method(cls)

    @classmethod
    def construct_named(cls, name: str, args: Dict[str, Any]) -> AggregateBase:
        """Construct an aggregate object of the given class name with the given data."""
        return cls.__by_name[name](**args)

    @staticmethod
    def _validate_aggregate_id(the_cls: AggregateMeta):
        if not (the_cls.__agg_id__ and the_cls.__agg_id__ in the_cls.__schema__):
            raise AggregateDefinitionError(
                the_cls.__name__,
                "must define an 'id' field, or must specify __agg_id__",
            )

    @staticmethod
    def _validate_creation_event(the_cls: AggregateMeta):
        if not isinstance(getattr(the_cls, "__agg_create__", None), EventMeta):
            raise AggregateDefinitionError(
                the_cls.__name__, "must specify a creation event in __agg_create__"
            )
        if the_cls.__agg_create__ not in the_cls.__agg_events__:
            raise AggregateDefinitionError(
                the_cls.__name__,
                f"must specify a handler for creation event {the_cls.__agg_create__.__name__}",
            )

    @staticmethod
    def _validate_load_events_method(the_cls: AggregateMeta):
        load_events = getattr(the_cls, "load_events", None)
        if load_events is None or getattr(load_events, "__isabstractmethod__", False):
            raise AggregateDefinitionError(
                the_cls.__name__, "must define a load_events class method"
            )
        if not callable(load_events):
            raise AggregateDefinitionError(
                the_cls.__name__, "the load_events method must be callable"
            )


TAggSelf = TypeVar("TAggSelf", bound="AggregateBase")


class AggregateBase(Generic[EventRoot], schema.SchemaBase, metaclass=AggregateMeta):
    """The base class that all aggregate classes must inherit from."""

    __agg_mixin__ = True

    def apply_event(self, event: EventRoot):
        """Call the appropriate handler for the given event."""
        try:
            handler_name = type(self).__agg_events__[type(event)]
        except KeyError as err:
            raise ValueError(
                f"{type(self).__name__} has no handler for event {type(event).__name__}"
            ) from err
        getattr(self, handler_name)(event)

    @classmethod
    @abstractmethod
    async def load_events(
        cls, storage: Storage, ids: List[Any]
    ) -> Dict[Any, List[EventRoot]]:
        """Load the events for aggregates of this type with the given ids."""
        ...

    @classmethod
    async def load(cls: Type[TAggSelf], storage: Storage, agg_id: Any) -> TAggSelf:
        """Derive a single aggregate from the event stream."""
        loaded = await cls.load_all(storage, [agg_id])
        if not loaded:
            raise ValueError(f"no {cls.__name__} with id {agg_id}")
        return loaded[0]

    @classmethod
    async def load_all(
        cls: Type[TAggSelf], storage: Storage, ids: List[Any]
    ) -> List[TAggSelf]:
        """Derive multiple aggregates from the event stream."""
        events = await cls.load_events(storage, ids)
        return [cls.from_events(evts) for evts in events.values()]

    @classmethod
    def from_events(cls: Type[TAggSelf], events: List[EventRoot]) -> TAggSelf:
        """Derive an aggregate from a list of events."""
        if not events:
            raise ValueError(f"no {cls.__name__} with id {id}")
        if not isinstance(events[0], cls.__agg_create__):
            raise ValueError("first event must be creation event")
        loaded = cls.__new__(cls)
        for src_evt in events:
            loaded.apply_event(src_evt)
        return loaded

    @classmethod
    async def sync_snapshots(cls: Type[TAggSelf], storage: Storage, ids: List[Any]):
        """Derive aggregates from the event stream, then save them as snapshots."""
        snaps = await cls.load_all(storage, ids)
        await storage.save_snapshots(snaps)

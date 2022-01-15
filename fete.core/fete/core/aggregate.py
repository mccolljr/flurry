"""Aggregates, data models derived from a stream of events."""

from __future__ import annotations
from typing import Any, Dict, Generic, List, Tuple, Type, TypeVar
from abc import ABCMeta, abstractmethod

from . import schema
from .context import Context
from .event import EventBase, EventMeta

# pylint: disable=invalid-name
_T_Context = TypeVar("_T_Context", bound=Context)
_T_EventRoot = TypeVar("_T_EventRoot", bound=EventBase)
_T_AggSelf = TypeVar("_T_AggSelf", bound="AggregateBase")
_T_AggLoadSelf = TypeVar("_T_AggLoadSelf", bound="AggregateLoader")
# pylint: enable=invalid-name


class AggregateDefinitionError(Exception):
    """An exception caused by an invalid aggregate class definition."""

    def __init__(self, agg_name: str, problem: str):
        """Initialize a new AggregateDefinitionError."""
        self.agg_name = agg_name
        self.problem = problem
        super().__init__(f"{self.agg_name}: {self.problem}")


class AggregateMeta(schema.SchemaMeta, ABCMeta):
    """The metaclass that all aggregate classes must inherit from."""

    __agg_name__: str
    __agg_mixin__: bool
    __agg_create__: EventMeta
    __agg_events__: Dict[EventMeta, str]

    __by_name: Dict[str, AggregateMeta] = {}

    def __new__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **extra
    ):
        """Construct a new Aggregate class."""
        if not attrs.setdefault("__agg_mixin__", extra.pop("mixin", False)):
            if name in cls.__by_name:
                raise TypeError(f"duplicate definition for aggregate {name}")
            extra.setdefault("id", "id")
            attrs.setdefault("__agg_name__", name)
            attrs.setdefault("__agg_create__", extra.pop("create", None))
            attrs.setdefault("__agg_events__", {})
        new_class = super().__new__(cls, name, bases, attrs, **extra)
        if not new_class.__agg_mixin__:
            cls.__by_name[name] = new_class
        return new_class

    def __init__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **_extra
    ):
        """Initialize and validate a newly created Aggregate class."""
        super().__init__(name, bases, attrs)
        if not cls.__agg_mixin__:
            AggregateMeta._validate_aggregate_id(cls)
            AggregateMeta._validate_creation_event(cls)

    @classmethod
    def construct_named(cls, name: str, args: Dict[str, Any]) -> AggregateBase:
        """Construct an aggregate object of the given class name with the given data."""
        return cls.__by_name[name](**args)

    @staticmethod
    def _validate_aggregate_id(the_cls: AggregateMeta):
        if the_cls.__schema__.id_field is None:
            raise AggregateDefinitionError(
                the_cls.__name__, "aggregates must specify an id field"
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


class AggregateBase(
    Generic[_T_EventRoot], schema.SchemaBase, metaclass=AggregateMeta, mixin=True
):
    """The base class that all aggregate classes must inherit from."""

    def apply_event(self, event: _T_EventRoot):
        """Call the appropriate handler for the given event."""
        try:
            handler_name = type(self).__agg_events__[type(event)]
        except KeyError as err:
            raise ValueError(
                f"{type(self).__name__} has no handler for event {type(event).__name__}"
            ) from err
        getattr(self, handler_name)(event)

    @classmethod
    def from_events(cls: Type[_T_AggSelf], events: List[_T_EventRoot]) -> _T_AggSelf:
        """Derive an aggregate from a list of events."""
        if not events:
            raise ValueError(f"no {cls.__name__} with id {id}")
        if not isinstance(events[0], cls.__agg_create__):
            raise ValueError("first event must be creation event")
        loaded = cls.__new__(cls)
        for src_evt in events:
            loaded.apply_event(src_evt)
        return loaded


class AggregateLoader(
    Generic[_T_EventRoot, _T_Context], AggregateBase[_T_EventRoot], mixin=True
):
    """A base class for aggregates who know how to load themselves from a storage context to inherit from."""

    @classmethod
    @abstractmethod
    async def load_events(
        cls, context: _T_Context, ids: List[Any]
    ) -> Dict[Any, List[_T_EventRoot]]:
        """Load the events for aggregates of this type with the given ids."""
        ...

    @classmethod
    async def load(
        cls: Type[_T_AggLoadSelf], context: _T_Context, agg_id: Any
    ) -> _T_AggLoadSelf:
        """Derive a single aggregate from the event stream."""
        loaded = await cls.load_all(context, [agg_id])
        if not loaded:
            raise ValueError(f"no {cls.__name__} with id {agg_id}")
        return loaded[0]

    @classmethod
    async def load_all(
        cls: Type[_T_AggLoadSelf], context: _T_Context, ids: List[Any]
    ) -> List[_T_AggLoadSelf]:
        """Derive multiple aggregates from the event stream."""
        events = await cls.load_events(context, ids)
        return [cls.from_events(evts) for evts in events.values()]

    @classmethod
    async def sync_snapshots(
        cls: Type[_T_AggLoadSelf], context: _T_Context, ids: List[Any]
    ):
        """Derive aggregates from the event stream, then save them as snapshots."""
        snaps = await cls.load_all(context, ids)
        await context.storage.save_snapshots(snaps)

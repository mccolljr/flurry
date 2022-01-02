from typing import Any, Dict, List, Tuple, TypeVar

import money.framework.schema as schema
from money.framework.event import EventMeta

E = TypeVar("E", bound=type)


class AggregateDefinitionError(Exception):
    def __init__(self, agg_name: str, problem: str):
        self.agg_name = agg_name
        self.problem = problem
        super().__init__(f"{self.agg_name}: {self.problem}")


class AggregateMeta(schema.SchemaMeta):
    __agg_id__: str
    __agg_name__: str
    __agg_mixin__: bool
    __agg_events__: List[EventMeta]
    __agg_create__: EventMeta

    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        # create aggregate metadata
        if not attrs.setdefault("__agg_mixin__", False):
            attrs.setdefault("__agg_id__", "id")
            attrs.setdefault("__agg_name__", name)
            attrs.setdefault("__agg_events__", [])
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
                "must specify a handler for creation event {cls.__agg_create__.__name__}",
            )
        if not (cls.__agg_id__ and cls.__agg_id__ in cls.__schema__):
            raise AggregateDefinitionError(
                name, "must define an id field, or must specify __agg_id__"
            )


class AggregateBase(schema.SchemaBase, metaclass=AggregateMeta):
    __agg_mixin__ = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

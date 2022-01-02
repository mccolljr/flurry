from typing import Any, Callable, Dict, Generic, Tuple, Type, TypeVar, Union, overload

from money.framework.schema import SchemaBase, SchemaMeta


E = TypeVar("E", bound="EventBase", contravariant=True)

EventHandlerFunction = Callable[[Any, E], None]
EventHandlerDecorator = Callable[[EventHandlerFunction[E]], "EventHandler[E]"]


class EventMeta(SchemaMeta):
    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        x = super().__new__(cls, name, bases, attrs)
        return x


class EventBase(SchemaBase, metaclass=EventMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


def handle_event(evt_class: Type[E]) -> EventHandlerDecorator[E]:
    def event_handler_decorator(handler: EventHandlerFunction[E]):
        return EventHandler(handler, evt_class)

    return event_handler_decorator


class EventHandler(Generic[E]):
    _evt_class: Type[E]

    def __init__(self, handler: EventHandlerFunction[E], evt_class: Type[E]):
        self.__dict__["_handler"] = handler
        self._evt_class = evt_class

    def __set_name__(self, owner: Any, name: str):
        owner.__agg_events__[self._evt_class] = name

    @overload
    def __get__(self, obj: None, objtype: Any) -> "EventHandler[E]":
        ...

    @overload
    def __get__(self, obj: Any, objtype: Any) -> Callable[[E], None]:
        ...

    def __get__(
        self, obj: Any, objtype=None
    ) -> Union["EventHandler[E]", Callable[[E], None]]:
        if obj is None:
            return self
        return lambda evt: self.__dict__["_handler"](obj, evt)

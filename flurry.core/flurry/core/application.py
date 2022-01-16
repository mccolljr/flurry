"""The core Application type."""

from typing import Callable, List, Optional, TypeVar, Union, overload

import logging

from .query import QueryMeta
from .event import EventMeta
from .schema import SchemaMeta
from .command import CommandMeta
from .aggregate import AggregateMeta

# pylint: disable=invalid-name
_T_EventMeta = TypeVar("_T_EventMeta", bound=EventMeta)
_T_QueryMeta = TypeVar("_T_QueryMeta", bound=QueryMeta)
_T_CommandMeta = TypeVar("_T_CommandMeta", bound=CommandMeta)
_T_AggregateMeta = TypeVar("_T_AggregateMeta", bound=AggregateMeta)
_T_AnyMeta = TypeVar("_T_AnyMeta", bound=SchemaMeta)
_Decorator = Callable[[_T_AnyMeta], _T_AnyMeta]
# pylint: enable=invalid-name

LOG = logging.getLogger("application")


class Application:
    """Application is the core type for applications using the framework.

    The base class handles registration of events, queries, commands,
    aggregates, modules, etc. Subclasses are responsible for providing
    network interfaces and the like.
    """

    _events: List[EventMeta]
    _queries: List[QueryMeta]
    _commands: List[CommandMeta]
    _aggregates: List[AggregateMeta]

    def __init__(self):
        """Initialize the application data."""
        self._events = []
        self._queries = []
        self._commands = []
        self._aggregates = []
        self._related_events = {}
        self._creation_events = {}

    @overload
    def event(self, event: None, **extra) -> _Decorator[_T_EventMeta]:
        ...

    @overload
    def event(self, event: _T_EventMeta, **extra) -> _T_EventMeta:
        ...

    def event(
        self, event: Optional[_T_EventMeta] = None, **_extra
    ) -> Union[_T_EventMeta, _Decorator[_T_EventMeta]]:
        """Register an event type."""

        def decorator(event: _T_EventMeta) -> _T_EventMeta:
            self._events.append(event)
            LOG.info("application event: %s", event.__name__)
            return event

        if event is not None:
            return decorator(event)
        return decorator

    @overload
    def query(self, query: None, **extra) -> _Decorator[_T_QueryMeta]:
        ...

    @overload
    def query(self, query: _T_QueryMeta, **extra) -> _T_QueryMeta:
        ...

    def query(
        self, query: Optional[_T_QueryMeta] = None, **_extra
    ) -> Union[_T_QueryMeta, _Decorator[_T_QueryMeta]]:
        """Register a query type."""

        def decorator(query: _T_QueryMeta) -> _T_QueryMeta:
            self._queries.append(query)
            LOG.info("application query: %s", query.__name__)
            return query

        if query is not None:
            return decorator(query)
        return decorator

    @overload
    def command(self, command: None, **extra) -> _Decorator[_T_CommandMeta]:
        ...

    @overload
    def command(self, command: _T_CommandMeta, **extra) -> _T_CommandMeta:
        ...

    def command(
        self, command: Optional[_T_CommandMeta] = None, **_extra
    ) -> Union[_T_CommandMeta, _Decorator[_T_CommandMeta]]:
        """Register a command type."""

        def decorator(command: _T_CommandMeta) -> _T_CommandMeta:
            self._commands.append(command)
            LOG.info("application command: %s", command.__name__)
            return command

        if command is not None:
            return decorator(command)
        return decorator

    @overload
    def aggregate(self, aggregate: None, **extra) -> _Decorator[_T_AggregateMeta]:
        ...

    @overload
    def aggregate(self, aggregate: _T_AggregateMeta, **extra) -> _T_AggregateMeta:
        ...

    def aggregate(
        self, aggregate: Optional[_T_AggregateMeta], **_extra
    ) -> Union[_T_AggregateMeta, _Decorator[_T_AggregateMeta]]:
        """Register an aggregate type."""

        def decorator(aggregate: _T_AggregateMeta) -> _T_AggregateMeta:
            self._aggregates.append(aggregate)
            for evt_class in aggregate.__agg_events__:
                self._related_events.setdefault(evt_class, []).append(aggregate)
            self._creation_events.setdefault(aggregate.__agg_create__, []).append(
                aggregate
            )
            LOG.info("application aggregate: %s", aggregate.__name__)
            return aggregate

        if aggregate is not None:
            return decorator(aggregate)
        return decorator

    def register_modules(
        self,
        *imports: str,
    ):
        """Register a full python module.

        You may want to define different components of your app in separate files.
        The `register_modules` method informs the app of these module locations, and
        the app will import them. If the imported module defines a `register_module`
        function, it will be called with the app instance.

        ## Example:

        in `yourproject/app.py`:
        ```
        APP: Application = ... # create and configure an Application instance
        ```

        in `yourproject/agg.py`:
        ```
        from yourproject.app import APP

        @APP.aggregate
        class YourAggregate(AggregateBase):
            # define some aggregate
            ...
        ```

        in `yourproject/event.py`:
        ```
        from yourproject.app import APP

        @APP.event
        class YourEvent(EventBase):
            # define some event
            ...
        ```

        in `thirdparty/utilmodule.py`:
        ```
        def register_module(app: Application):
            # registers one or more aggregates, events, commands, queries, or modules
            ...
        ```

        in `yourproject/__main__.py`:
        ```
        from yourproject.app import APP

        APP.register_modules(
            "yourproject.agg",
            "yourproject.event",
            "thirdparty.utilmodule"
        )

        if __name__ == "__main__":
            APP.run()
        ```
        """
        import importlib

        for mod in imports:
            imp = importlib.import_module(mod, None)
            registration_fn = getattr(imp, "register_module", None)
            if callable(registration_fn):
                registration_fn(self)

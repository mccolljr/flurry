"""The core Application type."""

import logging
from typing import Iterable, List, Tuple, TypeVar, Union


from money.aggregate import AggregateMeta
from money.command import CommandMeta
from money.event import EventMeta
from money.query import QueryMeta

TEventMeta = TypeVar("TEventMeta", bound=EventMeta)
TQueryMeta = TypeVar("TQueryMeta", bound=QueryMeta)
TCommandMeta = TypeVar("TCommandMeta", bound=CommandMeta)
TAggregateMeta = TypeVar("TAggregateMeta", bound=AggregateMeta)

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

    def event(self, evt: TEventMeta) -> TEventMeta:
        """Register an event type."""
        self._events.append(evt)
        LOG.info("application event: %s", evt.__name__)
        return evt

    def query(self, qry: TQueryMeta) -> TQueryMeta:
        """Register a query type."""
        self._queries.append(qry)
        LOG.info("application query: %s", qry.__name__)
        return qry

    def command(self, cmd: TCommandMeta) -> TCommandMeta:
        """Register a command type."""
        self._commands.append(cmd)
        LOG.info("application command: %s", cmd.__name__)
        return cmd

    def aggregate(self, agg: TAggregateMeta) -> TAggregateMeta:
        """Register an aggregate type."""
        self._aggregates.append(agg)
        for evt_class in agg.__agg_events__:
            self._related_events.setdefault(evt_class, []).append(agg)
        self._creation_events.setdefault(agg.__agg_create__, []).append(agg)
        LOG.info("application aggregate: %s", agg.__name__)
        return agg

    def register_modules(
        self,
        *imports: Union[str, Iterable[str], Tuple[str, str], Tuple[str, Iterable[str]]],
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
            pkg, names = None, []
            if isinstance(mod, tuple):
                pkg = mod[0]
                if isinstance(mod[1], str):
                    names = [mod[1]]
                else:
                    names = list(mod[1])
            else:
                if isinstance(mod, str):
                    names = [mod]
                else:
                    names = list(mod)

            for name in names:
                imp = importlib.import_module(name, pkg)
                registration_fn = getattr(imp, "register_module", None)
                if callable(registration_fn):
                    registration_fn(self)

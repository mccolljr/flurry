import json
import logging
from typing import Iterable, List, NamedTuple, Tuple, TypeVar, Union

import graphene
from aiohttp import web
from aiohttp_middlewares.cors import cors_middleware

from money.framework.aggregate import AggregateMeta
from money.framework.command import CommandMeta
from money.framework.event import EventMeta
from money.framework.graphql.generator import GraphQLGenerator
from money.framework.query import QueryMeta
from money.framework.storage import Storage

TEventMeta = TypeVar("TEventMeta", bound=EventMeta)
TQueryMeta = TypeVar("TQueryMeta", bound=QueryMeta)
TCommandMeta = TypeVar("TCommandMeta", bound=CommandMeta)
TAggregateMeta = TypeVar("TAggregateMeta", bound=AggregateMeta)


class Application:
    """Application is the core type for applications using the framework.
    It is responsible for registering events, queries, commands, aggregates,
    modules, etc.

    By itself, an Application doesn't know how to run a web server or handle requests.
    This behavior is delegated to subclasses of Application.
    """

    _events: List[EventMeta]
    _queries: List[QueryMeta]
    _commands: List[CommandMeta]
    _aggregates: List[AggregateMeta]

    def __init__(self):
        self._events = []
        self._queries = []
        self._commands = []
        self._aggregates = []
        self._related_events = {}
        self._creation_events = {}

    def event(self, evt: TEventMeta) -> TEventMeta:
        self._events.append(evt)
        return evt

    def query(self, qry: TQueryMeta) -> TQueryMeta:
        self._queries.append(qry)
        return qry

    def command(self, cmd: TCommandMeta) -> TCommandMeta:
        self._commands.append(cmd)
        return cmd

    def aggregate(self, agg: TAggregateMeta) -> TAggregateMeta:
        self._aggregates.append(agg)
        for evt_class in agg.__agg_events__:
            self._related_events.setdefault(evt_class, []).append(agg)
        self._creation_events.setdefault(agg.__agg_create__, []).append(agg)
        return agg

    def register_modules(
        self,
        *imports: Union[str, Iterable[str], Tuple[str, str], Tuple[str, Iterable[str]]],
    ):
        """
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

    def run(self):
        raise NotImplementedError


class CorsOptions(NamedTuple):
    """CORS configuration options."""

    allow_origin: str


class GraphQLApplication(Application):
    """An Application that provides a GraphQL interface over HTTP."""

    def __init__(self, storage: Storage, cors_opts: CorsOptions = None):
        super().__init__()
        self.storage = storage
        self.cors_opts = cors_opts

    @property
    def gql_schema(self) -> graphene.Schema:
        existing = getattr(self, "__gql_schema", None)
        if existing is None:
            logging.info("building graphql schema")
            existing = GraphQLGenerator(self).generate_schema()
            setattr(self, "__gql_schema", existing)
            logging.info("schema built successfully")
        return existing

    def run(self):
        web.run_app(self._setup_app())

    def _setup_app(self):
        _ = self.gql_schema
        web_app = web.Application(
            middlewares=[
                cors_middleware(origins=[self.cors_opts.allow_origin])
                if self.cors_opts is not None
                else cors_middleware(allow_all=True)
            ]
        )
        web_app.add_routes([web.post("/", self._handle_req)])
        return web_app

    async def _handle_req(self, req: web.Request):
        body = await req.json()
        query = body.get("query", None)
        var_vals = body.get("variables", None)
        result = await self.gql_schema.execute_async(
            query,
            variable_values=var_vals,
            context=graphene.Context(storage=self.storage),
        )
        if result.errors:
            return web.Response(body=",".join(map(str, result.errors)), status=500)
        return web.Response(body=json.dumps(result.data), status=200)

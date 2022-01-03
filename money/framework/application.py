import json
import logging
from inspect import isawaitable
from typing import Any, Dict, Iterable, List, Tuple, Type, TypeVar, Union

import graphene
from aiohttp import web


from money.framework import schema
from money.framework.aggregate import AggregateMeta
from money.framework.command import CommandMeta
from money.framework.event import EventMeta
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


class GraphQLApplication(Application):
    """An Application that provides a GraphQL interface over HTTP."""

    def __init__(self, storage: Storage):
        super().__init__()
        self.storage = storage

    @property
    def gql_schema(self) -> graphene.Schema:
        existing = getattr(self, "__gql_schema", None)
        if existing is None:
            logging.info("building graphql schema")
            existing = self._build_schema()
            setattr(self, "__gql_schema", existing)
            logging.info("schema built successfully")
        return existing

    def _build_schema(self) -> graphene.Schema:
        from graphene.types.unmountedtype import UnmountedType

        gql_types: List[Type[graphene.ObjectType]] = []

        def field_to_graphql(
            field: schema.Field[Any],
        ) -> graphene.Field:
            return graphene.Field(
                field_kind_to_graphql(field.kind), required=not field.nullable
            )

        def arg_to_graphql(
            field: schema.Field[Any],
        ) -> graphene.Argument:
            return graphene.Argument(
                field_kind_to_graphql(field.kind), required=not field.nullable
            )

        def field_kind_to_graphql(
            kind: schema.FieldKind[Any],
        ) -> Union[Type[graphene.ObjectType], Type[UnmountedType], UnmountedType]:
            if isinstance(kind, schema.Str):
                return graphene.String
            if isinstance(kind, schema.Int):
                return graphene.Int
            if isinstance(kind, schema.Float):
                return graphene.Float
            if isinstance(kind, schema.Bool):
                return graphene.Boolean
            if isinstance(kind, schema.Bytes):
                return graphene.String
            if isinstance(kind, schema.DateTime):
                return graphene.DateTime
            if isinstance(kind, schema.Collection):
                return graphene.List(
                    graphene.NonNull(field_kind_to_graphql(kind.of_kind))
                )
            if isinstance(kind, schema.Object):
                assert isinstance(kind.of_typ, schema.SchemaMeta)
                obj_name = f"Gql{kind.of_typ.__name__}Object"
                obj_type = next(
                    (ot for ot in gql_types if ot.__name__ == obj_name), None
                )
                if obj_type is None:
                    obj_type = schema_to_graphql(obj_name, kind.of_typ.__schema__)
                    gql_types.append(obj_type)
                return obj_type
            if hasattr(kind, "to_graphql"):
                return getattr(kind, "to_graphql")()
            raise TypeError(f"unknown field kind {type(kind)}")

        def schema_to_graphql(
            name: str, convert_schema: Dict[str, schema.Field[Any]]
        ) -> Type[graphene.ObjectType]:
            return type(
                name,
                (graphene.ObjectType,),
                {
                    name: field_to_graphql(field)
                    for name, field in convert_schema.items()
                },
            )

        def query_to_graphql(
            query_name: str, query: QueryMeta
        ) -> Type[graphene.ObjectType]:
            query_attrs: Dict[str, Any] = {
                name: field_to_graphql(field)
                for name, field in query.Result.__schema__.items()
            }
            return type(query_name, (graphene.ObjectType,), query_attrs)

        def agg_to_graphql(agg: AggregateMeta) -> Type[graphene.ObjectType]:
            return schema_to_graphql(f"Gql{agg.__name__}Object", agg.__schema__)

        def cmd_to_graphql(cmd: CommandMeta) -> graphene.Field:
            mutation_class: Type[graphene.Mutation]
            custom_results = getattr(cmd, "Result", None) is not None
            result_fields = (
                {
                    name: field_to_graphql(field)
                    for name, field in cmd.Result.__schema__.items()
                }
                if hasattr(cmd, "Result") and cmd.Result
                else {"ok": graphene.Field(graphene.Boolean, required=True)}
            )
            arg_fields = {
                name: arg_to_graphql(field) for name, field in cmd.__schema__.items()
            }
            Arguments = type("Arguments", (), arg_fields)

            async def mutate(*_, **args):
                try:
                    cmd_inst = cmd(**args)
                    result = cmd_inst.exec(storage=self.storage)
                    if isawaitable(result):
                        result = await result
                    if custom_results:
                        return mutation_class(**result.to_dict())
                    return mutation_class(ok=True)
                except Exception as err:
                    logging.error(
                        "failure while processing command: %s",
                        cmd.__name__,
                        exc_info=err,
                    )
                    raise

            mutation_class = type(
                f"Gql{cmd.__name__}Mutation",
                (graphene.Mutation,),
                dict(
                    Arguments=Arguments,
                    mutate=mutate,
                    **result_fields,
                ),
            )
            return mutation_class.Field()

        def gen_gql_types():
            return [agg_to_graphql(agg) for agg in self._aggregates]

        def gen_gql_mutations():
            return type(
                "GqlMutations",
                (graphene.ObjectType,),
                {
                    f"Gql{cmd.__name__}Mutation": cmd_to_graphql(cmd)
                    for cmd in self._commands
                },
            )

        def gen_gql_query():
            def gen_resolver(name: str, query_typ: QueryMeta):
                resolver_arg = None
                query_argtyp = None
                if hasattr(query_typ, "Arguments") and query_typ.Arguments is not None:
                    query_argtyp = query_typ.Arguments
                    resolver_arg = {
                        name: arg_to_graphql(field)
                        for name, field in query_argtyp.__schema__.items()
                    }

                async def resolver_fn(*_, **args):
                    query_val = query_typ()
                    if query_argtyp is not None:
                        result = query_val.fetch(self.storage, query_argtyp(**args))
                    else:
                        result = query_val.fetch(self.storage)
                    if isawaitable(result):
                        result = await result
                    return result

                resolver_fn.__name__ = name
                return resolver_fn, resolver_arg

            attrs = {}
            for query in self._queries:
                query_name = f"Gql{query.__name__}Query"
                gql_obj = query_to_graphql(query_name, query)
                resolver_name = f"resolve_{gql_obj.__name__}"
                resolver_fn, resolver_arg = gen_resolver(resolver_name, query)
                attrs[query_name] = graphene.Field(
                    gql_obj, **(resolver_arg if resolver_arg else {})
                )
                attrs[resolver_name] = resolver_fn
            return type("GqlQuery", (graphene.ObjectType,), attrs)

        gql_types.extend(gen_gql_types())
        gql_mutations = gen_gql_mutations()
        gql_query = gen_gql_query()
        return graphene.Schema(query=gql_query, mutation=gql_mutations, types=gql_types)

    def run(self):
        web.run_app(self._setup_app())

    def _setup_app(self):
        _ = self.gql_schema
        web_app = web.Application()
        web_app.add_routes([web.post("/", self._handle_req)])
        return web_app

    async def _handle_req(self, req: web.Request):
        body = await req.json()
        query = body.get("query", None)
        var_vals = body.get("variables", None)
        result = await self.gql_schema.execute_async(query, variable_values=var_vals)
        if result.errors:
            return web.Response(body=",".join(map(str, result.errors)), status=500)
        return web.Response(body=json.dumps(result.data), status=200)

from inspect import isawaitable
import json
import logging
import asyncio
from typing import Any, Dict, List, Set, Type, TypeVar, Union

import graphene
import aiohttp.web as web
from graphene.types.scalars import Scalar


import money.framework.schema as schema
from money.framework.aggregate import AggregateMeta
from money.framework.command import CommandMeta
from money.framework.event import EventMeta
from money.framework.query import QueryMeta
from money.framework.storage import Storage

TEvtMeta = TypeVar("TEvtMeta", bound=EventMeta)
TQryMeta = TypeVar("TQryMeta", bound=QueryMeta)
TCmdMeta = TypeVar("TCmdMeta", bound=CommandMeta)
TAggMeta = TypeVar("TAggMeta", bound=AggregateMeta)


class Application:
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
        pass

    def event(self, evt: TEvtMeta) -> TEvtMeta:
        self._events.append(evt)
        return evt

    def query(self, qry: TQryMeta) -> TQryMeta:
        self._queries.append(qry)
        return qry

    def command(self, cmd: TCmdMeta) -> TCmdMeta:
        self._commands.append(cmd)
        return cmd

    def aggregate(self, agg: TAggMeta) -> TAggMeta:
        self._aggregates.append(agg)
        for e in agg.__agg_events__:
            self._related_events.setdefault(e, []).append(agg)
        self._creation_events.setdefault(agg.__agg_create__, []).append(agg)
        return agg

    def run(self):
        raise NotImplementedError


class GraphQLApplication(Application):
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
                field_kind_to_graphql(field._kind), required=not field._nullable
            )

        def arg_to_graphql(
            field: schema.Field[Any],
        ) -> graphene.Argument:
            return graphene.Argument(
                field_kind_to_graphql(field._kind), required=not field._nullable
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
                return graphene.List(graphene.NonNull(field_kind_to_graphql(kind.of)))
            if isinstance(kind, schema.Object):
                assert isinstance(kind.of, schema.SchemaMeta)
                obj_name = f"Gql{kind.of.__name__}Object"
                obj_type = next(
                    (ot for ot in gql_types if ot.__name__ == obj_name), None
                )
                if obj_type is None:
                    obj_type = schema_to_graphql(obj_name, kind.of.__schema__)
                    gql_types.append(obj_type)
                return obj_type
            if hasattr(kind, "to_graphql"):
                return getattr(kind, "to_graphql")()
            raise TypeError(f"unknown field kind {type(kind)}")

        def schema_to_graphql(
            name: str, schema: Dict[str, schema.Field[Any]]
        ) -> Type[graphene.ObjectType]:
            return type(
                name,
                (graphene.ObjectType,),
                {name: field_to_graphql(field) for name, field in schema.items()},
            )

        def query_to_graphql(query: QueryMeta) -> Type[graphene.ObjectType]:
            query_name = f"Gql{query.__name__}Query"
            query_attrs: Dict[str, Any] = {
                name: field_to_graphql(field)
                for name, field in query.Result.__schema__.items()
            }
            return type(query_name, (graphene.ObjectType,), query_attrs)

        def agg_to_graphql(agg: AggregateMeta) -> Type[graphene.ObjectType]:
            return schema_to_graphql(f"Gql{agg.__name__}Object", agg.__schema__)

        def cmd_to_graphql(cmd: CommandMeta) -> graphene.Field:
            Mutation: Type[graphene.Mutation]
            custom_results = getattr(cmd, "Result", None) is not None
            result_fields = (
                {
                    name: field_to_graphql(field)
                    for name, field in cmd.Result.__schema__.items()
                }
                if hasattr(cmd, "Result") and cmd.Result
                else {"ok": graphene.Boolean(required=True)}
            )
            arg_fields = {
                name: arg_to_graphql(field) for name, field in cmd.__schema__.items()
            }
            Arguments = type("Arguments", (), arg_fields)

            async def mutate(parent: Any, info: graphene.ResolveInfo, **args):
                cmd_inst = cmd(**args)
                result = cmd_inst.exec(storage=self.storage)
                if info.is_awaitable(result):
                    result = await result
                if custom_results:
                    return Mutation(**result.to_dict())
                return Mutation(ok=True)

            Mutation = type(
                f"Gql{cmd.__name__}Mutation",
                (graphene.Mutation,),
                dict(
                    Arguments=Arguments,
                    mutate=mutate,
                    **result_fields,
                ),
            )
            return Mutation.Field()

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

                async def resolver_fn(parent: Any, info: graphene.ResolveInfo, **args):
                    q = query_typ()
                    if query_argtyp is not None:
                        result = q.fetch(self.storage, query_argtyp(**args))
                    else:
                        result = q.fetch(self.storage)
                    if isawaitable(result):
                        result = await result
                    return result

                resolver_fn.__name__ = name
                return resolver_fn, resolver_arg

            attrs = {}
            for query in self._queries:
                gql_obj = query_to_graphql(query)
                resolver_name = f"resolve_{gql_obj.__name__}"
                resolver_fn, resolver_arg = gen_resolver(resolver_name, query)
                attrs[gql_obj.__name__] = graphene.Field(
                    gql_obj, **(resolver_arg if resolver_arg else {})
                )
                attrs[resolver_name] = resolver_fn
            return type("GqlQuery", (graphene.ObjectType,), attrs)

        gql_types.extend(gen_gql_types())
        gql_mutations = gen_gql_mutations()
        gql_query = gen_gql_query()
        return graphene.Schema(query=gql_query, mutation=gql_mutations, types=gql_types)

    def run(self):
        web_app = self._setup_app()
        loop = asyncio.new_event_loop()
        runner = web.AppRunner(web_app)
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner)
        loop.run_until_complete(site.start())
        try:

            def on_exc(loop: asyncio.AbstractEventLoop, ctx: dict):
                logging.error(ctx["message"], exc_info=ctx["exception"])

            loop.set_exception_handler(on_exc)
            loop.run_forever()
        except KeyboardInterrupt:
            print("shutting down...")
            loop.run_until_complete(site.stop())
        finally:
            loop.close()

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

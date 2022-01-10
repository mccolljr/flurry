"""Graphql schema generation."""

from __future__ import annotations
import logging
from inspect import isawaitable
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)


import graphene
from graphene.types.unmountedtype import UnmountedType

from money import schema

if TYPE_CHECKING:
    from money.query import QueryMeta
    from money.command import CommandMeta
    from money.application import Application

    GraphQLAnyObject = Union[graphene.ObjectType, graphene.InputObjectType]
    GraphQLConvertedField = Union[
        Type[GraphQLAnyObject],
        Type[UnmountedType],
        UnmountedType,
    ]


class GraphqlGenerator:
    """A class capable of generating a graphql schema for the given target application."""

    gql_types: Dict[Type[GraphQLAnyObject], Dict[str, Type[GraphQLAnyObject]]]

    def __init__(self, target_app: Application):
        """Initialize a GraphqlGenerator for the target application."""
        self.target_app = target_app
        self.gql_types = {graphene.ObjectType: {}, graphene.InputObjectType: {}}

    def mutation_name(self, cmd: CommandMeta) -> str:
        """Get the name of the mutation generated for this command class."""
        return cmd.__name__

    def mutation_result_name(self, cmd: CommandMeta) -> str:
        """Get the name of the mutation result generated for this command class."""
        return f"{cmd.__name__}Result"

    def query_name(self, query: QueryMeta) -> str:
        """Get the name of the query generated for this query class."""
        return query.__name__

    def query_result_name(self, query: QueryMeta) -> str:
        """Get the name of the query result generated for this query class."""
        return f"{query.__name__}Result"

    def query_resolver_name(self, query: QueryMeta) -> str:
        """Get the name of the query resolver generated for this query class."""
        return f"resolve_{query.__name__}"

    def field_to_graphql_field(
        self,
        field: schema.Field[Any],
        containing_type: Type[GraphQLAnyObject],
    ) -> graphene.Field:
        """Generate a graphql object field from the given Field."""
        return graphene.Field(
            self.field_kind_to_graphql_kind(
                field.kind, containing_type=containing_type
            ),
            required=not field.nullable,
        )

    def field_to_graphql_argument(
        self,
        field: schema.Field[Any],
    ) -> graphene.Argument:
        """Generate a graphql argument field from the given Field."""
        return graphene.Argument(
            self.field_kind_to_graphql_kind(
                field.kind, containing_type=graphene.InputObjectType
            ),
            required=not field.nullable,
        )

    def get_graphql_object_type(
        self, source: schema.SchemaMeta, base: Type[GraphQLAnyObject]
    ) -> Type[GraphQLAnyObject]:
        """Generate or look up the object type of the appropriate kind."""
        obj_name = source.__name__
        if base is graphene.InputObjectType:
            obj_name += "Input"
        obj_type: Type[GraphQLAnyObject]
        if obj_name in self.gql_types[base]:
            obj_type = self.gql_types[base][obj_name]
        else:
            obj_type = self.schema_to_graphql_object(
                obj_name, source.__schema__, object_type=base
            )
            self.gql_types[base][obj_name] = obj_type
        return obj_type

    def field_kind_to_graphql_kind(
        self,
        kind: schema.FieldKind[Any],
        containing_type: Type[GraphQLAnyObject],
    ) -> GraphQLConvertedField:
        """Generate the appropriate graphql field type based on the given FieldKind."""
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
            subtyp = self.field_kind_to_graphql_kind(
                kind.of_kind, containing_type=containing_type
            )
            return graphene.List(graphene.NonNull(subtyp))
        if isinstance(kind, schema.Object):
            return self.get_graphql_object_type(
                cast(schema.SchemaMeta, kind.of_typ), containing_type
            )
        if hasattr(kind, "to_graphql"):
            return getattr(kind, "to_graphql")()
        raise TypeError(f"unknown field kind {type(kind)}")

    def schema_to_graphql_object(
        self,
        name: str,
        the_schema: Dict[str, schema.Field[Any]],
        object_type: Type[GraphQLAnyObject] = graphene.ObjectType,
    ) -> Type[Union[graphene.ObjectType, graphene.InputObjectType]]:
        """Convert a schema to a graphql object type of the appropriate kind."""
        return type(
            name,
            (object_type,),
            {
                name: self.field_to_graphql_field(field, containing_type=object_type)
                for name, field in the_schema.items()
            },
        )

    def generate_graphql_mutation(
        self, command: CommandMeta
    ) -> Type[graphene.Mutation]:
        """Generate the mutation for a command, including the mutation function."""
        mutation_result_name = self.mutation_result_name(command)
        argument_type = type(
            "Arguments",
            (),
            {
                name: self.field_to_graphql_argument(field)
                for name, field in command.__schema__.items()
            },
        )
        if hasattr(command, "Result") and command.Result is not None:
            custom_results = True
            result_fields = {
                name: self.field_to_graphql_field(
                    field, containing_type=graphene.ObjectType
                )
                for name, field in command.Result.__schema__.items()
            }
        else:
            custom_results = False
            result_fields = {"ok": graphene.Field(graphene.Boolean, required=True)}

        mutation_type: Type[graphene.Mutation]

        async def mutate(_root: Any, info: graphene.ResolveInfo, **kwargs):
            try:
                command_inst = command(**kwargs)
                result = command_inst.exec(storage=info.context.storage)
                if isawaitable(result):
                    result = await result
                if custom_results:
                    return mutation_type(**result.to_dict())
                return mutation_type(ok=True)
            except Exception as err:
                logging.error(
                    "failure while processing command: %s",
                    command.__name__,
                    exc_info=err,
                )
                raise

        mutation_type = type(
            mutation_result_name,
            (graphene.Mutation,),
            dict(Arguments=argument_type, mutate=mutate, **result_fields),
        )
        return mutation_type

    def generate_graphql_query(
        self, query: QueryMeta
    ) -> Tuple[
        Type[graphene.ObjectType], Dict[str, graphene.Argument], Callable[..., Any]
    ]:
        """Generate the result object type, the argument mapping, and the resolver for a query."""
        result_type = type(
            self.query_result_name(query),
            (graphene.ObjectType,),
            {
                name: self.field_to_graphql_field(
                    field, containing_type=graphene.ObjectType
                )
                for name, field in query.Result.__schema__.items()
            },
        )

        arg_type: Optional[schema.SchemaMeta] = None
        resolver_args: Dict[str, graphene.Argument] = {}
        if hasattr(query, "Arguments") and query.Arguments is not None:
            arg_type = query.Arguments
            resolver_args = {
                name: self.field_to_graphql_argument(field)
                for name, field in query.Arguments.__schema__.items()
            }

        async def resolver_fn(_root: Any, info: graphene.ResolveInfo, **kwargs):
            try:
                query_inst = query()
                result = (
                    query_inst.fetch(info.context.storage, arg_type(**kwargs))
                    if arg_type is not None
                    else query_inst.fetch(info.context.storage)
                )
                if isawaitable(result):
                    result = await result
                return result
            except Exception as err:
                logging.error(
                    "failure while processing query: %s",
                    query.__name__,
                    exc_info=err,
                )
                raise

        resolver_fn.__name__ = self.query_resolver_name(query)
        return result_type, resolver_args, resolver_fn

    def generate_graphql_query_root(self) -> Type[graphene.ObjectType]:
        """Generate the schema's root mutation query type."""
        attrs: dict = {}
        for query in getattr(self.target_app, "_queries"):
            query_name = self.query_name(query)
            result_type, resolver_args, resolver_fn = self.generate_graphql_query(query)
            query_field = graphene.Field(
                result_type, resolver=resolver_fn, args=resolver_args
            )
            attrs[query_name] = query_field
        return type("QueryRoot", (graphene.ObjectType,), attrs)

    def generate_graphql_mutation_root(self) -> Type[graphene.ObjectType]:
        """Generate the schema's root mutation object type."""
        return type(
            "MutationRoot",
            (graphene.ObjectType,),
            {
                self.mutation_name(command): self.generate_graphql_mutation(
                    command
                ).Field()
                for command in getattr(self.target_app, "_commands")
            },
        )

    def collect_graphql_object_types(self):
        """Collect all of the generated input and output object types into a unified list."""
        all_types: List[Type[GraphQLAnyObject]] = []
        for otyp in self.gql_types[graphene.ObjectType].values():
            all_types.append(otyp)
        for otyp in self.gql_types[graphene.InputObjectType].values():
            all_types.append(otyp)
        return all_types

    def generate_schema(self) -> graphene.Schema:
        """Generate a schema from the commands, queries, etc. registered on the target application."""
        gql_query = self.generate_graphql_query_root()
        gql_mutation = self.generate_graphql_mutation_root()
        gql_types = self.collect_graphql_object_types()
        return graphene.Schema(query=gql_query, mutation=gql_mutation, types=gql_types)


__all__ = ["GraphqlGenerator"]

# pylint: disable=missing-class-docstring
from typing import cast

from flurry.core import schema
from flurry.graphql import GraphqlApplication
from flurry.core.query import QueryBase
from flurry.core.context import Context
from flurry.core.command import CommandBase

_APP = GraphqlApplication(cast(Context, None))


@_APP.query()
class FooQuery(QueryBase):
    a = schema.Field(schema.Str)
    b = schema.Field(schema.DateTime)

    class Result(schema.SchemaBase):
        x = schema.Field(schema.Float)
        y = schema.Field(schema.Collection(schema.Bool()))

    async def fetch(self, context: Context) -> Result:
        return self.Result(x=1.0, y=[True, False])


class QuuxData(schema.SchemaBase):
    q = schema.Field(schema.Str)
    r = schema.Field(schema.Str)
    s = schema.Field(schema.Float)


@_APP.command()
class BarCommand(CommandBase):
    a = schema.Field(schema.Str)
    b = schema.Field(schema.DateTime)
    z = schema.Field(schema.Object(QuuxData))

    class Result(schema.SchemaBase):
        x = schema.Field(schema.Float)
        y = schema.Field(schema.Object(QuuxData))

    async def exec(self, context: Context) -> Result:
        return self.Result(x=1, y=QuuxData())


def test_schema(snapshot):
    assert str(_APP.gql_schema) == snapshot(name="graphql-schema")

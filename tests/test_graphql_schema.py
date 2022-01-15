from typing import cast

from fete.core import schema
from fete.graphql import GraphqlApplication
from fete.core.query import QueryBase
from fete.core.context import Context
from fete.core.command import CommandBase

_APP = GraphqlApplication(cast(Context, None))


@_APP.query()
class FooQuery(QueryBase):
    a = schema.Field(schema.Str)
    b = schema.Field(schema.DateTime)

    class Result(schema.SchemaBase):
        x = schema.Field(schema.Float)
        y = schema.Field(schema.Collection(schema.Bool()))

    def fetch(self, context: Context) -> Result:
        return self.Result(1)


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

    def exec(self, context: Context) -> Result:
        return self.Result(1)


def test_schema():
    assert (
        str(_APP.gql_schema)
        == """schema {
  query: QueryRoot
  mutation: MutationRoot
}

type QuuxData {
  q: String
  r: String
  s: Float
}

input QuuxDataInput {
  q: String = null
  r: String = null
  s: Float = null
}

type QueryRoot {
  FooQuery(a: String = null, b: DateTime = null): FooQueryResult
}

type FooQueryResult {
  x: Float
  y: [Boolean!]
}

\"\"\"
The `DateTime` scalar type represents a DateTime
value as specified by
[iso8601](https://en.wikipedia.org/wiki/ISO_8601).
\"\"\"
scalar DateTime

type MutationRoot {
  BarCommand(a: String = null, b: DateTime = null, z: QuuxDataInput = null): BarCommandResult
}

type BarCommandResult {
  x: Float
  y: QuuxData
}
"""
    )

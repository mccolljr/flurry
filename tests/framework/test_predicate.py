from typing import Any, Dict
import pytest

import money.framework.schema as schema
from money.framework.predicate import (
    And,
    Between,
    Eq,
    Is,
    Less,
    LessEq,
    More,
    MoreEq,
    OneOf,
    Or,
    Predicate,
    Where,
)


class First(schema.SchemaBase, metaclass=schema.SchemaMeta):
    a = schema.Field(schema.Str)
    b = schema.Field(schema.Int)


class Second(schema.SchemaBase, metaclass=schema.SchemaMeta):
    x = schema.Field(schema.Bytes)
    y = schema.Field(schema.Float)


@pytest.mark.parametrize(
    ("value", "tests"),
    [
        (
            First(a="a", b=2),
            {
                # passes
                Is(First): True,
                Or[Any](Is(First), Is(Second)): True,
                Where(a=Eq("a"), b=Eq(2)): True,
                Where(a=Less("b"), b=Less(3)): True,
                Where(a=More(""), b=More(1)): True,
                Where(a=LessEq("a"), b=LessEq(2)): True,
                Where(a=MoreEq("a"), b=MoreEq(2)): True,
                Where(a=OneOf("a", "z"), b=OneOf(2, 9999)): True,
                Where(a=Between("", "z"), b=Between(1, 3)): True,
                # fails
                Is(Second): False,
                And[Any](Is(First), Is(Second)): False,
                Where(a=Eq("z"), b=Eq(9999)): False,
                Where(a=Less(""), b=Less(0)): False,
                Where(a=More("z"), b=More(3)): False,
                Where(a=LessEq(""), b=LessEq(0)): False,
                Where(a=MoreEq("z"), b=MoreEq(3)): False,
                Where(a=OneOf("x", "y"), b=OneOf(3, 4)): False,
                Where(a=Between("z", "zz"), b=Between(3, 5)): False,
            },
        ),
    ],
)
def test_predicates(value: Any, tests: Dict[Predicate[Any], bool]):
    for pred, want_pass in tests.items():
        assert pred(value) == want_pass, f"pred={pred}"

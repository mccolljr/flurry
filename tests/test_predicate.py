from typing import Any, Dict
import pytest

from fete.core import predicate as P, schema


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
                P.Is(First): True,
                P.Or(P.Is(First), P.Is(Second)): True,
                P.Where(a=P.Eq("a"), b=P.Eq(2)): True,
                P.Where(a=P.NotEq("z"), b=P.NotEq(9999)): True,
                P.Where(a=P.Less("b"), b=P.Less(3)): True,
                P.Where(a=P.More(""), b=P.More(1)): True,
                P.Where(a=P.LessEq("a"), b=P.LessEq(2)): True,
                P.Where(a=P.MoreEq("a"), b=P.MoreEq(2)): True,
                P.Where(a=P.OneOf("a", "z"), b=P.OneOf(2, 9999)): True,
                P.Where(a=P.Between("", "z"), b=P.Between(1, 3)): True,
                # fails
                P.Is(Second): False,
                P.And(P.Is(First), P.Is(Second)): False,
                P.Where(a=P.Eq("z"), b=P.Eq(9999)): False,
                P.Where(a=P.NotEq("a"), b=P.NotEq(2)): False,
                P.Where(a=P.Less(""), b=P.Less(0)): False,
                P.Where(a=P.More("z"), b=P.More(3)): False,
                P.Where(a=P.LessEq(""), b=P.LessEq(0)): False,
                P.Where(a=P.MoreEq("z"), b=P.MoreEq(3)): False,
                P.Where(a=P.OneOf("x", "y"), b=P.OneOf(3, 4)): False,
                P.Where(a=P.Between("z", "zz"), b=P.Between(3, 5)): False,
            },
        ),
        (
            Second(x=b"a", y=2.5),
            {
                # passes
                P.Is(Second): True,
                P.Or(P.Is(Second), P.Is(First)): True,
                P.Where(x=P.Eq(b"a"), y=P.Eq(2.5)): True,
                P.Where(x=P.NotEq(b"b"), y=P.NotEq(2.6)): True,
                P.Where(x=P.Less(b"c"), y=P.Less(3)): True,
                P.Where(x=P.More(b""), y=P.More(2.1)): True,
                P.Where(x=P.LessEq(b"a"), y=P.LessEq(2.6)): True,
                P.Where(x=P.MoreEq(b"a"), y=P.MoreEq(2.4)): True,
                P.Where(x=P.OneOf(b"a", b"z"), y=P.OneOf(2.5, 2.6)): True,
                P.Where(x=P.Between(b"", b"z"), y=P.Between(2.4, 2.6)): True,
                # fails
                P.Is(First): False,
                P.And(P.Is(Second), P.Is(First)): False,
                P.Where(x=P.Eq(b"z"), y=P.Eq(2.5)): False,
                P.Where(x=P.NotEq(b"a"), y=P.NotEq(2.5)): False,
                P.Where(x=P.Less(b""), y=P.Less(0)): False,
                P.Where(x=P.More(b"z"), y=P.More(3)): False,
                P.Where(x=P.LessEq(b""), y=P.LessEq(0)): False,
                P.Where(x=P.MoreEq(b"z"), y=P.MoreEq(3)): False,
                P.Where(x=P.OneOf(b"x", b"y"), y=P.OneOf(3, 4)): False,
                P.Where(x=P.Between(b"z", b"zz"), y=P.Between(3, 5)): False,
            },
        ),
    ],
)
def test_predicates(value: Any, tests: Dict[P.Predicate, bool]):
    for pred, want_pass in tests.items():
        assert pred == pred
        assert pred(value) == want_pass, f"pred={pred}"

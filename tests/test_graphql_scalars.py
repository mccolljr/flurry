import json
import pytest
import graphql.language.parser as gqlparse

from flurry.core import predicate as P
from flurry.graphql.scalars import JSONScalar, PredicateScalar


@pytest.mark.parametrize(
    ["have", "want"],
    [
        (
            P.Where(
                a=P.Eq(1),
                b=P.Less(2),
                c=P.More(3),
                d=P.LessEq(4),
                e=P.MoreEq(5),
                f=P.Between(6, 7),
                g=P.OneOf(8, 9, 10),
            ),
            "{ where: { a: { eq: 1 }, b: { less: 2 }, c: { more: 3 }, d: { less_eq: 4 }, e: { more_eq: 5 }, f: { between: [6, 7] }, g: { one_of: [8, 9, 10] } } }",
        ),
        (
            P.And(
                P.Where(a=P.Eq(1)),
                P.Where(b=P.Less(2)),
                P.Where(c=P.More(3)),
                P.Where(d=P.LessEq(4)),
                P.Where(e=P.MoreEq(5)),
                P.Where(f=P.Between(6, 7)),
                P.Where(g=P.OneOf(8, 9, 10)),
            ),
            "{ and: [{ where: { a: { eq: 1 } } }, { where: { b: { less: 2 } } }, { where: { c: { more: 3 } } }, { where: { d: { less_eq: 4 } } }, { where: { e: { more_eq: 5 } } }, { where: { f: { between: [6, 7] } } }, { where: { g: { one_of: [8, 9, 10] } } }] }",
        ),
        (
            P.Or(
                P.Where(a=P.Eq(1)),
                P.Where(b=P.Less(2)),
                P.Where(c=P.More(3)),
                P.Where(d=P.LessEq(4)),
                P.Where(e=P.MoreEq(5)),
                P.Where(f=P.Between(6, 7)),
                P.Where(g=P.OneOf(8, 9, 10)),
            ),
            "{ or: [{ where: { a: { eq: 1 } } }, { where: { b: { less: 2 } } }, { where: { c: { more: 3 } } }, { where: { d: { less_eq: 4 } } }, { where: { e: { more_eq: 5 } } }, { where: { f: { between: [6, 7] } } }, { where: { g: { one_of: [8, 9, 10] } } }] }",
        ),
        (
            P.Or(P.And(P.Or(P.And()))),
            "{ or: [{ and: [{ or: [{ and: [] }] }] }] }",
        ),
    ],
)
def test_predicate_scalar_serde(have, want):
    serialized = PredicateScalar.serialize(have)
    assert serialized == want
    deserialized = PredicateScalar.parse_literal(gqlparse.parse_value(want))
    assert deserialized == have


@pytest.mark.parametrize(
    ["have", "want"],
    [
        (None, "null"),
        (1, json.dumps(1)),
        (1.5, json.dumps(1.5)),
        (True, json.dumps(True)),
        (False, json.dumps(False)),
        ({"a": 1}, json.dumps({"a": 1})),
        ([1, 2], json.dumps([1, 2])),
    ],
)
def test_json_scalar_serde(have, want):
    serialized = JSONScalar.serialize(have)
    assert serialized == want
    deserialized = JSONScalar.parse_literal(gqlparse.parse_value(json.dumps(want)))
    assert deserialized == have

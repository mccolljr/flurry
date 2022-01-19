from flurry.core import predicate as P
from flurry.core.utils import visit_predicate
from flurry.postgres.postgres import _PostgreSQLSimplifier

predicates_to_simplify = {
    "empty_or": P.Or(),
    "empty_is": P.Is(),
    "empty_and": P.And(),
    "empty_where": P.Where(),
    "simple_and": P.And(P.Where(a=1), P.Where(b=2)),
    "simple_or": P.Or(P.Where(a=1), P.Where(b=2)),
    "simple_is": P.Is(str, int, float),
    "simple_where": P.Where(
        a=P.Eq(1),
        b=P.NotEq(2),
        c=P.Less(3),
        d=P.More(4),
        e=P.LessEq(5),
        f=P.MoreEq(6),
        g=P.Between(7, 8),
        h=P.OneOf(9, 10),
    ),
    "complex": P.Or(
        P.Is(int, str, float),
        P.And(
            P.Where(a=P.Eq(1)),
            P.Where(b=P.NotEq(2)),
            P.Where(c=P.Less(3)),
        ),
        P.And(
            P.Where(d=P.More(4)),
            P.Where(e=P.LessEq(5)),
            P.Where(f=P.MoreEq(6)),
        ),
        P.Where(
            g=P.Between(7, 8),
            h=P.OneOf(9, 10),
        ),
        P.And(P.Is(), P.Where()),
    ),
}


# def test_sqlite_simplify(snapshot):
#     visitor = _SqliteSimplifier("type_field", "data_field")
#     for name, pred in predicates_to_simplify.items():
#         result = visit_predicate(visitor, pred)
#         assert result == snapshot(name=name)


def test_postgresql_simplify(snapshot):
    visitor = _PostgreSQLSimplifier("type_field", "data_field")
    for name, pred in predicates_to_simplify.items():
        result = visit_predicate(visitor, pred)
        assert result == snapshot(name=name)

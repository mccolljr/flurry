# pylint: disable=invalid-name
from __future__ import annotations
from typing import (
    Any,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
    cast,
)


from money import predicate as P
from money.predicate import FieldPredicate, Predicate

T = TypeVar("T", covariant=True)


class PredicateVisitor(Protocol[T]):
    """
    A class with methods that can be called to process
    individual predicates while walking a predicate tree.
    """

    def on_is(self, p_is: P.Is) -> T:
        ...

    def on_or(self, p_or: P.Or) -> T:
        ...

    def on_and(self, p_and: P.And) -> T:
        ...

    def on_where(self, p_where: P.Where) -> T:
        ...

    def on_eq(self, field: str, p_eq: P.Eq) -> T:
        ...

    def on_not_eq(self, field: str, p_neq: P.NotEq) -> T:
        ...

    def on_less(self, field: str, p_less: P.Less) -> T:
        ...

    def on_more(self, field: str, p_more: P.More) -> T:
        ...

    def on_less_eq(self, field: str, p_less_eq: P.LessEq) -> T:
        ...

    def on_more_eq(self, field: str, p_more_eq: P.MoreEq) -> T:
        ...

    def on_between(self, field: str, p_between: P.Between) -> T:
        ...

    def on_one_of(self, field: str, p_one_of: P.OneOf) -> T:
        ...


def visit_predicate(visitor: PredicateVisitor[T], pred: Predicate) -> T:
    if isinstance(pred, P.Is):
        return visitor.on_is(pred)
    if isinstance(pred, P.Or):
        return visitor.on_or(pred)
    if isinstance(pred, P.And):
        return visitor.on_and(pred)
    if isinstance(pred, P.Where):
        return visitor.on_where(pred)
    raise TypeError(f"unknown Predicate type {type(pred).__name__}")


def visit_field_predicate(
    visitor: PredicateVisitor[T], field: str, pred: FieldPredicate
) -> T:
    if isinstance(pred, P.Eq):
        return visitor.on_eq(field, pred)
    if isinstance(pred, P.NotEq):
        return visitor.on_not_eq(field, pred)
    if isinstance(pred, P.Less):
        return visitor.on_less(field, pred)
    if isinstance(pred, P.More):
        return visitor.on_more(field, pred)
    if isinstance(pred, P.LessEq):
        return visitor.on_less_eq(field, pred)
    if isinstance(pred, P.MoreEq):
        return visitor.on_more_eq(field, pred)
    if isinstance(pred, P.Between):
        return visitor.on_between(field, pred)
    if isinstance(pred, P.OneOf):
        return visitor.on_one_of(field, pred)
    raise TypeError(f"unknown FieldPredicate type {type(pred).__name__}")


AnyPredicate = Union[Predicate, FieldPredicate]
SimplifiedPredicate = Tuple[
    Optional[AnyPredicate], Optional[str], Optional[Iterable[Any]]
]


def cast_simplified_predicate(src: SimplifiedPredicate):
    """Casts a SimplifiedPredicate value to one that explicitly deals with Predicate values."""
    pred, clause, params = src
    return cast(Optional[Predicate], pred), clause, params


def cast_simplified_field_predicate(src: SimplifiedPredicate):
    """Casts a SimplifiedPredicate value to one that explicitly deals with FieldPredicate values."""
    pred, clause, params = src
    return cast(Optional[FieldPredicate], pred), clause, params


class PredicateSQLSimplifier:
    """A base class for predicate visitors that perform SQL generation."""

    def on_is(self, p_is: P.Is) -> SimplifiedPredicate:
        return p_is, None, None

    def on_or(self, p_or: P.Or) -> SimplifiedPredicate:
        items = [visit_predicate(self, p) for p in p_or.alts]
        preds: List[AnyPredicate] = []
        clauses: List[str] = []
        params: List[Any] = []
        for pr, c, pa in items:
            if pr is not None:
                preds.append(pr)
            if c is not None:
                clauses.append(c)
            if pa is not None:
                params.extend(pa)
        ret_pred = None
        ret_clauses = None
        ret_params = None
        if preds:
            ret_pred = P.Or(*cast(List[Predicate], preds))
        if clauses:
            ret_clauses = f"({' OR '.join(clauses)})"
        if params:
            ret_params = params
        return ret_pred, ret_clauses, ret_params

    def on_and(self, p_and: P.And) -> SimplifiedPredicate:
        items = [visit_predicate(self, p) for p in p_and.preds]
        preds: List[AnyPredicate] = []
        clauses: List[str] = []
        params: List[Any] = []
        for pr, c, pa in items:
            if pr is not None:
                preds.append(pr)
            if c is not None:
                clauses.append(c)
            if pa is not None:
                params.extend(pa)
        ret_pred = None
        ret_clauses = None
        ret_params = None
        if preds:
            ret_pred = P.And(*cast(List[Predicate], preds))
        if clauses:
            ret_clauses = f"({' AND '.join(clauses)})"
        if params:
            ret_params = params
        return ret_pred, ret_clauses, ret_params

    def on_where(self, p_where: P.Where) -> SimplifiedPredicate:
        return p_where, None, None

    def on_eq(self, _field: str, p_eq: P.Eq) -> SimplifiedPredicate:
        return p_eq, None, None

    def on_not_eq(self, _field: str, p_neq: P.NotEq) -> SimplifiedPredicate:
        return p_neq, None, None

    def on_less(self, _field: str, p_less: P.Less) -> SimplifiedPredicate:
        return p_less, None, None

    def on_more(self, _field: str, p_more: P.More) -> SimplifiedPredicate:
        return p_more, None, None

    def on_less_eq(self, _field: str, p_less_eq: P.LessEq) -> SimplifiedPredicate:
        return p_less_eq, None, None

    def on_more_eq(self, _field: str, p_more_eq: P.MoreEq) -> SimplifiedPredicate:
        return p_more_eq, None, None

    def on_between(self, _field: str, p_between: P.Between) -> SimplifiedPredicate:
        return p_between, None, None

    def on_one_of(self, _field: str, p_one_of: P.OneOf) -> SimplifiedPredicate:
        return p_one_of, None, None

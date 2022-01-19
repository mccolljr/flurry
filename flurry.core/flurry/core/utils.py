"""Utilities shared by storage solutions."""

# pylint: disable=invalid-name
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


from . import predicate as P

_T_co = TypeVar("_T_co", covariant=True)


class PredicateVisitor(Protocol[_T_co]):
    """The interface implemented by classes that visit the parts of a Predicate."""

    def on_is(self, p_is: P.Is) -> _T_co:
        """Process the predicate."""
        ...

    def on_or(self, p_or: P.Or) -> _T_co:
        """Process the predicate."""
        ...

    def on_and(self, p_and: P.And) -> _T_co:
        """Process the predicate."""
        ...

    def on_where(self, p_where: P.Where) -> _T_co:
        """Process the predicate."""
        ...

    def on_eq(self, field: str, p_eq: P.Eq) -> _T_co:
        """Process the field predicate."""
        ...

    def on_not_eq(self, field: str, p_neq: P.NotEq) -> _T_co:
        """Process the field predicate."""
        ...

    def on_less(self, field: str, p_less: P.Less) -> _T_co:
        """Process the field predicate."""
        ...

    def on_more(self, field: str, p_more: P.More) -> _T_co:
        """Process the field predicate."""
        ...

    def on_less_eq(self, field: str, p_less_eq: P.LessEq) -> _T_co:
        """Process the field predicate."""
        ...

    def on_more_eq(self, field: str, p_more_eq: P.MoreEq) -> _T_co:
        """Process the field predicate."""
        ...

    def on_between(self, field: str, p_between: P.Between) -> _T_co:
        """Process the field predicate."""
        ...

    def on_one_of(self, field: str, p_one_of: P.OneOf) -> _T_co:
        """Process the field predicate."""
        ...


def visit_predicate(visitor: PredicateVisitor[_T_co], pred: P.Predicate) -> _T_co:
    """Visit the predicate and its parts of the predicate using the given visitor."""
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
    visitor: PredicateVisitor[_T_co], field: str, pred: P.FieldPredicate
) -> _T_co:
    """Visit the field predicate using the given visitor."""
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


AnyPredicate = Union[P.Predicate, P.FieldPredicate]
SimplifiedPredicate = Tuple[
    Optional[AnyPredicate], Optional[str], Optional[Iterable[Any]]
]


def cast_simplified_predicate(src: SimplifiedPredicate):
    """Cast a SimplifiedPredicate value to one that explicitly deals with Predicate values."""
    pred, clause, params = src
    return cast(Optional[P.Predicate], pred), clause, params


def cast_simplified_field_predicate(src: SimplifiedPredicate):
    """Cast a SimplifiedPredicate value to one that explicitly deals with FieldPredicate values."""
    pred, clause, params = src
    return cast(Optional[P.FieldPredicate], pred), clause, params


class PredicateSQLSimplifier:
    """A base class for predicate visitors that perform SQL generation."""

    def on_is(self, p_is: P.Is) -> SimplifiedPredicate:
        """Do nothing."""
        return p_is, None, None

    def on_or(self, p_or: P.Or) -> SimplifiedPredicate:
        """Generate SQL representing the OR of all sub-predicates."""
        if not p_or.alts:
            return p_or, None, None
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
            ret_pred = P.Or(*cast(List[P.Predicate], preds))
        if clauses:
            ret_clauses = f"({' OR '.join(clauses)})"
        if params:
            ret_params = params
        return ret_pred, ret_clauses, ret_params

    def on_and(self, p_and: P.And) -> SimplifiedPredicate:
        """Generate SQL representing the AND of all sub-predicates."""
        if not p_and.preds:
            return p_and, None, None
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
            ret_pred = P.And(*cast(List[P.Predicate], preds))
        if clauses:
            ret_clauses = f"({' AND '.join(clauses)})"
        if params:
            ret_params = params
        return ret_pred, ret_clauses, ret_params

    def on_where(self, p_where: P.Where) -> SimplifiedPredicate:
        """Do nothing."""
        return p_where, None, None

    def on_eq(self, _field: str, p_eq: P.Eq) -> SimplifiedPredicate:
        """Do nothing."""
        return p_eq, None, None

    def on_not_eq(self, _field: str, p_neq: P.NotEq) -> SimplifiedPredicate:
        """Do nothing."""
        return p_neq, None, None

    def on_less(self, _field: str, p_less: P.Less) -> SimplifiedPredicate:
        """Do nothing."""
        return p_less, None, None

    def on_more(self, _field: str, p_more: P.More) -> SimplifiedPredicate:
        """Do nothing."""
        return p_more, None, None

    def on_less_eq(self, _field: str, p_less_eq: P.LessEq) -> SimplifiedPredicate:
        """Do nothing."""
        return p_less_eq, None, None

    def on_more_eq(self, _field: str, p_more_eq: P.MoreEq) -> SimplifiedPredicate:
        """Do nothing."""
        return p_more_eq, None, None

    def on_between(self, _field: str, p_between: P.Between) -> SimplifiedPredicate:
        """Do nothing."""
        return p_between, None, None

    def on_one_of(self, _field: str, p_one_of: P.OneOf) -> SimplifiedPredicate:
        """Do nothing."""
        return p_one_of, None, None

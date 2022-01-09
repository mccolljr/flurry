from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Union

from money.framework.schema import Field


class Predicate(ABC):
    "The base class for object predicates."
    __slots__: tuple = ()

    @abstractmethod
    def __call__(self, item: Any) -> bool:
        pass

    def __str__(self) -> str:
        return str(self.to_dict())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        for slot in self.__slots__:
            if getattr(self, slot) != getattr(other, slot):
                return False
        return True

    def __hash__(self) -> int:
        return hash(
            tuple(self.__hashable(getattr(self, slot)) for slot in self.__slots__)
        )

    def __hashable(self, val: Any):
        if isinstance(val, dict):
            return tuple(
                (self.__hashable(k), self.__hashable(v)) for k, v in val.items()
            )
        if isinstance(val, list):
            return tuple(self.__hashable(v) for v in val)
        return val

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(src: Dict[str, Any]) -> Predicate:
        if isinstance(src, dict) and len(src) == 1:
            name, val = next(iter(src.items()))
            if name == "and" and isinstance(val, list):
                return And(*(Predicate.from_dict(v) for v in val))
            if name == "or" and isinstance(val, list):
                return Or(*(Predicate.from_dict(v) for v in val))
            if name == "is" and all(isinstance(v, type) for v in val):
                return Is(*(v for v in val))
            if name == "where" and isinstance(val, dict):
                return Where(
                    **{name: FieldPredicate.from_dict(val) for name, val in val.items()}
                )
        raise ValueError(f"{src} ({type(src)}) is not a valid predicate")


class And(Predicate):
    """A predicate combinator, checking that all sub-predicates pass."""

    __slots__ = ("preds",)

    def __init__(self, *preds: Predicate):
        self.preds = preds

    def __call__(self, item: Any) -> bool:
        return all(pred(item) for pred in self.preds)

    def to_dict(self) -> Dict[str, Any]:
        return {"and": [p.to_dict() for p in self.preds]}


class Or(Predicate):
    """A predicate combinator, checking that at least one sub-predicate passes."""

    __slots__ = ("alts",)

    def __init__(self, *alts: Predicate):
        self.alts = alts

    def __call__(self, item: Any) -> bool:
        return any(pred(item) for pred in self.alts)

    def to_dict(self) -> Dict[str, Any]:
        return {"or": [p.to_dict() for p in self.alts]}


class Is(Predicate):
    """A predicate to check that a value is of a certain type."""

    __slots__ = ("types",)

    def __init__(self, *types: type):
        self.types = types

    def __call__(self, item: Any) -> bool:
        return isinstance(item, self.types)

    def to_dict(self) -> Dict[str, Any]:
        return {"is": list(self.types)}


class Where(Predicate):
    """A predicate to check that a value's fields meet certain criteria."""

    __slots__ = ("fields",)

    def __init__(self, **fields: Union[FieldPredicate, Any]):
        self.fields = {
            field: pred if isinstance(pred, FieldPredicate) else Eq(pred)
            for field, pred in fields.items()
        }

    def __call__(self, item: Any) -> bool:
        return all(
            pred(self.__get_field(item, name)) for name, pred in self.fields.items()
        )

    def __get_field(self, item: Any, name: str) -> Any:
        attr_name = name
        __schema__ = getattr(item, "__schema__", None)
        if __schema__ is not None and name in __schema__:
            field = __schema__[name]
            if isinstance(field, Field):
                attr_name = field.attr_name
        return getattr(item, attr_name, None)

    def to_dict(self) -> Dict[str, Any]:
        return {"where": {name: fp.to_dict() for name, fp in self.fields.items()}}


class FieldPredicate(ABC):
    "The base class for field predicates."
    __slots__: tuple = ()

    @abstractmethod
    def __call__(self, value: Any) -> bool:
        pass

    def __str__(self) -> str:
        return str(self.to_dict())

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and all(
            getattr(self, slot) == getattr(other, slot) for slot in self.__slots__
        )

    def __hash__(self) -> int:
        return hash(
            tuple(self.__hashable(getattr(self, slot)) for slot in self.__slots__)
        )

    def __hashable(self, val: Any):
        if isinstance(val, dict):
            return tuple(
                (self.__hashable(k), self.__hashable(v)) for k, v in val.items()
            )
        if isinstance(val, list):
            return tuple(self.__hashable(v) for v in val)
        return val

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Generates the dictionary form of a FieldPredicate"""
        ...

    @staticmethod
    def from_dict(src: Dict[str, Any]) -> FieldPredicate:
        """Constructs a FieldPredicate from its dictionary form"""

        if isinstance(src, dict) and len(src) == 1:
            name, val = next(iter(src.items()))
            if name == "eq":
                return Eq(FieldPredicate.__decode_value(val))
            if name == "not_eq":
                return Eq(FieldPredicate.__decode_value(val))
            if name == "less":
                return Less(FieldPredicate.__decode_value(val))
            if name == "more":
                return More(FieldPredicate.__decode_value(val))
            if name == "less_eq":
                return LessEq(FieldPredicate.__decode_value(val))
            if name == "more_eq":
                return MoreEq(FieldPredicate.__decode_value(val))
            if name == "between" and isinstance(val, list) and len(val) == 2:
                return Between(
                    FieldPredicate.__decode_value(val[0]),
                    FieldPredicate.__decode_value(val[1]),
                )
            if name == "one_of" and isinstance(val, (list, tuple)):
                return OneOf(*(FieldPredicate.__decode_value(v) for v in val))
        raise ValueError("{src} is not a valid field predicate")

    @staticmethod
    def __decode_value(src: Any) -> Union[str, int, float, bool, None, datetime]:
        if src is None or isinstance(src, (str, int, float, bool, datetime)):
            return src
        raise ValueError("{src} is not a valid field predicate value")


class Eq(FieldPredicate):
    """Checks that a field value is equal to a value."""

    __slots__ = ("expect",)

    def __init__(self, expect: Any):
        self.expect = expect

    def __call__(self, value: Any) -> bool:
        return value == self.expect

    def to_dict(self):
        return {"eq": self.expect}


class NotEq(FieldPredicate):
    """Checks that a field value is not equal to a value."""

    __slots__ = ("expect",)

    def __init__(self, expect: Any):
        self.expect = expect

    def __call__(self, value: Any) -> bool:
        return value != self.expect

    def to_dict(self):
        return {"not_eq": self.expect}


class OneOf(FieldPredicate):
    """Checks that a field value is one of the expected values."""

    __slots__ = ("options",)

    def __init__(self, *options: Any):
        self.options = options

    def __call__(self, value: Any) -> bool:
        return value in self.options

    def to_dict(self):
        return {"one_of": list(self.options)}


class Less(FieldPredicate):
    """Checks that a field value is less than the limit value."""

    __slots__ = ("limit",)

    def __init__(self, limit: Any):
        self.limit = limit

    def __call__(self, value: Any) -> bool:
        return value < self.limit

    def to_dict(self):
        return {"less": self.limit}


class More(FieldPredicate):
    """Checks that a field value is greater than the limit value."""

    __slots__ = ("limit",)

    def __init__(self, limit: Any):
        self.limit = limit

    def __call__(self, value: Any) -> bool:
        return value > self.limit

    def to_dict(self):
        return {"more": self.limit}


class LessEq(FieldPredicate):
    """Checks that a field value is less than or equal to the limit value."""

    __slots__ = ("limit",)

    def __init__(self, limit: Any):
        self.limit = limit

    def __call__(self, value: Any) -> bool:
        return value <= self.limit

    def to_dict(self):
        return {"less_eq": self.limit}


class MoreEq(FieldPredicate):
    """Checks that a field value is greater than or equal to the limit value."""

    __slots__ = ("limit",)

    def __init__(self, limit: Any):
        self.limit = limit

    def __call__(self, value: Any) -> bool:
        return value >= self.limit

    def to_dict(self):
        return {"more_eq": self.limit}


class Between(FieldPredicate):
    """Checks that a field value is between the upper and lower limit values, inclusive."""

    __slots__ = ("upper", "lower")

    def __init__(self, lower: Any, upper: Any):
        self.upper = upper
        self.lower = lower

    def __call__(self, value: Any) -> bool:
        return self.lower <= value <= self.upper

    def to_dict(self):
        return {"between": [self.lower, self.upper]}

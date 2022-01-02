from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from os import stat
from typing import Any, Dict, Generic, Type, TypeVar, Union

from money.framework.schema import Field

T = TypeVar("T")


class Predicate(Generic[T], ABC):
    __slots__ = tuple()

    @abstractmethod
    def __call__(self, item: T) -> bool:
        pass

    def __str__(self) -> str:
        return str(self.to_dict())

    def __eq__(self, other: Predicate) -> bool:
        return self.__class__ == other.__class__ and all(
            getattr(self, slot) == getattr(other, slot) for slot in self.__slots__
        )

    def __hash__(self) -> int:
        return super(object).__hash__()

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(src: Dict[str, Any]) -> Predicate:
        if isinstance(src, dict) and len(src) == 1:
            name, val = next(iter(src.items()))
            if name == "and" and isinstance(val, list):
                return And(*(Predicate.from_dict(v) for v in val))
            elif name == "or" and isinstance(val, list):
                return Or(*(Predicate.from_dict(v) for v in val))
            elif name == "is" and all(isinstance(v, type) for v in val):
                return Is(*(v for v in val))
            elif name == "where" and isinstance(val, dict):
                return Where(
                    **{name: FieldPredicate.from_dict(val) for name, val in val.items()}
                )
        raise ValueError(f"{src} ({type(src)}) is not a valid predicate")


class And(Predicate[T]):
    __slots__ = ("preds",)

    def __init__(self, *preds: Predicate[T]):
        self.preds = preds

    def __call__(self, item: T) -> bool:
        return all(pred(item) for pred in self.preds)

    def to_dict(self) -> Dict[str, Any]:
        return {"and": [p.to_dict() for p in self.preds]}


class Or(Predicate[T]):
    __slots__ = ("alts",)

    def __init__(self, *alts: Predicate[T]):
        self.alts = alts

    def __call__(self, item: T) -> bool:
        return any(pred(item) for pred in self.alts)

    def to_dict(self) -> Dict[str, Any]:
        return {"or": [p.to_dict() for p in self.alts]}


class Is(Predicate[T]):
    __slots__ = ("types",)

    def __init__(self, *types: Type[T]):
        self.types = types

    def __call__(self, item: T) -> bool:
        return isinstance(item, self.types)

    def to_dict(self) -> Dict[str, Any]:
        return {"is": list(self.types)}


class Where(Predicate[T]):
    __slots__ = ("fields",)

    def __init__(self, **fields: FieldPredicate):
        self.fields = fields

    def __call__(self, item: T) -> bool:
        return all(
            pred(self.__get_field(item, name)) for name, pred in self.fields.items()
        )

    def __get_field(self, item: T, name: str) -> Any:
        attr_name = name
        __schema__ = getattr(item, "__schema__", None)
        if __schema__ is not None and name in __schema__:
            field = __schema__[name]
            if isinstance(field, Field):
                attr_name = field._attr_name
        return getattr(item, attr_name, None)

    def to_dict(self) -> Dict[str, Any]:
        return {"where": {name: fp.to_dict() for name, fp in self.fields.items()}}


class FieldPredicate(ABC):
    __slots__ = tuple()

    @abstractmethod
    def __call__(self, value: Any) -> bool:
        pass

    def __str__(self) -> str:
        return str(self.to_dict())

    def __eq__(self, other: FieldPredicate) -> bool:
        return isinstance(other, self.__class__) and all(
            getattr(self, slot) == getattr(other, slot) for slot in self.__slots__
        )

    def __hash__(self) -> int:
        return super(object).__hash__()

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(src: Dict[str, Any]) -> FieldPredicate:
        if isinstance(src, dict) and len(src) == 1:
            name, val = next(iter(src.items()))
            if name == "eq":
                return Eq(FieldPredicate.__decode_value(val))
            elif name == "less":
                return Less(FieldPredicate.__decode_value(val))
            elif name == "more":
                return More(FieldPredicate.__decode_value(val))
            elif name == "less_eq":
                return LessEq(FieldPredicate.__decode_value(val))
            elif name == "more_eq":
                return MoreEq(FieldPredicate.__decode_value(val))
            elif name == "between" and isinstance(val, list) and len(val) == 2:
                return Between(
                    FieldPredicate.__decode_value(val[0]),
                    FieldPredicate.__decode_value(val[1]),
                )
            elif name == "one_of" and isinstance(val, (list, tuple)):
                return OneOf(*(FieldPredicate.__decode_value(v) for v in val))
        raise ValueError("{src} is not a valid field predicate")

    @staticmethod
    def __decode_value(src: Any) -> Union[str, int, float, bool, None, datetime]:
        if src is None or isinstance(src, (str, int, float, bool, datetime)):
            return src
        raise ValueError("{src} is not a valid field predicate value")


class Eq(FieldPredicate):
    __slots__ = ("expect",)

    def __init__(self, expect: Any):
        self.expect = expect

    def __call__(self, value: Any) -> bool:
        return value == self.expect

    def to_dict(self):
        return {"eq": self.expect}


class OneOf(FieldPredicate):
    __slots__ = ("options",)

    def __init__(self, *options: Any):
        self.options = options

    def __call__(self, value: Any) -> bool:
        return value in self.options

    def to_dict(self):
        return {"one_of": list(self.options)}


class Less(FieldPredicate):
    __slots__ = ("limit",)

    def __init__(self, limit: Any):
        self.limit = limit

    def __call__(self, value: Any) -> bool:
        return value < self.limit

    def to_dict(self):
        return {"less": self.limit}


class More(FieldPredicate):
    __slots__ = ("limit",)

    def __init__(self, limit: Any):
        self.limit = limit

    def __call__(self, value: Any) -> bool:
        return value > self.limit

    def to_dict(self):
        return {"more": self.limit}


class LessEq(FieldPredicate):
    __slots__ = ("limit",)

    def __init__(self, limit: Any):
        self.limit = limit

    def __call__(self, value: Any) -> bool:
        return value <= self.limit

    def to_dict(self):
        return {"less_eq": self.limit}


class MoreEq(FieldPredicate):
    __slots__ = ("limit",)

    def __init__(self, limit: Any):
        self.limit = limit

    def __call__(self, value: Any) -> bool:
        return value >= self.limit

    def to_dict(self):
        return {"more_eq": self.limit}


class Between(FieldPredicate):
    __slots__ = ("upper", "lower")

    def __init__(self, lower: Any, upper: Any):
        self.upper = upper
        self.lower = lower

    def __call__(self, value: Any) -> bool:
        return self.lower < value < self.upper

    def to_dict(self):
        return {"between": [self.lower, self.upper]}

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
import json
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeGuard,
    Union,
    TypeVar,
    overload,
)


T = TypeVar("T")


class Field(Generic[T]):
    _kind: FieldKind[T]
    _default: Optional[Union[T, Callable[[], T]]]
    _nullable: bool
    _attr_name: str
    _field_name: str

    def __init__(
        self,
        kind: Union[FieldKind[T], Type[FieldKind[T]]],
        *,
        name: Optional[str] = None,
        default: Optional[Union[T, Callable[[], T]]] = None,
        nullable: bool = True,
    ):
        """Initializes the Field with options.

        Keyword Args:
            name (str, optional): The field name. Defaults to None, meaning that the attr name will be used.
            default (value or callable, optional): Default value or value factory. Defaults to None.
            nullable (bool, optional): Is None a valid value for this field? Defaults to True.
        """
        self._kind = kind() if isinstance(kind, type) else kind
        self._default = default
        self._nullable = nullable
        if name is not None:
            self._field_name = name

    def __set_name__(self, owner: Any, name: str):
        self._attr_name = name
        if not hasattr(self, "_field_name"):
            self._field_name = name
        self._append_to_schema(owner)

    def _append_to_schema(self, owner: Any):
        if not hasattr(owner, "__schema__"):
            raise RuntimeError(f"cannot register {self} to {owner}, no __schema__")
        if self._field_name in owner.__schema__:
            raise RuntimeError(f"duplicate definition for field {self._field_name}")
        owner.__schema__[self._field_name] = self

    @overload
    def __get__(self, obj: None, objtype: Any) -> Field[T]:
        ...

    @overload
    def __get__(self, obj: Any, objtype: Any) -> T:
        ...

    def __get__(self, obj: Any, objtype=None) -> Union[T, Field[T]]:
        if obj is None:
            return self
        return obj.__dict__.get(f"__f_{self._attr_name}")

    def __set__(self, obj: Any, value: Any):
        if value is None and self._nullable:
            obj.__dict__[f"__f_{self._attr_name}"] = None
        else:
            obj.__dict__[f"__f_{self._attr_name}"] = self._kind.convert(value)

    def __delete__(self, obj: Any):
        data_name = f"__f_{self._attr_name}"
        if data_name in obj.__dict__:
            del obj.__dict__[data_name]


class SchemaMeta(type):
    __schema__: Dict[str, Field[Any]]

    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        __schema__ = attrs.setdefault("__schema__", {})
        x = super().__new__(cls, name, bases, attrs)
        for base in x.__mro__:
            base_schema = getattr(base, "__schema__", None)
            if base_schema is not None:
                for k in base_schema:
                    __schema__.setdefault(k, base_schema[k])
        return x


class SchemaBase:
    __schema__: ClassVar[Dict[str, Field[Any]]]

    def __init__(self, **kwargs):
        for field in self.__schema__.values():
            attr = field._attr_name
            name = field._field_name
            if name in kwargs:
                setattr(self, attr, kwargs[name])
            elif field._default is not None:
                default_val = field._default
                if callable(default_val):
                    default_val = default_val()
                setattr(self, attr, default_val)
            elif field._nullable:
                setattr(self, attr, None)
            else:
                raise ValueError(f"missing initializer for {name}")

    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for field in self.__schema__.values():
            attr = field._attr_name
            name = field._field_name
            if hasattr(self, attr):
                result[name] = self.__to_dict_helper(field._kind, getattr(self, attr))
        return result

    def __to_dict_helper(self, kind: FieldKind[Any], val: Any) -> Any:
        if isinstance(kind, Object):
            assert isinstance(val, SchemaBase)
            return val.to_dict()

        if isinstance(kind, Collection):
            assert isinstance(val, list)
            return [self.__to_dict_helper(kind.of, v) for v in val]

        return val

    def __str__(self):
        return f"<{self.__class__.__name__} {self.to_dict()}>"


class SimpleSchema(SchemaBase, metaclass=SchemaMeta):
    pass


class FieldKind(Generic[T], ABC):
    @abstractmethod
    def convert(self, value: Any) -> T:
        """Tries to convert a value to the value type expected by this field.

        Args:
            value (Any): The value to try to convert

        Returns:
            The converted value

        Raises:
            An exception indicating why the conversion failed
        """
        pass

    @abstractmethod
    def validate(self, value: T):
        pass


class Int(FieldKind[int]):
    def convert(self, value: Any) -> int:
        if isinstance(value, str):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        raise ValueError(f"{value} cannot be converted to int")

    def validate(self, value: int):
        pass


class Float(FieldKind[float]):
    def convert(self, value: Any) -> float:
        if isinstance(value, str):
            return float(value)
        if isinstance(value, int):
            return float(value)
        if isinstance(value, float):
            return value
        raise ValueError(f"{value} cannot be converted to float")

    def validate(self, value: float):
        pass


class Str(FieldKind[str]):
    def convert(self, value: Any) -> str:
        if isinstance(value, str):
            return value
        return str(value)

    def validate(self, value: str):
        pass


class Bool(FieldKind[bool]):
    def convert(self, value: Any) -> bool:
        if isinstance(value, int):
            return value != 0
        if isinstance(value, bool):
            return value
        raise ValueError(f"{value} cannot be converted to bool")

    def validate(self, value: bool):
        pass


class Bytes(FieldKind[bytes]):
    def convert(self, value: Any) -> bytes:
        if isinstance(value, str):
            return bytes(value, "utf-8")
        if isinstance(value, bytes):
            return value
        raise ValueError(f"{value} cannot be converted to bytes")

    def validate(self, value: bytes):
        pass


class DateTime(FieldKind[datetime]):
    format_str: str

    def __init__(self, format_str: str = "%a %b %d %H:%M:%S %Y"):
        self.format_str = format_str

    def convert(self, value: Any) -> datetime:
        if isinstance(value, str):
            return datetime.strptime(value, self.format_str)
        if isinstance(value, int):
            return datetime.utcfromtimestamp(float(value))
        if isinstance(value, float):
            return datetime.utcfromtimestamp(value)
        if isinstance(value, datetime):
            return value.replace(microsecond=0)
        raise ValueError(f"{value} cannot be converted to datetime")

    def validate(self, value: datetime):
        pass


class Collection(FieldKind[List[T]]):
    of: FieldKind[T]

    def __init__(self, of: Union[FieldKind[T], Type[FieldKind[T]]]):
        self.of = of if isinstance(of, FieldKind) else of()

    def convert(self, value: Any) -> List[T]:
        try:
            return [self.of.convert(v) for v in iter(value)]
        except TypeError:
            raise ValueError(
                f"{value} cannot be converted to list of {type(self.of).__name__}"
            )

    def validate(self, value: List[T]):
        pass


TSchema = TypeVar("TSchema", bound=SchemaBase)


class Object(FieldKind[TSchema]):
    of: Type[TSchema]

    def __init__(self, of: Type[TSchema]):
        self.of = of

    def convert(self, value: Any) -> TSchema:
        if isinstance(value, dict):
            return self.of(**value)
        if isinstance(value, SchemaBase):
            return self.of(**value.to_dict())
        raise ValueError(f"{value} cannot be converted to {type(self.of).__name__}")

    def validate(self, value: TSchema):
        pass

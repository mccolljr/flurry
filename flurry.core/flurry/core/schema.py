"""Schema, the data model for everything."""

from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    TypeVar,
    overload,
)


from abc import ABC, abstractmethod
from datetime import datetime, timezone

# pylint: disable=invalid-name
_T_Any = TypeVar("_T_Any")
# pylint: enable=invalid-name


class SchemaDefinitionError(Exception):
    """An exception caused by an invalid schema definition."""

    def __init__(self, cls_name: str, problem: str):
        """Initialize a new SchemaDefinitionError."""
        self.cls_name = cls_name
        self.problem = problem
        super().__init__(f"{self.cls_name}: {self.problem}")


class _SchemaDef(Mapping[str, "Field[Any]"]):
    __slots__ = ("fields", "id_field")

    def __init__(self, *, id_field: str = None):
        self.fields: Dict[str, "Field[Any]"] = {}
        self.id_field: Optional[str] = id_field

    def __getitem__(self, __k: str) -> "Field[Any]":
        if __k not in self.fields:
            raise KeyError
        return self.fields[__k]

    def __setitem__(self, __k: str, val: "Field[Any]") -> None:
        if __k in self.fields:
            raise ValueError(f"field {repr(__k)} already defined")
        self.fields[__k] = val

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self) -> Iterator[str]:
        return iter(self.fields)


class SchemaMeta(type):
    """The metaclass that all classes with a schema must inherit from."""

    __schema__: _SchemaDef

    def __new__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **extra
    ):
        """Construct a new class with a schema."""
        attrs["__schema__"] = _SchemaDef(id_field=extra.pop("id", None))
        new_class = super().__new__(cls, name, bases, attrs)
        for base in new_class.__mro__:
            base_schema = getattr(base, "__schema__", None)
            if base_schema is not None:
                for k in base_schema:
                    if k not in new_class.__schema__:
                        new_class.__schema__[k] = base_schema[k]
        return new_class

    def __init__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **_extra
    ):
        """Initialize and validate a newly created class with a schema."""
        super().__init__(name, bases, attrs)
        id_field = cls.__schema__.id_field
        if id_field is not None and id_field not in cls.__schema__:
            raise SchemaDefinitionError(
                cls.__name__, f"id field {repr(id_field)} does not exist"
            )


class SchemaBase(metaclass=SchemaMeta):
    """A base class for types that have a schema.

    This class provides utilities like a default initializing constructor,
    a string representation, and a to_dict method.
    """

    __schema__: ClassVar[_SchemaDef]

    def __init__(self, **kwargs):
        """Assign values to the object's fields based on the keyword arguments."""
        for field in self.__schema__.values():
            attr = field.attr_name
            name = field.field_name
            if name in kwargs:
                setattr(self, attr, kwargs[name])
            elif field.default is not None:
                default_val = field.default
                if callable(default_val):
                    default_val = default_val()
                setattr(self, attr, default_val)
            elif field.nullable:
                setattr(self, attr, None)
            else:
                raise ValueError(f"missing initializer for {name}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert this object to its dictionary representation."""
        result = {}
        for field in self.__schema__.values():
            attr = field.attr_name
            name = field.field_name
            if hasattr(self, attr):
                result[name] = self.__to_dict_helper(field.kind, getattr(self, attr))
        return result

    def __to_dict_helper(self, kind: "FieldKind[Any]", val: Any) -> Any:
        if val is not None:
            if isinstance(kind, Object):
                assert isinstance(val, SchemaBase)
                return val.to_dict()

            if isinstance(kind, Collection):
                assert isinstance(val, list)
                return [self.__to_dict_helper(kind.of_kind, v) for v in val]

        return val

    def __str__(self):
        """Get a string representation of this object."""
        return f"<{self.__class__.__name__} {self.to_dict()}>"


class Field(Generic[_T_Any]):
    """A descriptor for a single field in a schema."""

    kind: "FieldKind[_T_Any]"
    default: Optional[Union[_T_Any, Callable[[], _T_Any]]]
    nullable: bool
    attr_name: str
    field_name: str

    def __init__(
        self,
        kind: Union["FieldKind[_T_Any]", Type["FieldKind[_T_Any]"]],
        *,
        name: Optional[str] = None,
        default: Optional[Union[_T_Any, Callable[[], _T_Any]]] = None,
        nullable: bool = True,
    ):
        """Initialize the Field.

        Keyword Args:
            name (str, optional): The field name. Defaults to None, meaning that the attr name will be used.
            default (value or callable, optional): Default value or value factory. Defaults to None.
            nullable (bool, optional): Is None a valid value for this field? Defaults to True.
        """
        self.kind = kind() if isinstance(kind, type) else kind
        self.default = default
        self.nullable = nullable
        if name is not None:
            self.field_name = name

    def __set_name__(self, owner: "SchemaMeta", name: str):
        """Save this field in the owner's schema."""
        self.attr_name = name
        if not hasattr(self, "_field_name"):
            self.field_name = name
        self._append_to_schema(owner)

    def _append_to_schema(self, owner: "SchemaMeta"):
        if not hasattr(owner, "__schema__"):
            raise RuntimeError(f"cannot register {self} to {owner}, no __schema__")
        if self.field_name in owner.__schema__:
            raise RuntimeError(f"duplicate definition for field {self.field_name}")
        owner.__schema__[self.field_name] = self

    @overload
    def __get__(self, obj: None, objtype: Any) -> "Field[_T_Any]":
        """Get the corresponding schema field from the owner."""
        ...

    @overload
    def __get__(self, obj: Any, objtype: Any) -> _T_Any:
        """Get the corresponding schema field value from the object."""
        ...

    def __get__(self, obj: Any, objtype=None) -> Union[_T_Any, "Field[_T_Any]"]:
        """Get the corresponding field or field value from the owner or object."""
        if obj is None:
            return self
        return obj.__dict__.get(f"__f_{self.attr_name}")

    def __set__(self, obj: Any, value: Any):
        """Set the corresponding field value on the object, respecting nullability and conversion."""
        if value is None and self.nullable:
            obj.__dict__[f"__f_{self.attr_name}"] = None
        else:
            obj.__dict__[f"__f_{self.attr_name}"] = self.kind.convert(value)

    def __delete__(self, obj: Any):
        """Remove the corresponding field value from the object."""
        data_name = f"__f_{self.attr_name}"
        if data_name in obj.__dict__:
            del obj.__dict__[data_name]


class FieldKind(Generic[_T_Any], ABC):
    """The base FieldKind type."""

    @abstractmethod
    def convert(self, value: Any) -> _T_Any:
        """Try to convert a value to the value type expected by this field.

        Args:
            value (Any): The value to try to convert

        Returns:
            The converted value

        Raises:
            An exception indicating why the conversion failed
        """

    @abstractmethod
    def validate(self, value: _T_Any):
        """Validate a value of this kind."""


class Int(FieldKind[int]):
    """A FieldType describing an int value."""

    def convert(self, value: Any) -> int:
        """Try to convert a value to an int."""
        if isinstance(value, str):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        raise ValueError(f"{repr(value)} cannot be converted to int")

    def validate(self, value: int):
        """Do nothing."""


class Float(FieldKind[float]):
    """A FieldType describing a float value."""

    def convert(self, value: Any) -> float:
        """Try to convert a value to a float."""
        if isinstance(value, str):
            return float(value)
        if isinstance(value, int):
            return float(value)
        if isinstance(value, float):
            return value
        raise ValueError(f"{repr(value)} cannot be converted to float")

    def validate(self, value: float):
        """Do nothing."""


class Str(FieldKind[str]):
    """A FieldType describing a str value."""

    def convert(self, value: Any) -> str:
        """Try to convert a value to a str."""
        if isinstance(value, str):
            return value
        if isinstance(value, bytes):
            return str(value, "utf-8")
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def validate(self, value: str):
        """Do nothing."""


class Bool(FieldKind[bool]):
    """A FieldType describing a boolean value."""

    def convert(self, value: Any) -> bool:
        """Try to convert a value to a bool."""
        if isinstance(value, int):
            return value != 0
        if isinstance(value, bool):
            return value
        raise ValueError(f"{repr(value)} cannot be converted to bool")

    def validate(self, value: bool):
        """Do nothing."""


class Bytes(FieldKind[bytes]):
    """A FieldType describing a binary value."""

    def convert(self, value: Any) -> bytes:
        """Try to convert a value to bytes."""
        if isinstance(value, str):
            return bytes(value, "utf-8")
        if isinstance(value, bytes):
            return value
        raise ValueError(f"{repr(value)} cannot be converted to bytes")

    def validate(self, value: bytes):
        """Do nothing."""


class DateTime(FieldKind[datetime]):
    """A FieldType describing a datetime value."""

    def __init__(self, tzinfo: timezone = timezone.utc):
        """Create a new DateTime with the specified timezone (default UTC)."""
        self.tzinfo = tzinfo

    def convert(self, value: Any) -> datetime:
        """Try to convert a value to a datetime with the appropriate timezone."""
        if isinstance(value, str):
            return self._from_isofmt(value)
        if isinstance(value, datetime):
            return value
        raise ValueError(f"{repr(value)} cannot be converted to datetime")

    def _from_isofmt(self, src: str) -> datetime:
        try:
            return datetime.fromisoformat(src).astimezone(self.tzinfo)
        except ValueError:
            pass
        if src.endswith("Z"):
            return datetime.fromisoformat(f"{src[:-1]}+00:00")
        raise ValueError(f"{repr(src)} is not a valid ISO 8601 string")

    def validate(self, value: datetime):
        """Do nothing."""


class Collection(FieldKind[List[_T_Any]]):
    """A FieldType describing a list of some other FieldKind."""

    of_kind: FieldKind[_T_Any]

    def __init__(self, of_kind: Union[FieldKind[_T_Any], Type[FieldKind[_T_Any]]]):
        """Create a FieldKind to describe a list of values of some other FieldKind."""
        self.of_kind = of_kind if isinstance(of_kind, FieldKind) else of_kind()

    def convert(self, value: Any) -> List[_T_Any]:
        """Try to convert a value to a list of values of the appropriate FieldKind."""
        try:
            return [self.of_kind.convert(v) for v in iter(value)]
        except TypeError as terr:
            raise ValueError(
                f"{value} cannot be converted to list of {type(self.of_kind).__name__}"
            ) from terr

    def validate(self, value: List[_T_Any]):
        """Do nothing."""


TSchema = TypeVar("TSchema", bound=SchemaBase)


class Object(FieldKind[TSchema]):
    """A FieldType describing some object with its own schema."""

    of_typ: Type[TSchema]

    def __init__(self, of_typ: Type[TSchema]):
        """Create a FieldKind to describe an object with its own schema."""
        self.of_typ = of_typ

    def convert(self, value: Any) -> TSchema:
        """Attempt to convert a value to the appropriate object."""
        if isinstance(value, dict):
            return self.of_typ(**value)
        if isinstance(value, SchemaBase):
            return self.of_typ(**value.to_dict())
        raise ValueError(f"{value} cannot be converted to {type(self.of_typ).__name__}")

    def validate(self, value: TSchema):
        """Do nothing."""

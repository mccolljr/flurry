"""Queries, the Q in CQRS."""

from typing import Any, Dict, Generic, Tuple, TypeVar, Union
from abc import ABCMeta, abstractmethod

from . import schema
from .context import Context

# pylint: disable=invalid-name
_T_Context = TypeVar("_T_Context", bound=Context)
_T_Result = TypeVar("_T_Result", bound=Union[schema.SchemaBase, None])
# pylint: enable=invalid-name


class QueryDefinitionError(Exception):
    """An exception caused by an invalid query class definition."""

    def __init__(self, query_name: str, problem: str):
        """Initialize a new QueryDefinitionError."""
        self.query_name = query_name
        self.problem = problem
        super().__init__(f"{self.query_name}: {self.problem}")


class QueryMeta(ABCMeta, schema.SchemaMeta):
    """The metaclass all query classes must have."""

    __query_mixin__: bool
    Result: schema.SchemaMeta

    def __new__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **extra
    ):
        """Construct a new Query class."""
        attrs.setdefault("__query_mixin__", extra.pop("mixin", False))
        mewcls = super().__new__(cls, name, bases, attrs)
        return mewcls

    def __init__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **_extra
    ):
        """Initialize and validate a newly created Query class."""
        super().__init__(name, bases, attrs)
        if not cls.__query_mixin__:
            QueryMeta._validate_fetch(cls)
            QueryMeta._validate_result(cls)

    @staticmethod
    def _validate_result(the_cls: type):

        result_class = getattr(the_cls, "Result", None)
        if result_class is None:
            raise QueryDefinitionError(the_cls.__name__, "Result must be defined")
        if not isinstance(result_class, schema.SchemaMeta):
            raise QueryDefinitionError(
                the_cls.__name__,
                f"Result must be a type with a schema (have {type(result_class)})",
            )

    @staticmethod
    def _validate_fetch(the_cls: type):
        fetch = getattr(the_cls, "fetch", None)
        if fetch is None or getattr(fetch, "__isabstractmethod__", False):
            raise QueryDefinitionError(the_cls.__name__, "fetch method must be defined")
        if not callable(fetch):
            raise QueryDefinitionError(the_cls.__name__, "fetch must be callable")


class QueryBase(Generic[_T_Context, _T_Result], schema.SchemaBase, metaclass=QueryMeta):
    """The class that all queries must subclass.

    Each subclass must specify a `Result` member which points to some class
    where `isinstance(Result, SchemaMeta)` is `True`

    If a subclass specifies a member `__query_mixin__` with a value of `True`,
    that class can serve as a common base class for multiple queries.
    """

    __query_mixin__ = True

    @abstractmethod
    async def fetch(self, context: _T_Context) -> _T_Result:
        """Implement the fetch operation for this query."""
        ...

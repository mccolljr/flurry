from __future__ import annotations
from abc import ABCMeta, abstractmethod

from typing import Any, Callable, Dict, Generic, Optional, Tuple, TypeVar, Union

from money.framework import schema
from money.framework.storage import Storage


class QueryDefinitionError(Exception):
    """An exception caused by an invalid query class definition"""

    def __init__(self, query_name: str, problem: str):
        self.query_name = query_name
        self.problem = problem
        super().__init__(f"{self.query_name}: {self.problem}")


class QueryMeta(ABCMeta):
    """The metaclass all query classes must have."""

    __query_mixin__: bool
    Result: schema.SchemaMeta
    Arguments: Optional[schema.SchemaMeta]
    fetch: Callable[[QueryBase, Any], Any]

    def __new__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **kwargs
    ):
        attrs.setdefault("__query_mixin__", False)
        mewcls = super().__new__(cls, name, bases, attrs)
        return mewcls

    def __init__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        super().__init__(name, bases, attrs)
        if not cls.__query_mixin__:
            QueryMeta._validate_fetch(cls)
            QueryMeta._validate_result(cls)
            QueryMeta._validate_arguments(cls)

    @staticmethod
    def _validate_result(the_cls: type):

        result_class = getattr(the_cls, "Result", None)
        if result_class is None:
            raise QueryDefinitionError(the_cls.__name__, "Result must be defined")
        if not isinstance(result_class, schema.SchemaMeta):
            raise QueryDefinitionError(
                the_cls.__name__, f"Result must be a type (have {type(result_class)})"
            )

    @staticmethod
    def _validate_arguments(the_cls: type):
        argument_class = getattr(the_cls, "Arguments", None)
        if argument_class is None:
            return
        if not isinstance(argument_class, schema.SchemaMeta):
            raise QueryDefinitionError(
                the_cls.__name__,
                f"Arguments must be a type (have {type(argument_class)})",
            )

    @staticmethod
    def _validate_fetch(the_cls: type):
        fetch = getattr(the_cls, "fetch", None)
        if fetch is None or getattr(fetch, "__isabstractmethod__", False):
            raise QueryDefinitionError(the_cls.__name__, "fetch method must be defined")
        if not callable(fetch):
            raise QueryDefinitionError(the_cls.__name__, "fetch must be callable")


TQueryArgs = TypeVar("TQueryArgs", bound=Union[schema.SchemaBase, None])
TQueryResult = TypeVar("TQueryResult", bound=Union[schema.SchemaBase, None])


class QueryBase(Generic[TQueryArgs, TQueryResult], metaclass=QueryMeta):
    """The class that all queries must subclass.

    Each subclass must specify a `Result` member which points to some class
    where `isinstance(Result, SchemaMeta)` is `True`

    If a subclass may specify an `Arguments` member which points to some class
    where `isinstance(Arguments, SchemaMeta)` is `True`. If `Arguments` is specified,
    this class will be pused as the arguments to the query

    If a subclass specifies a member `__query_mixin__` with a value of `True`,
    that class can serve as a common base class for multiple queries.
    """

    __query_mixin__ = True

    @abstractmethod
    async def fetch(self, storage: Storage, args: TQueryArgs) -> TQueryResult:
        """Subclasses must implement a fetch method

        Args:
            storage (Storage): The app storage
            args (Arguments, optional): The query arguments, if any (default None)

        Returns:
            Result: The query result, if any (default None)
        """
        ...

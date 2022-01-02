from __future__ import annotations
from abc import ABCMeta, abstractmethod

from typing import Any, Callable, Dict, Optional, Tuple, Union

import money.framework.schema as schema
from money.framework.storage import Storage


class QueryDefinitionError(Exception):
    def __init__(self, query_name: str, problem: str):
        self.query_name = query_name
        self.problem = problem
        super().__init__(f"{self.query_name}: {self.problem}")


class QueryMeta(ABCMeta):
    __query_mixin__: bool
    Result: schema.SchemaMeta
    Arguments: Optional[schema.SchemaMeta]
    fetch: Callable[[QueryBase, Any], Any]

    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        attrs.setdefault("__query_mixin__", False)
        x = super().__new__(cls, name, bases, attrs)
        return x

    def __init__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        if not cls.__query_mixin__:
            QueryMeta._validate_fetch(cls)
            QueryMeta._validate_result(cls)
            QueryMeta._validate_arguments(cls)

    @staticmethod
    def _validate_result(the_cls: type):
        Result = getattr(the_cls, "Result", None)
        if Result is None:
            raise QueryDefinitionError(the_cls.__name__, "Result must be defined")
        if not isinstance(Result, schema.SchemaMeta):
            raise QueryDefinitionError(
                the_cls.__name__, f"Result must be a type (have {type(Result)})"
            )

    @staticmethod
    def _validate_arguments(the_cls: type):
        Arguments = getattr(the_cls, "Arguments", None)
        if Arguments is None:
            return
        if not isinstance(Arguments, schema.SchemaMeta):
            raise QueryDefinitionError(
                the_cls.__name__, f"Arguments must be a type (have {type(Arguments)})"
            )

    @staticmethod
    def _validate_fetch(the_cls: type):
        fetch = getattr(the_cls, "fetch", None)
        if fetch is None or getattr(fetch, "__isabstractmethod__", False):
            raise QueryDefinitionError(the_cls.__name__, "fetch method must be defined")
        if not callable(fetch):
            raise QueryDefinitionError(the_cls.__name__, f"fetch must be callable")


class QueryBase(metaclass=QueryMeta):
    __query_mixin__ = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @abstractmethod
    async def fetch(self, storage: Storage) -> Union[schema.SchemaBase, None]:
        ...

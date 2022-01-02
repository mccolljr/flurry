from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

import money.framework.schema as schema


class QueryDefinitionError(Exception):
    def __init__(self, query_name: str, problem: str):
        self.query_name = query_name
        self.problem = problem
        super().__init__(f"{self.query_name}: {self.problem}")


class QueryMeta(type):
    Result: schema.SchemaMeta
    Arguments: Optional[schema.SchemaMeta]
    fetch: Callable[[QueryBase, Any], Any]

    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        x = super().__new__(cls, name, bases, attrs)
        return x

    def __init__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        if cls.__name__ == "QueryBase":
            return
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
        if fetch is None:
            raise QueryDefinitionError(the_cls.__name__, "fetch method must be defined")
        if not callable(fetch):
            raise QueryDefinitionError(the_cls.__name__, f"fetch must be callable")


class QueryBase(metaclass=QueryMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class QueryResult(schema.SchemaBase, metaclass=schema.SchemaMeta):
    pass


class QueryArguments(schema.SchemaBase, metaclass=schema.SchemaMeta):
    pass

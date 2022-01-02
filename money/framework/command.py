from abc import ABCMeta, abstractmethod
from typing import Any, Awaitable, Dict, Optional, Tuple

import money.framework.schema as schema
from money.framework.storage import Storage


class CommandDefinitionError(Exception):
    def __init__(self, query_name: str, problem: str):
        self.query_name = query_name
        self.problem = problem
        super().__init__(f"{self.query_name}: {self.problem}")


class CommandMeta(schema.SchemaMeta, ABCMeta):
    __cmd_mixin__: bool
    Result: Optional[schema.SchemaMeta]

    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        attrs.setdefault("__cmd_mixin__", False)
        x = super().__new__(cls, name, bases, attrs)
        return x

    def __init__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        if not cls.__cmd_mixin__:
            CommandMeta._validate_exec(cls)
            CommandMeta._validate_result(cls)

    @staticmethod
    def _validate_result(the_cls: type):
        Result = getattr(the_cls, "Result", None)
        if Result is not None and not isinstance(Result, schema.SchemaMeta):
            raise CommandDefinitionError(
                the_cls.__name__,
                f"Result must be a type (have {type(Result)})",
            )

    @staticmethod
    def _validate_exec(the_cls: type):
        fetch = getattr(the_cls, "exec", None)
        if fetch is None:
            raise CommandDefinitionError(
                the_cls.__name__, "exec method must be defined"
            )
        if not callable(fetch):
            raise CommandDefinitionError(the_cls.__name__, f"exec must be callable")


class CommandBase(schema.SchemaBase, metaclass=CommandMeta):
    __cmd_mixin__ = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @abstractmethod
    async def exec(self, storage: Storage) -> Any:
        ...

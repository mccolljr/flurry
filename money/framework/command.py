from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Generic, Optional, Tuple, TypeVar, Union

from money.framework import schema
from money.framework.storage import Storage


class CommandDefinitionError(Exception):
    """An exception caused by an invalid command class definition."""

    def __init__(self, query_name: str, problem: str):
        self.query_name = query_name
        self.problem = problem
        super().__init__(f"{self.query_name}: {self.problem}")


class CommandMeta(schema.SchemaMeta, ABCMeta):
    """The metaclass that all command classes must inherit from."""

    __cmd_mixin__: bool
    Result: Optional[schema.SchemaMeta]

    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        attrs.setdefault("__cmd_mixin__", False)
        new_class = super().__new__(cls, name, bases, attrs)
        return new_class

    def __init__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]):
        super().__init__(name, bases, attrs)
        if not cls.__cmd_mixin__:
            CommandMeta._validate_exec(cls)
            CommandMeta._validate_result(cls)

    @staticmethod
    def _validate_result(the_cls: type):
        result_class = getattr(the_cls, "Result", None)
        if result_class is not None and not isinstance(result_class, schema.SchemaMeta):
            raise CommandDefinitionError(
                the_cls.__name__,
                f"Result must be a type (have {type(result_class)})",
            )

    @staticmethod
    def _validate_exec(the_cls: type):
        exec_method = getattr(the_cls, "exec", None)
        if exec_method is None or getattr(exec_method, "__isabstractmethod__", False):
            raise CommandDefinitionError(
                the_cls.__name__, "exec method must be defined"
            )
        if not callable(exec_method):
            raise CommandDefinitionError(the_cls.__name__, "exec must be callable")


TCommandResult = TypeVar("TCommandResult", bound=Union[schema.SchemaBase, None])


class CommandBase(Generic[TCommandResult], schema.SchemaBase, metaclass=CommandMeta):
    """The class that all commands must subclass.

    A subclass may specify a `Result` member which points to some class
    where `isinstance(Result, SchemaMeta)` is `True`, If `Result` is specified,
    this class will be used as the result of the query.

    If a subclass specifies a member `__cmd_mixin__` with a value of `True`,
    that class can serve as a common base class for multiple commands.
    """

    __cmd_mixin__ = True

    @abstractmethod
    async def exec(self, storage: Storage) -> TCommandResult:
        ...

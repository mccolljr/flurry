"""Queries, the Q in CQRS."""

from typing import Any, AsyncGenerator, Dict, Generic, Tuple, TypeVar
from abc import ABCMeta, abstractmethod

from . import schema
from .context import Context

# pylint: disable=invalid-name
_T_Context = TypeVar("_T_Context", bound=Context)
_T_Result = TypeVar("_T_Result", bound=schema.SchemaBase)
# pylint: enable=invalid-name


class SubscriptionDefinitionError(Exception):
    """An exception caused by an invalid subscription class definition."""

    def __init__(self, query_name: str, problem: str):
        """Initialize a new SubscriptionDefinitionError."""
        self.query_name = query_name
        self.problem = problem
        super().__init__(f"{self.query_name}: {self.problem}")


class SubscriptionMeta(ABCMeta, schema.SchemaMeta):
    """The metaclass all subscription classes must have."""

    __subscription_mixin__: bool
    Result: schema.SchemaMeta

    def __new__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **extra
    ):
        """Construct a new Query class."""
        attrs.setdefault("__subscription_mixin__", extra.pop("mixin", False))
        mewcls = super().__new__(cls, name, bases, attrs)
        return mewcls

    def __init__(
        cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **_extra
    ):
        """Initialize and validate a newly created Subscription class."""
        super().__init__(name, bases, attrs)
        if not cls.__subscription_mixin__:
            SubscriptionMeta._validate_subscribe(cls)
            SubscriptionMeta._validate_result(cls)

    @staticmethod
    def _validate_result(the_cls: type):

        result_class = getattr(the_cls, "Result", None)
        if result_class is None:
            raise SubscriptionDefinitionError(
                the_cls.__name__, "Result must be defined"
            )
        if not isinstance(result_class, schema.SchemaMeta):
            raise SubscriptionDefinitionError(
                the_cls.__name__,
                f"Result must be a type with a schema (have {type(result_class)})",
            )

    @staticmethod
    def _validate_subscribe(the_cls: type):
        fetch = getattr(the_cls, "fetch", None)
        if fetch is None or getattr(fetch, "__isabstractmethod__", False):
            raise SubscriptionDefinitionError(
                the_cls.__name__, "subscribe method must be defined"
            )
        if not callable(fetch):
            raise SubscriptionDefinitionError(
                the_cls.__name__, "subscribe must be callable"
            )


class SubscriptionBase(
    Generic[_T_Context, _T_Result], schema.SchemaBase, metaclass=SubscriptionMeta
):
    """The class that all subscriptions must subclass.

    Each subclass must specify a `Result` member which points to some class
    where `isinstance(Result, SchemaMeta)` is `True`

    If a subclass specifies a member `__subscription_mixin__` with a value of `True`,
    that class can serve as a common base class for multiple queries.
    """

    __subscription_mixin__ = True

    @abstractmethod
    def subscribe(self, context: _T_Context) -> AsyncGenerator[_T_Result, None]:
        """Generate the result items for this subscription."""
        ...

from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    from .storage import Storage

# pylint: disable=invalid-name
_T_Storage = TypeVar("_T_Storage", bound="Storage")
# pylint: enable=invalid-name


class Context(Protocol[_T_Storage]):
    """The minimum interface that a Context type must provice."""

    storage: _T_Storage

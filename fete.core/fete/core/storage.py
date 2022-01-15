"""The Storage protocol."""
from typing import Iterable, Optional, Protocol, TypeVar

from .predicate import Predicate


from .event import EventBase
from .aggregate import AggregateBase

# pylint: disable=invalid-name
_T_AggRoot = TypeVar("_T_AggRoot", bound=AggregateBase, covariant=True)
_T_EventRoot = TypeVar("_T_EventRoot", bound=EventBase, covariant=True)
# pylint: enable=invalid-name


class Storage(Protocol):
    """A protocol describing the shared capabilities of different storage providers."""

    async def load_events(
        self, query: Optional[Predicate] = None
    ) -> Iterable[_T_EventRoot]:
        """Load events that match the predicate."""
        ...

    async def save_events(self, events: Iterable[_T_EventRoot]):
        """Save new events."""
        ...

    async def save_snapshots(self, snaps: Iterable[_T_AggRoot]):
        """Save new snapshots."""
        ...

    async def load_snapshots(self, query: Predicate = None) -> Iterable[_T_AggRoot]:
        """Load snapshots that match the predicate."""
        ...

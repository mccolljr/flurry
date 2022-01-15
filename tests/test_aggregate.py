import pytest

from fete.core import schema
from fete.core.aggregate import (
    AggregateMeta,
    AggregateBase,
    AggregateDefinitionError,
)
from fete.core.event import EventMeta, EventBase, EventHandler, handle_event


@pytest.fixture(autouse=True)
def unregister_names_and_stuff():
    yield
    setattr(EventMeta, "_EventMeta__by_name", {})
    setattr(AggregateMeta, "_AggregateMeta__by_name", {})


@pytest.mark.xfail(raises=schema.SchemaDefinitionError, strict=True)
def test_agg_def_empty_fail():
    class EmptyAgg(AggregateBase):
        pass


@pytest.mark.xfail(raises=AggregateDefinitionError, strict=True)
def test_agg_def_missing_creation_event_fail():
    class NoCreationEventAgg(AggregateBase):
        id = schema.Field(schema.Str)


@pytest.mark.xfail(raises=AggregateDefinitionError, strict=True)
def test_agg_def_missing_creation_event_handler_fail():
    class CreationEvent(EventBase):
        pass

    class NoCreationEventHandlerAgg(AggregateBase):
        __agg_create__ = CreationEvent

        id = schema.Field(schema.Str)


@pytest.mark.xfail(raises=schema.SchemaDefinitionError, strict=True)
def test_agg_def_missing_id_fail():
    class CreationEvent(EventBase):
        pass

    class NoIdAggregate(AggregateBase):
        __agg_create__ = CreationEvent

        @handle_event(CreationEvent)
        def handle_created(self, e: CreationEvent):
            pass


def test_agg_def_trivial():
    class CreationEvent(EventBase):
        pass

    class TrivialAggregate(AggregateBase):
        __agg_create__ = CreationEvent

        id = schema.Field(schema.Str)

        @handle_event(CreationEvent)
        def handle_created(self, e: CreationEvent):
            pass

        @classmethod
        def load_events(cls):
            return []

    assert TrivialAggregate.__schema__ == {
        "id": TrivialAggregate.id,
    }
    assert TrivialAggregate.__schema__.id_field == "id"
    assert TrivialAggregate.__agg_name__ == "TrivialAggregate"
    assert TrivialAggregate.__agg_mixin__ == False
    assert TrivialAggregate.__agg_create__ == CreationEvent
    assert TrivialAggregate.__agg_events__ == {CreationEvent: "handle_created"}
    assert isinstance(TrivialAggregate.handle_created, EventHandler)


def test_agg_def_custom_id():
    class CreationEvent(EventBase):
        pass

    class TrivialAggregate(AggregateBase, id="unique_id"):
        __agg_create__ = CreationEvent

        unique_id = schema.Field(schema.Str)

        @handle_event(CreationEvent)
        def handle_created(self, e: CreationEvent):
            pass

        @classmethod
        def load_events(cls):
            return []

    assert TrivialAggregate.__schema__ == {
        "unique_id": TrivialAggregate.unique_id,
    }
    assert TrivialAggregate.__schema__.id_field == "unique_id"
    assert TrivialAggregate.__agg_name__ == "TrivialAggregate"
    assert TrivialAggregate.__agg_mixin__ == False
    assert TrivialAggregate.__agg_create__ == CreationEvent
    assert TrivialAggregate.__agg_events__ == {CreationEvent: "handle_created"}
    assert isinstance(TrivialAggregate.handle_created, EventHandler)


def test_agg_def_two_events():
    class CreationEvent(EventBase):
        val = schema.Field(schema.Str)

    class UpdateEvent(EventBase):
        val = schema.Field(schema.Str)

    class TwoEventAggregate(AggregateBase):
        __agg_create__ = CreationEvent

        id = schema.Field(schema.Str, default="default")
        val = schema.Field(schema.Str)

        @handle_event(CreationEvent)
        def handle_created(self, e: CreationEvent):
            self.val = e.val

        @handle_event(UpdateEvent)
        def handle_updated(self, e: UpdateEvent):
            self.val = e.val

        @classmethod
        def load_events(cls):
            return []

    assert TwoEventAggregate.__schema__ == {
        "id": TwoEventAggregate.id,
        "val": TwoEventAggregate.val,
    }
    assert TwoEventAggregate.__schema__.id_field == "id"
    assert TwoEventAggregate.__agg_name__ == "TwoEventAggregate"
    assert TwoEventAggregate.__agg_mixin__ == False
    assert TwoEventAggregate.__agg_create__ == CreationEvent
    assert TwoEventAggregate.__agg_events__ == {
        CreationEvent: "handle_created",
        UpdateEvent: "handle_updated",
    }
    assert isinstance(TwoEventAggregate.handle_created, EventHandler)
    assert isinstance(TwoEventAggregate.handle_updated, EventHandler)

    agg = TwoEventAggregate()

    agg.handle_created(CreationEvent(val="created"))
    assert agg.val == "created"

    agg.handle_updated(UpdateEvent(val="updated"))
    assert agg.val == "updated"

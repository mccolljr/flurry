import pytest

import money.framework.schema as schema
from money.framework.aggregate import AggregateBase, AggregateDefinitionError
from money.framework.event import EventBase, EventHandler, handle_event


@pytest.mark.xfail(raises=AggregateDefinitionError, strict=True)
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


@pytest.mark.xfail(raises=AggregateDefinitionError, strict=True)
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

    assert TrivialAggregate.__schema__ == {
        "id": TrivialAggregate.id,
    }
    assert TrivialAggregate.__agg_id__ == "id"
    assert TrivialAggregate.__agg_name__ == "TrivialAggregate"
    assert TrivialAggregate.__agg_mixin__ == False
    assert TrivialAggregate.__agg_create__ == CreationEvent
    assert TrivialAggregate.__agg_events__ == [CreationEvent]
    assert isinstance(TrivialAggregate.handle_created, EventHandler)


def test_agg_def_custom_id():
    class CreationEvent(EventBase):
        pass

    class TrivialAggregate(AggregateBase):
        __agg_id__ = "unique_id"
        __agg_create__ = CreationEvent

        unique_id = schema.Field(schema.Str)

        @handle_event(CreationEvent)
        def handle_created(self, e: CreationEvent):
            pass

    assert TrivialAggregate.__schema__ == {
        "unique_id": TrivialAggregate.unique_id,
    }
    assert TrivialAggregate.__agg_id__ == "unique_id"
    assert TrivialAggregate.__agg_name__ == "TrivialAggregate"
    assert TrivialAggregate.__agg_mixin__ == False
    assert TrivialAggregate.__agg_create__ == CreationEvent
    assert TrivialAggregate.__agg_events__ == [CreationEvent]
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

    assert TwoEventAggregate.__schema__ == {
        "id": TwoEventAggregate.id,
        "val": TwoEventAggregate.val,
    }
    assert TwoEventAggregate.__agg_id__ == "id"
    assert TwoEventAggregate.__agg_name__ == "TwoEventAggregate"
    assert TwoEventAggregate.__agg_mixin__ == False
    assert TwoEventAggregate.__agg_create__ == CreationEvent
    assert TwoEventAggregate.__agg_events__ == [CreationEvent, UpdateEvent]
    assert isinstance(TwoEventAggregate.handle_created, EventHandler)
    assert isinstance(TwoEventAggregate.handle_updated, EventHandler)

    agg = TwoEventAggregate()

    agg.handle_created(CreationEvent(val="created"))
    assert agg.val == "created"

    agg.handle_updated(UpdateEvent(val="updated"))
    assert agg.val == "updated"

import uuid

import money.framework.schema as schema
from money.framework.aggregate import AggregateBase
from money.framework.application import Application
from money.framework.command import CommandBase
from money.framework.event import EventBase, handle_event


def test_application():
    app = Application()

    @app.event
    class TodoCreatedEvent(EventBase):
        id = schema.Field(schema.Str, nullable=False, default=lambda: str(uuid.uuid4()))
        title = schema.Field(schema.Str, default="To Do")
        description = schema.Field(schema.Str, default="")

    @app.command
    class CreateTodoCommand(CommandBase):
        title = schema.Field(schema.Str)
        description = schema.Field(schema.Str)

    @app.aggregate
    class TodoAggregate(AggregateBase):
        __agg_create__ = TodoCreatedEvent

        id = schema.Field(schema.Str, nullable=False, default=lambda: str(uuid.uuid4()))
        title = schema.Field(schema.Str, default="To Do")
        description = schema.Field(schema.Str, default="")

        @handle_event(TodoCreatedEvent)
        def handle_created(self, evt: TodoCreatedEvent):
            self.id = evt.id
            self.title = evt.title
            self.description = evt.description

    @app.aggregate
    class TodoStatsAggregate(AggregateBase):
        __agg_create__ = TodoCreatedEvent

        id = schema.Field(schema.Str, nullable=False, default=lambda: str(uuid.uuid4()))
        num_events = schema.Field(schema.Int, nullable=False, default=0)

        @handle_event(TodoCreatedEvent)
        def handle_created(self, evt: TodoCreatedEvent):
            self.id = evt.id
            self.num_events = 1

    assert TodoAggregate.__agg_id__ in TodoAggregate.__schema__
    assert TodoStatsAggregate.__agg_id__ in TodoStatsAggregate.__schema__
    assert app._events == [TodoCreatedEvent]
    assert app._commands == [CreateTodoCommand]
    assert app._aggregates == [TodoAggregate, TodoStatsAggregate]
    assert app._related_events == {
        TodoCreatedEvent: [TodoAggregate, TodoStatsAggregate]
    }
    assert app._creation_events == {
        TodoCreatedEvent: [TodoAggregate, TodoStatsAggregate]
    }

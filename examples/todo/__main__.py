from typing import Any, Dict, List

import attrs
import datetime as dt

from flurry.core import schema, predicate as P
from flurry.core.query import QueryBase
from flurry.core.event import EventBase, handle_event
from flurry.core.command import CommandBase
from flurry.core.aggregate import AggregateLoader
from flurry.graphql.application import GraphqlApplication
from flurry.postgres.postgres import PostgreSQLStorage


@attrs.define
class AppContext:
    storage: PostgreSQLStorage


app = GraphqlApplication(
    context=AppContext(
        storage=PostgreSQLStorage(
            host="localhost",
            port="31415",
            user="postgres",
            password="unsafe",
            database="postgres",
        )
    )
)


class TodoEvent(EventBase):
    """Base class for Todo-related events."""

    todo_id = schema.Field(schema.Str, nullable=False)
    timestamp = schema.Field(
        schema.DateTime, default=lambda: dt.datetime.now(dt.timezone.utc)
    )


@app.event
class TodoCreatedEvent(TodoEvent):
    """The creation event for the TodoAggregate."""

    title = schema.Field(schema.Str, nullable=False)
    description = schema.Field(schema.Str, nullable=False)


@app.event
class TodoCompletedEvent(TodoEvent):
    """The event for marking a Todo as complete."""


@app.aggregate
class TodoAggregate(AggregateLoader, create=TodoCreatedEvent, id="todo_id"):
    """Information about a single Todo record."""

    todo_id = schema.Field(schema.Str, nullable=False)
    title = schema.Field(schema.Str, nullable=False)
    description = schema.Field(schema.Str, nullable=False)
    created_at = schema.Field(schema.DateTime, nullable=False)
    updated_at = schema.Field(schema.DateTime, nullable=False)
    completed_at = schema.Field(schema.DateTime)

    @handle_event(TodoCreatedEvent)
    def handle_create(self, evt: TodoCreatedEvent):
        self.todo_id = evt.todo_id
        self.title = evt.title
        self.description = evt.description
        self.created_at = evt.timestamp
        self.updated_at = evt.timestamp

    @handle_event(TodoCompletedEvent)
    def handle_complete(self, evt: TodoCompletedEvent):
        self.updated_at = evt.timestamp
        self.completed_at = evt.timestamp

    @classmethod
    async def load_events(
        cls, context: AppContext, ids: List[Any]
    ) -> Dict[Any, List[TodoEvent]]:
        result: Dict[Any, List[TodoEvent]] = {}
        for evt in await context.storage.load_events(
            P.And(P.Is(*cls.__agg_events__.keys()), P.Where(todo_id=P.OneOf(*ids)))
        ):
            assert isinstance(evt, TodoEvent)
            result.setdefault(evt.todo_id, []).append(evt)
        return result


@app.query
class ListTodosQuery(QueryBase):
    """A query for fetching all todos."""

    class Result(schema.SchemaBase):
        """The results of the query."""

        todos = schema.Field(
            schema.Collection(schema.Object(TodoAggregate)), default=lambda: []
        )

    async def fetch(self, context: AppContext) -> Result:
        return self.Result(
            todos=list(await context.storage.load_snapshots(P.Is(TodoAggregate)))
        )


@app.query
class FindTodoQuery(QueryBase):
    """A query for fetching a single todo by id."""

    todo_id = schema.Field(schema.Str, nullable=False)

    class Result(schema.SchemaBase):
        """The results of the query."""

        found = schema.Field(schema.Object(TodoAggregate))

    async def fetch(self, context: AppContext) -> Result:
        found = list(
            await context.storage.load_snapshots(
                P.And(P.Is(TodoAggregate), P.Where(todo_id=self.todo_id))
            )
        )
        if found:
            return self.Result(found=found[0])
        return self.Result(found=None)


@app.command
class CreateTodoCommand(CommandBase):
    """The command to create a Todo record."""

    title = schema.Field(schema.Str, nullable=False)
    description = schema.Field(schema.Str, nullable=False)

    def __gen_id(self) -> str:
        return f"{self.title}_{dt.datetime.now(dt.timezone.utc).timestamp()*1000}"

    async def exec(self, context: AppContext):
        creation_event = TodoCreatedEvent(
            todo_id=self.__gen_id(), title=self.title, description=self.description
        )
        await context.storage.save_events([creation_event])
        await TodoAggregate.sync_snapshots(context, [creation_event.todo_id])


@app.command
class CompleteTodoCommand(CommandBase):
    """The command to complete a Todo."""

    todo_id = schema.Field(schema.Str, nullable=False)

    async def exec(self, context: AppContext):
        _existing = await TodoAggregate.load(context, self.todo_id)
        completion_event = TodoCompletedEvent(todo_id=self.todo_id)
        await context.storage.save_events([completion_event])
        await TodoAggregate.sync_snapshots(context, [completion_event.todo_id])


if __name__ == "__main__":
    import sys
    import logging

    logging.basicConfig(level=logging.ERROR, force=True)

    if len(sys.argv) > 1 and sys.argv[1] == "--print-schema":
        print(app.gql_schema)
        sys.exit(0)

    logging.basicConfig(level=logging.INFO, force=True)
    app.run(host="localhost", port=8080)

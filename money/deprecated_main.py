# pylint: disable-all
from __future__ import annotations

import os
import uuid
import logging
from typing import Dict, List, cast

import money.framework.schema as schema
import money.framework.predicate as P
from money.framework.application import GraphQLApplication
from money.framework.aggregate import AggregateBase
from money.framework.command import CommandBase
from money.framework.query import QueryBase
from money.framework.event import EventBase, handle_event
from money.framework.storage import MemoryStorage, Storage

app = GraphQLApplication(storage=MemoryStorage())


class TodoEvent(EventBase):
    todo_id = schema.Field(schema.Str, nullable=False)


@app.event
class TodoCreatedEvent(TodoEvent):
    title = schema.Field(schema.Str, nullable=False)
    description = schema.Field(schema.Str, nullable=False)


@app.event
class TodoModifiedEvent(TodoEvent):
    title = schema.Field(schema.Str)
    description = schema.Field(schema.Str)


@app.event
class TodoCompletedEvent(TodoEvent):
    pass


@app.aggregate
class TodoAggregate(AggregateBase[TodoEvent]):
    __agg_create__ = TodoCreatedEvent

    id = schema.Field(schema.Str)
    title = schema.Field(schema.Str)
    description = schema.Field(schema.Str)
    completed = schema.Field(schema.Bool, default=False)

    @handle_event(TodoCreatedEvent)
    def handle_created(self, evt: TodoCreatedEvent):
        self.id = evt.todo_id
        self.title = evt.title
        self.description = evt.description

    @handle_event(TodoModifiedEvent)
    def handle_modified(self, evt: TodoModifiedEvent):
        self.title = self.title if evt.title is None else evt.title
        self.description = (
            self.description if evt.description is None else evt.description
        )

    @handle_event(TodoCompletedEvent)
    def handle_completed(self, evt: TodoCompletedEvent):
        self.completed = True

    @classmethod
    async def load_events(
        cls, storage: Storage, ids: List[str]
    ) -> Dict[str, List[TodoEvent]]:
        loaded = await storage.load_events(P.Or(*(P.Where(todo_id=id) for id in ids)))
        events: Dict[str, List[TodoEvent]] = {}
        for evt in cast(List[TodoEvent], loaded):
            events.setdefault(evt.todo_id, []).append(evt)
        return events


@app.command
class CreateTodo(CommandBase):
    class Result(schema.SimpleSchema):
        created = schema.Field(schema.Object(TodoAggregate), nullable=False)

    title = schema.Field(schema.Str, nullable=False)
    description = schema.Field(schema.Str, nullable=False)

    async def exec(self, storage: Storage) -> Result:
        id = str(uuid.uuid4())
        await storage.save_events(
            [
                TodoCreatedEvent(
                    todo_id=id, title=self.title, description=self.description
                )
            ]
        )
        return self.Result(created=await TodoAggregate.load(storage, id))


@app.command
class ModifyTodo(CommandBase):
    todo_id = schema.Field(schema.Str, nullable=False)
    title = schema.Field(schema.Str)
    description = schema.Field(schema.Str)

    async def exec(self, storage: Storage):
        existing = await TodoAggregate.load(storage, self.todo_id)
        if (self.title is not None and existing.title != self.title) or (
            self.description is not None and existing.description != self.description
        ):
            await storage.save_events(
                [
                    TodoModifiedEvent(
                        todo_id=existing.id,
                        title=self.title,
                        description=self.description,
                    )
                ]
            )


@app.command
class CompleteTodo(CommandBase):
    todo_id = schema.Field(schema.Str, nullable=False)

    async def exec(self, storage: Storage):
        existing = await TodoAggregate.load(storage, self.todo_id)
        if not existing.completed:
            await storage.save_events([TodoCompletedEvent(todo_id=existing.id)])


@app.query
class CountTodos(QueryBase):
    class Result(schema.SimpleSchema):
        count = schema.Field(schema.Int, nullable=False)

    async def fetch(self, storage: Storage, args=None):
        creation_events = list(await storage.load_events(P.Is(TodoCreatedEvent)))
        return self.Result(count=len(creation_events))


@app.query
class FindTodo(QueryBase):
    Result = TodoAggregate

    class Arguments(schema.SimpleSchema):
        id = schema.Field(schema.Str, nullable=False)

    async def fetch(self, storage: Storage, args: Arguments = None) -> TodoAggregate:
        assert args
        return await TodoAggregate.load(storage, args.id)


@app.query
class ListTodos(QueryBase):
    class Result(schema.SimpleSchema):
        todos = schema.Field(
            schema.Collection(schema.Object(TodoAggregate)), nullable=False
        )

    async def fetch(self, storage: Storage, args=None) -> Result:
        all_events = await storage.load_events(P.Is(TodoEvent))
        agg_events: Dict[str, List[TodoEvent]] = {}
        for evt in all_events:
            evt = cast(TodoEvent, evt)
            agg_events.setdefault(evt.todo_id, []).append(evt)
        todos = [TodoAggregate.from_events(evts) for evts in agg_events.values()]
        return self.Result(todos=todos)


def main():
    schema = app.gql_schema
    fd = os.open("./schema.gql", os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
    os.write(fd, bytes(str(schema), "utf-8"))
    os.close(fd)
    app.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

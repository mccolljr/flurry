import os
import asyncio
import logging
from typing import Any
from money.framework.predicate import Predicate

import money.framework.schema as schema
from money.framework.application import GraphQLApplication
from money.framework.aggregate import AggregateBase
from money.framework.command import CommandBase
from money.framework.query import QueryArguments, QueryBase, QueryResult
from money.framework.event import EventBase, handle_event

app = GraphQLApplication()


class TodoEvent(EventBase):
    todo_id = schema.Field(schema.Str, nullable=False)


@app.event
class TodoCreatedEvent(TodoEvent):
    title = schema.Field(schema.Str, nullable=False)
    description = schema.Field(schema.Str, nullable=False)


@app.event
class TodoModifiedEvent(TodoEvent):
    todo_id = schema.Field(schema.Str, nullable=False)
    title = schema.Field(schema.Str)
    description = schema.Field(schema.Str)


@app.event
class TodoCompletedEvent(TodoEvent):
    todo_id = schema.Field(schema.Str, nullable=False)


@app.aggregate
class TodoAggregate(AggregateBase):
    __agg_create__ = TodoCreatedEvent

    id = schema.Field(schema.Str)
    title = schema.Field(schema.Str)
    description = schema.Field(schema.Str)
    completed = schema.Field(schema.Bool, default=False)

    @handle_event(TodoCreatedEvent)
    def handle_created(self, evt: TodoCreatedEvent):
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


@app.command
class CreateTodo(CommandBase):
    title = schema.Field(schema.Str, nullable=False)
    description = schema.Field(schema.Str, nullable=False)


@app.command
class ModifyTodo(CommandBase):
    todo_id = schema.Field(schema.Str, nullable=False)
    title = schema.Field(schema.Str)
    description = schema.Field(schema.Str)


@app.command
class CompleteTodo(CommandBase):
    todo_id = schema.Field(schema.Str, nullable=False)


@app.query
class CountTodos(QueryBase):
    class Result(QueryResult):
        count = schema.Field(schema.Int, nullable=False)

    def fetch(self):
        future = asyncio.Future()
        asyncio.get_event_loop().call_later(
            2, lambda: future.set_result(self.Result(count=100))
        )
        return future


@app.query
class FindTodo(QueryBase):
    Result = TodoAggregate

    class Arguments(QueryArguments):
        id = schema.Field(schema.Str, nullable=False)

    async def fetch(self, args: Arguments):
        return TodoAggregate(id=args.id)


@app.query
class ListTodos(QueryBase):
    class Result(QueryResult):
        todos = schema.Field(
            schema.Collection(schema.Object(TodoAggregate)), nullable=False
        )

    class Arguments(QueryArguments):
        updated_before = schema.Field(schema.DateTime)
        updated_after = schema.Field(schema.DateTime)

    async def fetch(self, args: Arguments):
        return []


def main():
    schema = app.gql_schema
    fd = os.open("./schema.gql", os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
    os.write(fd, bytes(str(schema), "utf-8"))
    os.close(fd)
    app.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

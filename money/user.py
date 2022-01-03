# pylint: disable=missing-class-docstring

from __future__ import annotations


import uuid
import bcrypt

import aiohttp.web_exceptions as webexc
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, cast
from money.framework.query import QueryBase

import money.framework.schema as schema
import money.framework.predicate as P
from money.framework.storage import Storage
from money.framework.aggregate import AggregateBase
from money.framework.event import EventBase, handle_event
from money.framework.command import CommandBase

from .app import APP


class UserEvent(EventBase):
    user_id = schema.Field(schema.Str, nullable=False)


@APP.event
class UserCreatedEvent(UserEvent):
    first_name = schema.Field(schema.Str, nullable=False)
    last_name = schema.Field(schema.Str, nullable=False)
    username = schema.Field(schema.Str, nullable=False)
    email = schema.Field(schema.Str, nullable=False)
    hashed_password = schema.Field(schema.Str, nullable=False)


@APP.event
class UserLoggedInEvent(UserEvent):
    timestamp = schema.Field(schema.DateTime, default=datetime.utcnow)


@APP.aggregate
class UserAggregate(AggregateBase):
    __agg_id__ = "uuid"
    __agg_create__ = UserCreatedEvent

    uuid = schema.Field(schema.Str, nullable=False)
    first_name = schema.Field(schema.Str, nullable=False)
    last_name = schema.Field(schema.Str, nullable=False)
    username = schema.Field(schema.Str, nullable=False)
    email = schema.Field(schema.Str, nullable=False)
    hashed_password = schema.Field(schema.Str, nullable=False)
    last_login = schema.Field(schema.DateTime, default=None)

    @handle_event(UserCreatedEvent)
    def handle_create(self, evt: UserCreatedEvent):
        self.uuid = evt.user_id
        self.first_name = evt.first_name
        self.last_name = evt.last_name
        self.username = evt.username
        self.email = evt.email
        self.hashed_password = evt.hashed_password

    @handle_event(UserLoggedInEvent)
    def handle_logged_in(self, evt: UserLoggedInEvent):
        self.last_login = evt.timestamp

    @classmethod
    async def load_events(
        cls, storage: Storage, ids: List[Any]
    ) -> Dict[str, List[UserEvent]]:
        events = await storage.load_events(
            P.And(P.Is(UserEvent), P.Where(user_id=P.OneOf(ids)))
        )
        result: Dict[str, List[UserEvent]] = {}
        for evt in events:
            evt = cast(UserEvent, evt)
            result.setdefault(evt.user_id, []).append(evt)
        return result

    @classmethod
    async def find_by(
        cls, storage: Storage, query: P.Predicate
    ) -> Optional[UserAggregate]:
        user = next(
            iter(await storage.load_snapshots(P.And(P.Is(UserAggregate), query))),
            None,
        )
        return cast(Optional[UserAggregate], user)

    @classmethod
    async def find_all_by(
        cls, storage: Storage, query: P.Predicate
    ) -> Iterable[UserAggregate]:
        return cast(
            Iterable[UserAggregate],
            await storage.load_snapshots(P.And(P.Is(UserAggregate), query)),
        )


@APP.query
class FindUserByUsername(QueryBase):
    class Arguments(schema.SimpleSchema):
        username = schema.Field(schema.Str, nullable=False)

    class Result(schema.SimpleSchema):
        user = schema.Field(schema.Object(UserAggregate))

    async def fetch(self, storage: Storage, args: Arguments = None) -> Result:
        assert args
        return self.Result(
            user=await UserAggregate.find_by(storage, P.Where(username=args.username))
        )


@APP.command
class CreateUserCommand(CommandBase):
    first_name = schema.Field(schema.Str, nullable=False)
    last_name = schema.Field(schema.Str, nullable=False)
    username = schema.Field(schema.Str, nullable=False)
    email = schema.Field(schema.Str, nullable=False)
    password = schema.Field(schema.Str, nullable=False)

    async def exec(self, storage: Storage):
        found = await UserAggregate.find_by(
            storage, P.Or(P.Where(username=self.username), P.Where(email=self.email))
        )
        if found:
            raise webexc.HTTPBadRequest()
        hashed_password = str(
            bcrypt.hashpw(bytes(self.password, "utf-8"), bcrypt.gensalt()),
            "utf-8",
        )
        user_events: List[UserEvent] = [
            UserCreatedEvent(
                user_id=str(uuid.uuid4()),
                first_name=self.first_name,
                last_name=self.last_name,
                username=self.username,
                email=self.email,
                hashed_password=hashed_password,
            )
        ]
        user_obj = UserAggregate.from_events(user_events)
        await storage.save_events(user_events)
        await storage.save_snapshots([user_obj])
        # breakpoint()


@APP.command
class LogInCommand(CommandBase):
    username = schema.Field(schema.Str, nullable=False)
    password = schema.Field(schema.Str, nullable=False)

    class Result(schema.SimpleSchema):
        success = schema.Field(schema.Object(UserAggregate), nullable=True)
        failure = schema.Field(schema.Str, nullable=True)

    async def exec(self, storage: Storage):
        user_obj = await UserAggregate.find_by(storage, P.Where(username=self.username))
        if not user_obj or not bcrypt.checkpw(
            bytes(self.password, "utf-8"), bytes(user_obj.hashed_password, "utf-8")
        ):
            return self.Result(failure="invalid username/password combination")
        login_event = UserLoggedInEvent(user_id=user_obj.uuid)
        user_obj.apply_event(login_event)
        await storage.save_events([login_event])
        await storage.save_snapshots([user_obj])
        return self.Result(success=user_obj)

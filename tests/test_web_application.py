from typing import Any, Dict, cast

import pytest
import asyncio
import aiohttp

from contextlib import asynccontextmanager

from fete.core import schema
from fete.core.context import Context
from fete.core.query import QueryBase
from fete.core.command import CommandBase
from fete.web.application import WebApplication

APP = WebApplication(cast(Context, None))


@APP.command
class Command1(CommandBase):
    arg = schema.Field(schema.Str)

    class Result(schema.SchemaBase):
        echo = schema.Field(schema.Str)

    async def exec(self, context: Context) -> "Result":
        return self.Result(echo=self.arg)


@APP.command(path="/commandTwo/{arg}")
class Command2(CommandBase):
    arg = schema.Field(schema.Int)

    class Result(schema.SchemaBase):
        double = schema.Field(schema.Int)
        triple = schema.Field(schema.Int)

    async def exec(self, context: Context) -> Result:
        if self.arg is None:
            return self.Result()
        return self.Result(double=self.arg * 2, triple=self.arg * 3)


@APP.command(path="/command3", method="GET")
class CommandIII(CommandBase):
    arg = schema.Field(schema.DateTime)

    class Result(schema.SchemaBase):
        unix = schema.Field(schema.Float)

    async def exec(self, context: Context) -> Result:
        if self.arg is None:
            return self.Result()
        return self.Result(unix=self.arg.timestamp())


@APP.query
class Query1(QueryBase):
    class Result(schema.SchemaBase):
        hello = schema.Field(schema.Str, nullable=False)

    async def fetch(self, context: Context) -> Result:
        return self.Result(hello="Hello")


@APP.query(path="/queryTwo/{arg}")
class Query2(QueryBase):
    arg = schema.Field(schema.Str)
    arg2 = schema.Field(schema.Str)

    class Result(schema.SchemaBase):
        concat = schema.Field(schema.Str)

    async def fetch(self, context: Context):
        if self.arg is None or self.arg2 is None:
            return self.Result()
        return self.Result(concat=f"{self.arg}{self.arg2}")


@APP.query(path="/query3", method=["PUT", "PATCH"])
class QueryIII(QueryBase):
    arg = schema.Field(schema.Str)

    class Result(schema.SchemaBase):
        reverse = schema.Field(schema.Str)

    async def fetch(self, context: Context):
        if self.arg is None:
            return self.Result()
        return self.Result(reverse=self.arg[::-1])


@asynccontextmanager
async def run_application():
    loop = asyncio.get_event_loop()
    task = loop.create_task(APP.run(port=12345))
    try:
        yield
    finally:
        task.cancel()
        await asyncio.wait([task])


async def assert_response(
    method: str,
    url: str,
    *,
    query: Dict[str, str] = None,
    body: Any = None,
    want_status: int = None,
    want_body: Any = None,
):
    async with aiohttp.request(
        method,
        url,
        params=query,
        json=body,
        headers={"Content-Type": "application/json"},
    ) as resp:
        if want_status is not None:
            assert want_status == resp.status
        else:
            assert resp.status < 400, f"got status {resp.status} {resp.reason}"
        if want_body is not None:
            assert (await resp.json()) == want_body


@pytest.mark.asyncio
async def test_web_application():
    import datetime as dt

    get_url = lambda path: f"http://localhost:12345/{path}"
    async with run_application():
        now = dt.datetime.now(dt.timezone.utc)
        await asyncio.sleep(2)
        await assert_response(
            "POST",
            get_url("command1"),
            body={"arg": "test"},
            want_body={"echo": "test"},
        )
        await assert_response(
            "POST",
            get_url("commandTwo/10"),
            want_body={"double": 20, "triple": 30},
        )
        await assert_response(
            "GET",
            get_url("command3"),
            query={"arg": now.isoformat()},
            want_body={"unix": now.timestamp()},
        )
        await assert_response(
            "GET",
            get_url("query1"),
            want_body={"hello": "Hello"},
        )
        await assert_response(
            "GET",
            get_url("queryTwo/abc"),
            query={"arg2": "xyz"},
            want_body={"concat": "abcxyz"},
        )
        await assert_response(
            "PUT",
            get_url("query3"),
            body={"arg": "hazmat"},
            want_body={"reverse": "tamzah"},
        )

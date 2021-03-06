from typing import Any, Dict, List, cast

import pytest
import asyncio
import aiohttp
import aiohttp.web

from contextlib import asynccontextmanager

from flurry.core import schema
from flurry.web import WebApplication
from flurry.core.context import Context
from flurry.core.query import QueryBase
from flurry.core.command import CommandBase
from flurry.core.subscription import SubscriptionBase

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


@APP.subscription(path="/sub1")
class SubscriptionA(SubscriptionBase):
    class Result(schema.SchemaBase):
        value = schema.Field(schema.Int, nullable=False)

    async def subscribe(
        self, context: Context
    ):  # pylint: disable=invalid-overridden-method
        for i in range(0, 10):
            await asyncio.sleep(1)
            yield self.Result(value=i)


@asynccontextmanager
async def run_application():
    async def test_mid(req, handler):
        resp: aiohttp.web.StreamResponse = await handler(req)
        resp.headers["X-MiddlewareWorks"] = "Yes"
        return resp

    loop = asyncio.get_event_loop()
    task = loop.create_task(APP.run(port=12345, middlewares=(test_mid,)))
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
        assert resp.headers["X-MiddlewareWorks"] == "Yes"
        if want_status is not None:
            assert want_status == resp.status
        else:
            assert resp.status < 400, f"got status {resp.status} {resp.reason}"
        if want_body is not None:
            assert (await resp.json()) == want_body


async def assert_subscription(url: str, values: List[Any]):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url) as client_websock:
            while values:
                expect = values.pop(0)
                got = await client_websock.receive_json(timeout=2.0)
                assert got == expect, f"expected {expect!r}, got {got!r}"
            msg = await client_websock.receive()
            assert msg.type == aiohttp.WSMsgType.CLOSE


@pytest.mark.asyncio
async def test_web_application():
    import datetime as dt

    get_url = lambda path: f"http://localhost:12345/{path}"
    async with run_application():
        now = dt.datetime.now(dt.timezone.utc)
        await asyncio.sleep(2)

        await asyncio.gather(
            assert_subscription(
                "ws://localhost:12345/sub1",
                [
                    {"value": 0},
                    {"value": 1},
                    {"value": 2},
                    {"value": 3},
                    {"value": 4},
                    {"value": 5},
                    {"value": 6},
                    {"value": 7},
                    {"value": 8},
                    {"value": 9},
                ],
            ),
            assert_response(
                "POST",
                get_url("command1"),
                body={"arg": "test"},
                want_body={"echo": "test"},
            ),
            assert_response(
                "POST",
                get_url("commandTwo/10"),
                want_body={"double": 20, "triple": 30},
            ),
            assert_response(
                "GET",
                get_url("command3"),
                query={"arg": now.isoformat()},
                want_body={"unix": now.timestamp()},
            ),
            assert_response(
                "GET",
                get_url("query1"),
                want_body={"hello": "Hello"},
            ),
            assert_response(
                "GET",
                get_url("queryTwo/abc"),
                query={"arg2": "xyz"},
                want_body={"concat": "abcxyz"},
            ),
            assert_response(
                "PUT",
                get_url("query3"),
                body={"arg": "hazmat"},
                want_body={"reverse": "tamzah"},
            ),
        )

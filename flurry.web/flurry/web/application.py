"""Service HTTP requests."""

from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    TypeVar,
    Union,
    overload,
)
from inspect import isawaitable

import attr
import asyncio
import logging
import stringcase  # type: ignore
import aiohttp
import aiohttp.web

from flurry.util import JSON
from flurry.core import schema
from flurry.core.context import Context
from flurry.core.application import Application
from flurry.core.query import QueryBase, QueryMeta
from flurry.core.schema import SchemaBase, SchemaMeta
from flurry.core.command import CommandBase, CommandMeta
from flurry.core.subscription import SubscriptionBase, SubscriptionMeta

LOG = logging.getLogger("flurry.web")

__all__ = ("WebApplication",)

# pylint: disable=invalid-name
_T_QueryMeta = TypeVar("_T_QueryMeta", bound=QueryMeta)
_T_CommandMeta = TypeVar("_T_CommandMeta", bound=CommandMeta)
_T_SubscriptionMeta = TypeVar("_T_SubscriptionMeta", bound=SubscriptionMeta)
_T_AnyMeta = TypeVar("_T_AnyMeta", bound=SchemaMeta)
_T_Context = TypeVar("_T_Context", bound=Context)
_T_Result = TypeVar("_T_Result", bound=schema.SchemaBase)
_T_MaybeResult = TypeVar("_T_MaybeResult", bound=Union[schema.SchemaBase, None])
_Decorator = Callable[[_T_AnyMeta], _T_AnyMeta]
# pylint: enable=invalid-name


Handler = Callable[[aiohttp.web.Request], Awaitable[aiohttp.web.StreamResponse]]
Middleware = Callable[
    [aiohttp.web.Request, Handler], Awaitable[aiohttp.web.StreamResponse]
]
Guard = Callable[[_T_Context, aiohttp.web.Request], Union[None, Awaitable[None]]]


@attr.define
class _CommandHandler(Generic[_T_Context, _T_MaybeResult]):
    command: CommandMeta
    guards: List[Guard[_T_Context]]
    context: _T_Context
    getargs: Optional[Callable[[aiohttp.web.Request], Awaitable[Dict[str, Any]]]]

    async def __call__(self, req: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        try:
            for guard in self.guards:
                guard_outcome = guard(self.context, req)
                if isawaitable(guard_outcome):
                    await guard_outcome
            args = {}
            if self.getargs:
                args.update(await self.getargs(req))
            inst: CommandBase[_T_Context, _T_MaybeResult] = self.command(**args)
            result: Any = await inst.exec(self.context)
            if isawaitable(result):
                result = await result
            if isinstance(result, SchemaBase):
                result = result.to_dict()
            if result is None:
                return aiohttp.web.json_response(status=200)
            return aiohttp.web.json_response(result, status=200, dumps=JSON.dumps)
        except aiohttp.web.HTTPError:
            raise
        except BaseException as err:
            LOG.error("request failed: %s", err, exc_info=err)
            raise aiohttp.web.HTTPInternalServerError from err


@attr.define
class _QueryHandler(Generic[_T_Context, _T_Result]):
    query: QueryMeta
    guards: List[Guard[_T_Context]]
    context: _T_Context
    getargs: Optional[Callable[[aiohttp.web.Request], Awaitable[Dict[str, Any]]]]

    async def __call__(self, req: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        try:
            for guard in self.guards:
                guard_outcome = guard(self.context, req)
                if isawaitable(guard_outcome):
                    await guard_outcome
            args = {}
            if self.getargs:
                args.update(await self.getargs(req))
            inst: QueryBase[_T_Context, _T_Result] = self.query(**args)
            result: Any = inst.fetch(self.context)
            if isawaitable(result):
                result = await result
            if isinstance(result, SchemaBase):
                result = result.to_dict()
            if result is None:
                return aiohttp.web.json_response(status=200)
            return aiohttp.web.json_response(result, status=200, dumps=JSON.dumps)
        except aiohttp.web.HTTPError:
            raise
        except BaseException as err:
            LOG.error("request failed: %s", err, exc_info=err)
            raise aiohttp.web.HTTPInternalServerError from err


@attr.define
class _SubscriptionHandler(Generic[_T_Context, _T_Result]):
    subscription: SubscriptionMeta
    guards: List[Guard[_T_Context]]
    context: _T_Context
    getargs: Optional[Callable[[aiohttp.web.Request], Awaitable[Dict[str, Any]]]]

    async def __call__(self, req: aiohttp.web.Request) -> aiohttp.web.WebSocketResponse:
        try:
            for guard in self.guards:
                guard_outcome = guard(self.context, req)
                if isawaitable(guard_outcome):
                    await guard_outcome
            args = {}
            if self.getargs:
                args.update(await self.getargs(req))
            inst: SubscriptionBase[_T_Context, _T_Result] = self.subscription(**args)
            websock = aiohttp.web.WebSocketResponse(heartbeat=5)
            await websock.prepare(req)

            async def consume():
                async for _msg in websock:
                    pass

            async def produce():
                async for item in inst.subscribe(self.context):
                    await websock.send_json(item.to_dict(), dumps=JSON.dumps)
                await websock.close(code=aiohttp.WSCloseCode.OK)

            try:
                await asyncio.gather(consume(), produce())
            except asyncio.CancelledError:
                pass
            finally:
                if not websock.closed:
                    await websock.close(code=aiohttp.WSCloseCode.ABNORMAL_CLOSURE)
            return websock
        except aiohttp.web.HTTPError:
            raise
        except BaseException as err:
            LOG.error("request failed: %s", err, exc_info=err)
            raise aiohttp.web.HTTPInternalServerError from err


class WebApplication(Generic[_T_Context], Application):
    """An Application that provides servicing for HTTP requests."""

    _routes: aiohttp.web.RouteTableDef

    def __init__(
        self,
        context: _T_Context,
        name_to_path: Callable[[str], str] = stringcase.snakecase,
    ):
        """Initialize a new WebApplication."""
        super().__init__()
        self._context = context
        self._routes = aiohttp.web.RouteTableDef()
        self._name_to_path = name_to_path

    @overload
    def query(self, query: None, **extra) -> _Decorator[_T_QueryMeta]:
        ...

    @overload
    def query(self, query: _T_QueryMeta, **extra) -> _T_QueryMeta:
        ...

    def query(
        self, query: Optional[_T_QueryMeta] = None, **extra
    ) -> Union[_T_QueryMeta, _Decorator[_T_QueryMeta]]:
        """Add docs later."""
        super_impl = super().query

        def query_decorator(query: _T_QueryMeta) -> _T_QueryMeta:
            path = extra.get("path", "/" + self._name_to_path(query.__name__))
            guards = extra.get("guards", [])
            methods = extra.get("method", ["GET"])
            if isinstance(methods, str):
                methods = [methods]
            for meth in methods:
                self.route(
                    meth,
                    path,
                    _QueryHandler(
                        query,
                        guards,
                        self._context,
                        self.__get_args if meth == "GET" else self.__post_args,
                    ),
                )
            return super_impl(query)

        if query is not None:
            return query_decorator(query)
        return query_decorator

    @overload
    def command(self, command: None, **extra) -> _Decorator[_T_CommandMeta]:
        ...

    @overload
    def command(self, command: _T_CommandMeta, **extra) -> _T_CommandMeta:
        ...

    def command(
        self, command: Optional[_T_CommandMeta] = None, **extra
    ) -> Union[_T_CommandMeta, _Decorator[_T_CommandMeta]]:
        """Add docs later."""
        super_impl = super().command

        def command_decorator(command: _T_CommandMeta) -> _T_CommandMeta:
            path = extra.get("path", "/" + self._name_to_path(command.__name__))
            guards = extra.get("guards", [])
            methods = extra.get("method", ["POST"])
            if isinstance(methods, str):
                methods = [methods]
            for meth in methods:
                self.route(
                    meth,
                    path,
                    _CommandHandler(
                        command,
                        guards,
                        self._context,
                        self.__get_args if meth == "GET" else self.__post_args,
                    ),
                )
            return super_impl(command)

        if command is not None:
            return command_decorator(command)
        return command_decorator

    @overload
    def subscription(
        self, subscription: None, **extra
    ) -> _Decorator[_T_SubscriptionMeta]:
        ...

    @overload
    def subscription(
        self, subscription: _T_SubscriptionMeta, **extra
    ) -> _T_SubscriptionMeta:
        ...

    def subscription(
        self, subscription: Optional[_T_SubscriptionMeta] = None, **extra
    ) -> Union[_T_SubscriptionMeta, _Decorator[_T_SubscriptionMeta]]:
        """Add docs later."""
        super_impl = super().subscription

        def subscription_decorator(
            subscription: _T_SubscriptionMeta,
        ) -> _T_SubscriptionMeta:
            path = extra.get("path", "/" + self._name_to_path(subscription.__name__))
            guards = extra.get("guards", [])
            methods = extra.get("method", ["GET"])
            if isinstance(methods, str):
                methods = [methods]
            if methods != ["GET"]:
                raise RuntimeError("subscriptions only support the GET method")
            for meth in methods:
                self.route(
                    meth,
                    path,
                    _SubscriptionHandler(
                        subscription,
                        guards,
                        self._context,
                        self.__get_args if meth == "GET" else self.__post_args,
                    ),
                )
            return super_impl(subscription)

        if subscription is not None:
            return subscription_decorator(subscription)
        return subscription_decorator

    def route(self, method: str, path: str, handler: Handler):
        async def do_request(req: aiohttp.web.Request):
            return await handler(req)

        self._routes.route(method, path)(do_request)

    async def __get_args(self, req: aiohttp.web.Request):
        return dict(**req.match_info, **req.query)

    async def __post_args(self, req: aiohttp.web.Request):
        args = await self.__get_args(req)
        if req.can_read_body:
            args.update(await req.json(loads=JSON.loads))
        return args

    async def run(
        self,
        *,
        host: str = "localhost",
        port: int = 8080,
        middlewares: Iterable[Middleware] = (),
    ):
        web_app = aiohttp.web.Application(
            middlewares=tuple(aiohttp.web.middleware(f) for f in middlewares)
        )
        web_app.add_routes(self._routes)
        for route in self._routes:
            print(f"{getattr(route, 'method')} {getattr(route, 'path')}")
        runner = aiohttp.web.AppRunner(app=web_app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, host=host, port=port)
        try:
            await site.start()
            while True:
                await asyncio.sleep(1800)
        finally:
            await site.stop()

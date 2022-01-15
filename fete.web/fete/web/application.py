"""Service HTTP requests."""

from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
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
from aiohttp import web as _web_

from fete.core import schema
from fete.core.context import Context
from fete.core.application import Application
from fete.core.query import QueryBase, QueryMeta
from fete.core.schema import SchemaBase, SchemaMeta
from fete.core.command import CommandBase, CommandMeta


# pylint: disable=invalid-name
_T_QueryMeta = TypeVar("_T_QueryMeta", bound=QueryMeta)
_T_CommandMeta = TypeVar("_T_CommandMeta", bound=CommandMeta)
_T_AnyMeta = TypeVar("_T_AnyMeta", bound=SchemaMeta)
_T_Context = TypeVar("_T_Context", bound=Context)
_T_Result = TypeVar("_T_Result", bound=Union[schema.SchemaBase, None])
_Decorator = Callable[[_T_AnyMeta], _T_AnyMeta]
# pylint: enable=invalid-name


@attr.define
class _CommandHandler(Generic[_T_Context, _T_Result]):
    command: CommandMeta
    context: _T_Context
    getargs: Optional[Callable[[_web_.Request], Awaitable[Dict[str, Any]]]]

    async def __call__(self, req: _web_.Request) -> _web_.StreamResponse:
        try:
            args = {}
            if self.getargs:
                args.update(await self.getargs(req))
            inst: CommandBase[_T_Context, _T_Result] = self.command(**args)
            result: Any = await inst.exec(self.context)
            if isawaitable(result):
                result = await result
            if isinstance(result, SchemaBase):
                result = result.to_dict()
            if result is None:
                return _web_.json_response(status=200)
            return _web_.json_response(result, status=200)
        except _web_.HTTPError:
            raise
        except BaseException as err:
            logging.error(err)
            raise _web_.HTTPInternalServerError from err


@attr.define
class _QueryHandler(Generic[_T_Context, _T_Result]):
    query: QueryMeta
    context: _T_Context
    getargs: Optional[Callable[[_web_.Request], Awaitable[Dict[str, Any]]]]

    async def __call__(self, req: _web_.Request) -> _web_.StreamResponse:
        try:
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
                return _web_.json_response(status=200)
            return _web_.json_response(result, status=200)
        except _web_.HTTPError:
            raise
        except BaseException as err:
            logging.error(err)
            raise _web_.HTTPInternalServerError from err


class WebApplication(Generic[_T_Context], Application):
    """An Application that provides servicing for HTTP requests."""

    _routes: _web_.RouteTableDef

    def __init__(
        self,
        context: _T_Context,
        name_to_path: Callable[[str], str] = stringcase.snakecase,
    ):
        """Initialize a new WebApplication."""
        super().__init__()
        self._context = context
        self._routes = _web_.RouteTableDef()
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
            methods = extra.get("method", ["GET"])
            if isinstance(methods, str):
                methods = [methods]
            for meth in methods:
                self._route(
                    meth,
                    path,
                    _QueryHandler(
                        query,
                        self._context,
                        self.__get_args if meth == "GET" else self.__post_args,
                    ),
                )
            return super_impl(query)

        if query is not None:
            return query_decorator(query)
        return query_decorator

    def _route(self, method: str, path: str, handler: Callable[[_web_.Request], Any]):
        async def do_request(req: _web_.Request):
            return await handler(req)

        self._routes.route(method, path)(do_request)

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
            methods = extra.get("method", ["POST"])
            if isinstance(methods, str):
                methods = [methods]
            for meth in methods:
                self._route(
                    meth,
                    path,
                    _CommandHandler(
                        command,
                        self._context,
                        self.__get_args if meth == "GET" else self.__post_args,
                    ),
                )
            return super_impl(command)

        if command is not None:
            return command_decorator(command)
        return command_decorator

    async def __get_args(self, req: _web_.Request):
        return dict(**req.match_info, **req.query)

    async def __post_args(self, req: _web_.Request):
        args = await self.__get_args(req)
        if req.can_read_body:
            args.update(await req.json())
        return args

    async def run(self, host: str = "localhost", port: int = 8080):
        web_app = _web_.Application()
        web_app.add_routes(self._routes)
        for route in self._routes:
            print(f"{getattr(route, 'method')} {getattr(route, 'path')}")
        runner = _web_.AppRunner(app=web_app)
        await runner.setup()
        site = _web_.TCPSite(runner, host=host, port=port)
        try:
            await site.start()
            while True:
                await asyncio.sleep(1800)
        finally:
            await site.stop()

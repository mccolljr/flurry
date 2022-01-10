"""Graphql over HTTP."""

import json
import logging
from typing import NamedTuple

import graphene
from aiohttp import web
from aiohttp_middlewares.cors import cors_middleware

from .generator import GraphqlGenerator
from ..application import Application
from money.storage import Storage


class CorsOptions(NamedTuple):
    """CORS configuration options."""

    allow_origin: str


class GraphqlApplication(Application):
    """An Application that provides a Graphql interface over HTTP."""

    def __init__(self, storage: Storage, cors_opts: CorsOptions = None):
        """Initialize the application."""
        super().__init__()
        self.storage = storage
        self.cors_opts = cors_opts

    @property
    def gql_schema(self) -> graphene.Schema:
        """Get the graphql schema serviced by this application."""
        existing = getattr(self, "__gql_schema", None)
        if existing is None:
            logging.info("building graphql schema")
            existing = GraphqlGenerator(self).generate_schema()
            setattr(self, "__gql_schema", existing)
            logging.info("schema built successfully")
        return existing

    def run(self, *, host: str = "localhost", port: int = 8080, **kwargs):
        """Run the server."""
        logging.info("===== LISTENING ON %s:%d =====", host, port)
        web.run_app(self._setup_app(), host=host, port=port, **kwargs)

    def _setup_app(self):
        _ = self.gql_schema
        web_app = web.Application(
            middlewares=[
                cors_middleware(origins=[self.cors_opts.allow_origin])
                if self.cors_opts is not None
                else cors_middleware(allow_all=True)
            ]
        )
        web_app.add_routes([web.post("/", self._handle_req)])
        return web_app

    async def _handle_req(self, req: web.Request):
        body = await req.json()
        query = body.get("query", None)
        var_vals = body.get("variables", None)
        result = await self.gql_schema.execute_async(
            query,
            variable_values=var_vals,
            context=graphene.Context(storage=self.storage),
        )
        if result.errors:
            return web.Response(
                content_type="application/json",
                body=json.dumps({"errors": ",".join(map(str, result.errors))}),
                status=500,
            )
        return web.Response(
            content_type="application/json", body=json.dumps(result.data), status=200
        )

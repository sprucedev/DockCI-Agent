"""
Parallel test control server
"""
import asyncio
import concurrent
import json

from aiohttp import web
from marshmallow.exceptions import ValidationError

from .models.parallel import ShardDetail


class ParallelTestController(object):
    """
    HTTP server to serve local Docker image tars, and control the image chain
    for the master/peer transfer
    """

    def __init__(self, logger):
        self._logger = logger
        self._shard_details = {}

        self.app = web.Application()

        self.app.router.add_route(
            'GET', '/image/{id}',
            self.handle_get_image,
        )
        self.app.router.add_route(
            'GET', '/shard/{id}',
            self.handle_get_shard_detail,
        )
        self.app.router.add_route(
            'PATCH', '/shard/{id}',
            self.handle_patch_shard_detail,
        )

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            self.app.executor = executor
            web.run_app(self.app)

    @asyncio.coroutine
    async def handle_get_image(self, request):
        """ Get image tar and stream """
        return web.Response(
            body='{"message": "Not Implemented"}'.encode(),
            status=501,
        )

    @asyncio.coroutine
    async def handle_get_shard_detail(self, request):
        """ Get request detail for pulling an image """
        params = request.match_info

        shard_detail = self._shard_details.get(params['id'], None)
        if shard_detail is None:
            return web.Response(status=404)

        return web.Response(
            body=json.dumps(
                shard_detail.SCHEMA.dump(shard_detail).data,
            ).encode(),
            status=200,
        )

    @asyncio.coroutine
    async def handle_patch_shard_detail(self, request):
        """ Save request image URL """
        params = request.match_info
        data = await request.post()

        # XXX needs cleanup
        shard_detail = self._shard_details.setdefault(
            params['id'],
            ShardDetail(),
        )

        try:
            shard_detail_data = shard_detail.SCHEMA.load(data, partial=True).data
        except ValidationError as ex:
            return web.Response(body=json.dumps(ex.messages), status=400)

        shard_detail.set_all(**shard_detail_data)

        return web.Response(
            body=json.dumps(
                shard_detail.SCHEMA.dump(shard_detail).data,
            ).encode(),
            status=200,
        )

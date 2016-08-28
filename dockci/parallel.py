"""
Parallel test control server
"""
import asyncio
import concurrent
import json

from urllib.parse import parse_qs

from aiohttp import web
from marshmallow.exceptions import ValidationError

from .models.parallel import ShardDetail
from .util import all_attrs_filled


WAIT_TIMEOUT = 60 * 10


async def resource_wait(key, resources, handlers):
    """
    Return ``resources[key]``, waiting for ``Event`` ``handlers[key]``
    to be fired when ready if not found
    """
    try:
        resource = resources[key]
    except KeyError:
        pass
    else:
        if all_attrs_filled(resource):
            return resource

    handler = handlers.setdefault(key, asyncio.Event())
    try:
        await asyncio.wait_for(handler.wait(), WAIT_TIMEOUT)
    except concurrent.futures.TimeoutError:
        return None

    return resources[key]


class ParallelTestController(object):
    """
    HTTP server to serve local Docker image tars, and control the image chain
    for the master/peer transfer
    """

    def __init__(self, logger):
        self._logger = logger
        self._shard_details = {}
        self._shard_details_handlers = {}

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
        qs_args = parse_qs(request.query_string, keep_blank_values=True)

        if 'wait' in qs_args:
            shard_detail = await resource_wait(
                params['id'],
                self._shard_details,
                self._shard_details_handlers,
            )
            if shard_detail is None:
                return web.Response(status=404)
        else:
            try:
                shard_detail = self._shard_details[params['id']]
            except KeyError:
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

        # Trigger anyone waiting on the full details
        handler = self._shard_details_handlers.get(params['id'], None)
        if handler is not None and all_attrs_filled(shard_detail):
            handler.set()

        return web.Response(
            body=json.dumps(
                shard_detail.SCHEMA.dump(shard_detail).data,
            ).encode(),
            status=200,
        )

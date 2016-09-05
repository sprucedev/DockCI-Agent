"""
Parallel test control server
"""
import asyncio
import concurrent
import concurrent.futures
import functools
import os
import json

from contextlib import contextmanager
from urllib.parse import parse_qs

import aiofiles
import aiohttp

from aiohttp import web

from marshmallow.exceptions import ValidationError

from .models.parallel import ShardDetail
from .util import all_attrs_filled


WAIT_TIMEOUT = 60 * 10
CACHE_DIR = '/Users/ricky/a'  # XXX
DOCKER_TAR_MIME = 'application/x-tar'

SHARD_DETAIL_COMPLETE_WITHOUT = ('next_detail',)


class ReverseSemaphore(object):
    """
    Like ``asyncio.Semaphore``, except the lock is released when ``value`` is
    zero, rather than non-zero

    Examples:

      >>> sem = ReverseSemaphore()
      >>> sem.locked()
      False

      >>> sem.acquire()
      >>> sem.locked()
      True

      >>> sem.release()
      >>> sem.locked()
      False

      >>> sem.acquire(3)
      >>> sem.release()
      >>> sem.locked()
      True

      >>> sem.release(2)
      >>> sem.locked()
      False

      >>> sem.release(20)
      >>> sem.acquire(1)
      >>> sem.locked()
      True
    """

    def __init__(self, value=0, loop=None):
        self._value = value
        self._lock = asyncio.Event(loop=loop)

    async def wait(self):
        """ Wait for the counter to be 0 """
        await self._lock.wait()

    def locked(self, ):
        """ Non-blocking check to see if wait will block """
        return self._lock.is_set()

    def _sync_lock(self):
        """ Sync the lock state with value, and ensure value is never below 0
        """
        if self._value >= 1:
            self._lock.set()
        else:
            self._lock.clear()
        if self._value < 0:
            self._value = 0

    def acquire(self, count=1):
        """ Increase the counter """
        if count < 1:
            raise ValueError('Count must be > 1')
        self._value += count
        self._sync_lock()

    def release(self, count=1):
        """ Decrease the counter """
        if count < 1:
            raise ValueError('Count must be > 1')
        self._value -= count
        self._sync_lock()

    @contextmanager
    def acquired(self, count=1):
        """ Context manager for acquiring then releasing a given amount """
        self.acquire(count)
        try:
            yield
        finally:
            self.release(count)


@contextmanager
def aiohttp_docker():
    """ Create an aiohttp session to the Docker socket """
    conn = aiohttp.UnixConnector(path='/var/run/docker.sock')
    with aiohttp.ClientSession(connector=conn) as session:
        yield session


async def resource_wait(key, resources, handlers, ignore=()):
    """
    Return ``resources[key]``, waiting for ``Event`` ``handlers[key]``
    to be fired when ready if not found
    """
    try:
        resource = resources[key]
    except KeyError:
        pass
    else:
        if all_attrs_filled(resource, ignore=ignore):
            return resource

    handler = handlers.setdefault(key, asyncio.Event())
    try:
        await asyncio.wait_for(handler.wait(), WAIT_TIMEOUT)
    except concurrent.futures.TimeoutError:
        return None

    return resources[key]


def cache_file_path(image_id):
    """ Path to the cache for a given image """
    return '{cache}/{id}.tar.incomplete'.format(cache=CACHE_DIR, id=image_id)


class ParallelTestController(object):
    """
    HTTP server to serve local Docker image tars, and control the image chain
    for the master/peer transfer
    """

    def __init__(self, port, logger):
        self._logger = logger
        self._shard_details = {}
        self._shard_details_handlers = {}
        self._image_writers = {}
        self._image_complete_events = {}
        self._image_cache_blockers = {}
        self.port = port

        self.app = web.Application(middlewares=[self.exception_logger])

        self.app.router.add_route(
            'GET', '/image/{id}',
            self.handle_get_image,
        )
        self.app.router.add_route(
            'POST', '/image/{id}',
            self.handle_post_image,
        )
        self.app.router.add_route(
            'GET', '/shard/{id}',
            self.handle_get_shard_detail,
        )
        self.app.router.add_route(
            'PATCH', '/shard/{id}',
            self.handle_patch_shard_detail,
        )

    @asyncio.coroutine
    async def exception_logger(self, app, handler):
        """ Log exceptions during handler """
        @asyncio.coroutine
        async def middleware(request):
            """ Try req handler, logging any non-HttpException exceptions """
            try:
                return await handler(request)
            except Exception as ex:
                if not isinstance(ex, web.HTTPException):
                    self._logger.exception('Exception in handler')
                raise
        return middleware

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            self.app.executor = executor
            web.run_app(self.app, port=self.port)

    @asyncio.coroutine
    async def handle_get_image(self, request):
        """ Get image tar and stream """
        params = request.match_info

        # XXX sanitize id
        with aiohttp_docker() as session:
            async with session.get(
                # XXX sanitize id
                'http://docker/images/get?names={id}'.format(id=params['id']),
            ) as resp:
                if resp.status == 404:
                    cache_file = cache_file_path(params['id'])
                    try:
                        with self._image_cache_blockers[params['id']].acquired():
                            async with aiofiles.open(cache_file, 'rb') as handle:
                                our_resp = aiohttp.web.StreamResponse(
                                    status=200,
                                    headers={'Content-Type': DOCKER_TAR_MIME},
                                )
                                await our_resp.prepare(request)
                                while True:
                                    chunk = await handle.read(1024)
                                    if not chunk:
                                        break
                                    our_resp.write(chunk)

                        try:
                            writers = self._image_writers[params['id']]
                            completion_event = self._image_complete_events[params['id']]
                        except KeyError:
                            pass  # Non-existent event means it's done
                        else:
                            writers.append(our_resp)
                            await completion_event.wait()

                            return our_resp
                    except FileNotFoundError:
                        return web.Response(status=404)
                elif 200 < resp.status <= 300:
                    # XXX JSON error?
                    return web.Response(status=500, body=b'Unknown HTTP status from Docker: %s' % resp.status)

                our_resp = web.StreamResponse(
                    status=200,
                    headers={'Content-Type': DOCKER_TAR_MIME},
                )
                await our_resp.prepare(request)

                try:
                    while not resp.content.at_eof():
                        our_resp.write(await resp.content.readany())
                except concurrent.futures.CancelledError:  # XXX probably not good
                    pass

                return our_resp

    @asyncio.coroutine
    async def cleanup_image_cache(self, path):
        """ Clean up an image tar from the cache after writers are done """
        await self._image_cache_blockers[image_id].wait()
        os.unlink(path)

    @asyncio.coroutine
    async def handle_post_image(self, request):
        """ Download an image tar into Docker, making it available for
        streaming at the same time """
        params = request.match_info
        data = await request.post()
        # XXX get from a specified URL
        # XXX handle HTTP error

        # XXX don't just assert; HTTP status and error
        assert params['id'] not in self._image_writers, 'Image writers exist'
        assert params['id'] not in self._image_complete_events, 'Image complete event exists'
        assert params['id'] not in self._image_cache_blockers, 'Image cache blocker exists'

        async with aiofiles.open(data['url'], 'rb') as in_handle:
            cache_file = cache_file_path(params['id'])
            async with aiofiles.open(cache_file, 'wb') as cache_handle:

                upload_stream = asyncio.StreamReader()
                # Upload stream must be first
                self._image_writers[params['id']] = image_writers = [upload_stream, cache_handle]
                self._image_complete_events[params['id']] = complete_event = asyncio.Event()
                self._image_cache_blockers[params['id']] = delete_blocker = ReverseSemaphore(value=1)

                async def read_write():
                    while True:
                        print('read start')
                        try:
                            chunk = await in_handle.read(1024)
                        except concurrent.futures.CancelledError:
                            print('read break')
                            break
                        print('read done')
                        if not chunk:
                            break
                        for writer in image_writers:
                            print('write')
                            try:
                                writer.feed_data(chunk)
                            except AttributeError:
                                writer.write(chunk)
                        print('end writes')

                    # Don't end the upload stream
                    for writer in image_writers[1:]:
                        print('end writer')
                        try:
                            writer.feed_eof()
                        except AttributeError:
                            writer.close()

                    print('complete/release')
                    complete_event.set()
                    delete_blocker.release()

                    del self._image_writers[params['id']]
                    del self._image_complete_events[params['id']]
                    del self._image_cache_blockers[params['id']]

                asyncio.ensure_future(self.cleanup_image_cache(cache_file))
                asyncio.ensure_future(read_write())

                with aiohttp_docker() as docker_session:
                    async with docker_session.post(
                        'http://docker/images/load',
                        headers={
                            'Content-Type': DOCKER_TAR_MIME,
                              # XXX copy from request HTTP
                            'Content-Length': '%d' % os.stat(in_handle.fileno()).st_size,
                        },
                        data=upload_stream,
                    ) as resp:
                        return web.Response(
                            status=resp.status,
                            body=await resp.read(),
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
                ignore=SHARD_DETAIL_COMPLETE_WITHOUT,
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
            headers={'Content-Type': 'application/json'},
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
        if handler is not None and all_attrs_filled(
            shard_detail,
            ignore=SHARD_DETAIL_COMPLETE_WITHOUT,
        ):
            handler.set()

        return web.Response(
            body=json.dumps(
                shard_detail.SCHEMA.dump(shard_detail).data,
            ).encode(),
            headers={'Content-Type': 'application/json'},
            status=200,
        )

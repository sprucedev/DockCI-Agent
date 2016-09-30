"""
Parallel test control server
"""
import asyncio
import concurrent
import concurrent.futures
import functools
import json
import os
import tempfile

from contextlib import contextmanager
from urllib.parse import parse_qs

import aiofiles
import aiohttp

from aiohttp import web

from marshmallow.exceptions import ValidationError

from .models.parallel import ShardDetail
from .util import all_attrs_filled


CHUNK_SIZE = 1024 * 1024 # 1mb
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

      >>> sem.acquire()
      >>> sem.acquire()
      >>> sem.release()
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
        self._sync_lock()

    async def wait(self):
        """ Wait for the counter to be 0 """
        await self._lock.wait()

    def locked(self):
        """ Non-blocking check to see if wait will block """
        return not self._lock.is_set()

    def _sync_lock(self):
        """ Sync the lock state with value, and ensure value is never below 0
        """
        if self._value >= 1:
            self._lock.clear()
        else:
            self._lock.set()
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


async def gather_futures(possible_futures):
    """ Schedule, and gather any futures, ignoring things that aren't futures
    """
    tasks = []
    for possible_future in possible_futures:
        try:
            tasks.append(asyncio.ensure_future(possible_future))
        except TypeError:
            pass
    return asyncio.gather(*tasks)


async def ensure_write(handle, data):
    """ Write to the given writer's write attribute (first ``write``, then
    ``feed_data``) and await if the result is awaitable

    Examples:

      >>> import io
      >>> loop = asyncio.get_event_loop()

      >>> handle = io.BytesIO()
      >>> loop.run_until_complete(ensure_write(handle, b'test'))
      >>> handle.getbuffer().tobytes()
      b'test'

      >>> handle = asyncio.StreamReader()
      >>> async def test_1_read():
      ...   print('READ:', await handle.read())
      >>> async def test_1_write():
      ...   await ensure_write(handle, b'test')
      ...   handle.feed_eof()
      >>> async def test_1_main():
      ...   await asyncio.gather(*[
      ...     task() for task in (test_1_read, test_1_write)
      ...   ])
      >>> loop.run_until_complete(test_1_main())
      READ: b'test'
    """
    try:
        possible_future = handle.write(data)
    except AttributeError:
        possible_future = handle.feed_data(data)

    try:
        await possible_future
    except TypeError:
        pass


def ew_gatherable(chunk, handle):
    try:
        return handle.write(chunk)
    except AttributeError:
        return handle.feed_data(chunk)

def ew_flush_gatherable(chunk, handle):
    ew_fut = ew_gatherable(chunk, handle)
    try:
        return asyncio.gather(ew_fut, handle.flush())
    except AttributeError:
        return ew_fut

async def write_all_no_lock(chunk, out_handles, a=False):
    c = [b for b in [
        ew_flush_gatherable(chunk, handle)
        for handle in out_handles
    ] if b is not None]
    if a:
        print(c)
    g = asyncio.gather(*c)
    await g


async def write_all(chunk, out_handles, write_lock=None, a=False):
    if write_lock is None:
        await write_all_no_lock(chunk, out_handles, a=a)
    else:
        with (await write_lock):
            await write_all_no_lock(chunk, out_handles)


async def read_write_all(in_handle, out_handles, write_lock=None, a=False):
    if hasattr(in_handle, 'at_eof'):
        while not in_handle.at_eof():
            chunk = await in_handle.read(CHUNK_SIZE)
            await write_all(chunk, out_handles, write_lock)
    else:
        while True:
            chunk = await in_handle.read(CHUNK_SIZE)
            if not chunk:
                return
            await write_all(chunk, out_handles, write_lock, a=a)


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

        self._cache_writers = {}
        self._cache_write_locks = {}
        self._cache_complete_events = {}

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
                    try:
                        cache_writers = self._cache_writers[params['id']]
                        write_lock = self._cache_write_locks[params['id']]
                        complete_event = self._cache_complete_events[params['id']]
                    except KeyError:
                        return web.Response(status=404)

                    try:
                        async with aiofiles.open('/tmp/%s.bin' % params['id'], 'rb') as cache_file:
                            fresp = aiohttp.web.StreamResponse(status=200)
                            await fresp.prepare(request)
                            print('R1 start')
                            await read_write_all(cache_file, (fresp,), a=True)
                            print('R2 start')
                            await read_write_all(cache_file, (fresp,), a=True)
                            print('WLOCK start')
                            with (await write_lock):
                                print('R3 start')
                                await read_write_all(cache_file, (fresp,), a=True)
                                cache_writers.append(fresp)
                            print('WAIT')

                            await complete_event.wait()
                            return fresp

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
    async def handle_post_image(self, request):
        """ Download an image tar into Docker, making it available for
        streaming at the same time """
        params = request.match_info
        data = await request.post()
        # XXX get from a specified URL
        # XXX handle HTTP error

        # XXX don't just assert; HTTP status and error
        assert params['id'] not in self._cache_writers, 'Cache writers exist'
        assert params['id'] not in self._cache_write_locks, 'Cache write lock exists'
        assert params['id'] not in self._cache_complete_events, 'Cache completion event exists'

        cache_writers = self._cache_writers[params['id']] = []
        write_lock = self._cache_write_locks[params['id']] = asyncio.Lock()
        complete_event = self._cache_complete_events[params['id']] = asyncio.Event()

        try:
            async with aiofiles.open(data['url'], 'rb') as in_handle:
                async with aiofiles.open('/tmp/%s.bin' % params['id'], 'wb') as cache_file:
                    upload_stream = asyncio.StreamReader()

                    cache_writers.append(cache_file)
                    cache_writers.append(upload_stream)

                    async def read_write():
                        try:
                            await read_write_all(in_handle, cache_writers, write_lock)
                            upload_stream.feed_eof()
                        except Exception as ex:
                            self._logger.exception('Exception in read/write')

                    asyncio.ensure_future(read_write())

                    with aiohttp_docker() as docker_session:
                        print('POSTING to docker')
                        async with docker_session.post(
                            'http://docker/images/load',
                            headers={
                                'Content-Type': DOCKER_TAR_MIME,
                                  # XXX copy from request HTTP
                                'Content-Length': '%d' % os.stat(in_handle.fileno()).st_size,
                            },
                            data=upload_stream,
                        ) as resp:
                            complete_event.set()
                            return web.Response(
                                status=resp.status,
                                body=await resp.read(),
                            )

        finally:
            del self._cache_writers[params['id']]
            del self._cache_write_locks[params['id']]
            del self._cache_complete_events[params['id']]

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

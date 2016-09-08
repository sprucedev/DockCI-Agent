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


class AIOCacheFile(object):
    """ File-like object that writes to a set of writers, and cleans up when
    done

    Examples:

      >>> import io

      >>> writer1 = io.BytesIO()
      >>> writer2 = io.BytesIO()
      >>> writer3 = io.BytesIO()

      >>> async def test_1():
      ...   global test_file
      ...   test_file = await AIOCacheFile.open()
      ...   test_file.add_writer(writer1)
      ...   test_file.add_writer(writer2)
      ...   await test_file.write(b'testing')
      ...   await test_file.flush()
      >>> asyncio.get_event_loop().run_until_complete(test_1())
      >>> writer1.getbuffer().tobytes()
      b'testing'
      >>> writer2.getbuffer().tobytes()
      b'testing'

      >>> async def test_2():
      ...   test_file.add_writer(writer3)
      ...   await test_file.flush()
      >>> asyncio.get_event_loop().run_until_complete(test_2())
      >>> writer3.getbuffer().tobytes()
      b'testing'

      >>> async def test_3():
      ...   await test_file.write(b'more')
      ...   await test_file.flush()
      >>> asyncio.get_event_loop().run_until_complete(test_3())
      >>> writer1.getbuffer().tobytes()
      b'testingmore'
      >>> writer3.getbuffer().tobytes()
      b'testingmore'
    """

    def __init__(self, tmpfile, aiohandle):
        self._tmpfile = tmpfile
        self._aiohandle = aiohandle
        self._close_event = asyncio.Event()
        self._writers = []
        self._writer_lock = ReverseSemaphore()

    @contextmanager
    def cleanup_blocked(self):
        """ Block cleanup and write events """
        self._writer_lock.acquire()
        try:
            yield
        finally:
            self._writer_lock.release()

    def add_writer(self, writer):
        self._writer_lock.acquire()
        asyncio.ensure_future(self._write_and_add_writer(writer))

    async def _write_and_add_writer(self, writer):
        try:
            await self._aiohandle.flush()
            async with aiofiles.open(self._tmpfile.name, mode='rb') as handle:
                while True:
                    chunk = await handle.read(1024)
                    if not chunk:
                        break
                    await ensure_write(writer, chunk)
            self._writers.append(writer)
        finally:
            self._writer_lock.release()

    async def write(self, data):
        # XXX do we need to await writer_lock here?
        await self._aiohandle.write(data)
        await gather_futures([
            ensure_write(writer, data)
            for writer in self._writers
        ])

    async def flush(self):
        await self._writer_lock.wait()
        await gather_futures([
            writer.flush() if hasattr(writer, 'flush') else None
            for writer in self._writers
        ])

    async def close(self):
        await self.flush()
        self._tmpfile.close()
        self._close_event.set()

    async def wait(self):
        """ Wait for file to be closed """
        await self._close_event.wait()

    @classmethod
    async def open(cls):
        tmpfile = tempfile.NamedTemporaryFile()
        return AIOCacheFile(
            tmpfile=tmpfile,
            aiohandle=await aiofiles.open(tmpfile.name, mode='wb'),
        )


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
        self._cache_files = {}
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
                        cache_file = self._cache_files[params['id']]
                    except KeyError:
                        return web.Response(status=404)

                    with cache_file.cleanup_blocked():
                        our_resp = web.StreamResponse(
                            status=200,
                            headers={'Content-Type': DOCKER_TAR_MIME},
                        )
                        await our_resp.prepare(request)
                        cache_file.add_writer(our_resp)

                    await cache_file.wait()
                    return our_resp

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
        assert params['id'] not in self._cache_files, 'Cache file exists'

        async with aiofiles.open(data['url'], 'rb') as in_handle:
            self._cache_files[params['id']] = cache_file = await AIOCacheFile.open()
            try:
                upload_stream = asyncio.StreamReader()
                cache_file.add_writer(upload_stream)

                async def read_write():
                    while True:
                        chunk = await in_handle.read(1024)
                        if not chunk:
                            break
                        await cache_file.write(chunk)

                    upload_stream.feed_eof()

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
            finally:
                del self._cache_files[params['id']]
                await cache_file.close()

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

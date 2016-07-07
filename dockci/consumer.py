""" Setup and run the DockCI agent consumer """
import asyncio
import json

from concurrent import futures

import aioamqp

from aioamqp.exceptions import AmqpClosedConnection

from dockci.models.job import Job
from dockci.server import CONFIG


ROUTING_KEY = '*'
RECONNECT_TIMER = 5

class Consumer(object):
    def __init__(self, conn_params, logger):
        self._conn_params = conn_params
        self._logger = logger
        self._transport = None
        self._connect_future = None

    def run(self):
        loop = asyncio.get_event_loop()
        loop.set_default_executor(futures.ProcessPoolExecutor(max_workers=1))
        self._init_future = self._rmq_init()
        loop.run_until_complete(self._init_future)
        loop.run_forever()

    @asyncio.coroutine
    def _rmq_init(self):
        while True:
            if self._transport is not None:
                self._transport.close()

            try:
                self._logger.info('Connecting to RabbitMQ')
                self._transport, protocol = yield from asyncio.wait_for(
                    aioamqp.connect(
                        self._conn_params.host,
                        self._conn_params.port,
                        self._conn_params.credentials.username,
                        self._conn_params.credentials.password,
                        on_error=self._on_connect_error,
                    ),
                    timeout=30,
                )

                self._logger.info('Creating channel')
                channel = yield from protocol.channel()

                self._logger.info('Declaring queue "%s"', CONFIG.rabbitmq_queue)
                yield from channel.queue_declare(
                    CONFIG.rabbitmq_queue,
                    passive=True,
                    exclusive=False,
                    auto_delete=False,
                )

                self._logger.info('Binding queue')
                yield from channel.queue_bind(
                    CONFIG.rabbitmq_queue,
                    CONFIG.rabbitmq_exchange,
                    ROUTING_KEY,
                )

                self._logger.debug('Setting prefetch')
                yield from channel.basic_qos(prefetch_count=1, prefetch_size=0)

                break  # Break out of inifinite loop

            except (
                OSError,
                AmqpClosedConnection,
                aioamqp.exceptions.ChannelClosed,
                futures.TimeoutError,
            ) as err:
                if isinstance(err, futures.TimeoutError):
                    self._logger.error('Timed out')
                else:
                    try:
                        self._logger.error(err.message)
                    except AttributeError:
                        self._logger.error(err)

                self._logger.info('Reconnecting in %d seconds', RECONNECT_TIMER)
                yield from asyncio.sleep(RECONNECT_TIMER)

        self._init_future = None
        asyncio.async(self._rmq_consume(channel))

    @asyncio.coroutine
    def _on_connect_error(self, err):
        try:
            self._logger.error(err.message)
        except AttributeError:
            self._logger.error(err)

        if self._init_future is None:
            self._init_future = self._rmq_init()
            asyncio.async(self._init_future)

    @asyncio.coroutine
    def _rmq_consume(self, channel):
        self._logger.info('Starting to consume')
        yield from channel.basic_consume(
            self._on_message,
            queue_name=CONFIG.rabbitmq_queue,
        )

    def _on_message(self, channel, body, envelope, properties):
        try:
            yield from self._process_message(
                channel, body, envelope, properties,
            )
        finally:
            asyncio.async(self._rmq_consume(channel))

    @asyncio.coroutine
    def _process_message(self, channel, body, envelope, properties):
        self._logger.info('Received message')
        self._logger.debug('Message body: %s', body)

        try:
            job_data = json.loads(body.decode())
            project_slug = job_data.pop('project_slug')
            job_slug = job_data.pop('job_slug')
            job = Job.load(project_slug, job_slug, **job_data)

        except (ValueError, KeyError):
            self._logger.exception('Failed to load job message: %s', body)
            self._logger.info('Rejecting message')
            yield from channel.basic_client_nack(
                delivery_tag=envelope.delivery_tag,
                requeue=False,
            )

        else:
            self._logger.info('Acknowleding message')
            yield from channel.basic_client_ack(
                delivery_tag=envelope.delivery_tag,
            )

            self._logger.info('Running job %s/%s', project_slug, job_slug)
            yield from asyncio.get_event_loop().run_in_executor(
                None, job.run,
            )

            self._logger.info('Job completed')

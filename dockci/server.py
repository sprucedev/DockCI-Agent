"""
Functions for setting up and starting the DockCI application server
"""
import logging
import mimetypes
import os

from contextlib import contextmanager

import click
import pika
import redis
import rollbar

from .util import project_root


LOG_FORMAT = ('%(levelname) -7s %(asctime)s %(name) -10s %(funcName) '
              '-10s %(lineno) -4d: %(message)s')


class Config(object):  # pylint:disable=too-many-instance-attributes
    """ Ad-hoc application configuration tied to click context """
    @property
    def config_dict(self):  # pylint:disable=no-self-use
        """ Get the dict containing all config attached to the click context

        :return dict: All app config
        """
        context = click.get_current_context()
        if context.obj is None:
            context.obj = {}

        return context.obj

    def __getattr__(self, name):
        try:
            return self.config_dict[name]
        except KeyError:
            if name.startswith('__') and name.endswith('__'):
                raise AttributeError()

    def __setattr__(self, name, value):
        self.config_dict[name] = value


CONFIG = Config()


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def cli(_, debug):
    """ Placeholder for global CLI group """
    # pylint:disable=attribute-defined-outside-init
    CONFIG.debug = debug

    init_logging()
    CONFIG.logger = logging.getLogger('dockci')
    CONFIG.logger.debug("Running in DEBUG mode")

    init_config()
    init_rollbar()

    mimetypes.add_type('application/x-yaml', 'yaml')


def init_logging():
    """ Logging setup """
    logging.basicConfig(level=logging.DEBUG if CONFIG.debug else logging.INFO,
                        format=LOG_FORMAT)


def init_config():
    """ Pre-run app setup """
    logger = CONFIG.logger.getChild('init')
    logger.info("Loading app config")

    # pylint:disable=attribute-defined-outside-init
    CONFIG.rabbitmq_user = os.environ.get(
        'RABBITMQ_ENV_BACKEND_USER', 'guest')
    CONFIG.rabbitmq_password = os.environ.get(
        'RABBITMQ_ENV_BACKEND_PASSWORD', 'guest')
    CONFIG.rabbitmq_host = os.environ.get(
        'RABBITMQ_PORT_5672_TCP_ADDR', 'localhost')
    CONFIG.rabbitmq_port = int(os.environ.get(
        'RABBITMQ_PORT_5672_TCP_PORT', 5672))
    CONFIG.rabbitmq_exchange = os.environ.get(
        'RABBITMQ_EXCHANGE', 'dockci.queue')
    CONFIG.rabbitmq_queue = os.environ.get(
        'RABBITMQ_QUEUE', 'dockci.agent')

    CONFIG.redis_host = os.environ.get(
        'REDIS_PORT_6379_TCP_ADDR', 'localhost')
    CONFIG.redis_port = int(os.environ.get(
        'REDIS_PORT_6379_TCP_PORT', 6379))

    CONFIG.redis_len_expire = 60 * 60  # 1hr


def get_redis_pool():
    """ Create a configured Redis connection pool """
    return redis.ConnectionPool(host=CONFIG.redis_host,
                                port=CONFIG.redis_port,
                                socket_timeout=1,
                                socket_connect_timeout=1,
                                )


@contextmanager
def redis_pool():
    """ Context manager for getting and disconnecting a Redis pool """
    pool = get_redis_pool()
    try:
        yield pool

    finally:
        pool.disconnect()


def pika_conn_params():
    """ Connection params for a pika connection """
    return pika.ConnectionParameters(
        host=CONFIG.rabbitmq_host,
        port=CONFIG.rabbitmq_port,
        credentials=pika.credentials.PlainCredentials(
            CONFIG.rabbitmq_user,
            CONFIG.rabbitmq_password,
        ),
    )


# NOTE: only used for WRITE ops! CONSUMER is different for now
def get_pika_conn():
    """ Create a connection to RabbitMQ """
    return pika.BlockingConnection(pika_conn_params())


# NOTE: only used for WRITE ops! CONSUMER is different for now
@contextmanager
def pika_conn():
    """ Context manager for getting and closing a pika connection """
    conn = get_pika_conn()
    try:
        yield conn

    finally:
        conn.close()


def wrapped_report_exception(app, exception):
    """ Wrapper for ``report_exception`` to ignore some exceptions """
    if getattr(exception, 'no_rollbar', False):
        return

    return rollbar.contrib.flask.report_exception(app, exception)


def init_rollbar():
    """ Initialize Rollbar for error/exception reporting """
    try:
        api_key = os.environ['ROLLBAR_API_KEY']
        environment = os.environ['ROLLBAR_ENVIRONMENT']
    except KeyError:
        logging.error('No Rollbar settings found')
        return

    rollbar.init(
        api_key,
        environment,
        root=project_root().strpath,
        allow_logging_basic_config=False,
    )

"""
Functions for setting up and starting the DockCI application server
"""
import logging
import mimetypes
import multiprocessing
import os

from contextlib import contextmanager

import click
import pika
import redis
import rollbar
#import rollbar.contrib.flask

#from dockci.models.config import Config
#from dockci.util import project_root, setup_templates, tokengetter_for


class Config(object):
    @property
    def config_dict(_):
        context = click.get_current_context()
        if context.obj is None:
            context.obj = {}

        return context.obj

    def __getattr__(self, name):
        return self.config_dict[name]
    def __setattr__(self, name, value):
        self.config_dict[name] = value


CONFIG = Config()


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def cli(ctx, debug):
    """ Placeholder for global CLI group """
    CONFIG.logger = logging.getLogger('dockci')
    CONFIG.debug = debug

    init_config()

    mimetypes.add_type('application/x-yaml', 'yaml')


def init_config():
    """ Pre-run app setup """
    #app_init_rollbar()

    logger = CONFIG.logger.getChild('init')
    logger.info("Loading app config")

    # APP.config['MAIL_SERVER'] = CONFIG.mail_server
    # APP.config['MAIL_PORT'] = CONFIG.mail_port
    # APP.config['MAIL_USE_TLS'] = CONFIG.mail_use_tls
    # APP.config['MAIL_USE_SSL'] = CONFIG.mail_use_ssl
    # APP.config['MAIL_USERNAME'] = CONFIG.mail_username
    # APP.config['MAIL_PASSWORD'] = CONFIG.mail_password
    # APP.config['MAIL_DEFAULT_SENDER'] = CONFIG.mail_default_sender
    #
    CONFIG.rabbitmq_user = os.environ.get(
        'RABBITMQ_ENV_BACKEND_USER', 'guest')
    CONFIG.rabbitmq_password = os.environ.get(
        'RABBITMQ_ENV_BACKEND_PASSWORD', 'guest')
    CONFIG.rabbitmq_host = os.environ.get(
        'RABBITMQ_PORT_5672_TCP_ADDR', 'localhost')
    CONFIG.rabbitmq_port = int(os.environ.get(
        'RABBITMQ_PORT_5672_TCP_PORT', 5672))

    CONFIG.redis_host = os.environ.get(
        'REDIS_PORT_6379_ADDR', 'redis')
    CONFIG.redis_port = int(os.environ.get(
        'REDIS_PORT_6379_PORT', 6379))

    # app_init_workers()


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
        heartbeat_interval=60 * 30,  # 30min
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


def app_init_rollbar():
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

    flask.got_request_exception.connect(wrapped_report_exception, APP)


def app_init_workers():
    """
    Initialize the worker job queue
    """
    from .workers import start_workers
    APP.worker_queue = multiprocessing.Queue()

    try:
        start_workers()
    except Exception:
        rollbar.report_exc_info()
        raise

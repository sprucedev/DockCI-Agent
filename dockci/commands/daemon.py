""" Click commands for the agent daemon """
import os

import click
import py.path  # pylint:disable=import-error

from dockci.consumer import Consumer
from dockci.parallel import ParallelTestController
from dockci.server import cli, CONFIG, pika_conn_params


@cli.command()
@click.option('--dockci-url', required=True)
@click.option('--dockci-apikey', required=True)
@click.option('--blob-path', default='/tmp/dockci-blobs')
@click.option('--agent-port', default=8080)
@click.pass_context
def run(_, dockci_url, dockci_apikey, blob_path, agent_port):
    """ Run the agent daemon """
    CONFIG.dockci_url = dockci_url
    CONFIG.api_key = dockci_apikey
    CONFIG.blob_path = py.path.local(blob_path)
    CONFIG.agent_port = agent_port
    if os.fork():
        consumer = Consumer(
            pika_conn_params(),
            CONFIG.logger.getChild('consumer'),
        )
        consumer.run()
    else:
        # XXX configuration
        controller = ParallelTestController(
            agent_port,
            CONFIG.logger.getChild('parallel'),
        )
        controller.run()

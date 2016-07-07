""" Click commands for the agent daemon """
import click
import py.path  # pylint:disable=import-error

from dockci.consumer import Consumer
from dockci.server import cli, CONFIG, pika_conn_params


@cli.command()
@click.option('--dockci-url', required=True)
@click.option('--dockci-apikey', required=True)
@click.option('--blob-path', default='/tmp/dockci-blobs')
@click.pass_context
def run(_, dockci_url, dockci_apikey, blob_path):
    """ Run the agent daemon """
    CONFIG.dockci_url = dockci_url
    CONFIG.api_key = dockci_apikey
    CONFIG.blob_path = py.path.local(blob_path)
    consumer = Consumer(
        pika_conn_params(),
        CONFIG.logger.getChild('consumer'),
    )
    consumer.run()

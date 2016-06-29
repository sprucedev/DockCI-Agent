""" Click commands for the agent daemon """
import click

from dockci.consumer import Consumer
from dockci.server import cli, CONFIG, pika_conn_params


@cli.command()
@click.option('--dockci-url', required=True)
@click.option('--dockci-apikey', required=True)
@click.pass_context
def run(_, dockci_url, dockci_apikey):
    """ Run the agent daemon """
    CONFIG.dockci_url = dockci_url
    CONFIG.api_key = dockci_apikey
    consumer = Consumer(
        pika_conn_params(),
        CONFIG.logger.getChild('consumer'),
    )
    consumer.run()

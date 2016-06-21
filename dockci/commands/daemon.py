""" Click commands for the agent daemon """
import click
import pika

from dockci.consumer import Consumer
from dockci.server import cli, CONFIG


@cli.command()
@click.option('--dockci-url', required=True)
@click.option('--dockci-apikey', required=True)
@click.pass_context
def run(ctx, dockci_url, dockci_apikey):
    """ Run the agent daemon """
    CONFIG.dockci_url = dockci_url
    CONFIG.api_key = dockci_apikey
    consumer = Consumer(
        pika.ConnectionParameters(
            host=CONFIG.rabbitmq_host,
            port=CONFIG.rabbitmq_port,
            credentials=pika.credentials.PlainCredentials(
                CONFIG.rabbitmq_user,
                CONFIG.rabbitmq_password,
            ),
        ),
        CONFIG.logger.getChild('consumer'),
    )
    consumer.run()

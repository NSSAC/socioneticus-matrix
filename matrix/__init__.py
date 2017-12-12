"""
Matrix: CLI Interface
"""

import click
import logbook

from .controller import main_controller
from .simpleagent import main_agent

ENVVAR_ADDRESS = "MATRIX_ADDRESS"
ENVVAR_EVENT_DB = "MATRIX_EVENT_DB"

@click.group()
def cli():
    pass

@cli.command()
@click.option("-a", "--address",
              envvar=ENVVAR_ADDRESS,
              required=True,
              help="Controller address in the format [IP:PORT]")
@click.option("-e", "--event-db",
              envvar=ENVVAR_EVENT_DB,
              required=True,
              help="Event database location")
@click.option("-n", "--num-agents",
              required=True,
              help="Number of agents")
def controller(address, event_db, num_agents):

    # Convert address to tuple format
    # Input format: 127.0.0.1:1600
    address = address.strip().split(":")
    address = (address[0], int(address[1]))

    logbook.StderrHandler().push_application()
    main_controller(address, event_db, int(num_agents))

@cli.command()
@click.option("-a", "--address",
              envvar=ENVVAR_ADDRESS,
              required=True,
              help="Controller address in the format [IP:PORT]")
@click.option("-e", "--event-db",
              envvar=ENVVAR_EVENT_DB,
              required=True,
              help="Event database location")
def simpleagent(address, event_db):

    # Convert address to tuple format
    # Input format: 127.0.0.1:1600
    address = address.strip().split(":")
    address = (address[0], int(address[1]))

    logbook.StderrHandler().push_application()
    main_agent(address, event_db)

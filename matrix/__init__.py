"""
Matrix: CLI Interface
"""

import click
import logbook

from .controller import main_controller
from .simpleagent import main_agent as main_simpleagent
from .thresholdagent import main_agent_single as main_thresholdagent, \
                            main_agent_multi as main_thresholdagent_multi
from .initdb import main_initdb

ENVVAR_ADDRESS = "MATRIX_ADDRESS"
ENVVAR_EVENT_DB = "MATRIX_EVENT_DB"

@click.group()
def cli():
    """
    Matrix: A distributed ABM platform.
    """

    pass

@cli.command()
@click.option("-a", "--address",
              envvar=ENVVAR_ADDRESS,
              required=True,
              help="Controller address in the format [IP:PORT]")
@click.option("-e", "--event-db",
              envvar=ENVVAR_EVENT_DB,
              required=True,
              type=click.Path(exists=True, dir_okay=False, writable=True),
              help="Event database location")
@click.option("-n", "--num-agents",
              required=True,
              type=int,
              help="Number of agents")
@click.option("-r", "--num-rounds",
              required=True,
              type=int,
              help="Number of rounds")
@click.option("-t", "--start-time-real",
              default=0,
              help="Start time (real time) of the simulation")
@click.option("-p", "--period-real",
              default=300,
              help="Number of seconds in real time that every round represents")
def controller(**kwargs):
    """
    Start a controller process.
    """

    return main_controller(**kwargs)

@cli.command()
@click.option("-e", "--event-db",
              envvar=ENVVAR_EVENT_DB,
              required=True,
              type=click.Path(exists=False),
              help="Event database location")
@click.option("-n", "--num-agents",
              required=True,
              type=int,
              help="Number of agents")
@click.option("-m", "--num-repos",
              required=True,
              type=int,
              help="Number of repos")
@click.option("-t", "--start-time-real",
              default=0,
              help="Start time (real time) of the simulation")
def initdb(**kwargs):
    """
    Initialize event database.
    """

    return main_initdb(**kwargs)

@cli.command()
@click.option("-a", "--address",
              envvar=ENVVAR_ADDRESS,
              required=True,
              help="Controller address in the format [IP:PORT]")
@click.option("-e", "--event-db",
              envvar=ENVVAR_EVENT_DB,
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Event database location")
@click.option("-i", "--agent-id",
              required=True,
              type=int,
              help="The ID of the agent")
def simpleagent(**kwargs):
    """
    Start a simple agent process.
    """

    return main_simpleagent(**kwargs)

@cli.command()
@click.option("-a", "--address",
              envvar=ENVVAR_ADDRESS,
              required=True,
              help="Controller address in the format [IP:PORT]")
@click.option("-e", "--event-db",
              envvar=ENVVAR_EVENT_DB,
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Event database location")
@click.option("-i", "--agent-id",
              required=True,
              type=int,
              help="The ID of the agent")
@click.option("-p", "--nl-prob",
              default=0.5,
              help="New repository lookup probability")
@click.option("-n", "--con-thres",
              default=2,
              help="Repository consideration threshold")
def thresholdagent(**kwargs):
    """
    Start a threshold agent process.
    """

    return main_thresholdagent(**kwargs)

@cli.command()
@click.option("-a", "--address",
              envvar=ENVVAR_ADDRESS,
              required=True,
              help="Controller address in the format [IP:PORT]")
@click.option("-e", "--event-db",
              envvar=ENVVAR_EVENT_DB,
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Event database location")
@click.option("-I", "--agent-id-range",
              required=True,
              nargs=2,
              type=int,
              help="The start and stop range of the agent IDs (endpoints are inclusive)")
@click.option("-p", "--nl-prob",
              default=0.5,
              help="New repository lookup probability")
@click.option("-n", "--con-thres",
              default=2,
              help="Repository consideration threshold")
def thresholdagent_multi(**kwargs):
    """
    Start a threshold agent process.
    """

    start, stop = kwargs.pop("agent_id_range")
    agent_ids = list(range(start, stop + 1))
    kwargs["agent_ids"] = agent_ids

    return main_thresholdagent_multi(**kwargs)

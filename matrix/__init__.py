"""
Matrix: CLI Interface
"""

import sys

import click
import logbook

from .controller import main_controller
from .simpleagent import main_agent as main_simpleagent
from .thresholdagent import main_agent_single as main_thresholdagent_single, \
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
              type=int,
              help="The ID of the agent")
@click.option("-I", "--agent-id-range",
              nargs=2,
              type=int,
              help="The start and stop range of the agent IDs (endpoints are inclusive)")
@click.option("-p", "--nl-prob",
              default=0.5,
              help="New repository lookup probability")
@click.option("-n", "--con-thres",
              default=2,
              help="Repository consideration threshold")
def thresholdagent(**kwargs):
    """
    Start threshold agent processes.

    Note: either specify a single agent ID using --agent-id,
    or specify multiple agent IDs using --agent-id-range.
    Giving both or neither is an error.
    """

    agent_id = kwargs.pop("agent_id")
    agent_id_range = kwargs.pop("agent_id_range")

    if bool(agent_id) == bool(agent_id_range):
        print("Specifying only one of --agent-id and --agent-id-range is required.", file=sys.stderr)
        return 1

    if agent_id:
        kwargs["agent_id"] = agent_id
        return main_thresholdagent_single(**kwargs)

    start, stop = agent_id_range
    agent_ids = list(range(start, stop + 1))
    kwargs["agent_ids"] = agent_ids
    return main_thresholdagent_multi(**kwargs)

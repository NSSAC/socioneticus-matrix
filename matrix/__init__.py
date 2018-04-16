"""
Matrix: CLI Interface
"""

import sys

import click
import logbook

from .controller import main_controller
from .dummyagent import main_dummyagent
from .dummystore import main_dummystoreinit

@click.group()
def cli():
    """
    Matrix: A distributed ABM platform.
    """

    pass

@cli.command()
@click.option("-p", "--ctrl-port",
              required=True,
              type=int,
              help="Controller port")
@click.option("-l", "--log-fname",
              required=True,
              type=click.Path(dir_okay=False, writable=True),
              help="Event log file location")
@click.option("-s", "--state-dsn",
              required=True,
              type=click.Path(exists=True, dir_okay=False, writable=True),
              help="System state data source name")
@click.option("-m", "--state-store-module",
              required=True,
              type=str,
              help="State store module")
@click.option("-n", "--num-agentprocs",
              required=True,
              type=int,
              help="Number of agent processes")
@click.option("-r", "--num-rounds",
              required=True,
              type=int,
              help="Number of rounds")
@click.option("-t", "--start-time",
              default=0,
              type=int,
              help="Start time (realtime) of the simulation in unix timestamp")
@click.option("-q", "--round-time",
              default=300,
              type=int,
              help="Number of seconds in realtime that every round represents")
@click.option("-S", "--controller-seed",
              default=42,
              type=int,
              help="Random seed for the controller")
def controller(**kwargs):
    """
    Start a controller process.
    """

    return main_controller(**kwargs)

@cli.command()
@click.option("-s", "--state-dsn",
              required=True,
              type=click.Path(dir_okay=False, writable=True),
              help="System state data source name")
def dummystoreinit(**kwargs):
    """
    Initialize the dummystore database.
    """

    main_dummystoreinit(**kwargs)

@cli.command()
@click.option("-p", "--ctrl-port",
              required=True,
              type=int,
              help="Controller port")
@click.option("-s", "--state-dsn",
              required=True,
              type=click.Path(exists=True, dir_okay=False, writable=True),
              help="System state data source name")
@click.option("-i", "--agentproc-id",
              required=True,
              type=int,
              help="Agent process id")
@click.option("--num-agents",
              default=10,
              help="Number of agents this process simulates")
def dummyagent(**kwargs):
    """
    Start a dummyagent process.
    """

    return main_dummyagent(**kwargs)

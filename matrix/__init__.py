"""
Matrix: Agent based social simulation
"""

import click
import logbook

from .controller import main_controller
from .agent import main_agent

@click.group()
def cli():
    pass

@cli.command()
def controller():
    logbook.StderrHandler().push_application()
    main_controller(('127.0.0.1', 16000))

@cli.command()
def agent():
    logbook.StderrHandler().push_application()
    main_agent(('127.0.0.1', 16000))

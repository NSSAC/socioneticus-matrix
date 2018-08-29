"""
Blue Pill: Matrix's in built agent and store inteface
"""

import click
from attrdict import AttrDict
from blessings import Terminal
import logbook
from logbook.compat import redirect_logging
from qz7.logbook import ColorLogFormatter, ChannelFilterHandler

from .bluepill_store import main_store_init
from .bluepill_agent import main_agent

@click.group()
@click.option('--debug/--no-debug',
              default=False,
              help="Enable/disable debug logging")
@click.option('--logtostderr/--no-logtostderr',
              default=True,
              help="Enable/disable logging to stderr")
@click.pass_context
def cli(ctx, debug, logtostderr):
    """
    Bluepill agents and stores.
    """

    cfg = AttrDict()
    cfg.terminal = Terminal()

    ctx.obj = cfg

    if logtostderr:
        if debug:
            handler = logbook.StderrHandler(logbook.DEBUG)
            handler.formatter = ColorLogFormatter(cfg.terminal)
            handler.push_application()
        else:
            handler = logbook.StderrHandler(logbook.INFO)
            handler.formatter = ColorLogFormatter(cfg.terminal)
            handler.push_application()
            ChannelFilterHandler(["aioamqp.protocol"]).push_application()

        redirect_logging()

@cli.group()
def store():
    """
    The blue pill state store.
    """

    pass

@store.command("init")
@click.option("-s", "--state-dsn",
              required=True,
              type=click.Path(dir_okay=False, writable=True),
              help="System state data source name")
def store_init(**kwargs):
    """
    Initialize the BluePill store.
    """

    main_store_init(**kwargs)

@cli.group()
def agent():
    """
    The blue pill agent.
    """

@agent.command("start")
@click.option("-n", "--ctrl-node",
              required=True,
              type=str,
              help="Controller node name")
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
@click.option("-m", "--num-agents",
              default=1,
              help="Number of agents this process simulates")
def agent_start(**kwargs):
    """
    Start a BluePill agent.
    """

    return main_agent(**kwargs)

if __name__ == "__main__":
    cli() # pylint: disable=no-value-for-parameter

"""
BluePill: Matrix's in built agent
"""

import click
from attrdict import AttrDict
import logbook
from logbook.compat import redirect_logging

from .bluepill_agent import main_agent, main_store_init


@click.group()
@click.option("--debug/--no-debug", default=False, help="Enable/disable debug logging")
@click.option(
    "--logtostderr/--no-logtostderr",
    default=True,
    help="Enable/disable logging to stderr",
)
@click.pass_context
def cli(ctx, debug, logtostderr):
    """
    Bluepill agents.
    """

    cfg = AttrDict()

    ctx.obj = cfg

    if logtostderr:
        if debug:
            handler = logbook.StderrHandler(logbook.DEBUG)
            handler.push_application()
        else:
            handler = logbook.StderrHandler(logbook.INFO)
            handler.push_application()

        redirect_logging()


@cli.command("store-init")
@click.option(
    "-s",
    "--store-dsn",
    required=True,
    type=click.Path(dir_okay=False, writable=True),
    help="State store data source name",
)
def store_init(**kwargs):
    """
    Initialize the BluePill store.
    """

    main_store_init(**kwargs)


@cli.command("agent-start")
@click.option("-n", "--ctrl-node", required=True, type=str, help="Controller node name")
@click.option("-p", "--ctrl-port", required=True, type=int, help="Controller port")
@click.option(
    "-s",
    "--store-dsn",
    required=True,
    type=click.Path(exists=True, dir_okay=False, writable=True),
    help="State store data source name",
)
@click.option("-i", "--agentproc-id", required=True, type=int, help="Agent process id")
@click.option(
    "-m", "--num-agents", default=1, help="Number of agents this process simulates"
)
def agent_start(**kwargs):
    """
    Start a BluePill agent process.
    """

    return main_agent(**kwargs)


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
    cli(prog_name="bluepill")

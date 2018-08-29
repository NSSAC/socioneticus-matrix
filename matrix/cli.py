"""
Matrix: CLI Interface
"""

import sys
import configparser

import yaml
import click
from attrdict import AttrDict
from blessings import Terminal
import logbook
from logbook.compat import redirect_logging
from qz7.logbook import ColorLogFormatter, ChannelFilterHandler

from . import parse_config

from .controller import main_controller
from .eventlog import main_eventlog
from .run_rabbitmq import main_rabbitmq_start, main_rabbitmq_stop

log = logbook.Logger(__name__)

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
    Matrix: A distributed ABM platform.
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

@cli.command()
@click.option("-c", "--config",
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Controller configuration file")
@click.option("-n", "--nodename",
              required=True,
              type=str,
              help="Controller nodename")
def controller(config, nodename):
    """
    Start a controller process.
    """

    cfg = parse_config(config, nodename)

    return main_controller(cfg, nodename)

@cli.command()
@click.option("-c", "--config",
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Controller configuration file")
@click.option("-o", "--output",
              required=True,
              type=click.Path(exists=False, dir_okay=False),
              help="Event log file")
def eventlog(config, output):
    """
    Start the event log collecter.
    """

    cfg = parse_config(config)
    return main_eventlog(cfg, output)


@cli.group()
def rabbitmq():
    """
    Start/stop rabbitmq.
    """

@rabbitmq.command("start")
@click.option("-c", "--config",
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Rabbitmq configuration file")
@click.option("-r", "--runtime-dir",
              required=True,
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              help="Rabbitmq runtime directory")
@click.option("-h", "--hostname",
              required=True,
              type=str,
              help="Hostname for rabbitmq to bind to")
def rabbitmq_start(config, runtime_dir, hostname):
    """
    Start the rabbitmq server.
    """

    main_rabbitmq_start(config, runtime_dir, hostname)

@rabbitmq.command("stop")
@click.option("-r", "--runtime-dir",
              required=True,
              type=click.Path(exists=True, file_okay=False, dir_okay=True),
              help="Rabbitmq runtime directory")
def rabbitmq_stop(runtime_dir):
    """
    Stop the rabbitmq server.
    """

    main_rabbitmq_stop(runtime_dir)

@cli.group()
def updateconfig():
    """
    Update controller configuration file.
    """

@updateconfig.command("rabbitmq")
@click.option("-i", "--rabbitmq-config",
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Rabbitmq configuration file")
@click.option("-h", "--hostname",
              required=True,
              type=str,
              help="Hostname where rabbitmq is running")
@click.option("-o", "--controller-config",
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Controller configuration file")
def updateconfig_rabbitmq(controller_config, rabbitmq_config, hostname):
    """
    Add rabbitmq details to controller configuration.
    """

    with open(controller_config, "rt") as fobj:
        ccfg = yaml.load(fobj)

    with open(rabbitmq_config, "rt") as fobj:
        rcfg = configparser.ConfigParser()
        rcfg.read_string("[default]\n" + fobj.read())

    username = rcfg["default"].get("default_user", "guest")
    password = rcfg["default"].get("default_pass", "guest")
    port = int(rcfg["default"].get("listeners.tcp.default", "5672"))

    ccfg["rabbitmq_host"] = hostname
    ccfg["rabbitmq_port"] = port
    ccfg["rabbitmq_username"] = username
    ccfg["rabbitmq_password"] = password

    with open(controller_config, "wt") as fobj:
        yaml.dump(ccfg, fobj, default_flow_style=False)

@updateconfig.command("nodes")
@click.option("-p", "--controller-port",
              required=True,
              type=int,
              help="Port where controller will be running")
@click.option("-n", "--num-agentprocs",
              required=True,
              type=int,
              help="Number of agent processes per node")
@click.option("-m", "--num-storeprocs",
              required=True,
              type=int,
              help="Number of store processes per node")
@click.option("-o", "--controller-config",
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Controller configuration file")
@click.argument("nodes", nargs=-1, type=str)
def updateconfig_nodes(controller_port, num_agentprocs, num_storeprocs, controller_config, nodes):
    """
    Add node specific stuff to controller configuration.
    """

    if not nodes:
        log.error("Need at-least one node name for updating configuration.")
        sys.exit(1)

    with open(controller_config, "rt") as fobj:
        ccfg = yaml.load(fobj)

    ccfg["sim_nodes"] = list(nodes)
    ccfg["controller_port"] = {node: controller_port for node in nodes}
    ccfg["num_agentprocs"] = {node: num_agentprocs for node in nodes}
    ccfg["num_storeprocs"] = {node: num_storeprocs for node in nodes}

    with open(controller_config, "wt") as fobj:
        yaml.dump(ccfg, fobj, default_flow_style=False)

if __name__ == "__main__":
    cli() # pylint: disable=no-value-for-parameter

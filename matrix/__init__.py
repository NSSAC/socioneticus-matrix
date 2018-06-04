"""
Matrix: CLI Interface
"""

import os
import sys
from datetime import datetime, date
from calendar import timegm

import yaml
import click
import logbook
from logbook.compat import redirect_logging
from attrdict import AttrDict

from .controller import main_controller
from .dummyagent import main_dummyagent
from .dummystore import main_dummystoreinit
from .run_rabbitmq import main_run_rabbitmq

log = logbook.Logger(__name__)

def parse_timestamp(dt):
    """
    Parse timestamp from date.
    """

    if not isinstance(dt, date):
        log.error(f"Invalid date '{dt}'")
        sys.exit(1)

    ts = datetime(dt.year, dt.month, dt.day)
    ts = ts.utctimetuple()
    ts = timegm(ts)
    return ts

INTERVAL_SUFFIXES = { "s": 1, "m": 60, "h": 3600, "d": 86400 }
def parse_interval(text):
    """
    Parse interval from text.
    """

    parts = text.split()
    interval = 0
    for part in parts:
        x, suffix = part[:-1], part[-1]
        try:
            interval += int(x) * INTERVAL_SUFFIXES[suffix]
        except (ValueError, KeyError):
            log.error(f"Invalid interval '{text}'")
            sys.exit(1)
    return interval

def parse_config(config_fname, hostname):
    """
    Parse the matrix controller configuration file.
    """

    with open(config_fname) as fobj:
        cfg = yaml.load(fobj)
    cfg = AttrDict(cfg)

    if len(set(cfg.sim_nodes)) != len(cfg.sim_nodes):
        log.error("Duplicate hostnames in node list")
        sys.exit(1)

    for node_name in cfg.sim_nodes:
        if node_name not in cfg.num_agentprocs:
            log.error(f"Number of agents on host {node_name} is not defined")
            sys.exit(1)

    if hostname not in cfg.sim_nodes:
        log.error(f"Hostname not in configured node list")
        sys.exit(1)

    cfg.state_dsn = os.path.expandvars(cfg.state_dsn)
    cfg.start_time = parse_timestamp(cfg.start_time)
    cfg.round_time = parse_interval(cfg.round_time)

    return cfg

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--logtostderr/--no-logtostderr', default=True)
def cli(debug, logtostderr):
    """
    Matrix: A distributed ABM platform.
    """

    if logtostderr:
        if debug:
            logbook.StderrHandler(logbook.DEBUG).push_application()
        else:
            logbook.StderrHandler(logbook.INFO).push_application()
        redirect_logging()

@cli.command()
@click.option("-c", "--config",
              required=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Controller configuration file")
@click.option("-h", "--hostname",
              required=True,
              type=str,
              help="Controller hostname")
def controller(config, hostname):
    """
    Start a controller process.
    """

    cfg = parse_config(config, hostname)

    return main_controller(cfg, hostname)

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
@click.option("-h", "--ctrl-host",
              required=True,
              type=str,
              help="Controller host")
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
              default=1,
              help="Number of agents this process simulates")
def dummyagent(**kwargs):
    """
    Start a dummyagent process.
    """

    return main_dummyagent(**kwargs)

@cli.command("run-rabbitmq")
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
def run_rabbitmq(config, runtime_dir, hostname):
    """
    Start the rabbitmq server.
    """

    main_run_rabbitmq(config, runtime_dir, hostname)

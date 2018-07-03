"""
Matrix: CLI Interface
"""

import os
import sys
from datetime import datetime, date
from calendar import timegm

import yaml
from attrdict import AttrDict
import logbook
from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

INTERVAL_SUFFIXES = { "s": 1, "m": 60, "h": 3600, "d": 86400 }

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

def parse_config(config_fname, nodename=None):
    """
    Parse the matrix controller configuration file.
    """

    with open(config_fname) as fobj:
        cfg = yaml.load(fobj)
    cfg = AttrDict(cfg)

    if len(set(cfg.sim_nodes)) != len(cfg.sim_nodes):
        log.error("Duplicate nodename in node list")
        sys.exit(1)

    for node in cfg.sim_nodes:
        if node not in cfg.num_agentprocs:
            log.error(f"Number of agents on node {node} is not defined")
            sys.exit(1)
        if node not in cfg.controller_port:
            log.error(f"Controller port for node {node} is not defined")
            sys.exit(1)
        if node not in cfg.state_dsn:
            log.error(f"Data store location for node {node} is not defined")
            sys.exit(1)

    if nodename is not None and nodename not in cfg.sim_nodes:
        log.error(f"Nodename not in configured node list")
        sys.exit(1)

    cfg.state_dsn = {k: os.path.expandvars(v) for k, v in cfg.state_dsn.items()}
    cfg.start_time = parse_timestamp(cfg.start_time)
    cfg.round_time = parse_interval(cfg.round_time)

    return cfg

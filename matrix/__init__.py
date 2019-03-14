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

def parse_config(config_fname, nodename=None):
    """
    Parse the matrix controller configuration file.
    """

    with open(config_fname) as fobj:
        cfg = yaml.load(fobj, Loader=yaml.Loader)
    cfg = AttrDict(cfg)

    if len(set(cfg.sim_nodes)) != len(cfg.sim_nodes):
        log.error("Duplicate nodename in node list")
        sys.exit(1)

    for node in cfg.sim_nodes:
        if node not in cfg.num_agentprocs:
            log.error(f"Number of agents on node {node} is not defined")
            sys.exit(1)
        if node not in cfg.num_storeprocs:
            log.error(f"Number of stores on node {node} is not defined")
            sys.exit(1)
        if node not in cfg.controller_port:
            log.error(f"Controller port for node {node} is not defined")
            sys.exit(1)

    if nodename is not None and nodename not in cfg.sim_nodes:
        log.error(f"Nodename not in configured node list")
        sys.exit(1)

    return cfg

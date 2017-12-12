"""
Matrix: CLI Interface
"""

import time
import json
import random
import sqlite3
from os.path import dirname, join

import click
import logbook

from .controller import main_controller
from .simpleagent import main_agent

ENVVAR_ADDRESS = "MATRIX_ADDRESS"
ENVVAR_EVENT_DB = "MATRIX_EVENT_DB"

_curdir = dirname(__file__)
_event_db_schema_fname = join(dirname(_curdir), "event_db_schema.sql")

@click.group()
def cli():
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
def controller(address, event_db, num_agents, num_rounds, start_time_real, period_real):
    """
    Start a controller process.
    """

    # Convert address to tuple format
    # Input format: 127.0.0.1:1600
    address = address.strip().split(":")
    address = (address[0], int(address[1]))

    if start_time_real == 0:
        start_time_real = int(time.time())

    logbook.StderrHandler().push_application()
    main_controller(address, event_db, num_agents, num_rounds, start_time_real, period_real)

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
def simpleagent(address, event_db, agent_id):
    """
    Start a simple agent process.
    """

    # Convert address to tuple format
    # Input format: 127.0.0.1:1600
    address = address.strip().split(":")
    address = (address[0], int(address[1]))

    logbook.StderrHandler().push_application()
    main_agent(address, event_db, agent_id)

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
def initdb(event_db, num_agents, num_repos, start_time_real):
    """
    Initialize a db with num_repos created by num_agents.
    """

    # This is only a temp constraint for the simple agents
    # There is no need for this to be there with the full agents.
    if not num_repos == num_agents:
        print("Need equal number of repos and agents.")

    print("Creating event database ...")
    con = sqlite3.connect(event_db)
    con.executescript(open(_event_db_schema_fname).read())

    # Create the list of agent and repo ids
    # This will be more complex when number of agents
    # and the number of repos are different
    repo_ids = list(range(1, num_repos + 1))
    agent_ids = list(range(1, num_agents + 1))
    repo_owners = agent_ids

    if start_time_real == 0:
        start_time_real = int(time.time())
    time_min = start_time_real - 43200 # Last month
    time_max = start_time_real
    insert_sql = "insert into event values (?,?,?,?,?,?)"
    with con:
        for agent_id, repo_id in zip(repo_owners, repo_ids):
            ltime = 0
            rtime = random.randint(time_min, time_max)
            event_type = "CreateEvent"
            payload = { "ref_type": "repo" }
            payload = json.dumps(payload)

            row = (agent_id, repo_id, ltime, rtime, event_type, payload)
            con.execute(insert_sql, row)

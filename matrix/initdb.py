"""
Matrix: Initialize database.
"""

import time
import json
import random
import sqlite3

def main_initdb(event_db, num_agents, num_repos, start_time_real):
    """
    Initialize a db with num_repos created by num_agents.
    """

    # This is only a temp constraint for the simple agents
    # There is no need for this to be there with the full agents.
    if not num_repos == num_agents:
        print("Need equal number of repos and agents.")
        return

    con = sqlite3.connect(event_db)

    schema_sql = """
    create table
    event (
        agent_id bigint,
        repo_id bigint,
        ltime bigint, -- logical time
        rtime bigint, -- real time, unix timestamps
        event_type text,
        payload text
    );

    create index
    idx1 on event (agent_id, repo_id);
    create index
    idx2 on event (repo_id);
    """
    con.executescript(schema_sql)

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

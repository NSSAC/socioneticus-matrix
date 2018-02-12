"""
Matrix: Initialize database.
"""

import time
import json
import random
import sqlite3

def main_initdb(event_db, num_agents, num_repos, ownership_model, start_time_real):
    """
    Initialize a db with num_repos created by num_agents.
    """

    if not num_repos > 0:
        print("Number of repos needs to be > 0")
        return
    if not num_agents > 0:
        print("Number of agents needs to be > 0")
        return

    con = sqlite3.connect(event_db)

    # Some simple optimizations
    # Use larger block size
    # Use WAL journal mode
    pragma_sql = """
    PRAGMA page_size = 65536;
    PRAGMA journal_mode = WAL;
    """
    con.executescript(pragma_sql)

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
    repo_ids = list(range(1, num_repos + 1))
    agent_ids = list(range(1, num_agents + 1))

    if ownership_model == "independent":
        # For every repo select owner are selected iid
        repo_owners = random.choices(agent_ids, k=num_repos)
    elif ownership_model == "balanced":
        # every agent owns at-least floor(num_repos / num_agent) repos
        repo_owners = agent_ids * (num_repos // num_agents)
        repo_owners += random.sample(agent_ids, num_repos % num_agents)
        assert len(repo_owners) == len(repo_ids)
        random.shuffle(repo_owners)
    elif ownership_model == "preferential":
        # The more repos you own now, the more you will own later
        pool = list(agent_ids)
        repo_owners = []
        for _ in repo_ids:
            agent_id = random.choice(pool)
            repo_owners.append(agent_id)
            pool.append(agent_id)
    else:
        raise ValueError(f"Unknown ownership model '{ownership_model}'")

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

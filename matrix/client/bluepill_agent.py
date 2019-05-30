"""
The BluePill agent process.
"""

import random
import sqlite3

import logbook

from .rpcproxy import RPCProxy

log = logbook.Logger(__name__)

def get_prev_state(con, agent_id):
    """
    Get the last known state of the agent.
    """

    sql = """
        select state
        from event
        where
            agent_id = ?
        order by round_num desc
        limit 1
    """
    cur = con.cursor()
    cur.execute(sql, (agent_id,))
    row = cur.fetchone()
    if not row:
        return None
    return row[0]

def do_something(nodename, agentproc_id, num_agents, con, round_info):
    """
    Generate the updates for the current round.
    """


    updates = []
    for agent_idx in range(num_agents):
        agent_id = f"{nodename}-{agentproc_id}-{agent_idx}"
        prev_state = get_prev_state(con, agentproc_id)
        if prev_state is None:
            prev_state = random.choice(["rock", "paper", "scissors"])

        cur_state = {
            "rock": "paper",
            "paper": "scissors",
            "scissors": "rock"
        }[prev_state]

        sql = "insert into event values (?,?,?)"
        update = (
            "sqlite3", "event_store",
            (agent_id, round_info["cur_round"]),
            (sql, (agent_id, cur_state, round_info["cur_round"]))
        )

        updates.append(update)

    return updates

def main_agent(**kwargs):
    """
    BluePill agent process

    Agent Logic:
        Run num_agents, which cycle betweem states rock, paper, and scissors.
    """

    node = kwargs["ctrl_node"]
    port = kwargs["ctrl_port"]
    store_dsn = kwargs["store_dsn"]
    agentproc_id = kwargs["agentproc_id"]
    num_agents = kwargs["num_agents"]

    with RPCProxy("127.0.0.1", port) as proxy:
        con = sqlite3.connect(store_dsn)

        agentproc_seed = proxy.call("get_agentproc_seed", agentproc_id=agentproc_id)
        random.seed(agentproc_seed)

        while True:
            round_info = proxy.call("can_we_start_yet", agentproc_id=agentproc_id)
            log.info(f"round {round_info['cur_round']} ...")
            if round_info["cur_round"] == -1:
                return

            updates = do_something(node, agentproc_id, num_agents, con, round_info)
            proxy.call("register_events", agentproc_id=agentproc_id, events=updates)

def main_store_init(store_dsn):
    """
    Initialize the bluepill datastore.
    """

    con = sqlite3.connect(store_dsn)

    sql = """
    create table if not exists event (
        agent_id     text,
        state        text,
        round_num    bigint
    )
    """
    con.execute(sql)

    con.close()

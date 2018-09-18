"""
The BluePill agent process.
"""

import random

import logbook

from .rpcproxy import RPCProxy
from .bluepill_store import BluePillStore

log = logbook.Logger(__name__)

def do_something(nodename, agentproc_id, num_agents, state_store, round_info):
    """
    Generate the events for the current round.
    """

    events = []
    for agent_idx in range(num_agents):
        agent_id = f"{nodename}-{agentproc_id}-{agent_idx}"
        prev_state = state_store.get_prev_state(agentproc_id)
        if prev_state is None:
            prev_state = random.choice(["rock", "paper", "scissors"])

        cur_state = {
            "rock": "paper",
            "paper": "scissors",
            "scissors": "rock"
        }[prev_state]

        event = [agent_id, cur_state, round_info["cur_round"]]
        events.append(event)

    return events

def main_agent(**kwargs):
    """
    BluePill agent process

    Agent Logic:
        Run num_agents, which cycle betweem states rock, paper, and scissors.
    """

    node = kwargs["ctrl_node"]
    port = kwargs["ctrl_port"]
    state_dsn = kwargs["state_dsn"]
    agentproc_id = kwargs["agentproc_id"]
    num_agents = kwargs["num_agents"]

    with RPCProxy("127.0.0.1", port) as proxy:
        state_store = BluePillStore(state_dsn)

        agentproc_seed = proxy.call("get_agentproc_seed", agentproc_id=agentproc_id)
        random.seed(agentproc_seed)

        while True:
            round_info = proxy.call("can_we_start_yet", agentproc_id=agentproc_id)
            log.info(f"round {round_info['cur_round']} ...")
            if round_info["cur_round"] == -1:
                return

            events = do_something(node, agentproc_id, num_agents, state_store, round_info)
            proxy.call("register_events", agentproc_id=agentproc_id, events=events)

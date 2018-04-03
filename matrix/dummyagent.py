"""
Matrix: Dummy agent process.

"""

import random

import logbook

from .agent_common import RPCProxy
from .dummystore import get_state_store

_log = logbook.Logger(__name__)

def do_something(agentproc_id, num_agents, state_store, round_info):
    """
    Generate the events for the current round.
    """

    events = []
    for agent_id in range(num_agents):
        prev_state = state_store.get_prev_state(agentproc_id, agentproc_id)
        if prev_state is None:
            prev_state = random.choice(["rock", "paper", "scissors"])

        cur_state = {
            "rock": "paper",
            "paper": "scissors",
            "scissors": "rock"
        }[prev_state]

        cur_time = random.randint(round_info["start_time"], round_info["end_time"] - 1)

        event = [agentproc_id, agent_id, cur_state, cur_time, round_info["cur_round"]]
        events.append(event)

    return events

def main_dummyagent(**kwargs):
    """
    Dummy agent process

    Agent Logic:
        Run num_agents, which cycle betweem states rock, paper, and scissors.
    """

    logbook.StderrHandler().push_application()

    port = kwargs["ctrl_port"]
    state_dsn = kwargs["state_dsn"]
    agentproc_id = kwargs["agentproc_id"]
    num_agents = kwargs["num_agents"]

    with RPCProxy(port) as proxy:
        state_store = get_state_store(state_dsn)

        agentproc_seed = proxy.call("get_agentproc_seed", agentproc_id=agentproc_id)
        random.seed(agentproc_seed)

        while True:
            round_info = proxy.call("can_we_start_yet")
            if round_info["cur_round"] == -1:
                return

            events = do_something(agentproc_id, num_agents, state_store, round_info)
            proxy.call("register_events", events=events)

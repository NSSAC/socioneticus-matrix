"""
Matrix: Threshold agent process.
"""

import random
import sqlite3

import logbook

from .agent_common import RPCProxy

_log = logbook.Logger(__name__)

def get_agent_repos(agent_ids, con):
    """
    Select repos to which agent has contributed.
    """

    cur = con.cursor()
    sql = """
        select repo_id, count(*)
        from event
        where agent_id in (%s)
        group by agent_id, agent_id
        """
    params = map(str, agent_ids)
    params = ",".join(params)
    cur.execute(sql % params)
    return list(cur)

def get_repo_agents(repo_ids, con):
    """
    Select the agents who have contributed to repos.
    """

    cur = con.cursor()
    sql = """
        select agent_id, count(*)
        from event
        where repo_id in (%s)
        group by agent_id, agent_id
        """
    params = map(str, repo_ids)
    params = ",".join(params)
    cur.execute(sql % params)
    return list(cur)

def get_random_repo(con):
    """
    Select a random repo.
    """

    cur = con.cursor()
    sql = """
        select distinct repo_id
        from event
        order by random()
        limit 1
        """
    cur.execute(sql)
    return cur.fetchone()[0]

def make_push_event(agent_id, repo_id, round_num):
    """
    Create a push event.
    """

    return {
        "actor": { "id": agent_id },
        "repo": { "id": repo_id },
        "type": "PushEvent",
        "payload": { },

        # NOTE: This is a extra field not in real data,
        # that we add to the generate events.
        # Adding this to the events is mandatory
        "round_num": round_num
    }

def do_something(agent_id, nl_prob, con_thres, round_num, con):
    """
    Return the set of events that need to be done in this round.
    """

    # Let rs1 be the repos that I have contributed to.
    rs1 = get_agent_repos([agent_id], con)
    rs1 = [r for r, _ in rs1]

    # If I have not contributed to any repos,
    # then select a random repo r, push to it, and return.
    if not rs1:
        r = get_random_repo(con)
        return [make_push_event(agent_id, r, round_num)]

    # Generate a random number p
    # If p < nl_prob (new lookup probability)
    # then select a repo at random from rs1, push to it, and return
    p = random.random()
    if p < nl_prob:
        r = random.choice(rs1)
        return [make_push_event(agent_id, r, round_num)]

    # Let as1 be the set of agents who have contributed to rs1
    as1 = get_repo_agents(rs1, con)
    as1 = [a for a, _ in as1 if a != agent_id]

    # If no one other than me has contributed to repos in rs1,
    # then select a random repo r, push to it, and return.
    if not as1:
        r = get_random_repo(con)
        return [make_push_event(agent_id, r, round_num)]

    # Let rs2_thres be the set of repos which are not in rs1
    # to which at-least con_thres (consideration threshold)
    # number of agents in as1 have contributed.
    rs1_set = set(rs1)
    rs2 = get_agent_repos(as1, con)
    rs2_thres = [r for r, n in rs2 if n >= con_thres and r not in rs1_set]

    # If rs2_thres is empty,
    # then select a random repo r and push to it.
    if not rs2_thres:
        r = get_random_repo(con)
        return [make_push_event(agent_id, r, round_num)]

    r = random.choice(rs2_thres)
    return [make_push_event(agent_id, r, round_num)]

def main_agent_single(address, event_db, agent_id, nl_prob, con_thres):
    """
    Create one threshold agent.
    """

    logbook.StderrHandler().push_application()

    with RPCProxy(address) as proxy:
        _log.notice("Opening event database: {}", event_db)
        con = sqlite3.connect(event_db)

        while True:
            round_num = proxy.call("can_we_start_yet")
            _log.info("Round {}", round_num)

            # if round is -1 we end the simulation
            if round_num == -1:
                return

            events = do_something(agent_id, nl_prob, con_thres, round_num, con)
            proxy.call("register_events", events=events)

def main_agent_multi(address, event_db, agent_ids, nl_prob, con_thres):
    """
    Create multiple threshold agents.

    The run serially in every round.
    They share the same socket and same database handle.
    """

    logbook.StderrHandler().push_application()

    with RPCProxy(address) as proxy:
        _log.notice("Opening event database: {}", event_db)
        con = sqlite3.connect(event_db)

        while True:
            round_num = proxy.call("can_we_start_yet", n_agents=len(agent_ids))
            _log.info("Round {}", round_num)

            # if round is -1 we end the simulation
            if round_num == -1:
                return

            events = []
            for agent_id in agent_ids:
                ret = do_something(agent_id, nl_prob, con_thres, round_num, con)
                events.extend(ret)

            proxy.call("register_events", events=events, n_agents=len(agent_ids))

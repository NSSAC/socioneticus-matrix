"""
Matrix: Simple agent process.
"""

import time
import sqlite3
from random import randint

import logbook

from .agent_common import RPCProxy

_log = logbook.Logger(__name__)


def do_something(agent_id, repo_ids, round_num, con):
    """
    Return the set of events that need to be done in this round.
    """

    cur = con.cursor()

    for repo_id in repo_ids:
        # Find out if I have done anything in the last round.
        sql = """
            select count(*)
            from event
            where
                agent_id = ?
                and repo_id = ?
                and ltime = ?
                and event_type = 'PushEvent'
            """
        row = (agent_id, repo_id, round_num - 1)
        cur.execute(sql, row)

        # If i have done something last round
        # I do nothing this round
        if cur.fetchone()[0] > 0:
            events = []

        # Otherwise, write some code and push to repo
        else:
            # But, coding is hard
            # So, take a nap
            sleep_time = randint(1, 5)
            _log.info("Sleeping for {} seconds", sleep_time)
            time.sleep(sleep_time)

            # TODO - extract method here
            events = [{
                "actor": { "id": agent_id },
                "repo": { "id": repo_id },
                "type": "PushEvent",
                "payload": { },

                # NOTE: This is a extra field not in real data,
                # that we add to the generate events.
                # Adding this to the events is mandatory
                "round_num": round_num,
            }]

        return events


# def do_something(agent_id, repo_ids, con, proxy):
#     """
#     Return the set of events that need to be done in this round.
#     """
#
#     cur = con.cursor()
#
#     round_num = proxy.call("can_we_start_yet")
#     _log.info("Round {}", round_num)
#
#     # if round is -1 we end the simulation
#     if round_num == -1:
#         return False
#
#     for repo_id in repo_ids:
#         # Find out if I have done anything in the last round.
#         sql = """
#             select count(*)
#             from event
#             where
#                 agent_id = ?
#                 and repo_id = ?
#                 and ltime = ?
#                 and event_type = 'PushEvent'
#             """
#         row = (agent_id, repo_id, round_num - 1)
#         cur.execute(sql, row)
#
#         # If i have done something last round
#         # I do nothing this round
#         if cur.fetchone()[0] > 0:
#             events = []
#
#         # Otherwise, write some code and push to repo
#         else:
#             # But, coding is hard
#             # So, take a nap
#             sleep_time = randint(1, 5)
#             _log.info("Sleeping for {} seconds", sleep_time)
#             time.sleep(sleep_time)
#
#             events = [{
#                 "actor": { "id": agent_id },
#                 "repo": { "id": repo_id },
#                 "type": "PushEvent",
#                 "payload": { },
#
#                 # NOTE: This is a extra field not in real data,
#                 # that we add to the generate events.
#                 # Adding this to the events is mandatory
#                 "round_num": round_num,
#             }]
#
#         # Send the events to the controller
#         proxy.call("register_events", events=events)
#
#         return True


def main_agent_single(address, event_db, agent_id):
    """
    Simple agent.

    Agent Logic:
        Find out the repos that I have created (should already be in event db).
        If I have not pushed a commit to the repo in the last round,
        push a new commit to the repo.
    """

    logbook.StderrHandler().push_application()

    with RPCProxy(address) as proxy:
        _log.notice("Opening event database: {}", event_db)
        con = sqlite3.connect(event_db)
        cur = con.cursor()

        # Select the repos which I have created
        sql = """
            select repo_id
            from event
            where
                agent_id = ?
                and event_type = 'CreateEvent'
            """
        cur.execute(sql, (agent_id,))
        repo_ids = [row[0] for row in cur]

        # TODO - extract common method
        while True:
            round_num = proxy.call("can_we_start_yet")
            _log.info("Round {}", round_num)

            # if round is -1 we end the simulation
            if round_num == -1:
                return

            events = do_something(agent_id, repo_ids, round_num, con)
            proxy.call("register_events", events=events)


def main_agent_multi(address, event_db, agent_ids):
    """
        Simple agent.

        Agent Logic:
            Find out the repos that I have created (should already be in event db).
            If I have not pushed a commit to the repo in the last round,
            push a new commit to the repo.
        """
    print("TODO: FixMe Please")

    logbook.StderrHandler().push_application()

    with RPCProxy(address) as proxy:
        _log.notice("Opening event database: {}", event_db)
        con = sqlite3.connect(event_db)
        cur = con.cursor()

        # Select the repos which I have created
        sql = """
                    select repo_id
                    from event
                    where
                        agent_id = ?
                        and event_type = 'CreateEvent'
                    """
        repo_ids = []
        for agent_id in agent_ids:
            cur.execute(sql, (agent_id,))
            repo_ids.append([row[0] for row in cur])

        # TODO - extract common method
        while True:
            round_num = proxy.call("can_we_start_yet", n_agents=len(agent_ids))
            _log.info("Round {}", round_num)

            # if round is -1 we end the simulation
            if round_num == -1:
                return

            events = []
            for agent_id in agent_ids:
                # assume agent_id is 1 based index
                ret = do_something(agent_id, repo_ids[agent_id-1], round_num, con)
                events.extend(ret)

            proxy.call("register_events", events=events)


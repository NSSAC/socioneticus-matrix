"""
Matrix: Simple agent process.
"""

import json
import time
import socket
import sqlite3
from random import randint
from uuid import uuid4

import logbook

_log = logbook.Logger(__name__)

class RPCException(Exception):
    pass

class RPCProxy: # pylint: disable=too-few-public-methods
    """
    RPC Proxy class for calling controller functions.
    """

    def __init__(self, sock):
        self.sock = sock
        self.fobj = sock.makefile(mode="r", encoding="ascii")

    def __del__(self):
        self.fobj.close()

    def call(self, method, **params):
        """
        Call the remote function.
        """

        _log.info("Calling method: {}", method)

        msg = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": method,
            "params": params
        }
        msg = json.dumps(msg) + "\n" # NOTE: The newline is important
        msg = msg.encode("ascii")
        self.sock.sendall(msg)

        ret = self.fobj.readline()
        ret = json.loads(ret)

        if "jsonrpc" not in ret or ret["jsonrpc"] != "2.0":
            raise RPCException("Invalid RPC Response", ret)
        if "error" in ret:
            raise RPCException("RPCException", ret)

        return ret["result"]

def do_something(agent_id, repo_ids, con, proxy):
    """
    Return the set of events that need to be done in this round.
    """

    cur = con.cursor()

    round_num = proxy.call("can_we_start_yet")
    _log.info("Round {}", round_num)

    # if round is -1 we end the simulation
    if round_num == -1:
        return False

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

        # Send the events to the controller
        proxy.call("register_events", events=events)

        return True

def main_agent(address, event_db, agent_id):
    """
    Simple agent.

    Agent Logic:
        Find out the repos that I have created (should already be in event db).
        If I have not pushed a commit to the repo in the last round,
        push a new commit to the repo.
    """

    logbook.StderrHandler().push_application()

    # Convert address to tuple format
    # Input format: 127.0.0.1:1600
    address = address.strip().split(":")
    address = (address[0], int(address[1]))

    address_str = ":".join(map(str, address))
    _log.notice('Connecting to controller at: {0}', address_str)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(address)
        proxy = RPCProxy(sock)

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

        while do_something(agent_id, repo_ids, con, proxy):
            pass

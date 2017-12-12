"""
Matrix: Simple agent process.
"""

import json
import time
import socket
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

        msg = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": method,
            "params": params
        }
        msg = json.dumps(msg) + "\n" # The newline is important
        msg = msg.encode("ascii")
        self.sock.sendall(msg)

        ret = self.fobj.readline()
        ret = json.loads(ret)

        if "jsonrpc" not in ret or ret["jsonrpc"] != "2.0":
            raise RPCException("Invalid RPC Response", ret)
        if "error" in ret:
            raise RPCException("RPCException", ret)

        return ret["result"]


def main_agent(address, _event_db):
    """
    Dummy agent.

    For 10 rounds keep sending the canned push event.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(address)

        proxy = RPCProxy(sock)

        rounds = 10
        for _ in range(rounds):
            proxy.call("can_we_start_yet")

            sleep_time = randint(1, 5)
            print("Sleeping for %d seconds" % sleep_time)
            time.sleep(sleep_time)

            events = [{
                "actor": { "id": 111111 },
                "repo": { "id": 222222 },
                "type": "PushEvent",
                "payload": { "push_id": 333333, }
            }]
            proxy.call("register_events", events=events)

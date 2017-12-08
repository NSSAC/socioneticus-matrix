"""
Matrix: Simple agent process.
"""

import json
import socket

import logbook

_log = logbook.Logger(__name__)

def main_agent(address):
    """
    Dummy agent.

    For 10 rounds keep sending the canned push event.
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)

    with sock.makefile(mode="r", encoding="ascii") as fobj:
        rounds = 10
        for round_ in range(rounds):
            msg = {
                "jsonrpc": "2.0",
                "id": round_ * 10 + 1,
                "method": "has_round_started",
            }
            msg = json.dumps(msg) + "\n" # The newline is mandatory
            sock.sendall(msg.encode("ascii"))

            line = fobj.readline()
            print(line, end="")

            msg = {
                "jsonrpc": "2.0",
                "id": round_ * 10 + 2,
                "method": "register_event",
                "params": {
                    "actor": {
                        "id": 1800460,
                    },
                    "repo": {
                        "id": 111111,
                    },
                    "type": "PushEvent",
                    "created_at": "2015-02-26T00:00:00Z",
                    "payload": {
                        "push_id": 585260388,
                    }
                }
            }
            msg = json.dumps(msg) + "\n" # The newline is mandatory
            sock.sendall(msg.encode("ascii"))

            line = fobj.readline()
            print(line, end="")

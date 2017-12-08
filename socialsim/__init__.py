"""
SocialSim
"""

import json
import socket
from random import randint
from pprint import pprint

import click
import logbook
import gevent
from gevent.server import StreamServer
from jsonrpc import JSONRPCResponseManager, Dispatcher

_log = logbook.Logger(__name__)

def echo(text):
    return text

def register_event(**event):
    pprint(event)

def has_round_started():
    sleep_time = randint(1, 5)
    print("sleep time: %d" % sleep_time)
    gevent.sleep(sleep_time)
    return True

def serve(sock, address):
    """
    Serve this client.
    """

    dispatcher = Dispatcher({
        "echo": echo,
        "register_event": register_event,
        "has_round_started": has_round_started
    })

    address_str = ":".join(map(str, address))
    _log.info("New connection from {0}", address_str)

    # We are expecting json only
    # So encoding ascii shoud be sufficient
    with sock.makefile(mode='r', encoding='ascii') as fobj:
        for line in fobj:
            response = JSONRPCResponseManager.handle(line, dispatcher)
            response = response.json + "\n" # The newline is mandatory
            response = response.encode("ascii")

            sock.sendall(response)

        _log.info("{0} disconnected", address_str)

@click.group()
def cli():
    pass

@cli.command()
def controller():
    """
    Controller process
    """

    server = StreamServer(('127.0.0.1', 16000), serve)
    logbook.StderrHandler().push_application()
    # to make the server use SSL, pass certfile and keyfile arguments to the constructor
    # to start the server asynchronously, use its start() method;
    # we use blocking serve_forever() here because we have no other jobs
    _log.notice('Starting echo server on port 16000')
    server.serve_forever()

@cli.command()
def agent():
    """
    Dummy agent.

    For 10 rounds keep sending the canned push event.
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 16000))

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

"""
Matrix: Controller process
"""

from pprint import pprint
from random import randint

import gevent
import logbook
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
    Serve this new connection.
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

def main_controller(address):
    """
    Controller process
    """

    server = StreamServer(address, serve)
    # to make the server use SSL, pass certfile and keyfile arguments to the constructor
    # to start the server asynchronously, use its start() method;
    # we use blocking serve_forever() here because we have no other jobs
    _log.notice('Starting echo server on port 16000')
    server.serve_forever()

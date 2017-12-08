"""
SocialSim
"""

import click
import logbook
from gevent.server import StreamServer
from jsonrpc import JSONRPCResponseManager, Dispatcher

_log = logbook.Logger(__name__)

def rpc_echo(line):
    return line

def serve(socket, address):
    """
    Serve this client.
    """

    dispatcher = Dispatcher({
        "echo": rpc_echo
    })

    address_str = ":".join(map(str, address))
    _log.info("New connection from {0}", address_str)

    # We are expecting json only
    # So encoding ascii shoud be sufficient
    with socket.makefile(mode='r', encoding='ascii') as fobj:
        for line in fobj:
            response = JSONRPCResponseManager.handle(line, dispatcher)
            response = response.json + "\n"
            response = response.encode("ascii")
            socket.sendall(response)

        _log.info("{0} disconnected", address_str)

@click.command()
def cli():
    """
    Example script.
    """

    server = StreamServer(('127.0.0.1', 16000), serve)
    logbook.StderrHandler().push_application()
    # to make the server use SSL, pass certfile and keyfile arguments to the constructor
    # to start the server asynchronously, use its start() method;
    # we use blocking serve_forever() here because we have no other jobs
    _log.notice('Starting echo server on port 16000')
    server.serve_forever()

"""
Common utilites as needed by agent implementations.
"""

import json
import socket
from uuid import uuid4

import logbook

log = logbook.Logger(__name__)

class RPCException(Exception):
    pass

class RPCProxy: # pylint: disable=too-few-public-methods
    """
    RPC Proxy class for calling controller functions.
    """

    def __init__(self, host, port):
        address = (host, port)

        address_str = ":".join(map(str, address))
        log.notice(f"Connecting to controller at: {address_str}")

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(address)
        self.fobj = self.sock.makefile(mode="r", encoding="ascii")

    def close(self):
        if self.sock is not None:
            self.fobj.close()
            self.sock.close()

            self.fobj = None
            self.sock = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    def call(self, method, **params):
        """
        Call the remote function.
        """

        log.info("Calling method: {}", method)

        msg = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": method,
            "params": params
        }
        if __debug__:
            log.debug("RPC ->\n{}", json.dumps(msg, indent=2, sort_keys=True))

        msg = json.dumps(msg) + "\n" # NOTE: The newline is important
        msg = msg.encode("ascii")
        self.sock.sendall(msg)

        ret = self.fobj.readline()
        ret = json.loads(ret)

        if __debug__:
            log.debug("RPC <-\n{}", json.dumps(ret, indent=2, sort_keys=True))

        if "jsonrpc" not in ret or ret["jsonrpc"] != "2.0":
            raise RPCException("Invalid RPC Response", ret)
        if "error" in ret:
            raise RPCException("RPCException", ret)

        return ret["result"]

"""
Matrix: Controller
"""

import json
import gzip
import random
import importlib

import logbook
from gevent.event import Event
from gevent.server import StreamServer
from jsonrpc import JSONRPCResponseManager, Dispatcher

_log = logbook.Logger(__name__)

class Controller: # pylint: disable=too-many-instance-attributes
    """
    Controller object.
    """

    def __init__(self, config, state_store):
        self.num_agentprocs = config["num_agentprocs"]
        self.num_rounds = config["num_rounds"]
        self.start_time = config["start_time"]
        self.round_time = config["round_time"]
        self.log_fname = config["log_fname"]
        self.controller_seed = config["controller_seed"]

        self.state_store = state_store

        random.seed(self.controller_seed, version=2)
        self.agentproc_seeds = [random.randint(0, 2 ** 32 -1) for _ in range(self.num_agentprocs)]

        # NOTE: This is a hack
        # The server needs the controller, and controller needs server.
        # Thus after creation of both, the server attribute needs to be set
        # from the outside.
        self.server = None

        self.log_fobj = gzip.open(self.log_fname, "at", encoding="utf-8", compresslevel=6)

        self.cur_round = 0
        self.num_waiting = 0
        self.start_event = Event()

        self.dispatcher = Dispatcher({
            "can_we_start_yet": self.can_we_start_yet,
            "register_events": self.register_events,
            "get_agentproc_seed": self.get_agentproc_seed
        })

    def can_we_start_yet(self):
        """
        RPC method, returns when agent process to allowed to begin executing current round.
        """

        self.num_waiting += 1
        if self.num_waiting < self.num_agentprocs:
            _log.info(f"{self.num_waiting}/{self.num_agentprocs} agent processes are waiting.")
            self.start_event.wait()
        else:
            _log.info(f"{self.num_waiting}/{self.num_agentprocs} agent processes are ready.")

            self.state_store.flush()
            self.num_waiting = 0
            self.cur_round += 1

            if self.cur_round == self.num_rounds + 1:
                self.server.stop()
                self.log_fobj.close()
                self.state_store.close()

            self.start_event.set()
            self.start_event.clear()

        if self.cur_round == self.num_rounds + 1:
            return {
                "cur_round": -1,
                "start_time": -1,
                "end_time": -1
            }

        return {
            "cur_round": self.cur_round,
            "start_time": int(self.start_time + self.round_time * (self.cur_round - 1)),
            "end_time": int(self.start_time + self.round_time * self.cur_round)
        }

    def register_events(self, events):
        """
        RPC method, used by agent processes to hand over generated events to the system.

        events: list of events.
        """

        for event in events:
            self.log_fobj.write(json.dumps(event) + "\n")
        self.state_store.handle_events(events)
        return True

    def get_agentproc_seed(self, agentproc_id):
        """
        RPC method, used by agent processes to retrive random seed.

        agentproc_id: ID of the agent process (starts at 1)
        """

        return self.agentproc_seeds[agentproc_id -1]

    def serve(self, sock, address):
        """
        Serve this new connection.
        """

        address_str = ":".join(map(str, address))
        _log.info(f"New connection from {address_str}")

        # We are expecting json only
        # So encoding ascii shoud be sufficient
        with sock.makefile(mode='r', encoding='ascii') as fobj:
            for line in fobj:
                response = JSONRPCResponseManager.handle(line, self.dispatcher)
                response = response.json + "\n" # The newline important
                response = response.encode("ascii")

                sock.sendall(response)

            _log.info(f"{address_str} disconnected")

def main_controller(**kwargs):
    """
    Controller process starting point.
    """

    logbook.StderrHandler().push_application()

    port = kwargs.pop("ctrl_port")
    state_store_module = kwargs.pop("state_store_module")
    state_dsn = kwargs.pop("state_dsn")

    address = ("127.0.0.1", port)
    state_store_module = importlib.import_module(state_store_module)
    state_store = state_store_module.get_state_store(state_dsn)

    address_str = ":".join(map(str, address))
    _log.notice(f"Starting controller on: {address_str}")

    controller = Controller(kwargs, state_store)
    server = StreamServer(address, controller.serve)
    controller.server = server

    server.serve_forever()

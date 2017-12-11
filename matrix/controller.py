"""
Matrix: Controller
"""

from os.path import dirname


import logbook
from gevent.event import Event
from gevent.server import StreamServer
from jsonrpc import JSONRPCResponseManager, Dispatcher

_log = logbook.Logger(__name__)
_curdir = dirname(__file__)

class Controller:
    """
    Controller object.
    """

    def __init__(self, event_db_dsn, num_agents):
        self.event_db_dsn = event_db_dsn
        self.num_agents = num_agents
        #self.event_db = sqlite3.connect(event_db_dsn)

        self.start_event = Event()
        self.num_started = 0
        self.num_finished = 0

        self.event_list = []

        self.dispatcher = Dispatcher({
            "can_we_start_yet": self.can_we_start_yet,
            "register_events": self.register_events,
        })

    def can_we_start_yet(self):
        """
        Method called by agents to start executing current round.
        """

        self.num_started += 1
        if self.num_started < self.num_agents:
            print("Waiting ...")
            self.start_event.wait()
        else:
            print("Clearing ...")
            self.start_event.set()
            self.start_event.clear()
            self.num_started = 0
        print("Send start signal ...")
        return True

    def register_events(self, events):
        """
        Method called by agents to register events.
        """

        print("Received events ...")
        self.event_list.extend(events)
        self.num_finished += 1

        if self.num_finished < self.num_agents:
            return True

        self.num_finished = 0
        print("Flushing %d events" % len(self.event_list))
        self.event_list = []

    def serve(self, sock, address):
        """
        Serve this new connection.
        """

        address_str = ":".join(map(str, address))
        _log.info("New connection from {0}", address_str)

        # We are expecting json only
        # So encoding ascii shoud be sufficient
        with sock.makefile(mode='r', encoding='ascii') as fobj:
            for line in fobj:
                response = JSONRPCResponseManager.handle(line, self.dispatcher)
                response = response.json + "\n" # The newline important
                response = response.encode("ascii")

                sock.sendall(response)

            _log.info("{0} disconnected", address_str)

def main_controller(address, event_db_dsn, num_agents):
    """
    Controller process starting point.
    """

    address_str = ":".join(map(str, address))
    _log.notice('Starting echo server on: {0}', address_str)

    controller = Controller(event_db_dsn, num_agents)

    server = StreamServer(address, controller.serve)
    server.serve_forever()

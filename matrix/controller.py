"""
Matrix: Controller
"""

import json
import time
import random
import sqlite3

import logbook
import gevent
from gevent.event import Event
from gevent.server import StreamServer
from jsonrpc import JSONRPCResponseManager, Dispatcher

_log = logbook.Logger(__name__)

class Controller: # pylint: disable=too-many-instance-attributes
    """
    Controller object.
    """

    def __init__(self, event_db_dsn, num_agents, num_rounds, start_time_real, period_real):
        self.event_db_dsn = event_db_dsn
        self.num_agents = num_agents
        self.num_rounds = num_rounds
        self.start_time_real = start_time_real
        self.period_real = period_real

        # NOTE: This is a hack
        # The server needs the controller, and controller needs server.
        # Thus after creation of both, the server attribute needs to be set
        # from the outside.
        self.server = None

        self.event_db = sqlite3.connect(event_db_dsn)

        self.cur_round = 1
        self.start_event = Event()
        self.num_started = 0
        self.num_finished = 0
        self.num_exited = 0

        self.event_list = []

        self.dispatcher = Dispatcher({
            "can_we_start_yet": self.can_we_start_yet,
            "register_events": self.register_events,
        })

    def can_we_start_yet(self, n_agents=1):
        """
        Method called by agents to start executing current round.

        n_agents: number of agents who are jointly making this request.
        """

        n_agents = int(n_agents)

        if self.cur_round > self.num_rounds:
            self.num_exited += n_agents

            if self.num_exited == self.num_agents:
                self.server.stop()

            _log.info("Sending exit response.")
            return -1

        cur_round = self.cur_round
        self.num_started += n_agents
        if self.num_started < self.num_agents:
            _log.info("Waiting for next round.")
            self.start_event.wait()
        else:
            _log.info("Last agent finished.")
            self.start_event.set()
            self.start_event.clear()
            self.num_started = 0
            self.cur_round += 1
        _log.info("Sending ready signal.")
        return cur_round

    def register_events(self, events, n_agents=1):
        """
        Method called by agents to register events.

        events: list of events generated jointly by the agents.
        n_agents: number of agents who are jointly making this request.
        """

        n_agents = int(n_agents)

        _log.info("Received {0} events.", len(events))
        for event in events:
            ltime = event["round_num"]

            ## Generate the real time
            rtime = self.start_time_real
            rtime += self.period_real * (ltime - 1)
            rtime += random.randint(0, self.period_real)
            event["time"] = rtime

        self.event_list.extend(events)
        self.num_finished += n_agents

        # If there are more agents to finish
        # return quickly
        if self.num_finished < self.num_agents:
            return True

        _log.info("Flushing {0} events to database.", len(self.event_list))
        with self.event_db:
            cur = self.event_db.cursor()
            insert_sql = "insert into event values (?,?,?,?,?,?)"

            for event in self.event_list:
                agent_id = event["actor"]["id"]
                repo_id = event["repo"]["id"]
                event_type = event["type"]
                payload = json.dumps(event["payload"])
                ltime = event["round_num"]
                rtime = event["time"]

                row = (agent_id, repo_id, ltime, rtime, event_type, payload)
                cur.execute(insert_sql, row)

        self.num_finished = 0
        self.event_list = []
        return True

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

def main_controller(address, event_db, num_agents, num_rounds, start_time_real, period_real):
    """
    Controller process starting point.
    """

    logbook.StderrHandler().push_application()

    # Convert address to tuple format
    # Input format: 127.0.0.1:1600
    address = address.strip().split(":")
    address = (address[0], int(address[1]))

    if start_time_real == 0:
        start_time_real = int(time.time())

    address_str = ":".join(map(str, address))
    _log.notice('Starting controller on: {0}', address_str)

    controller = Controller(event_db, num_agents, num_rounds, start_time_real, period_real)
    server = StreamServer(address, controller.serve)
    controller.server = server

    server.serve_forever()

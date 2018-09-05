"""
The BluePill store process.
"""

import sqlite3

import logbook

from .rpcproxy import RPCProxy

log = logbook.Logger(__name__)

def event_sort_key(event):
    agent_id, _, round_num = event
    return round_num, agent_id

class BluePillStore:
    """
    Class for storing stuff.
    """

    def __init__(self, state_dsn):
        self.state_dsn = state_dsn
        self.con = sqlite3.connect(state_dsn)
        self.event_cache = []

    def handle_events(self, events):
        """
        Handle incoming events.
        """

        self.event_cache.extend(events)

    def flush(self):
        """
        Flush out cached events.
        """

        if not self.event_cache:
            return

        log.info("Ordering {} events ...", len(self.event_cache))
        self.event_cache.sort(key=event_sort_key)

        log.info("Applying {} events ...", len(self.event_cache))
        with self.con:
            cur = self.con.cursor()
            sql = "insert into event values (?,?,?)"
            for event in self.event_cache:
                cur.execute(sql, event)

        self.event_cache = []

    def close(self):
        self.flush()
        self.con.close()

    def initialize(self):
        """
        Initialize the storage.
        """

        sql = """
        create table if not exists event (
            agent_id     text,
            state        text,
            round_num    bigint
        )
        """
        self.con.execute(sql)

    def get_prev_state(self, agent_id):
        """
        Get the last known state of the agent.
        """

        sql = """
            select state
            from event
            where
                agent_id = ?
            order by round_num desc
            limit 1
        """
        cur = self.con.cursor()
        cur.execute(sql, (agent_id,))
        row = cur.fetchone()
        if not row:
            return None
        return row[0]

def main_store_init(state_dsn):
    """
    Initialize the datastore.
    """

    store = BluePillStore(state_dsn)
    store.initialize()
    store.close()

def main_store(**kwargs):
    """
    The main state store process.
    """

    port = kwargs["ctrl_port"]
    state_dsn = kwargs["state_dsn"]
    storeproc_id = kwargs["storeproc_id"]

    with RPCProxy("127.0.0.1", port) as proxy:
        state_store = BluePillStore(state_dsn)

        while True:
            ret = proxy.call("get_events", storeproc_id=storeproc_id)
            code = ret["code"]
            if code == "EVENTS":
                events = ret["events"]
                state_store.handle_events(events)
            elif code == "FLUSH":
                state_store.flush()
            elif code == "SIMEND":
                state_store.close()
                break

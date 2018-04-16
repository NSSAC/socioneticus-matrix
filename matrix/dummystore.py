"""
Dummy state store module.

This module serves as an example interface
for building state store modules
that play well with the controller.
"""

import sqlite3

class DummyStore:
    """
    Class for storing stuff.
    """

    def __init__(self, state_dsn):
        self.state_dsn = state_dsn
        self.con = sqlite3.connect(state_dsn)
        self.event_cache = []

    def initialize(self):
        """
        Initialize the storage.
        """

        sql = """
        create table if not exists event (
            agentproc_id bigint,
            agent_id     bigint,
            state        text,
            cur_time     bigint,
            round_num    bigint
        )
        """
        self.con.execute(sql)

    def handle_events(self, events):
        """
        Handle the events coming from controller.
        """

        self.event_cache.extend(events)

    def flush(self):
        """
        Flush out any cached event.
        """

        if not self.event_cache:
            return

        with self.con:
            cur = self.con.cursor()
            sql = "insert into event values (?,?,?,?,?)"
            for event in self.event_cache:
                cur.execute(sql, event)

        self.event_cache = []

    def close(self):
        self.flush()
        self.con.close()

    def get_prev_state(self, agentproc_id, agent_id):
        """
        Get the last known state of the agent.

        This method is used by dummyagent
        and is not called by the controller.
        """

        sql = """
            select state
            from event
            where
                agentproc_id = ?
                and agent_id = ?
            order by round_num desc
            limit 1
        """
        cur = self.con.cursor()
        cur.execute(sql, (agentproc_id, agent_id))
        row = cur.fetchone()
        if not row:
            return None
        return row[0]

def get_state_store(state_dsn):
    return DummyStore(state_dsn)


def main_dummystoreinit(**kwargs):
    """
    Initialize the datastore.
    """

    state_dsn = kwargs.pop("state_dsn")

    store = DummyStore(state_dsn)
    store.initialize()
    store.close()

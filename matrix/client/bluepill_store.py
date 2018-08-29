"""
BluePill state store module.

This module serves as an example interface
for building state store modules
that play well with the controller.
"""

import sqlite3

def event_sort_key(event):
    agent_id, _, cur_time, round_num = event
    return round_num, cur_time, agent_id

class BluePillStore:
    """
    Class for storing stuff.
    """

    def __init__(self, state_dsn):
        self.state_dsn = state_dsn
        self.con = sqlite3.connect(state_dsn)
        self.event_cache = []

    # NOTE: The following three methods must be implemented
    # by any state store objects.
    # These methods will be called by the matrix.

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

        self.event_cache.sort(key=event_sort_key)

        with self.con:
            cur = self.con.cursor()
            sql = "insert into event values (?,?,?,?)"
            for event in self.event_cache:
                cur.execute(sql, event)

        self.event_cache = []

    def close(self):
        self.flush()
        self.con.close()

    # The following methods are used internally
    # by the bluepill store implementation
    # and the are not visible to the matrix.

    def initialize(self):
        """
        Initialize the storage.
        """

        sql = """
        create table if not exists event (
            agent_id     text,
            state        text,
            cur_time     bigint,
            round_num    bigint
        )
        """
        self.con.execute(sql)


    def get_prev_state(self, agent_id):
        """
        Get the last known state of the agent.

        This method is used by bluepill agent
        and is not called by the controller.
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

# NOTE: Implementing this factory function
# is mandatory as this is how the matrix
# creates the state store object.
def get_state_store(state_dsn):
    return BluePillStore(state_dsn)

def main_store_init(**kwargs):
    """
    Initialize the datastore.
    """

    state_dsn = kwargs.pop("state_dsn")

    store = BluePillStore(state_dsn)
    store.initialize()
    store.close()

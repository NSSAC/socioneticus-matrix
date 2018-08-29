"""
JSON Store module.

Writes events to a json text file.
"""

import json

class JSONStore:
    """
    Class for storing stuff.
    """

    def __init__(self, state_dsn):
        self.state_dsn = state_dsn
        self.fobj = open(state_dsn, "wt")
        self.event_cache = []

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

        for event in self.event_cache:
            self.fobj.write(json.dumps(event) + "\n")
        self.fobj.flush()

        self.event_cache = []

    def close(self):
        self.flush()
        self.fobj.close()

def get_state_store(state_dsn):
    return JSONStore(state_dsn)

"""
Null store module.

Discards all events passed to it.
"""

class NullStore:
    """
    Class for not-storing stuff.
    """

    def handle_events(self, events):
        """
        Handle the events coming from controller.
        """

    def flush(self):
        """
        Flush out any cached event.
        """

    def close(self):
        pass

def get_state_store(state_dsn):
    return NullStore()

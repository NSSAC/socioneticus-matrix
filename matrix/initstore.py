"""
Initilize the datastore.
"""

import importlib

def main_initstore(**kwargs):
    """
    Initilize the datastore.
    """

    state_store_module = kwargs.pop("state_store_module")
    state_dsn = kwargs.pop("state_dsn")

    state_store_module = importlib.import_module(state_store_module)
    state_store = state_store_module.get_state_store(state_dsn)

    state_store.initialize()
    state_store.close()

"""
Sqlite3 store process code.

This module connects to the Matrix controller,
and retrieves updates using the get_events RPC call.

Every update is a 4 tuple: (store_type, store_id, order_key, update)

`store_type' defines the type of the store.
For sqlite3_store, store_type should always be "sqlite3"

`store_id' is an string identifier representing a store object.
For sqlite3_store, store_id is a string representing a sqlite3 database.

`order_key' enforces the order in which the updates are applied to the store.
If there are two updates with order_key ok1 and ok2 such that ok1 < ok2
the update with order key ok1 will be applied before the update
with order key ok2.

`update' is a store store specific data structure that actually
contains the update that is to be applied to the store object.
For sqlite3_store, every update is a two tuple (sql, params).
If params is None, it is assumed that the sql statement has no parameters.
"""

import sqlite3

import logbook
from sortedcontainers import SortedList

from .rpcproxy import RPCProxy

log = logbook.Logger(__name__)


def get_first(xs):
    return xs[0]


class Sqlite3Store:
    """
    Sqlite3 data store.

    Attributes:
        store_dsn: Path of the sqlite3 database file
        store_id: ID of the sqlite3 database file
        con: sqlite3 connection object
        update_cache: sorted list of updates
    """

    def __init__(self, store_dsn, store_id):
        self.store_dsn = store_dsn
        self.store_id = store_id

        self.con = sqlite3.connect(store_dsn)
        self.update_cache = SortedList(key=get_first)

    def handle_updates(self, updates):
        """
        Handle incoming updates.

        Args:
            updates: list of update 4 tuples.
        """

        for store_type, store_id, order_key, update in updates:
            if store_type != "sqlite3":
                continue
            if store_id != self.store_id:
                continue

            sql, params = update
            self.update_cache.add((order_key, sql, params))

    def flush(self):
        """
        Apply the cached updates onto the store object.
        """

        if not self.update_cache:
            return

        log.info("Applying {} updates ...", len(self.update_cache))
        with self.con:
            cur = self.con.cursor()
            for _, sql, params in self.update_cache:
                if params is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, tuple(params))

        self.update_cache = SortedList(key=get_first)

    def close(self):
        self.flush()
        self.con.close()


def main_sqlite3_store(store_dsn, store_id, controller_port, storeproc_id):
    """
    Sqlite3 store process starting point.

    Args:
        store_dsn: Path of the sqlite3 database file
        store_id: ID of the sqlite3 database file
        controller_port: Port of the Matrix controller process
        storeproc_id: ID of the current store process
    """

    with RPCProxy("127.0.0.1", controller_port) as proxy:
        state_store = Sqlite3Store(store_dsn, store_id)

        while True:
            ret = proxy.call("get_events", storeproc_id=storeproc_id)
            code = ret["code"]
            if code == "EVENTS":
                updates = ret["events"]
                state_store.handle_updates(updates)
            elif code == "FLUSH":
                state_store.flush()
            elif code == "SIMEND":
                state_store.close()
                break

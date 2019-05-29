"""
Sqlite3 store process code.

Every update is a 3 tuple: (store_type, store_dsn, order_key, update)

`store_type' defines the type of the store.
For sqlite3_store, store_type should always be "sqlite3"

`store_dsn' defines the location of the store object.
For sqlite3_store, store_dsn is the path to the sqlite3 file.

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
    Class for storing stuff.
    """

    def __init__(self, store_dsn):
        self.store_dsn = store_dsn
        self.con = sqlite3.connect(store_dsn)
        self.update_cache = SortedList(key=get_first)

    def handle_updates(self, updates):
        """
        Handle incoming updates.
        """

        for store_type, store_dsn, order_key, update in updates:
            if store_type != "sqlite3":
                continue
            if store_dsn != self.store_dsn:
                continue

            self.update_cache.add((order_key, update))

    def flush(self):
        """
        Apply the cached updates onto the store object.
        """

        if not self.update_cache:
            return

        log.info("Applying {} updates ...", len(self.update_cache))
        with self.con:
            cur = self.con.cursor()
            for _, (sql, params) in self.update_cache:
                if params is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, params)

        self.update_cache = SortedList(key=get_first)

    def close(self):
        self.flush()
        self.con.close()

def main_sqlite3_store(**kwargs):
    """
    The main state store process.
    """

    port = kwargs["ctrl_port"]
    store_dsn = kwargs["store_dsn"]
    storeproc_id = kwargs["storeproc_id"]

    with RPCProxy("127.0.0.1", port) as proxy:
        state_store = Sqlite3Store(store_dsn)

        while True:
            ret = proxy.call("get_updates", storeproc_id=storeproc_id)
            code = ret["code"]
            if code == "UPDATES":
                updates = ret["updates"]
                state_store.handle_updates(updates)
            elif code == "FLUSH":
                state_store.flush()
            elif code == "SIMEND":
                state_store.close()
                break

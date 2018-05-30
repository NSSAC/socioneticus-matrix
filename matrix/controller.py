"""
Matrix: Controller
"""
# pylint: disable=broad-except

import sys
import random
import asyncio
import importlib

import logbook

from matrix.json_rpc import rpc_dispatch

log = logbook.Logger(__name__)

def randint():
    return random.randint(0, 2 ** 32 - 1)

class StateStoreWrapper:
    """
    Wrapper for state store module.

    On any exception from the state store module, log and exit.
    """

    def __init__(self, store):
        self.store = store

    def handle_events(self, events):
        try:
            self.store.handle_events(events)
        except Exception:
            log.exception("StateStoreError: error handling events.")
            sys.exit(1)

    def flush(self):
        try:
            self.store.flush()
        except Exception:
            log.exception("StateStoreError: Error flushing events")
            sys.exit(1)

    def close(self):
        try:
            self.store.close()
        except Exception:
            log.exception("StateStoreError: Error closing event database")
            sys.exit(1)

class Controller: # pylint: disable=too-many-instance-attributes
    """
    Controller object.
    """

    def __init__(self, config, hostname, state_store, loop):
        self.num_controllers = len(config.sim_nodes)
        self.num_agentprocs = config.num_agentprocs[hostname]
        self.num_rounds = config.num_rounds
        self.start_time = config.start_time
        self.round_time = config.round_time

        random.seed(config.root_seed, version=2)
        controller_seeds = [randint() for _ in config.sim_nodes]
        controller_index = config.sim_nodes.index(hostname)
        self.controller_seed = controller_seeds[controller_index]

        random.seed(self.controller_seed, version=2)
        self.agentproc_seeds = [randint() for _ in range(self.num_agentprocs)]

        self.state_store = StateStoreWrapper(state_store)
        self.loop = loop

        self.cur_round = 0
        self.num_waiting = 0
        self.start_event = asyncio.Event()

    def is_sim_end(self):
        return self.cur_round == self.num_rounds + 1

    async def can_we_start_yet(self):
        """
        RPC method, returns when agent process to allowed to begin executing current round.
        """

        self.num_waiting += 1
        if self.num_waiting < self.num_agentprocs:
            log.info(f"{self.num_waiting}/{self.num_agentprocs} agent processes are waiting.")
            await self.start_event.wait()
        else:
            log.info(f"{self.num_waiting}/{self.num_agentprocs} agent processes are ready.")

            self.state_store.flush()
            self.num_waiting = 0
            self.cur_round += 1

            if self.is_sim_end():
                self.loop.stop()
                self.state_store.close()

            self.start_event.set()
            await asyncio.sleep(0)
            self.start_event.clear()

        if self.is_sim_end():
            return { "cur_round": -1, "start_time": -1, "end_time": -1 }

        return {
            "cur_round": self.cur_round,
            "start_time": int(self.start_time + self.round_time * (self.cur_round - 1)),
            "end_time": int(self.start_time + self.round_time * self.cur_round)
        }

    def register_events(self, events):
        """
        RPC method, used by agent processes to hand over generated events to the system.

        events: list of events.
        """

        self.state_store.handle_events(events)
        return True

    def get_agentproc_seed(self, agentproc_id):
        """
        RPC method, used by agent processes to retrive random seed.

        agentproc_id: ID of the agent process (starts at 1)
        """

        return self.agentproc_seeds[agentproc_id -1]


    async def handle_agent_process(self, reader, writer):
        """
        Serve this new agent process.
        """

        method_map = {
            "register_events": self.register_events,
            "get_agentproc_seed": self.get_agentproc_seed
        }

        async_method_map = {
            "can_we_start_yet": self.can_we_start_yet,
        }

        address = writer.get_extra_info('peername')
        address_str = ":".join(map(str, address))
        log.info(f"New connection from {address_str}")

        # We are expecting json only
        # So encoding ascii shoud be sufficient
        while True:
            line = await reader.readline()
            if not line:
                break
            line = line.decode("ascii")

            response = await rpc_dispatch(method_map, async_method_map, line)
            if response is None:
                continue

            response = response + "\n" # The newline important
            response = response.encode("ascii")
            writer.write(response)
            await writer.drain()

        log.info(f"{address_str} disconnected")

async def do_startup(config, hostname, state_store, loop):
    """
    Start the matrix controller.
    """

    port = config.controller_port

    controller = Controller(config, hostname, state_store, loop)

    server = await asyncio.start_server(controller.handle_agent_process, "127.0.0.1", port)

    return server

async def do_cleanup(server):
    """
    Cleanup the running processes.
    """

    server.close()
    await server.wait_closed()

def main_controller(config, hostname):
    """
    Controller process starting point.
    """

    logbook.StderrHandler().push_application()

    state_store_module = config.state_store_module
    state_dsn = config.state_dsn

    try:
        state_store_module = importlib.import_module(state_store_module)
    except ImportError as e:
        log.error(f"Failed to import state store module '{state_store_module}'\n{e}")
        sys.exit(1)

    try:
        state_store = state_store_module.get_state_store(state_dsn)
    except Exception: # pylint: disable=broad-except
        log.exception("StateStoreError: Error obtaining state store object")
        sys.exit(1)

    loop = asyncio.get_event_loop()

    server = loop.run_until_complete(do_startup(config, hostname, state_store, loop))
    loop.run_forever()
    loop.run_until_complete(do_cleanup(server))

    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))
    loop.close()

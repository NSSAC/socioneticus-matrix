"""
Matrix: Controller
"""
# pylint: disable=broad-except

import sys
import json
import gzip
import random
import asyncio
import importlib

import logbook

log = logbook.Logger(__name__)

def rpc_parse(line):
    """
    Parse a jsonrpc request and check correctness.
    """

    try:
        request = json.loads(line)
        assert isinstance(request, dict)
    except (ValueError, AssertionError):
        return None, "Malformatted RPC request"

    try:
        assert request["jsonrpc"] == "2.0"
    except (KeyError, AssertionError):
        return request, "Incompatible RPC version: jsonrpc != 2.0"

    try:
        assert isinstance(request["method"], str)
    except (KeyError, AssertionError):
        return request, "Method name is not a string"

    if "params" in request:
        if not isinstance(request["params"], (list, dict)):
            return request, "Parameters can only be of type object or array"

    return request, None

def rpc_error(message, request=None):
    """
    Generate the rpc error message.
    """

    response = {
        "jsonrpc": "2.0",
        "error": str(message)
    }

    if request is not None and "id" in request:
        response["id"] = request["id"]

    return json.dumps(response)

def rpc_response(result, request):
    """
    Generate the response message.
    """

    if "id" not in request:
        return None

    response = {
        "jsonrpc": "2.0",
        "result": result,
        "id": request["id"]
    }

    return json.dumps(response)

class Controller: # pylint: disable=too-many-instance-attributes
    """
    Controller object.
    """

    def __init__(self, config, state_store, loop):
        self.num_agentprocs = config["num_agentprocs"]
        self.num_rounds = config["num_rounds"]
        self.start_time = config["start_time"]
        self.round_time = config["round_time"]
        self.log_fname = config["log_fname"]
        self.controller_seed = config["controller_seed"]

        self.state_store = state_store
        self.loop = loop

        random.seed(self.controller_seed, version=2)
        self.agentproc_seeds = [random.randint(0, 2 ** 32 -1) for _ in range(self.num_agentprocs)]

        self.log_fobj = gzip.open(self.log_fname, "at", encoding="utf-8", compresslevel=6)

        self.cur_round = 0
        self.num_waiting = 0
        self.start_event = asyncio.Event()

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

            try:
                self.state_store.flush()
            except Exception:
                log.exception("StateStoreError: Error flushing events")
                sys.exit(1)

            self.num_waiting = 0
            self.cur_round += 1

            if self.cur_round == self.num_rounds + 1:
                self.loop.stop()
                self.log_fobj.close()

                try:
                    self.state_store.close()
                except Exception:
                    log.exception("StateStoreError: Error closing event database")
                    sys.exit(1)

            self.start_event.set()
            await asyncio.sleep(0)
            self.start_event.clear()

        if self.cur_round == self.num_rounds + 1:
            return {
                "cur_round": -1,
                "start_time": -1,
                "end_time": -1
            }

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

        for event in events:
            self.log_fobj.write(json.dumps(event) + "\n")

        try:
            self.state_store.handle_events(events)
        except Exception:
            log.exception("StateStoreError: error handling events.")
            sys.exit(1)

        return True

    def get_agentproc_seed(self, agentproc_id):
        """
        RPC method, used by agent processes to retrive random seed.

        agentproc_id: ID of the agent process (starts at 1)
        """

        return self.agentproc_seeds[agentproc_id -1]

    async def rpc_dispatch(self, line):
        """
        Dispatch the proper method.
        """

        method_map = {
            "register_events": self.register_events,
            "get_agentproc_seed": self.get_agentproc_seed
        }

        async_method_map = {
            "can_we_start_yet": self.can_we_start_yet,
        }

        request, error = rpc_parse(line)
        if error is not None:
            return rpc_error(error, request)

        method = request["method"]
        if method not in method_map and method not in async_method_map:
            return rpc_error("Unknown RPC method", request)

        if "params" in request:
            params = request["params"]
            if isinstance(params, list):
                args, kwargs = params, {}
            else: # isinstance(params, dict)
                args, kwargs = [], params

        if method in method_map:
            try:
                response = method_map[method](*args, **kwargs)
            except Exception as e:
                return rpc_error(e, request)
        else: # method in async_method_map:
            try:
                response = await async_method_map[method](*args, **kwargs)
            except Exception as e:
                return rpc_error(e, request)

        return rpc_response(response, request)

    async def handle_agent_process(self, reader, writer):
        """
        Serve this new agent process.
        """

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

            response = await self.rpc_dispatch(line)
            if response is None:
                continue

            response = response + "\n" # The newline important
            response = response.encode("ascii")
            writer.write(response)
            await writer.drain()

        log.info(f"{address_str} disconnected")

def main_controller(**kwargs):
    """
    Controller process starting point.
    """

    logbook.StderrHandler().push_application()

    port = kwargs.pop("ctrl_port")
    state_store_module = kwargs.pop("state_store_module")
    state_dsn = kwargs.pop("state_dsn")

    address = ("127.0.0.1", port)
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

    address_str = ":".join(map(str, address))
    log.notice(f"Starting controller on: {address_str}")

    loop = asyncio.get_event_loop()
    controller = Controller(kwargs, state_store, loop)
    srv_coro = asyncio.start_server(controller.handle_agent_process, *address, loop=loop)
    server = loop.run_until_complete(srv_coro)

    loop.run_forever()

    server.close()
    loop.run_until_complete(server.wait_closed())

    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))
    loop.close()

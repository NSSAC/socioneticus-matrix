"""
Matrix: Controller
"""
# pylint: disable=broad-except

import sys
import json
import random
import asyncio
import importlib
import signal
from functools import partial

import logbook
import aioamqp

from .json_rpc import rpc_dispatch, rpc_request

log = logbook.Logger(__name__)

BUFSIZE = 16 * 2 ** 30
RECEIVED_TERM = False

def randint():
    return random.randint(0, 2 ** 32 - 1)

def term_handler(signame, loop):
    """
    Signal handler for term signals.
    """

    global RECEIVED_TERM

    log.info(f"Received {signame}")

    if not RECEIVED_TERM:
        loop.stop()
        RECEIVED_TERM = True
    else:
        log.info("Already stopping; ignoring signal ...")

class StateStoreWrapper:
    """
    Wrapper for state store module.

    On any exception from the state store module, log and exit.
    """

    def __init__(self, store):
        self.store = store
        self.is_closed = False

    def __del__(self):
        self.close()

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
        """
        Close the underlying state store.
        """

        if self.is_closed:
            return
        self.is_closed = True

        try:
            self.store.close()
        except Exception:
            log.exception("StateStoreError: Error closing event database")
            sys.exit(1)

class Controller: # pylint: disable=too-many-instance-attributes
    """
    Controller object.
    """

    def __init__(self, config, nodename, state_store, loop):
        self.nodename = nodename

        self.num_controllers = len(config.sim_nodes)
        self.num_agentprocs = config.num_agentprocs[nodename]
        self.num_rounds = config.num_rounds
        self.start_time = config.start_time
        self.round_time = config.round_time

        # Generate the seed for the current controller
        random.seed(config.root_seed, version=2)
        controller_seeds = [randint() for _ in config.sim_nodes]
        controller_index = config.sim_nodes.index(nodename)
        self.controller_seed = controller_seeds[controller_index]

        # Generate seeds for agent processes
        random.seed(self.controller_seed, version=2)
        self.agentproc_seeds = [randint() for _ in range(self.num_agentprocs)]

        self.state_store = StateStoreWrapper(state_store)

        # The event loop
        self.loop = loop

        # The following four attributes
        # are the only mutable part of the class.
        self.cur_round = 0
        self.num_ap_waiting = 0
        self.num_cp_finished = 0
        self.start_event = asyncio.Event()

        # These attributes will be populated later
        # These should be bound to async functions
        # That can be used to send messages to the backend
        self.share_events = None
        self.send_controller_finished = None

    async def get_agentproc_seed(self, agentproc_id):
        """
        RPC method: Used by agent processes to retrive random seed.

        agentproc_id: ID of the agent process [1, num_agentprocs on this node]
        """

        return self.agentproc_seeds[agentproc_id -1]

    async def can_we_start_yet(self):
        """
        RPC method: Used by agent process to wait for start of current round.
        """

        self.num_ap_waiting += 1
        log.info(f"{self.num_ap_waiting}/{self.num_agentprocs} agent processes are waiting ...")
        if self.num_ap_waiting == self.num_agentprocs:
            await self.send_controller_finished(self.nodename)
        await self.start_event.wait()

        if self.is_sim_end():
            return { "cur_round": -1, "start_time": -1, "end_time": -1 }

        return {
            "cur_round": self.cur_round,
            "start_time": int(self.start_time + self.round_time * (self.cur_round - 1)),
            "end_time": int(self.start_time + self.round_time * self.cur_round)
        }

    async def register_events(self, events):
        """
        RPC method: Used by agent processes to hand over generated events.

        events: list of events.
        """

        await self.share_events(self.nodename, events)
        return True

    async def store_events(self, nodename, events):
        """
        RPC method: Used by other controllers to hand over events from their local node.

        events: list of events.
        """

        self.state_store.handle_events(events)

    async def controller_finished(self, nodename):
        """
        RPC method: Used by other controllers to signal they have finished.

        nodename: nodename of the finished controller.
        """

        self.num_cp_finished += 1
        log.info(f"{self.num_cp_finished}/{self.num_controllers} controllers are waiting ...")

        if self.num_cp_finished != self.num_controllers:
            return

        # Flush the store
        # and reset the state
        self.state_store.flush()
        self.cur_round += 1
        self.num_ap_waiting = 0
        self.num_cp_finished = 0

        if self.is_sim_end():
            log.info("Simulation completed!")
        else:
            log.info(f"Round {self.cur_round}/{self.num_rounds} starting ...")

        # Wake up all agent processes
        self.start_event.set()
        await asyncio.sleep(0)
        self.start_event.clear()

        # If simulation has ended
        # stop the event loop
        # and flush the state store
        if self.is_sim_end():
            self.loop.stop()

    def is_sim_end(self):
        """
        Has the simulation ended.
        """

        return self.cur_round == self.num_rounds + 1

    async def dispatch(self, message):
        """
        Dispatch a rpc method call.
        """

        method_map = {
            # RPC methods used by agent processes
            "get_agentproc_seed": self.get_agentproc_seed,
            "can_we_start_yet": self.can_we_start_yet,
            "register_events": self.register_events,

            # RPC methods used by other contollers
            "store_events": self.store_events,
            "controller_finished": self.controller_finished
        }

        response = await rpc_dispatch(method_map, message)
        return response

async def handle_agent_process(controller, reader, writer):
    """
    Callback handler, for new tcp connections from agents.

    reader: async stream reader object
    writer: async stream writer object
    """

    address = writer.get_extra_info('peername')
    address_str = ":".join(map(str, address))
    log.info(f"New connection from {address_str}")

    while True:
        line = await reader.readline()
        if not line:
            break
        line = line.decode("ascii")

        response = await controller.dispatch(line)
        assert response is not None

        response = json.dumps(response) + "\n" # NOTE: The newline important
        response = response.encode("ascii")

        writer.write(response)
        await writer.drain()

    log.info(f"{address_str} disconnected")

async def handle_broker_message(controller, channel, body, envelope, _properties):
    """
    Callback handler, for messages from amqp broker.

    controller  : the controller object.
    channel     : channel from which message was received
    body        : bytes object body of the message.
    envelope    : envelope
    _properties : properties
    """

    body = body.decode("utf-8")

    response = await controller.dispatch(body)
    assert response is None

    # Send ack back to server
    await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


async def share_events(nodename, events, chan, exchange_name):
    """
    Share the events with other controllers.
    """

    request = rpc_request("store_events", id=False,
                          nodename=nodename,
                          events=events)
    request = json.dumps(request)
    request = request.encode("utf-8")

    await chan.basic_publish(request,
                             exchange_name=exchange_name,
                             routing_key=nodename)

async def send_controller_finished(nodename, chan, exchange_name):
    """
    Signal other controllers that this one has finished.
    """

    request = rpc_request("controller_finished", id=False,
                          nodename=nodename)
    request = json.dumps(request)
    request = request.encode("utf-8")

    await chan.basic_publish(request,
                             exchange_name=exchange_name,
                             routing_key=nodename)

async def make_amqp_channel(config):
    """
    Create an async amqp channel.
    """

    transport, protocol = await aioamqp.connect(
        host=config.rabbitmq_host,
        port=config.rabbitmq_port,
        login=config.rabbitmq_username,
        password=config.rabbitmq_password)
    channel = await protocol.channel()
    return transport, protocol, channel

async def make_receiver_queue(callback, channel, config, nodename):
    """
    Make the receiver queue and bind to topics.
    """

    queue = await channel.queue_declare("", exclusive=True)
    queue_name = queue["queue"]

    await channel.queue_bind(exchange_name=config.event_exchange,
                             queue_name=queue_name,
                             routing_key=nodename)

    await channel.basic_consume(callback, queue_name=queue_name)
    return queue

async def do_startup(config, nodename, state_store, loop):
    """
    Start the matrix controller.
    """

    port = config.controller_port[nodename]

    log.info("Creating AMQP send channel ...")
    snd_trans, snd_proto, snd_chan = await make_amqp_channel(config)

    log.info("Creating AMQP receive channel ...")
    rcv_trans, rcv_proto, rcv_chan = await make_amqp_channel(config)

    log.info("Setting up event exchange ...")
    await snd_chan.exchange_declare(exchange_name=config.event_exchange, type_name='fanout')

    controller = Controller(config, nodename, state_store, loop)
    controller.share_events = partial(share_events,
                                      chan=snd_chan,
                                      exchange_name=config.event_exchange)
    controller.send_controller_finished = partial(send_controller_finished,
                                                  chan=snd_chan,
                                                  exchange_name=config.event_exchange)

    for signame in ["SIGINT", "SIGTERM", "SIGHUP"]:
        signum = getattr(signal, signame)
        handler = partial(term_handler, signame=signame, loop=loop)
        loop.add_signal_handler(signum, handler)

    log.info("Setting up AMQP receiver ...")
    bm_callback = partial(handle_broker_message, controller)
    await make_receiver_queue(bm_callback, rcv_chan, config, nodename)

    log.info(f"Starting local TCP server at 127.0.0.1:{port} ..." )
    tcon_callback = partial(handle_agent_process, controller)
    server = await asyncio.start_server(tcon_callback, "127.0.0.1", port, limit=BUFSIZE)

    return server, snd_trans, snd_proto, rcv_trans, rcv_proto

async def do_cleanup(server, snd_trans, snd_proto, rcv_trans, rcv_proto):
    """
    Cleanup the running processes.
    """

    log.info("Closing local TCP server ..")
    server.close()
    await server.wait_closed()

    log.info("Closing AMQP send and receive channels ...")
    await snd_proto.close()
    await rcv_proto.close()
    snd_trans.close()
    rcv_trans.close()

def main_controller(config, nodename):
    """
    Controller process starting point.
    """

    state_store_module = config.state_store_module
    state_dsn = config.state_dsn[nodename]

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

    resources = loop.run_until_complete(do_startup(config, nodename, state_store, loop))
    loop.run_forever()

    log.info("Running cleaunup tasks ...")
    loop.run_until_complete(do_cleanup(*resources))

    pending_tasks = asyncio.Task.all_tasks()
    for task in pending_tasks:
        if not task.done():
            task.cancel()
    loop.close()

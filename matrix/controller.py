"""
Matrix: Controller
"""
# pylint: disable=broad-except

import time
import json
import random
import asyncio
import signal
from functools import partial

import logbook
import aioamqp
from more_itertools import sliced

from .json_rpc import rpc_dispatch, rpc_request

log = logbook.Logger(__name__)

BUFSIZE = 16 * 2 ** 30
RECEIVED_TERM = False
EVENT_CHUNKSIZE = 1000

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

class Controller: # pylint: disable=too-many-instance-attributes
    """
    Controller object.
    """

    def __init__(self, config, nodename, loop):
        self.nodename = nodename

        self.num_controllers = len(config.sim_nodes)
        self.num_agentprocs = config.num_agentprocs[nodename]
        self.num_storeprocs = config.num_storeprocs[nodename]
        self.num_rounds = config.num_rounds

        # Generate the seed for the current controller
        random.seed(config.root_seed, version=2)
        controller_seeds = [randint() for _ in config.sim_nodes]
        controller_index = config.sim_nodes.index(nodename)
        self.controller_seed = controller_seeds[controller_index]

        # Generate seeds for agent processes
        random.seed(self.controller_seed, version=2)
        self.agentproc_seeds = [randint() for _ in range(self.num_agentprocs)]

        # The event loop
        self.loop = loop

        # The following four attributes
        # are the only mutable part of the class.
        self.cur_round = 0
        self.num_ap_waiting = 0
        self.num_cp_finished = 0

        # Local and All events queue
        self.ev_queue_local = asyncio.Queue(maxsize=0, loop=loop)
        self.ev_queue_all = []
        for _ in range(self.num_storeprocs):
            self.ev_queue_all.append(asyncio.Queue(maxsize=0, loop=loop))

        # Agent process queues
        self.ap_queue = asyncio.Queue(maxsize=self.num_agentprocs, loop=loop)

        # This attribute will be populated later
        # These should be bound to async functions
        # That can be used to send messages to the backend
        self.send_message = None

    async def get_agentproc_seed(self, agentproc_id):
        """
        RPC method: Used by agent processes to retrive random seed.

        agentproc_id: index of the agent process (starts at 0)
        """

        assert 0 <= agentproc_id < self.num_agentprocs

        return self.agentproc_seeds[agentproc_id]

    async def can_we_start_yet(self, agentproc_id):
        """
        RPC method: Used by agent process to wait for start of current round.

        agentproc_id: index of the agent process (starts at 0)
        """

        assert 0 <= agentproc_id < self.num_agentprocs

        self.num_ap_waiting += 1
        log.info(f"{self.num_ap_waiting}/{self.num_agentprocs} agent processes are waiting ...")
        if self.num_ap_waiting == self.num_agentprocs:
            # Wait for local events queue to be empty
            await self.ev_queue_local.join()

            # Signal the other controllers that we are done
            await self.send_message("controller_finished", nodename=self.nodename)

        await self.ap_queue.get()
        self.ap_queue.task_done()

        if self.is_sim_end():
            return { "cur_round": -1 }

        return { "cur_round": self.cur_round }

    async def register_events(self, agentproc_id, events):
        """
        RPC method: Used by agent processes to hand over generated events.

        agentproc_id: index of the agent process (starts at 0)
        events: list of events
        """

        assert 0 <= agentproc_id < self.num_agentprocs

        for event_chunk in sliced(events, EVENT_CHUNKSIZE):
            await self.ev_queue_local.put(event_chunk)
        return True

    async def get_events(self, storeproc_id):
        """
        RPC method: Used by store processes to retrieve generated events.

        storeproc_id: index of the store process (starts at 0)
        """

        assert 0 <= storeproc_id < self.num_storeprocs

        code, events = await self.ev_queue_all[storeproc_id].get()
        self.ev_queue_all[storeproc_id].task_done()
        return {"code": code, "events": events}

    async def store_events(self, nodename, events):
        """
        RPC method: Used by other controllers to hand over events from their local node.

        nodename: name of the soruce controller
        events: list of events
        """

        for i in range(self.num_storeprocs):
            await self.ev_queue_all[i].put(("EVENTS", events))

    async def controller_finished(self, nodename):
        """
        RPC method: Used by other controllers to signal they have finished.

        nodename: nodename of the finished controller.
        """

        self.num_cp_finished += 1
        log.info(f"{self.num_cp_finished}/{self.num_controllers} controllers are waiting ...")

        if self.num_cp_finished != self.num_controllers:
            return

        # Reset the state
        self.cur_round += 1
        self.num_ap_waiting = 0
        self.num_cp_finished = 0

        # Add flush signal for the event queues
        for i in range(self.num_storeprocs):
            await self.ev_queue_all[i].put(("FLUSH", None))

        # Wait for all events queue to be empty
        for i in range(self.num_storeprocs):
            await self.ev_queue_all[i].join()

        if self.is_sim_end():
            log.info("Simulation completed!")
        else:
            log.info(f"Round {self.cur_round}/{self.num_rounds} starting ...")

        # Wake up the agent processes
        for _ in range(self.num_agentprocs):
            await self.ap_queue.put(None)

        if self.is_sim_end():
            # Send terminal to local event queue
            # to stop the send_events loop
            await self.ev_queue_local.put(None)

            # Add simend signal for the event queues
            for i in range(self.num_storeprocs):
                await self.ev_queue_all[i].put(("SIMEND", None))

            # Wait for local events queue to be empty
            await self.ev_queue_local.join()

            # Wait for all events queue to be empty
            for i in range(self.num_storeprocs):
                await self.ev_queue_all[i].join()

            # Stop the main loop
            self.loop.stop()

    async def share_events_loop(self):
        """
        Keep sharing events put in local events queue with rest of the controllers.
        """

        while True:
            events = await self.ev_queue_local.get()
            if events is None:
                self.ev_queue_local.task_done()
                break

            await self.send_message("store_events", nodename=self.nodename, events=events)
            self.ev_queue_local.task_done()

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

            # RPC methods used by store processes
            "get_events": self.get_events,

            # RPC methods used by other contollers
            "store_events": self.store_events,
            "controller_finished": self.controller_finished
        }

        response = await rpc_dispatch(method_map, message)
        return response

async def handle_client_process(controller, reader, writer):
    """
    Callback handler, for new tcp connections from agents.

    controller: the controller object
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

    controller  : the controller object
    channel     : channel from which message was received
    body        : bytes object body of the message
    envelope    : envelope
    _properties : properties
    """

    body = body.decode("utf-8")

    response = await controller.dispatch(body)
    assert response is None

    # Send ack back to server
    await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

async def send_broker_message(chan, exchange_name, method, **kwargs):
    """
    Send a message to the broker to be shared with all controllers.
    """

    request = rpc_request(method, id=False, **kwargs)
    request = json.dumps(request)
    request = request.encode("utf-8")

    await chan.basic_publish(request,
                             exchange_name=exchange_name,
                             routing_key="*")

async def make_amqp_channel(config):
    """
    Create an async amqp channel.
    """

    timeout = 60
    start = time.time()
    while True:
        try:
            transport, protocol = await aioamqp.connect(
                host=config.rabbitmq_host,
                port=config.rabbitmq_port,
                login=config.rabbitmq_username,
                password=config.rabbitmq_password)
            break
        except OSError as e:
            log.info("Failed to connect to RabbitMQ: {}", e)

            since = time.time() - start
            if since > timeout:
                raise RuntimeError("Failed to connect to RabbitMQ")
            else:
                time.sleep(5)

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

async def do_startup(config, nodename, loop):
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

    controller = Controller(config, nodename, loop)
    controller.send_message = partial(send_broker_message,
                                      snd_chan,
                                      config.event_exchange)

    for signame in ["SIGINT", "SIGTERM", "SIGHUP"]:
        signum = getattr(signal, signame)
        handler = partial(term_handler, signame=signame, loop=loop)
        loop.add_signal_handler(signum, handler)

    log.info("Starting event share loop ...")
    asyncio.ensure_future(controller.share_events_loop())

    log.info("Setting up AMQP receiver ...")
    bm_callback = partial(handle_broker_message, controller)
    await make_receiver_queue(bm_callback, rcv_chan, config, nodename)

    log.info(f"Starting local TCP server at 127.0.0.1:{port} ..." )
    tcon_callback = partial(handle_client_process, controller)
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

    loop = asyncio.get_event_loop()

    resources = loop.run_until_complete(do_startup(config, nodename, loop))
    loop.run_forever()

    log.info("Running cleaunup tasks ...")
    loop.run_until_complete(do_cleanup(*resources))

    pending_tasks = asyncio.Task.all_tasks()
    for task in pending_tasks:
        if not task.done():
            task.cancel()
    loop.close()

"""
Matrix: Event log writer.
"""

import gzip
import json
import asyncio
import signal
from functools import partial

import logbook

from .controller import (
    make_amqp_channel,
    make_receiver_queue,
    term_handler,
    handle_broker_message,
)
from .json_rpc import rpc_dispatch

log = logbook.Logger(__name__)


class EventLogger:
    """
    Log events.
    """

    def __init__(self, config, output_fname, event_loop):
        self.num_controllers = len(config.sim_nodes)
        self.num_rounds = config.num_rounds
        self.event_loop = event_loop

        self.event_fobj = gzip.open(output_fname, "wt")

        self.cur_round = 0
        self.num_cp_finished = 0

    async def store_events(self, nodename, events):
        """
        RPC method: Used by other controllers to hand over events from their local node.

        events: list of events.
        """

        for event in events:
            event = json.dumps(event)
            self.event_fobj.write(event + "\n")

    async def controller_finished(self, nodename):
        """
        RPC method: Used by other controllers to signal they have finished.

        nodename: nodename of the finished controller.
        """

        self.num_cp_finished += 1
        log.info(
            f"{self.num_cp_finished}/{self.num_controllers} controllers are waiting ..."
        )

        if self.num_cp_finished != self.num_controllers:
            return

        self.cur_round += 1
        self.num_cp_finished = 0

        if self.is_sim_end():
            log.info("Simulation completed!")
        else:
            log.info(f"Round {self.cur_round}/{self.num_rounds} starting ...")

        if self.is_sim_end():
            # self.event_fobj.close()
            self.event_loop.stop()

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
            # RPC methods used by other contollers
            "store_events": self.store_events,
            "controller_finished": self.controller_finished,
        }

        response = await rpc_dispatch(method_map, message)
        return response


async def do_startup(config, output_fname, event_loop):
    """
    Start the event logger.
    """

    log.info("Creating AMQP receive channel ...")
    rcv_trans, rcv_proto, rcv_chan = await make_amqp_channel(config)

    log.info("Setting up event exchange ...")
    await rcv_chan.exchange_declare(
        exchange_name=config.event_exchange, type_name="fanout"
    )

    logger = EventLogger(config, output_fname, event_loop)

    for signame in ["SIGINT", "SIGTERM", "SIGHUP"]:
        signum = getattr(signal, signame)
        handler = partial(term_handler, signame=signame, loop=event_loop)
        event_loop.add_signal_handler(signum, handler)

    log.info("Setting up AMQP receiver ...")
    bm_callback = partial(handle_broker_message, logger)
    await make_receiver_queue(bm_callback, rcv_chan, config, "")

    return rcv_trans, rcv_proto


async def do_cleanup(rcv_trans, rcv_proto):
    """
    Cleanup the running processes.
    """

    log.info("Closing AMQP receive channel ...")
    await rcv_proto.close()
    rcv_trans.close()


def main_eventlog(config, output_fname):
    """
    Event logger starting point.
    """

    loop = asyncio.get_event_loop()

    resources = loop.run_until_complete(do_startup(config, output_fname, loop))
    loop.run_forever()

    log.info("Running cleaunup tasks ...")
    loop.run_until_complete(do_cleanup(*resources))

    pending_tasks = asyncio.Task.all_tasks()
    for task in pending_tasks:
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
    loop.close()

"""
Test the matrix controller with the bluepill agents.
"""
# pylint: disable=redefined-outer-name

import time
import random
import sqlite3

import yaml

CONFIG_BASE = """
rabbitmq_host: localhost
rabbitmq_port: 5672
rabbitmq_username: user
rabbitmq_password: user
event_exchange: events

root_seed: 42
state_store_module: matrix.client.bluepill_store
num_rounds: 10
start_time: 2018-06-01
round_time: 1h
"""

def assert_equal_event_tables(dsn1, dsn2):
    """
    Compare the tables in two sqlite3 databases.
    """

    con1 = sqlite3.connect(str(dsn1))
    con2 = sqlite3.connect(str(dsn2))

    cur1 = con1.cursor()
    cur2 = con2.cursor()

    rows1 = list(cur1.execute("select * from event order by rowid"))
    rows2 = list(cur2.execute("select * from event order by rowid"))

    assert rows1 == rows2

def do_test_bluepill(tempdir, popener, num_nodes, num_agentproc_range):
    """
    Do the tests.
    """

    random.seed(42)

    # Generate confguration for the controller
    config_fname = tempdir / "controller-config.yaml"
    cfg = yaml.load(CONFIG_BASE)

    node_idxs              = range(num_nodes)
    cfg["sim_nodes"]       = [f"node{i}" for i in node_idxs]
    cfg["controller_port"] = {f"node{i}": 17001 + i for i in node_idxs}
    cfg["num_agentprocs"]  = {f"node{i}": random.randint(*num_agentproc_range) for i in node_idxs}
    cfg["state_dsn"]       = {f"node{i}": tempdir / f"state{i}.db" for i in node_idxs}

    with open(config_fname, "wt") as fobj:
        fobj.write(yaml.dump(cfg))

    # Initialize all the event stores
    for node in cfg["sim_nodes"]:
        state_dsn = cfg["state_dsn"][node]
        cmd = f"bluepill store init -s {state_dsn}"
        assert popener(cmd, shell=True, output_prefix=f"storeinit-{node}").wait() == 0

    all_procs = []

    # Start the event logger
    log_fname = tempdir / "events.log.gz"
    cmd = f"matrix eventlog -c {config_fname} -o {log_fname}"
    logger = popener(cmd, shell=True, output_prefix="event-logger")
    all_procs.append(logger)

    # Start all the controllers
    for node in cfg["sim_nodes"]:
        cmd = f"matrix controller -c {config_fname} -n {node}"
        controller = popener(cmd, shell=True, output_prefix=f"controller-{node}")
        all_procs.append(controller)

    time.sleep(1)

    # Start all the agent processes
    for node in cfg["sim_nodes"]:
        state_dsn = cfg["state_dsn"][node]
        port = cfg["controller_port"][node]
        num_agentprocs = cfg["num_agentprocs"][node]

        for agentproc_id in range(1, num_agentprocs + 1):
            # Start bluepill agent process
            cmd = f"bluepill agent start -n {node} -p {port} -s {state_dsn} -i {agentproc_id}"
            agentproc = popener(cmd, shell=True, output_prefix=f"bluepill-{node}-{agentproc_id}")
            all_procs.append(agentproc)

    # Wait for the processes to finish
    for proc in all_procs:
        assert proc.wait() == 0

    # Check the tables
    if num_nodes > 1:
        first_node = cfg["sim_nodes"][0]
        first_state_dsn = cfg["state_dsn"][first_node]

        rest_nodes = cfg["sim_nodes"][1:]
        rest_state_dsns = [cfg["state_dsn"][n] for n in rest_nodes]

        for rest_state_dsn in rest_state_dsns:
            assert_equal_event_tables(first_state_dsn, rest_state_dsn)

def test_bluepill1(tempdir, popener):
    """
    Test the basic overall run with one agent.
    """

    num_nodes = 1
    num_agentproc_range = 1, 1

    do_test_bluepill(tempdir, popener, num_nodes, num_agentproc_range)

def test_bluepill17(tempdir, popener):
    """
    Test the basic overall run with one agent.
    """

    num_nodes = 7
    num_agentproc_range = 10, 20

    do_test_bluepill(tempdir, popener, num_nodes, num_agentproc_range)

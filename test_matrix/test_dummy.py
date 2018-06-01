"""
Test the matrix controller with the dummy agents.
"""
# pylint: disable=redefined-outer-name

import time

SINGLE_NODE_TEST_CONFIG = """
rabbitmq_host: localhost
rabbitmq_port: 5672
rabbitmq_username: user
rabbitmq_password: user
event_exchange: events
controller_port: 17001
sim_nodes:
    - 127.1.0.1
num_agentprocs:
    127.1.0.1: 1
root_seed: 42
state_store_module: matrix.dummystore
state_dsn: {state_dsn}
num_rounds: 10
start_time: 2018-06-01
round_time: 1h
"""

def test_dummy(tempdir, popener):
    """
    Test the basic overall run with one agent.
    """

    config_fname = tempdir / "matrix.conf"
    state_dsn = tempdir / "state.db"

    with open(config_fname, "wt") as fobj:
        fobj.write(SINGLE_NODE_TEST_CONFIG.format(state_dsn=state_dsn))

    # Initialize state store
    cmd = f"matrix dummystoreinit -s {state_dsn}"
    assert popener(cmd, shell=True, output_prefix="dummystoreinit").wait() == 0

    # Start controller
    cmd = f"matrix controller -c {config_fname} -h 127.1.0.1"
    controller = popener(cmd, shell=True, output_prefix="controller")

    time.sleep(1)

    # Start dummyagent process
    cmd = f"matrix dummyagent -h 127.1.0.1 -p 17001 -s {state_dsn} -i 1"
    agentproc = popener(cmd, shell=True, output_prefix="dummyagent-1")

    agentproc_retcode = agentproc.wait()
    assert agentproc_retcode == 0

    controller_retcode = controller.wait()
    assert controller_retcode == 0

SEVEN_NODE_TEST_CONFIG = """
rabbitmq_host: localhost
rabbitmq_port: 5672
rabbitmq_username: user
rabbitmq_password: user
event_exchange: events
controller_port: 17001
sim_nodes:
    - 127.1.0.1
    - 127.1.0.2
    - 127.1.0.3
    - 127.1.0.4
    - 127.1.0.5
    - 127.1.0.6
    - 127.1.0.7
num_agentprocs:
    127.1.0.1: 10
    127.1.0.2: 10
    127.1.0.3: 10
    127.1.0.4: 10
    127.1.0.5: 10
    127.1.0.6: 10
    127.1.0.7: 10
root_seed: 42
state_store_module: matrix.dummystore
state_dsn: {state_dsn}
num_rounds: 2
start_time: 2018-06-01
round_time: 1h
"""

def test_dummy7(tempdir, popener):
    """
    Test the basic overall run with 7 nodes with 10 agents each.
    """

    num_nodes = 7
    num_agentprocs = 10

    nodes = list(range(1, num_nodes + 1))
    procs = []

    for node in nodes:
        config_fname = tempdir / f"matrix-{node}.conf"
        state_dsn = tempdir / f"state-{node}.db"

        with open(config_fname, "wt") as fobj:
            fobj.write(SEVEN_NODE_TEST_CONFIG.format(state_dsn=state_dsn))

        cmd = f"matrix dummystoreinit -s {state_dsn}"
        assert popener(cmd, shell=True, output_prefix=f"dummystoreinit-{node}").wait() == 0

        # Start controller
        cmd = f"matrix controller -c {config_fname} -h 127.1.0.{node}"
        controller = popener(cmd, shell=True, output_prefix=f"controller-{node}")
        procs.append(controller)

    time.sleep(1)

    for node in nodes:
        for agentproc_id in range(1, num_agentprocs + 1):
            # Start dummyagent process
            cmd = f"matrix dummyagent -h 127.1.0.{node} -p 17001 -s {state_dsn} -i {agentproc_id}"
            agentproc = popener(cmd, shell=True, output_prefix=f"dummyagent-{node}-{agentproc_id}")
            procs.append(agentproc)

    for proc in procs:
        assert proc.wait() == 0

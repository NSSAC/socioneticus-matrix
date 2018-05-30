"""
Test the matrix controller with the dummy agents.
"""
# pylint: disable=redefined-outer-name

import random
import time
from subprocess import Popen as _Popen

import pytest

SAMPLE_CONFIG = """
controller_port: {port}
sim_nodes:
    - 127.0.0.1
num_agentprocs:
    127.0.0.1: {num_agentprocs}
root_seed: 42
state_store_module: matrix.dummystore
state_dsn: {state_dsn}
num_rounds: {rounds}
start_time: 2018-01-01
round_time: 1h
"""

@pytest.fixture
def random_tcp_port():
    """
    Return a random port.
    """

    port_range = list(range(16000, 17000))
    return random.choice(port_range)

@pytest.fixture
def popener():
    """
    Fixture for cleanly killing Popen objects.
    """

    procs = []

    def do_Popen(*args, **kwargs):
        proc = _Popen(*args, **kwargs)
        procs.append(proc)
        return proc

    yield do_Popen

    for proc in procs:
        if proc.poll() is None:
            proc.terminate()
            if proc.poll() is None:
                proc.kill()

def test_dummy(tmpdir, random_tcp_port, popener):
    """
    Test the basic overall run with one agent.
    """

    state_dsn = str(tmpdir.join("state.db"))
    config_fname = str(tmpdir.join("matrix.conf"))
    port = random_tcp_port
    num_agentprocs = 1
    rounds = 10

    with open(config_fname, "wt") as fobj:
        fobj.write(SAMPLE_CONFIG.format(
            state_dsn=state_dsn,
            config_fname=config_fname,
            port=port,
            num_agentprocs=num_agentprocs,
            rounds=rounds))

    # Initialize state store
    cmd = f"matrix dummystoreinit -s '{state_dsn}'"
    assert popener(cmd, shell=True).wait() == 0

    # Start controller
    cmd = f"matrix controller -c {config_fname} -h 127.0.0.1"
    controller = popener(cmd, shell=True)

    time.sleep(1)

    # Start dummyagent process
    cmd = f"matrix dummyagent -p {port} -s {state_dsn} -i 1"
    agentproc = popener(cmd, shell=True)

    agentproc_retcode = agentproc.wait()
    assert agentproc_retcode == 0

    controller_retcode = controller.wait()
    assert controller_retcode == 0


def test_dummy2(tmpdir, random_tcp_port, popener):
    """
    Test the basic overall run with two agents.
    """

    state_dsn = str(tmpdir.join("state.db"))
    config_fname = str(tmpdir.join("matrix.conf"))
    port = random_tcp_port
    num_agentprocs = 7
    rounds = 10

    with open(config_fname, "wt") as fobj:
        fobj.write(SAMPLE_CONFIG.format(
            state_dsn=state_dsn,
            config_fname=config_fname,
            port=port,
            num_agentprocs=num_agentprocs,
            rounds=rounds))

    # Initialize state store
    cmd = f"matrix dummystoreinit -s '{state_dsn}'"
    assert popener(cmd, shell=True).wait() == 0

    # Start controller
    cmd = f"matrix controller -c {config_fname} -h 127.0.0.1"
    controller = popener(cmd, shell=True)

    time.sleep(1)

    agentprocs = []
    for i in range(1, num_agentprocs + 1):
        # Start dummyagent processes
        cmd = f"matrix dummyagent -p {port} -s {state_dsn} -i {i}"
        agentproc = popener(cmd, shell=True)
        agentprocs.append(agentproc)

    for agentproc in agentprocs:
        assert agentproc.wait() == 0

    assert controller.wait() == 0

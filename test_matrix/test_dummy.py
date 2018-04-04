"""
Test the matrix controller with the dummy agents.
"""
# pylint: disable=redefined-outer-name

import random
import time
from subprocess import Popen as _Popen

import pytest

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
    log_fname = str(tmpdir.join("log.gz"))
    port = random_tcp_port
    rounds = 10

    # Initialize state store
    cmd = f"matrix initstore -s '{state_dsn}' -m matrix.dummystore"
    assert popener(cmd, shell=True).wait() == 0

    # Start controller
    cmd = f"matrix controller -p {port} -l {log_fname} -s {state_dsn} -m matrix.dummystore -n 1 -r {rounds}"
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
    log_fname = str(tmpdir.join("log.gz"))
    port = random_tcp_port
    num_agentprocs = 7
    rounds = 10

    # Initialize state store
    cmd = f"matrix initstore -s '{state_dsn}' -m matrix.dummystore"
    assert popener(cmd, shell=True).wait() == 0

    # Start controller
    cmd = f"matrix controller -p {port} -l {log_fname} -s {state_dsn} -m matrix.dummystore -n {num_agentprocs} -r {rounds}"
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

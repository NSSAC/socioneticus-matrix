"""
Run rabbitmq.
"""

import os
import sys
import time
import signal
from pathlib import Path
from subprocess import Popen, TimeoutExpired
from contextlib import contextmanager

import logbook

log = logbook.Logger(__name__)

TERM_SIGNALS = ["SIGINT", "SIGQUIT", "SIGHUP", "SIGTERM"]
RECEIVED_TERM_SIGNAL = False


def wait_and_kill(proc, timeout=5.0):
    """
    Wait for the proc to finish cleanly or kill.
    """

    try:
        proc.wait(timeout=timeout)
    except TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except TimeoutExpired:
            proc.kill()

    return proc.poll()


@contextmanager
def epmd_context():
    """
    Epmd context manager.
    """

    log.info("Starting empd ...")
    epmd = Popen(["epmd", "-relaxed_command_check"])
    try:
        yield epmd
    finally:
        log.info("Shutting down epmd ...")
        epmd_kill = Popen(["epmd", "-kill"])

        wait_and_kill(epmd_kill)
        wait_and_kill(epmd)


@contextmanager
def rabbitmq_context():
    """
    Rabbitmq context manager.
    """

    log.info("Starting rabbitmq-server ...")
    rabbitmq = Popen(["rabbitmq-server"])
    try:
        yield rabbitmq
    finally:
        log.info("Shutting down rabbitmq-server ...")
        rabbitmq_shutdown = Popen(["rabbitmqctl", "shutdown"])

        wait_and_kill(rabbitmq_shutdown, timeout=60)
        wait_and_kill(rabbitmq)


def cleanup(signame):
    """
    Return the cleanup function for this signal.
    """

    def do_cleanup(*args):  # pylint: disable=unused-argument
        """
        Handle the signal.
        """

        global RECEIVED_TERM_SIGNAL

        log.info("Received {}", signame)
        RECEIVED_TERM_SIGNAL = True

    return do_cleanup


def startup(config_fname, mnesia_base, hostname, pid_fname):
    """
    Start rabbitmq-server.
    """

    os.environ["RABBITMQ_CONFIG_FILE"] = str(config_fname)
    os.environ["RABBITMQ_MNESIA_BASE"] = str(mnesia_base)
    os.environ["RABBITMQ_LOGS"] = "-"
    os.environ["RABBITMQ_SASL_LOGS"] = "-"
    os.environ["HOSTNAME"] = hostname

    # Ignore the signals when starting
    for signame in TERM_SIGNALS:
        signal.signal(getattr(signal, signame), signal.SIG_IGN)

    with epmd_context() as epmd:
        with rabbitmq_context() as rabbitmq:
            # Setup handlers for signals
            for signame in TERM_SIGNALS:
                signal.signal(getattr(signal, signame), cleanup(signame))

            # Writing pid to file
            with open(pid_fname, "wt") as fobj:
                fobj.write(str(os.getpid()))
            try:
                log.info("Waiting for term signal ...")

                while True:
                    if epmd.poll() is not None:
                        log.error(
                            "Epmd exited prematurely with returncode: {}", epmd.poll()
                        )
                        sys.exit(1)

                    if rabbitmq.poll() is not None:
                        log.error(
                            "Rabbitmq exited prematurely with returncode: {}",
                            rabbitmq.poll(),
                        )
                        sys.exit(1)

                    if RECEIVED_TERM_SIGNAL:
                        log.info("Term signal received ...")
                        sys.exit(0)

                    time.sleep(5)
            finally:
                log.info("Removing pid file: {}", pid_fname)
                pid_fname.unlink()


def main_rabbitmq_start(config_fname, runtime_dir, hostname):
    """
    Start rabbitmq server.
    """

    config_fname = Path(config_fname).absolute()
    runtime_dir = Path(runtime_dir).absolute()

    # with open(config_fname, "rt") as fobj:
    #     rcfg = configparser.ConfigParser()
    #     rcfg.read_string("[default]\n" + fobj.read())
    # management_port = rcfg["default"].get("management.listener.port", 15672)

    config_fname = config_fname.parent / config_fname.stem
    mnesia_base = runtime_dir / "mnesia"
    pid_fname = runtime_dir / "matrix_rabbitmq.pid"

    if not mnesia_base.exists():
        log.info("Creating directory {}", mnesia_base)
        mnesia_base.mkdir(mode=0o700)

    if pid_fname.exists():
        log.info("Removing existing pid file: {}", pid_fname)
        pid_fname.unlink()

    startup(config_fname, mnesia_base, hostname, pid_fname)


def main_rabbitmq_stop(runtime_dir):
    """
    Stop rabbitmq server.
    """

    runtime_dir = Path(runtime_dir).absolute()
    pid_fname = runtime_dir / "matrix_rabbitmq.pid"

    with open(pid_fname, "rt") as fobj:
        pid = int(fobj.read())
        os.kill(pid, signal.SIGTERM)

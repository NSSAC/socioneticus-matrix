"""
Run rabbitmq.
"""
# pylint: disable=subprocess-popen-preexec-fn

import os
import time
import signal
from pathlib import Path
from subprocess import Popen
from contextlib import contextmanager
import configparser

import logbook

log = logbook.Logger(__name__)

TERM_SIGNALS = ["SIGINT", "SIGTERM", "SIGHUP"]

def preexecfn():
    """
    Disable handling of the above signals in child.
    """

    for signame in TERM_SIGNALS:
        signal.signal(getattr(signal, signame), signal.SIG_IGN)

@contextmanager
def epmd_context():
    """
    Epmd context manager.
    """

    log.info("Starting empd ...")
    epmd = Popen(["epmd"], preexec_fn=preexecfn)
    try:
        time.sleep(1)
        yield epmd
    finally:
        log.info("Shutting down epmd ...")
        Popen(["epmd", "-kill"], preexec_fn=preexecfn).wait()

        epmd.wait()

@contextmanager
def rabbitmq_context():
    """
    Rabbitmq context manager.
    """

    log.info("Starting rabbitmq-server ...")
    rabbitmq = Popen(["rabbitmq-server"], preexec_fn=preexecfn)
    try:
        yield rabbitmq
    finally:
        log.info("Shutting down rabbitmq-server ...")
        Popen(["rabbitmqctl", "shutdown"], preexec_fn=preexecfn).wait()

        rabbitmq.wait()

def cleanup(signame):
    """
    Return the cleanup function for this signal.
    """

    def do_cleanup(*args): # pylint: disable=unused-argument
        """
        Handle the signal.
        """

        log.info(f"Received {signame}")

    return do_cleanup

def startup(config_fname, mnesia_base, log_base, hostname, rabbitmq_pid_fname, runner_pid_fname, management_port):
    """
    Start rabbitmq-server.
    """

    os.environ["RABBITMQ_CONFIG_FILE"] = str(config_fname)
    os.environ["RABBITMQ_MNESIA_BASE"] = str(mnesia_base)
    os.environ["RABBITMQ_LOG_BASE"] = str(log_base)
    os.environ["RABBITMQ_PID_FILE"] = str(rabbitmq_pid_fname)
    os.environ["HOSTNAME"] = hostname

    log.info("Enable managment plugins ...")
    Popen(["rabbitmq-plugins", "enable", "rabbitmq_management"]).wait()

    # Ignore the signals when starting
    for signame in TERM_SIGNALS:
        signal.signal(getattr(signal, signame), signal.SIG_IGN)

    with epmd_context():
        with rabbitmq_context():

            # Wait for rabbitmq process to start
            log.info("Waiting for rabbitmq process to start ...")
            cmd = ["rabbitmqctl", "--timeout", "120", "wait", str(rabbitmq_pid_fname)]
            Popen(cmd, preexec_fn=preexecfn).wait()

            # Health check
            log.info("Rabbitmq process health check ...")
            cmd = ["rabbitmqctl", "node_health_check"]
            Popen(cmd, preexec_fn=preexecfn).wait()

            # Writing pid to file
            with open(runner_pid_fname, "wt") as fobj:
                fobj.write(str(os.getpid()))

            # Setup handlers for signals
            for signame in TERM_SIGNALS:
                signal.signal(getattr(signal, signame), cleanup(signame))

            log.info(f"Management UI: http://{hostname}:{management_port}")

            log.info("Waiting for term signal ...")
            signal.pause()
            log.info("Term signal received ...")

            log.info(f"Removing runner pid file: {runner_pid_fname}")
            runner_pid_fname.unlink()
            log.info(f"Removing rabbitmq pid file: {rabbitmq_pid_fname}")
            rabbitmq_pid_fname.unlink()

def main_rabbitmq_start(config_fname, runtime_dir, hostname):
    """
    Start rabbitmq server.
    """

    config_fname = Path(config_fname).absolute()
    runtime_dir = Path(runtime_dir).absolute()

    with open(config_fname, "rt") as fobj:
        rcfg = configparser.ConfigParser()
        rcfg.read_string("[default]\n" + fobj.read())
    management_port = rcfg["default"].get("management.listener.port", 15672)

    config_fname = config_fname.parent / config_fname.stem
    mnesia_base = runtime_dir / "mnesia"
    log_base = runtime_dir / "log"
    rabbitmq_pid_fname = runtime_dir / "rabbitmq.pid"
    runner_pid_fname = runtime_dir / "run_rabbitmq.pid"

    if not mnesia_base.exists():
        log.info(f"Creating directory {mnesia_base}")
        mnesia_base.mkdir(mode=0o700)
    if not log_base.exists():
        log.info(f"Creating directory {log_base}")
        log_base.mkdir(mode=0o700)

    if rabbitmq_pid_fname.exists():
        log.info(f"Removing existing rabbitmq pid file: {rabbitmq_pid_fname}")
        rabbitmq_pid_fname.unlink()
    if runner_pid_fname.exists():
        log.info(f"Removing existing runner pid file: {runner_pid_fname}")
        runner_pid_fname.unlink()

    startup(config_fname, mnesia_base, log_base, hostname, rabbitmq_pid_fname, runner_pid_fname, management_port)

def main_rabbitmq_stop(runtime_dir):
    """
    Stop rabbitmq server.
    """

    runtime_dir = Path(runtime_dir).absolute()
    pid_fname = runtime_dir / "run_rabbitmq.pid"

    with open(pid_fname, "rt") as fobj:
        pid = int(fobj.read())
        os.kill(pid, signal.SIGTERM)

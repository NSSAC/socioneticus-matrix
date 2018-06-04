"""
Run rabbitmq.
"""

import os
import time
import signal
from pathlib import Path
from subprocess import Popen

import logbook

log = logbook.Logger(__name__)

TERM_SIGNALS = ["SIGINT", "SIGQUIT", "SIGTERM", "SIGHUP"]

def preexecfn():
    """
    Disable handling of the above signals in child.
    """

    for signame in TERM_SIGNALS:
        signal.signal(getattr(signal, signame), signal.SIG_IGN)

is_cleaning_up = False

def cleanup(signame):
    """
    Return the cleanup function for this signal.
    """

    def do_cleanup(*args): # pylint: disable=unused-argument
        """
        Handle the signal.
        """

        global is_cleaning_up

        log.info(f"Received {signame}")
        if is_cleaning_up:
            return
        is_cleaning_up = True

        log.info("Shutting down rabbitmq-server ...")
        Popen(["rabbitmqctl", "shutdown"], preexec_fn=preexecfn).wait()

        log.info("Shutting down epmd ...")
        Popen(["epmd", "-kill"], preexec_fn=preexecfn).wait()

    return do_cleanup

def startup(config_fname, mnesia_base, log_base, hostname):
    """
    Start rabbitmq-server.
    """

    os.environ["RABBITMQ_CONFIG_FILE"] = str(config_fname)
    os.environ["RABBITMQ_MNESIA_BASE"] = str(mnesia_base)
    os.environ["RABBITMQ_LOG_BASE"] = str(log_base)
    os.environ["HOSTNAME"] = hostname

    log.info("Enable managment plugins ...")
    Popen(["rabbitmq-plugins", "enable", "rabbitmq_management"]).wait()

    # Ignore the signals when starting
    for signame in TERM_SIGNALS:
        signal.signal(getattr(signal, signame), signal.SIG_IGN)

    log.info("Starting empd ...")
    epmd = Popen(["epmd"], preexec_fn=preexecfn)
    time.sleep(1)

    log.info("Starting rabbitmq-server ...")
    rabbitmq = Popen(["rabbitmq-server"], preexec_fn=preexecfn)

    # Setup handlers for signals
    for signame in TERM_SIGNALS:
        signal.signal(getattr(signal, signame), cleanup(signame))

    log.info("Waiting for processes to finish ..")
    for proc in [rabbitmq, epmd]:
        proc.wait()

def main_run_rabbitmq(config_fname, runtime_dir, hostname):
    """
    Run rabbitmq.
    """

    config_fname = Path(config_fname).absolute()
    runtime_dir = Path(runtime_dir).absolute()

    config_fname = config_fname.parent / config_fname.stem
    mnesia_base = runtime_dir / "mnesia"
    log_base = runtime_dir / "log"

    if not mnesia_base.exists():
        log.info(f"Creating directory {mnesia_base}")
        mnesia_base.mkdir(mode=0o700)
    if not log_base.exists():
        log.info(f"Creating directory {log_base}")
        log_base.mkdir(mode=0o700)

    startup(config_fname, mnesia_base, log_base, hostname)

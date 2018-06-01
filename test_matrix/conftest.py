"""
Common fixtures for other tests.
"""

from pathlib import Path
from subprocess import Popen as _Popen, DEVNULL

import pytest

@pytest.fixture
def tempdir(tmpdir):
    return Path(tmpdir)

@pytest.fixture
def popener(tempdir):
    """
    Fixture for cleanly killing Popen objects.
    """

    procs = []
    outs = []
    errs = []

    def do_Popen(*args, **kwargs):
        """
        Do the actual running of Popen.
        """

        output_prefix = kwargs.pop("output_prefix", None)
        if output_prefix is not None:
            sout_fname = f"{tempdir}/{output_prefix}.out"
            serr_fname = f"{tempdir}/{output_prefix}.err"

            sout = open(sout_fname, "wb")
            serr = open(serr_fname, "wb")
            kwargs["stdin"] = DEVNULL
            kwargs["stdout"] = sout.fileno()
            kwargs["stderr"] = serr.fileno()

        proc = _Popen(*args, **kwargs)
        procs.append(proc)
        outs.append(sout)
        errs.append(serr)
        return proc

    yield do_Popen

    for proc in procs:
        if proc.poll() is None:
            proc.terminate()
            if proc.poll() is None:
                proc.kill()

    for fobj in outs:
        fobj.close()
    for fobj in errs:
        fobj.close()

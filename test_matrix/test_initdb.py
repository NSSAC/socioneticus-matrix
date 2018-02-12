"""
Tests for matrix initdb command.
"""

import sqlite3
from subprocess import Popen

def test_initdb_basic(tmpdir):
    """
    Test the basic invocation of initdb.
    """

    event_db_fname = tmpdir.join("events.db")
    num_agents = 2
    num_repos = 2

    cmd = f"matrix initdb -n {num_agents} -m {num_repos} -o balanced -e {event_db_fname}".split()
    proc = Popen(cmd)
    proc.wait()

    assert proc.returncode == 0

    con = sqlite3.connect(str(event_db_fname))
    cur = con.cursor()
    sql = "select agent_id, repo_id from event"
    cur.execute(sql)
    rows = cur.fetchall()
    rows.sort()

    expected_rows = [(i, i) for i in range(1, num_agents + 1)]

    assert rows == expected_rows

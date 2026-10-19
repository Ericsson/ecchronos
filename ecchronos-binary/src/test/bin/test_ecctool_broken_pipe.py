#
# Copyright 2026 Telefonaktiebolaget LM Ericsson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Tests for ecctool broken-pipe handling when output is piped to head/grep/less (Issue #1906)."""
import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pylib"))

BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "bin")
PYLIB_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "pylib")


def test_exit_on_broken_pipe_exits_141(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    calls = {}
    # Isolate from the real process: do not touch the test runner's fds.
    monkeypatch.setattr(ecctool.os, "open", lambda *a, **k: 999)
    monkeypatch.setattr(ecctool.os, "dup2", lambda *a, **k: calls.setdefault("dup2", a))

    with pytest.raises(SystemExit) as excinfo:
        ecctool._exit_on_broken_pipe()  # pylint: disable=protected-access
    assert excinfo.value.code == 141
    # It attempted to redirect stdout to the opened (devnull) fd before exiting.
    assert calls["dup2"][0] == 999


def test_exit_on_broken_pipe_exits_even_when_stdout_has_no_real_fd():
    # Defends the branch where stdout cannot be redirected (fileno() raises).
    # Run in a subprocess so we never disturb the test runner's own streams.
    script = (
        "import io, os, sys\n"
        "sys.path.insert(0, {bin!r}); sys.path.insert(0, {pylib!r})\n"
        "import ecctool\n"
        "sys.stdout = io.StringIO()  # StringIO.fileno() raises io.UnsupportedOperation (a ValueError)\n"
        "try:\n"
        "    ecctool._exit_on_broken_pipe()\n"
        "except SystemExit as e:\n"
        "    sys.stderr.write('code=%s' % e.code)\n"
        "    raise\n"
    ).format(bin=BIN_DIR, pylib=PYLIB_DIR)

    proc = subprocess.run([sys.executable, "-c", script], stderr=subprocess.PIPE, check=False)
    assert proc.returncode == 141
    assert b"code=141" in proc.stderr
    assert b"Traceback" not in proc.stderr


def test_piped_output_does_not_print_traceback():
    # End-to-end: a long print loop through the real table formatter, piped to
    # a reader that stops early, must not emit a BrokenPipeError traceback when
    # wrapped by ecctool's handler.
    script = (
        "import os, sys\n"
        "sys.path.insert(0, {bin!r}); sys.path.insert(0, {pylib!r})\n"
        "import ecctool\n"
        "from ecchronoslib import table_formatter\n"
        "def main():\n"
        "    data = [['col']] + [[str(i)] for i in range(100000)]\n"
        "    table_formatter.format_table(data, columns=None)\n"
        "try:\n"
        "    main()\n"
        "except BrokenPipeError:\n"
        "    ecctool._exit_on_broken_pipe()\n"
    ).format(bin=BIN_DIR, pylib=PYLIB_DIR)

    producer = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    head = subprocess.Popen(["head", "-2"], stdin=producer.stdout, stdout=subprocess.DEVNULL)
    producer.stdout.close()
    head.wait()
    _, stderr = producer.communicate()
    assert b"BrokenPipeError" not in stderr
    assert b"Traceback" not in stderr

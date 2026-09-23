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

import json

from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common import strip_and_collapse, run_ecctool

REPAIR_SESSIONS_HEADER = "| Node | Session Id | State | Coordinator | Last Update | Participants | Tables |"


@when("we list all repair sessions")
def step_list_repair_sessions(context):
    run_ecctool(context, ["repair-sessions", "list"])


@when("we list all repair sessions as json")
def step_list_repair_sessions_json(context):
    run_ecctool(context, ["repair-sessions", "list", "--output", "json"])


@when("we fail repair session {session_id} with yes")
def step_fail_repair_session(context, session_id):
    run_ecctool(context, ["repair-sessions", "fail", "--session", session_id, "--yes"])


@then("the output should contain a valid repair sessions header")
def step_validate_repair_sessions_header(context):
    output_data = context.out.decode("ascii").lstrip().rstrip().split("\n")
    # The status banner is printed first; find the table header line.
    header_line = None
    for line in output_data:
        if strip_and_collapse(line) == REPAIR_SESSIONS_HEADER:
            header_line = strip_and_collapse(line)
            break
    assert header_line == REPAIR_SESSIONS_HEADER, "Header not found in {0}".format(output_data)


@then("the repair sessions json output contains a repairSessions key")
def step_validate_repair_sessions_json(context):
    out = context.out.decode("ascii")
    start = out.index("{")
    payload = json.loads(out[start:])
    assert "repairSessions" in payload, "repairSessions key not found in {0}".format(payload)


@then("the repair sessions command fails")
def step_validate_repair_sessions_failed(context):
    assert context.proc.returncode != 0, "Expected a non-zero exit code, got {0}".format(context.proc.returncode)

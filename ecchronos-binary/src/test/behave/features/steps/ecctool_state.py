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
from ecc_step_library.common import run_ecctool, validate_header

STATE_NODES_HEADER = (
    r"| EcchronosID | Datacenter | NodeID | Last Connection | Next Connection | Endpoint | Node Status |"
)


def run_ecc_state_nodes(context, params):
    run_ecctool(context, ["state"] + params)


def handle_state_nodes_output(context):
    output = context.out.decode("ascii")
    output_data = output.lstrip().rstrip().split("\n")
    context.header = output_data[0:3]
    context.rows = output_data[3:-1]
    context.all = output


@when("we list all state nodes")
def step_list_state_nodes(context):
    run_ecc_state_nodes(context, ["nodes"])
    handle_state_nodes_output(context)


@when("we list all state nodes with json output option")
def step_list_state_nodes_with_json(context):
    run_ecc_state_nodes(context, ["-o", "json", "nodes"])
    handle_state_nodes_output(context)


@when("we list status")
def step_list_status(context):
    run_ecctool(context, ["status"])
    handle_state_nodes_output(context)


@when("we list status with json output option")
def step_list_status_with_json(context):
    run_ecctool(context, ["status", "-o", "json"])
    handle_state_nodes_output(context)


@when("we list local status")
def step_list_local_status(context):
    run_ecctool(context, ["status", "--local"])
    context.all = context.out.decode("ascii")


@then("the output should contain a valid state nodes header")
def step_validate_state_nodes_header(context):
    validate_header(context.header, STATE_NODES_HEADER)


@then("the json output should contain nodes")
def step_validate_state_nodes_json_content(context):
    data = json.loads(context.all)
    assert "nodes" in data
    assert isinstance(data["nodes"], list)
    assert len(data["nodes"]) > 0


@then("the output should contain local ecchronos running status")
def step_validate_local_status(context):
    assert "ecChronos is running." in context.all

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

"""Tests for ecctool state subcommand JSON output."""
import io
import json
import os
import sys
from contextlib import redirect_stdout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pylib"))

from ecchronoslib import table_printer  # pylint: disable=wrong-import-position
from ecchronoslib.types import NodeSyncState  # pylint: disable=wrong-import-position


def _sample_node():
    return NodeSyncState(
        {
            "ecchronosId": "ecc-1",
            "datacenterName": "datacenter1",
            "nodeId": "8a9d2a57-1388-42be-aab6-06a4e5f6fe84",
            "lastConnection": "2025-12-15T15:49:41.762Z",
            "nextConnection": "2025-12-15T16:19:41.762Z",
            "nodeEndpoint": "/127.0.0.1:9042",
            "nodeStatus": "AVAILABLE",
        }
    )


def test_print_nodes_json_output():
    output = io.StringIO()
    with redirect_stdout(output):
        table_printer.print_nodes([_sample_node()], output="json")

    data = json.loads(output.getvalue())
    assert "timestamp" in data
    assert "nodes" in data
    assert len(data["nodes"]) == 1
    node = data["nodes"][0]
    assert node["ecchronos_id"] == "ecc-1"
    assert node["datacenter_name"] == "datacenter1"
    assert node["node_id"] == "8a9d2a57-1388-42be-aab6-06a4e5f6fe84"
    assert node["node_status"] == "AVAILABLE"


def test_print_nodes_table_output():
    output = io.StringIO()
    with redirect_stdout(output):
        table_printer.print_nodes([_sample_node()], output="table")

    rendered = output.getvalue()
    assert "EcchronosID" in rendered
    assert "ecc-1" in rendered
    assert "AVAILABLE" in rendered


def test_state_command_parses_json_output_flag():
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    args = get_parser().parse_args(["state", "-o", "json", "nodes"])
    assert args.output == "json"
    assert args.state_subcommand == "nodes"

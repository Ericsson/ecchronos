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

"""Tests for ecctool status subcommand."""
import io
import json
import os
import sys
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pylib"))

from ecchronoslib.types import NodeSyncState  # pylint: disable=wrong-import-position


def _sample_node(ecchronos_id="ecc-1"):
    return NodeSyncState(
        {
            "ecchronosId": ecchronos_id,
            "datacenterName": "datacenter1",
            "nodeId": "8a9d2a57-1388-42be-aab6-06a4e5f6fe84",
            "lastConnection": "2025-12-15T15:49:41.762Z",
            "nextConnection": "2025-12-15T16:19:41.762Z",
            "nodeEndpoint": "/127.0.0.1:9042",
            "nodeStatus": "AVAILABLE",
        }
    )


def test_status_command_parses_output_flags():
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    args = get_parser().parse_args(["status", "-o", "json"])
    assert args.output == "json"
    assert args.subcommand == "status"


def test_status_command_parses_local_flag():
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    args = get_parser().parse_args(["status", "--local"])
    assert args.local is True


@patch("ecctool.rest.StateManagementRequest")
def test_status_command_prints_nodes_table(mock_request_class):
    from ecctool import status  # pylint: disable=import-outside-toplevel

    mock_result = MagicMock()
    mock_result.is_successful.return_value = True
    mock_result.data = [_sample_node()]
    mock_request_class.return_value.get_nodes.return_value = mock_result

    args = MagicMock()
    args.url = "http://localhost:8080"
    args.columns = None
    args.output = "table"
    args.local = False

    output = io.StringIO()
    with redirect_stdout(output):
        status(args)

    rendered = output.getvalue()
    assert "EcchronosID" in rendered
    assert "ecc-1" in rendered
    assert "AVAILABLE" in rendered
    mock_request_class.return_value.get_nodes.assert_called_once_with(all_instances=True)


@patch("ecctool.rest.StateManagementRequest")
def test_status_command_prints_nodes_json(mock_request_class):
    from ecctool import status  # pylint: disable=import-outside-toplevel

    mock_result = MagicMock()
    mock_result.is_successful.return_value = True
    mock_result.data = [_sample_node()]
    mock_request_class.return_value.get_nodes.return_value = mock_result

    args = MagicMock()
    args.url = "http://localhost:8080"
    args.columns = None
    args.output = "json"
    args.local = False

    output = io.StringIO()
    with redirect_stdout(output):
        status(args)

    data = json.loads(output.getvalue())
    assert "timestamp" in data
    assert "nodes" in data
    assert data["nodes"][0]["ecchronos_id"] == "ecc-1"


@patch("ecctool.rest.StateManagementRequest")
def test_status_command_prints_empty_nodes(mock_request_class):
    from ecctool import status  # pylint: disable=import-outside-toplevel

    mock_result = MagicMock()
    mock_result.is_successful.return_value = True
    mock_result.data = []
    mock_request_class.return_value.get_nodes.return_value = mock_result

    args = MagicMock()
    args.url = "http://localhost:8080"
    args.columns = None
    args.output = "json"
    args.local = False

    output = io.StringIO()
    with redirect_stdout(output):
        status(args)

    data = json.loads(output.getvalue())
    assert data["nodes"] == []


@patch("ecctool.rest.StateManagementRequest")
def test_status_command_exits_on_failure(mock_request_class):
    from ecctool import status  # pylint: disable=import-outside-toplevel

    mock_result = MagicMock()
    mock_result.is_successful.return_value = False
    mock_result.format_exception.return_value = "Encountered issue (404)"
    mock_request_class.return_value.get_nodes.return_value = mock_result

    args = MagicMock()
    args.url = "http://localhost:8080"
    args.columns = None
    args.output = "table"
    args.local = False

    try:
        status(args)
        assert False, "Expected SystemExit"
    except SystemExit as exc:
        assert exc.code == 1


@patch("ecctool.rest.RepairSchedulerRequest")
def test_status_local_reports_running(mock_request_class):
    from ecctool import status  # pylint: disable=import-outside-toplevel

    mock_result = MagicMock()
    mock_result.is_successful.return_value = True
    mock_request_class.return_value.list_schedules.return_value = mock_result

    args = MagicMock()
    args.url = "http://localhost:8080"
    args.output = ""
    args.local = True

    output = io.StringIO()
    with redirect_stdout(output):
        status(args)

    assert "ecChronos is running." in output.getvalue()


@patch("ecctool.rest.RepairSchedulerRequest")
def test_status_local_reports_not_running(mock_request_class):
    from ecctool import status  # pylint: disable=import-outside-toplevel

    mock_result = MagicMock()
    mock_result.is_successful.return_value = False
    mock_request_class.return_value.list_schedules.return_value = mock_result

    args = MagicMock()
    args.url = "http://localhost:8080"
    args.output = "json"
    args.local = True

    output = io.StringIO()
    try:
        with redirect_stdout(output):
            status(args)
        assert False, "Expected SystemExit"
    except SystemExit as exc:
        assert exc.code == 1

    data = json.loads(output.getvalue())
    assert data["running"] is False

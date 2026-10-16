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

"""Tests for the ecctool version subcommand and client version resolution (Issue #1900)."""
import io
import json
import os
import sys
from contextlib import redirect_stdout
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pylib"))

from ecchronoslib import rest, version as version_mod  # pylint: disable=wrong-import-position


def _version_args(**overrides):
    base = {"url": None, "timeout": None, "output": ""}
    base.update(overrides)
    return SimpleNamespace(**base)


# --- client version resolution -------------------------------------------------


def test_get_client_version_falls_back_to_unknown_from_source():
    # No generated _version.py when running from source.
    assert version_mod.get_client_version() == version_mod.UNKNOWN_VERSION


def test_get_client_version_reads_generated_module(monkeypatch):
    fake = SimpleNamespace(__version__="9.9.9")
    monkeypatch.setitem(sys.modules, "ecchronoslib._version", fake)
    assert version_mod.get_client_version() == "9.9.9"


def test_get_client_version_ignores_unfiltered_placeholder(monkeypatch):
    fake = SimpleNamespace(__version__="${project.version}")
    monkeypatch.setitem(sys.modules, "ecchronoslib._version", fake)
    assert version_mod.get_client_version() == version_mod.UNKNOWN_VERSION


# --- parser --------------------------------------------------------------------


def test_version_parser_flags_and_defaults():
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    args = get_parser().parse_args(["version", "-u", "http://host:8080", "--timeout", "5", "-o", "json"])
    assert args.subcommand == "version"
    assert args.url == "http://host:8080"
    assert args.timeout == 5.0
    assert args.output == "json"

    args = get_parser().parse_args(["version"])
    assert args.url is None
    assert args.timeout is None
    assert args.output == ""


# --- handler: success ----------------------------------------------------------


def _install_fake_version_request(monkeypatch, result, base_url_default="http://localhost:8080"):
    import ecctool  # pylint: disable=import-outside-toplevel

    class FakeVersionRequest:
        def __init__(self, base_url=None, timeout=None):  # pylint: disable=unused-argument
            self.base_url = base_url or base_url_default

        def get_version(self):
            return result

    monkeypatch.setattr(ecctool.rest, "VersionRequest", FakeVersionRequest)


def test_version_prints_client_and_node_version(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(ecctool, "get_client_version", lambda: "1.1.2")
    _install_fake_version_request(monkeypatch, rest.RequestResult(status_code=200, data={"version": "1.1.2"}))

    output = io.StringIO()
    with redirect_stdout(output):
        ecctool.version(_version_args())
    rendered = output.getvalue()
    assert "ecctool version:   1.1.2" in rendered
    assert "Connected node:    http://localhost:8080" in rendered
    assert "Node version:      1.1.2" in rendered
    assert "could not retrieve" not in rendered


def test_version_json_output_success(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(ecctool, "get_client_version", lambda: "1.1.2")
    _install_fake_version_request(monkeypatch, rest.RequestResult(status_code=200, data={"version": "1.1.2"}))

    output = io.StringIO()
    with redirect_stdout(output):
        ecctool.version(_version_args(output="json"))
    payload = json.loads(output.getvalue())
    assert payload["version"]["ecctool_version"] == "1.1.2"
    assert payload["version"]["connected_node"] == "http://localhost:8080"
    assert payload["version"]["node_version"] == "1.1.2"
    assert "node_error" not in payload["version"]


# --- handler: graceful degradation ---------------------------------------------


def test_version_node_unavailable_old_agent_404(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(ecctool, "get_client_version", lambda: "1.1.2")
    _install_fake_version_request(
        monkeypatch,
        rest.RequestResult(status_code=404, message="Unable to retrieve resource http://localhost:8080/..."),
    )

    output = io.StringIO()
    with redirect_stdout(output):
        ecctool.version(_version_args())
    rendered = output.getvalue()
    # Client version still printed; node version degraded; command does not exit non-zero.
    assert "ecctool version:   1.1.2" in rendered
    assert "Node version:      unavailable" in rendered
    assert "could not retrieve node version" in rendered


def test_version_node_unreachable(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(ecctool, "get_client_version", lambda: "1.1.2")
    _install_fake_version_request(
        monkeypatch,
        rest.RequestResult(status_code=404, message="Unable to connect to http://localhost:8080"),
    )

    output = io.StringIO()
    with redirect_stdout(output):
        ecctool.version(_version_args(output="json"))
    payload = json.loads(output.getvalue())
    assert payload["version"]["node_version"] == "unavailable"
    assert "node_error" in payload["version"]
    assert payload["version"]["ecctool_version"] == "1.1.2"


def test_version_uses_client_version_even_when_offline(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(ecctool, "get_client_version", lambda: "7.7.7")
    _install_fake_version_request(
        monkeypatch,
        rest.RequestResult(status_code=404, message="Unable to connect"),
        base_url_default="http://custom:9999",
    )

    output = io.StringIO()
    with redirect_stdout(output):
        ecctool.version(_version_args(url="http://custom:9999"))
    rendered = output.getvalue()
    assert "ecctool version:   7.7.7" in rendered
    assert "Connected node:    http://custom:9999" in rendered


# --- rest.VersionRequest -------------------------------------------------------


def test_version_request_hits_expected_url(monkeypatch):
    captured = {}

    def fake_request(self, url, method="GET", body=None, headers=None):  # pylint: disable=unused-argument
        captured["url"] = url
        return rest.RequestResult(status_code=200, data={"version": "1.1.2"})

    monkeypatch.setattr(rest.RestRequest, "request", fake_request)
    result = rest.VersionRequest(base_url="http://localhost:8080").get_version()
    assert captured["url"] == "repair-management/version"
    assert result.data == {"version": "1.1.2"}

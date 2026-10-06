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

"""Tests for the configurable request timeout in the ecctool REST client (Issue #1804)."""
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pylib"))

from ecchronoslib import rest  # pylint: disable=wrong-import-position
from urllib.error import URLError  # pylint: disable=wrong-import-position,wrong-import-order


def test_default_timeout_is_thirty_seconds(monkeypatch):
    monkeypatch.delenv(rest.TIMEOUT_ENV_VAR, raising=False)
    assert rest.RestRequest().timeout == rest.DEFAULT_TIMEOUT_SECONDS
    assert rest.DEFAULT_TIMEOUT_SECONDS == 30.0


def test_env_var_overrides_default(monkeypatch):
    monkeypatch.setenv(rest.TIMEOUT_ENV_VAR, "12.5")
    assert rest.RestRequest().timeout == 12.5


def test_constructor_takes_precedence_over_env(monkeypatch):
    monkeypatch.setenv(rest.TIMEOUT_ENV_VAR, "12.5")
    assert rest.RestRequest(timeout=5).timeout == 5.0


def test_invalid_env_var_falls_back_to_default(monkeypatch):
    monkeypatch.setenv(rest.TIMEOUT_ENV_VAR, "not-a-number")
    assert rest.RestRequest().timeout == rest.DEFAULT_TIMEOUT_SECONDS


def test_non_positive_timeout_falls_back_to_default(monkeypatch):
    monkeypatch.delenv(rest.TIMEOUT_ENV_VAR, raising=False)
    assert rest.RestRequest(timeout=0).timeout == rest.DEFAULT_TIMEOUT_SECONDS
    assert rest.RestRequest(timeout=-3).timeout == rest.DEFAULT_TIMEOUT_SECONDS


def test_urlopen_receives_timeout_plain(monkeypatch):
    monkeypatch.delenv(rest.TIMEOUT_ENV_VAR, raising=False)
    for var in ("ECCTOOL_CERT_FILE", "ECCTOOL_KEY_FILE", "ECCTOOL_CA_FILE"):
        monkeypatch.delenv(var, raising=False)
    captured = {}

    class FakeResponse:
        def read(self):
            return b"data"

        def close(self):
            pass

    def fake_urlopen(request, timeout=None):  # pylint: disable=unused-argument
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    rest.RestRequest(base_url="http://localhost:8080", timeout=7).basic_request("metrics")
    assert captured["timeout"] == 7.0


def test_urlopen_receives_timeout_tls(monkeypatch, tmp_path):
    # Point the cert env vars at dummy files and stub ssl so the TLS branch
    # is exercised without real certificates.
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    ca = tmp_path / "ca.pem"
    for path in (cert, key, ca):
        path.write_text("x")
    monkeypatch.setenv("ECCTOOL_CERT_FILE", str(cert))
    monkeypatch.setenv("ECCTOOL_KEY_FILE", str(key))
    monkeypatch.setenv("ECCTOOL_CA_FILE", str(ca))
    monkeypatch.delenv(rest.TIMEOUT_ENV_VAR, raising=False)

    class FakeContext:
        def load_cert_chain(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr(rest.ssl, "create_default_context", lambda cafile=None: FakeContext())

    captured = {}

    class FakeResponse:
        def read(self):
            return b"data"

        def close(self):
            pass

    def fake_urlopen(request, context=None, timeout=None):  # pylint: disable=unused-argument
        captured["timeout"] = timeout
        captured["context"] = context
        return FakeResponse()

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    rest.RestRequest(base_url="https://localhost:8080", timeout=9).basic_request("metrics")
    assert captured["timeout"] == 9.0
    assert isinstance(captured["context"], FakeContext)


def test_connect_phase_timeout_surfaces_as_request_result(monkeypatch):
    # Connect-phase timeout: urllib wraps socket.timeout in a URLError.
    monkeypatch.delenv(rest.TIMEOUT_ENV_VAR, raising=False)
    for var in ("ECCTOOL_CERT_FILE", "ECCTOOL_KEY_FILE", "ECCTOOL_CA_FILE"):
        monkeypatch.delenv(var, raising=False)

    def fake_urlopen(request, timeout=None):  # pylint: disable=unused-argument
        raise URLError(socket.timeout("timed out"))

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    result = rest.RestRequest(base_url="http://localhost:8080", timeout=3).request("state/nodes")
    assert isinstance(result, rest.RequestResult)
    assert result.status_code == 404
    assert "timed out" in result.message
    assert "3" in result.message


def test_read_phase_timeout_request_surfaces_friendly_message(monkeypatch):
    # Read-phase timeout: the connection opens but response.read() stalls,
    # raising a bare socket.timeout (not a URLError). This is the primary
    # stalled-agent scenario from Issue #1804.
    monkeypatch.delenv(rest.TIMEOUT_ENV_VAR, raising=False)
    for var in ("ECCTOOL_CERT_FILE", "ECCTOOL_KEY_FILE", "ECCTOOL_CA_FILE"):
        monkeypatch.delenv(var, raising=False)

    class StallingResponse:
        def read(self):
            raise socket.timeout("timed out")

        def close(self):
            pass

    def fake_urlopen(request, timeout=None):  # pylint: disable=unused-argument
        return StallingResponse()

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    result = rest.RestRequest(base_url="http://localhost:8080", timeout=4).request("state/nodes")
    assert isinstance(result, rest.RequestResult)
    assert result.status_code == 404
    assert "timed out" in result.message
    assert "4" in result.message


def test_read_phase_timeout_basic_request_surfaces_friendly_message(monkeypatch):
    # Same read-phase stall, but through basic_request() (metrics/running-job).
    monkeypatch.delenv(rest.TIMEOUT_ENV_VAR, raising=False)
    for var in ("ECCTOOL_CERT_FILE", "ECCTOOL_KEY_FILE", "ECCTOOL_CA_FILE"):
        monkeypatch.delenv(var, raising=False)

    class StallingResponse:
        def read(self):
            raise socket.timeout("timed out")

        def close(self):
            pass

    def fake_urlopen(request, timeout=None):  # pylint: disable=unused-argument
        return StallingResponse()

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    result = rest.RestRequest(base_url="http://localhost:8080", timeout=6).basic_request("metrics")
    assert isinstance(result, rest.RequestResult)
    assert result.status_code == 404
    assert "timed out" in result.message
    assert "6" in result.message


def test_connection_refused_keeps_generic_message(monkeypatch):
    monkeypatch.delenv(rest.TIMEOUT_ENV_VAR, raising=False)
    for var in ("ECCTOOL_CERT_FILE", "ECCTOOL_KEY_FILE", "ECCTOOL_CA_FILE"):
        monkeypatch.delenv(var, raising=False)

    def fake_urlopen(request, timeout=None):  # pylint: disable=unused-argument
        raise URLError("Connection refused")

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    result = rest.RestRequest(base_url="http://localhost:8080").request("state/nodes")
    assert result.status_code == 404
    assert "Unable to connect" in result.message


def test_parser_timeout_flag_and_default():
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    args = get_parser().parse_args(["schedules", "--timeout", "5"])
    assert args.timeout == 5.0

    args = get_parser().parse_args(["schedules"])
    assert args.timeout is None

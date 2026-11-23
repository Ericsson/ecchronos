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

"""Tests for the ecctool get-logging-level and set-logging-level subcommands (Issue #1999)."""
import io
import json
import os
import sys
import threading
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pylib"))

from ecchronoslib import logging_level, rest  # pylint: disable=wrong-import-position


def _parse(argv):
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    return get_parser().parse_args(argv)


def _args(**overrides):
    base = {"url": None, "timeout": None, "output": "", "logger": None, "level": None}
    base.update(overrides)
    return SimpleNamespace(**base)


def _run(handler, arguments):
    """Run a handler, returning (exit_code or None, stdout)."""
    output = io.StringIO()
    exit_code = None
    with redirect_stdout(output):
        try:
            handler(arguments)
        except SystemExit as exc:
            exit_code = exc.code
    return exit_code, output.getvalue()


class _Recorder:
    """Stands in for LoggingRequest, recording calls and returning a canned result."""

    def __init__(self, result):
        self.result = result
        self.calls = []

    def install(self, monkeypatch):
        recorder = self

        class FakeLoggingRequest:
            def __init__(self, base_url=None, timeout=None):
                recorder.calls.append(("init", base_url, timeout))

            def get(self, logger=None):
                recorder.calls.append(("get", logger))
                return recorder.result

            def put(self, logger, level):
                recorder.calls.append(("put", logger, level))
                return recorder.result

        monkeypatch.setattr(logging_level.rest, "LoggingRequest", FakeLoggingRequest)
        return self


ROOT_ENTRY = {"logger": "ROOT", "configured_level": "INFO", "effective_level": "INFO"}
DEBUG_ENTRY = {"logger": "com.ericsson.bss", "configured_level": "DEBUG", "effective_level": "DEBUG"}
INHERITING = {"logger": "com.ericsson.bss.Foo", "configured_level": None, "effective_level": "DEBUG"}


# --- parser --------------------------------------------------------------------


def test_get_parser_defaults_and_options():
    args = _parse(["get-logging-level"])
    assert args.subcommand == "get-logging-level"
    assert args.logger is None
    assert args.url is None and args.timeout is None and args.output == ""

    args = _parse(["get-logging-level", "com.x.Y", "-u", "http://h:1", "-T", "5", "-o", "json"])
    assert args.logger == "com.x.Y"
    assert args.url == "http://h:1"
    assert args.timeout == 5.0
    assert args.output == "json"


def test_set_parser_requires_logger_and_level():
    for argv in (["set-logging-level"], ["set-logging-level", "com.x"]):
        with pytest.raises(SystemExit) as exc:
            _parse(argv)
        assert exc.value.code == 2


@pytest.mark.parametrize("given,expected", [("debug", "DEBUG"), ("TRACE", "TRACE"), ("Warn", "WARN"), ("off", "OFF")])
def test_set_parser_normalizes_level_case(given, expected):
    assert _parse(["set-logging-level", "com.x", given]).level == expected


@pytest.mark.parametrize("given", ["reset", "RESET", "Reset"])
def test_set_parser_accepts_reset_in_any_case(given):
    assert _parse(["set-logging-level", "com.x", given]).level == "reset"


@pytest.mark.parametrize("bad", ["LOUD", "FATAL", "", "5", "inf0"])
def test_set_parser_rejects_invalid_level(bad):
    with pytest.raises(SystemExit) as exc:
        _parse(["set-logging-level", "com.x", bad])
    assert exc.value.code == 2


@pytest.mark.parametrize("bad", ["", "   ", "bad name", "bad\tname"])
def test_parsers_reject_empty_or_malformed_logger(bad):
    for argv in (["set-logging-level", bad, "DEBUG"], ["get-logging-level", bad]):
        with pytest.raises(SystemExit) as exc:
            _parse(argv)
        assert exc.value.code == 2


def test_subcommands_are_dispatched():
    import ecctool  # pylint: disable=import-outside-toplevel

    assert "get-logging-level" in ecctool.SUBCOMMANDS
    assert "set-logging-level" in ecctool.SUBCOMMANDS


# --- get handler ---------------------------------------------------------------


def test_get_all_prints_table_with_root_first(monkeypatch):
    recorder = _Recorder(rest.RequestResult(status_code=200, data={"loggers": [ROOT_ENTRY, DEBUG_ENTRY]}))
    recorder.install(monkeypatch)

    code, out = _run(logging_level.get_logging_level, _args())

    assert code is None
    lines = out.splitlines()
    assert lines[0].split() == ["LOGGER", "CONFIGURED", "EFFECTIVE"]
    assert lines[1].split() == ["ROOT", "INFO", "INFO"]
    assert lines[2].split() == ["com.ericsson.bss", "DEBUG", "DEBUG"]
    assert ("get", None) in recorder.calls


def test_get_one_shows_configured_and_effective_for_inheriting_logger(monkeypatch):
    _Recorder(rest.RequestResult(status_code=200, data=INHERITING)).install(monkeypatch)

    code, out = _run(logging_level.get_logging_level, _args(logger="com.ericsson.bss.Foo"))

    assert code is None
    assert out.splitlines()[1].split() == ["com.ericsson.bss.Foo", "-", "DEBUG"]


def test_get_passes_url_timeout_and_logger(monkeypatch):
    recorder = _Recorder(rest.RequestResult(status_code=200, data=ROOT_ENTRY)).install(monkeypatch)

    _run(logging_level.get_logging_level, _args(url="http://h:9", timeout=3.0, logger="ROOT"))

    assert ("init", "http://h:9", 3.0) in recorder.calls
    assert ("get", "ROOT") in recorder.calls


def test_get_json_output(monkeypatch):
    _Recorder(rest.RequestResult(status_code=200, data={"loggers": [ROOT_ENTRY]})).install(monkeypatch)

    code, out = _run(logging_level.get_logging_level, _args(output="json"))

    assert code is None
    payload = json.loads(out)
    assert payload["logging"]["loggers"] == [ROOT_ENTRY]
    assert "timestamp" in payload


def test_get_failure_exits_non_zero_and_prints_error(monkeypatch):
    _Recorder(rest.RequestResult(status_code=404, message="Unable to connect to http://h:1")).install(monkeypatch)

    code, out = _run(logging_level.get_logging_level, _args())

    assert code == 1
    assert "Unable to connect" in out


def test_get_failure_json_output(monkeypatch):
    _Recorder(rest.RequestResult(status_code=400, message="bad")).install(monkeypatch)

    code, out = _run(logging_level.get_logging_level, _args(output="json", logger="x"))

    assert code == 1
    assert "bad" in json.loads(out)["error"]


# --- set handler ---------------------------------------------------------------


def test_set_sends_logger_and_level_and_prints_new_state(monkeypatch):
    recorder = _Recorder(rest.RequestResult(status_code=200, data=DEBUG_ENTRY)).install(monkeypatch)

    code, out = _run(logging_level.set_logging_level, _args(logger="com.ericsson.bss", level="DEBUG"))

    assert code is None
    assert ("put", "com.ericsson.bss", "DEBUG") in recorder.calls
    assert out.splitlines()[1].split() == ["com.ericsson.bss", "DEBUG", "DEBUG"]


def test_set_reset_sends_reset_and_shows_inherited_state(monkeypatch):
    recorder = _Recorder(rest.RequestResult(status_code=200, data=INHERITING)).install(monkeypatch)

    code, out = _run(logging_level.set_logging_level, _args(logger="com.ericsson.bss.Foo", level="reset"))

    assert code is None
    assert ("put", "com.ericsson.bss.Foo", "reset") in recorder.calls
    assert out.splitlines()[1].split() == ["com.ericsson.bss.Foo", "-", "DEBUG"]


def test_set_rejected_by_agent_exits_non_zero(monkeypatch):
    _Recorder(rest.RequestResult(status_code=400, message="Invalid level 'LOUD'")).install(monkeypatch)

    code, out = _run(logging_level.set_logging_level, _args(logger="com.x", level="DEBUG"))

    assert code == 1
    assert "Invalid level" in out


def test_set_json_output(monkeypatch):
    _Recorder(rest.RequestResult(status_code=200, data=DEBUG_ENTRY)).install(monkeypatch)

    code, out = _run(logging_level.set_logging_level, _args(logger="com.ericsson.bss", level="DEBUG", output="json"))

    assert code is None
    assert json.loads(out)["logging"] == DEBUG_ENTRY


# --- rest.LoggingRequest -------------------------------------------------------


def test_logging_request_urls_and_bodies(monkeypatch):
    captured = []

    def fake_request(self, url, method="GET", body=None, headers=None):  # pylint: disable=unused-argument
        captured.append((url, method, body, headers))
        return rest.RequestResult(status_code=200, data={})

    monkeypatch.setattr(rest.RestRequest, "request", fake_request)
    request = rest.LoggingRequest(base_url="http://localhost:8080")

    request.get()
    request.get("ROOT")
    request.put("com.x.Y", "DEBUG")
    request.put("com.x.Y", "reset")

    assert captured[0][:2] == ("repair-management/v2/logging", "GET")
    assert captured[1][:2] == ("repair-management/v2/logging/ROOT", "GET")
    assert captured[2][:3] == ("repair-management/v2/logging/com.x.Y", "PUT", {"level": "DEBUG"})
    assert captured[2][3] == {"Content-Type": "application/json"}
    assert captured[3][2] == {"level": "reset"}


def test_logging_request_quotes_logger_so_it_cannot_alter_the_path(monkeypatch):
    captured = []
    monkeypatch.setattr(
        rest.RestRequest, "request", lambda self, url, *a, **k: captured.append(url) or rest.RequestResult(200, {})
    )

    rest.LoggingRequest(base_url="http://h").get("a/../../config?x=1#f")

    assert captured == ["repair-management/v2/logging/a%2F..%2F..%2Fconfig%3Fx%3D1%23f"]


# --- end to end against a real HTTP server -------------------------------------


class _Server:
    """A tiny HTTP server standing in for the agent, so the real request/exit-code path is exercised."""

    def __init__(self, status, body):
        outer = self
        self.requests = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # pylint: disable=arguments-differ
                pass

            def _respond(self):
                length = int(self.headers.get("Content-Length") or 0)
                outer.requests.append((self.command, self.path, self.rfile.read(length).decode() if length else ""))
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_PUT = _respond  # noqa: N815

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:{0}".format(self.httpd.server_port)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def test_end_to_end_set_ok():
    server = _Server(200, DEBUG_ENTRY)
    try:
        code, out = _run(
            logging_level.set_logging_level, _args(url=server.url, logger="com.ericsson.bss", level="DEBUG")
        )
    finally:
        server.close()

    assert code is None
    assert server.requests == [("PUT", "/repair-management/v2/logging/com.ericsson.bss", '{"level": "DEBUG"}')]
    assert "DEBUG" in out


def test_end_to_end_agent_400_gives_clear_message_and_non_zero_exit():
    server = _Server(400, {"error": "Invalid level 'LOUD', must be one of TRACE, DEBUG"})
    try:
        code, out = _run(logging_level.set_logging_level, _args(url=server.url, logger="com.x", level="DEBUG"))
    finally:
        server.close()

    assert code == 1
    assert "Invalid level 'LOUD'" in out
    assert '{"error"' not in out


def test_end_to_end_unreachable_agent_exits_non_zero():
    code, out = _run(logging_level.get_logging_level, _args(url="http://127.0.0.1:1", timeout=2.0))

    assert code == 1
    assert "Unable to connect" in out

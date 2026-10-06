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

"""Tests for ecctool metrics subcommand — passthrough and client-side name filter."""
import io
import os
import sys
from contextlib import redirect_stdout
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pylib"))

from ecchronoslib import rest  # pylint: disable=wrong-import-position


SAMPLE_SCRAPE = """# HELP ecc_scheduler_lock_latency_seconds Lock acquisition latency.
# TYPE ecc_scheduler_lock_latency_seconds histogram
ecc_scheduler_lock_latency_seconds_bucket{le="0.1"} 12
ecc_scheduler_lock_latency_seconds_sum 3.4
ecc_scheduler_lock_latency_seconds_count 12
# HELP jvm_memory_used_bytes Used memory.
# TYPE jvm_memory_used_bytes gauge
jvm_memory_used_bytes{area="heap"} 1000
# EOF
"""


def test_filter_metrics_text_no_filter_returns_text_unchanged():
    from ecctool import filter_metrics_text  # pylint: disable=import-outside-toplevel

    assert filter_metrics_text(SAMPLE_SCRAPE) == SAMPLE_SCRAPE


def test_filter_metrics_text_substring_match():
    from ecctool import filter_metrics_text  # pylint: disable=import-outside-toplevel

    result = filter_metrics_text(SAMPLE_SCRAPE, names=["saturation"])
    # Nothing matches, but the OpenMetrics end marker is preserved.
    assert result == "# EOF\n"

    result = filter_metrics_text(SAMPLE_SCRAPE, names=["jvm"])
    assert 'jvm_memory_used_bytes{area="heap"} 1000' in result
    assert "ecc_scheduler_lock_latency" not in result
    # HELP/TYPE of the match are kept, others are dropped.
    assert "# HELP jvm_memory_used_bytes" in result
    assert "# TYPE jvm_memory_used_bytes" in result
    assert "# HELP ecc_scheduler_lock_latency_seconds" not in result


def test_filter_metrics_text_case_insensitive():
    from ecctool import filter_metrics_text  # pylint: disable=import-outside-toplevel

    result = filter_metrics_text(SAMPLE_SCRAPE, names=["JVM_Memory"])
    assert "jvm_memory_used_bytes" in result


def test_filter_metrics_text_dot_underscore_equivalent():
    from ecctool import filter_metrics_text  # pylint: disable=import-outside-toplevel

    dotted = filter_metrics_text(SAMPLE_SCRAPE, names=["lock.latency"])
    underscored = filter_metrics_text(SAMPLE_SCRAPE, names=["lock_latency"])
    assert "ecc_scheduler_lock_latency_seconds_bucket" in dotted
    assert dotted == underscored


def test_filter_metrics_text_multiple_names_are_or_combined():
    from ecctool import filter_metrics_text  # pylint: disable=import-outside-toplevel

    result = filter_metrics_text(SAMPLE_SCRAPE, names=["lock.latency", "jvm"])
    assert "ecc_scheduler_lock_latency_seconds_count" in result
    assert "jvm_memory_used_bytes" in result


def test_filter_metrics_text_raw_suppresses_comments():
    from ecctool import filter_metrics_text  # pylint: disable=import-outside-toplevel

    result = filter_metrics_text(SAMPLE_SCRAPE, include_comments=False)
    assert "# HELP" not in result
    assert "# TYPE" not in result
    assert "# EOF" not in result
    assert "jvm_memory_used_bytes" in result


def test_filter_metrics_text_raw_with_name_filter():
    from ecctool import filter_metrics_text  # pylint: disable=import-outside-toplevel

    result = filter_metrics_text(SAMPLE_SCRAPE, names=["jvm"], include_comments=False)
    assert result.strip() == 'jvm_memory_used_bytes{area="heap"} 1000'


def test_filter_metrics_text_eof_kept_with_filter():
    from ecctool import filter_metrics_text  # pylint: disable=import-outside-toplevel

    result = filter_metrics_text(SAMPLE_SCRAPE, names=["jvm"])
    assert "# EOF" in result


def test_metrics_command_parses_flags():
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    args = get_parser().parse_args(["metrics", "--name", "lock.latency", "--format", "openmetrics", "--raw"])
    assert args.subcommand == "metrics"
    assert args.name == ["lock.latency"]
    assert args.format == "openmetrics"
    assert args.raw is True


def test_metrics_command_defaults():
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    args = get_parser().parse_args(["metrics"])
    assert args.name is None
    assert args.format == "prometheus"
    assert args.raw is False
    assert args.url is None


def test_metrics_command_rejects_bad_format():
    from ecctool import get_parser  # pylint: disable=import-outside-toplevel

    with pytest.raises(SystemExit):
        get_parser().parse_args(["metrics", "--format", "json"])


def _metrics_args(**overrides):
    base = {"url": None, "name": None, "format": "prometheus", "raw": False}
    base.update(overrides)
    return SimpleNamespace(**base)


def test_metrics_prints_scrape_passthrough(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    captured = {}

    class FakeMetricsRequest:
        def __init__(self, base_url=None):
            captured["base_url"] = base_url

        def get_metrics(self, open_metrics=False):
            captured["open_metrics"] = open_metrics
            return SAMPLE_SCRAPE

    monkeypatch.setattr(ecctool.rest, "MetricsRequest", FakeMetricsRequest)
    output = io.StringIO()
    with redirect_stdout(output):
        ecctool.metrics(_metrics_args())
    assert output.getvalue() == SAMPLE_SCRAPE
    assert captured == {"base_url": None, "open_metrics": False}


def test_metrics_applies_name_filter_and_raw(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    class FakeMetricsRequest:
        def __init__(self, base_url=None):
            pass

        def get_metrics(self, open_metrics=False):
            return SAMPLE_SCRAPE

    monkeypatch.setattr(ecctool.rest, "MetricsRequest", FakeMetricsRequest)
    output = io.StringIO()
    with redirect_stdout(output):
        ecctool.metrics(_metrics_args(name=["lock.latency"], raw=True))
    rendered = output.getvalue()
    assert "ecc_scheduler_lock_latency_seconds_sum 3.4" in rendered
    assert "#" not in rendered
    assert "jvm_memory_used_bytes" not in rendered


def test_metrics_openmetrics_format_requested(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel

    captured = {}

    class FakeMetricsRequest:
        def __init__(self, base_url=None):
            pass

        def get_metrics(self, open_metrics=False):
            captured["open_metrics"] = open_metrics
            return SAMPLE_SCRAPE

    monkeypatch.setattr(ecctool.rest, "MetricsRequest", FakeMetricsRequest)
    with redirect_stdout(io.StringIO()):
        ecctool.metrics(_metrics_args(format="openmetrics"))
    assert captured == {"open_metrics": True}


def test_metrics_disabled_reports_friendly_404(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel
    from urllib.error import HTTPError  # pylint: disable=import-outside-toplevel

    class FakeMetricsRequest:
        def __init__(self, base_url=None):
            pass

        def get_metrics(self, open_metrics=False):
            return rest.RequestResult(
                status_code=404,
                message="Unable to retrieve resource http://localhost:8080/metrics",
                exception=HTTPError("http://localhost:8080/metrics", 404, "Not Found", None, None),
            )

    monkeypatch.setattr(ecctool.rest, "MetricsRequest", FakeMetricsRequest)
    output = io.StringIO()
    with redirect_stdout(output):
        with pytest.raises(SystemExit) as excinfo:
            ecctool.metrics(_metrics_args())
    assert excinfo.value.code == 1
    assert "statistics.enabled" in output.getvalue()


def test_metrics_connection_error_reports_exception(monkeypatch):
    import ecctool  # pylint: disable=import-outside-toplevel
    from urllib.error import URLError  # pylint: disable=import-outside-toplevel

    class FakeMetricsRequest:
        def __init__(self, base_url=None):
            pass

        def get_metrics(self, open_metrics=False):
            return rest.RequestResult(
                status_code=404,
                message="Unable to connect to http://localhost:8080/metrics",
                exception=URLError("Connection refused"),
            )

    monkeypatch.setattr(ecctool.rest, "MetricsRequest", FakeMetricsRequest)
    output = io.StringIO()
    with redirect_stdout(output):
        with pytest.raises(SystemExit) as excinfo:
            ecctool.metrics(_metrics_args())
    assert excinfo.value.code == 1
    assert "Unable to connect" in output.getvalue()


def test_status_tolerates_missing_output_flag(monkeypatch):
    # 'metrics' defines no -o/--output flag; the status preflight must not crash on it.
    import ecctool  # pylint: disable=import-outside-toplevel

    class FakeSchedulerRequest:
        def __init__(self, base_url=None):
            pass

        def list_schedules(self):
            return rest.RequestResult(status_code=200, data=[])

    monkeypatch.setattr(ecctool.rest, "RepairSchedulerRequest", FakeSchedulerRequest)
    ecctool.status(_metrics_args())  # must not raise AttributeError


def test_metrics_request_uses_prometheus_accept_by_default(monkeypatch):
    captured = {}

    def fake_basic_request(self, url, method="GET", headers=None):
        captured["url"] = url
        captured["headers"] = headers
        return "ok"

    monkeypatch.setattr(rest.RestRequest, "basic_request", fake_basic_request)
    assert rest.MetricsRequest(base_url="http://localhost:8080").get_metrics() == "ok"
    assert captured["url"] == "metrics"
    assert captured["headers"]["Accept"].startswith("text/plain")


def test_metrics_request_openmetrics_accept(monkeypatch):
    captured = {}

    def fake_basic_request(self, url, method="GET", headers=None):
        captured["headers"] = headers
        return "ok"

    monkeypatch.setattr(rest.RestRequest, "basic_request", fake_basic_request)
    rest.MetricsRequest().get_metrics(open_metrics=True)
    assert "openmetrics" in captured["headers"]["Accept"]


def test_basic_request_forwards_headers(monkeypatch):
    captured = {}

    class FakeResponse:
        def read(self):
            return b"data"

        def close(self):
            pass

    def fake_urlopen(request, context=None):  # pylint: disable=unused-argument
        captured["accept"] = request.get_header("Accept")
        return FakeResponse()

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    result = rest.RestRequest(base_url="http://localhost:8080").basic_request(
        "metrics", headers={"Accept": "application/openmetrics-text"}
    )
    assert result == "data"
    assert captured["accept"] == "application/openmetrics-text"

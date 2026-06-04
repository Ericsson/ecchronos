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

"""Tests for ecctool config subcommand — duration parsing and CLI arg handling."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bin"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pylib"))


def test_parse_duration_ms_seconds():
    from ecctool import parse_duration_ms
    assert parse_duration_ms("30s") == 30000


def test_parse_duration_ms_minutes():
    from ecctool import parse_duration_ms
    assert parse_duration_ms("5m") == 300000


def test_parse_duration_ms_hours():
    from ecctool import parse_duration_ms
    assert parse_duration_ms("2h") == 7200000


def test_parse_duration_ms_milliseconds():
    from ecctool import parse_duration_ms
    assert parse_duration_ms("500ms") == 500


def test_parse_duration_ms_raw_int():
    from ecctool import parse_duration_ms
    assert parse_duration_ms("300000") == 300000


def test_parse_duration_ms_one_second():
    from ecctool import parse_duration_ms
    assert parse_duration_ms("1s") == 1000


def test_parse_duration_ms_zero():
    from ecctool import parse_duration_ms
    assert parse_duration_ms("0") == 0


def test_parse_duration_ms_invalid_format():
    from ecctool import parse_duration_ms
    try:
        parse_duration_ms("abc")
        assert False, "Should have raised SystemExit"
    except SystemExit as e:
        assert e.code == 1


def test_parse_duration_ms_negative():
    from ecctool import parse_duration_ms
    try:
        parse_duration_ms("-5m")
        assert False, "Should have raised SystemExit"
    except SystemExit as e:
        assert e.code == 1

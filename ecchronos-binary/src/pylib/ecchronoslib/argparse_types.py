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

"""argparse ``type=`` helpers shared by the ecctool CLI."""

from __future__ import print_function

import sys
from argparse import ArgumentTypeError


def comma_separated_ints(value):
    """Parse comma-separated integers for columns specification."""
    try:
        return [int(x.strip()) for x in value.split(",")]
    except ValueError as exc:
        raise ArgumentTypeError(f"'{value}' is not a valid comma-separated list of integers") from exc


def parse_duration_ms(value):
    """Parse duration string (e.g. '5m', '30s', '300000') to milliseconds."""
    try:
        if value.endswith("ms"):
            result = int(value[:-2])
        elif value.endswith("s"):
            result = int(value[:-1]) * 1000
        elif value.endswith("m"):
            result = int(value[:-1]) * 60 * 1000
        elif value.endswith("h"):
            result = int(value[:-1]) * 3600 * 1000
        else:
            result = int(value)
    except ValueError:
        print(f"Invalid duration format: '{value}'. Use e.g. 5m, 30s, 2h, 500ms, or raw milliseconds.")
        sys.exit(1)
    if result < 0:
        print(f"Duration must not be negative: '{value}'")
        sys.exit(1)
    return result

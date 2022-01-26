#
# Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import re
import os
from behave import given  # pylint: disable=no-name-in-module


def strip_and_collapse(line):
    return re.sub(' +', ' ', line.rstrip().lstrip())


def match_and_remove_row(rows, expected_row):
    row_idx = -1
    found_row = None

    for idx, row in enumerate(rows):
        row = strip_and_collapse(row)
        if re.match(expected_row, row):
            found_row = row
            row_idx = int(idx)
            break

    assert row_idx != -1, "{0} not found in {1}".format(expected_row, rows)
    del rows[row_idx]
    return found_row


def validate_header(header, expected_main_header):
    assert len(header) == 3, header

    assert header[0] == len(header[0]) * header[0][0], header[0]  # -----

    header[1] = strip_and_collapse(header[1])
    assert header[1] == expected_main_header, header[1]

    assert header[2] == len(header[2]) * header[2][0], header[2]  # -----


def validate_last_table_row(rows):
    assert len(rows) == 1, "Expecting last element to be '---' in {0}".format(rows)
    assert rows[0] == len(rows[0]) * rows[0][0], rows[0]  # -----
    assert len(rows) == 1, "{0} not empty".format(rows)

@given(u'we have access to ecctool')
def step_init(context):
    assert context.config.userdata.get("ecctool") is not False
    assert os.path.isfile(context.config.userdata.get("ecctool"))

#
# Copyright 2019 Telefonaktiebolaget LM Ericsson
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

from behave import given, when, then, step
import os
import re
from subprocess import Popen, PIPE


def run_ecc_config(context, params):
    cmd = [context.config.userdata.get("ecc-config")] + params
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    (context.out, context.err) = context.proc.communicate()


def table_row(keyspace, table):
    duration = "\\d+ day\\(s\\) \\d+h \\d+m \\d+s"
    return "\\| {0} \\| {1} \\| {dur} \\| PARALLEL | \\d+[.]\\d+ \\| {dur} \\| {dur} \\|".format(keyspace, table, dur=duration)


def strip_and_collapse(line):
    return re.sub(' +', ' ', line.rstrip().lstrip())


@given(u'we have access to ecc-config')
def step_init(context):
    assert context.config.userdata.get("ecc-config") is not False
    assert os.path.isfile(context.config.userdata.get("ecc-config"))
    pass


@when(u'we list config')
def step_list_tables(context):
    run_ecc_config(context, [])

    output_data = context.out.lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:]
    pass


@then(u'the config output should contain a valid header')
def step_validate_list_tables_header(context):
    header = context.header

    assert len(header) == 3, header

    assert header[0] == len(header[0]) * header[0][0], header[0]  # -----

    header[1] = strip_and_collapse(header[1])
    assert header[1] == "| Keyspace | Table | Interval | Parallelism | Unwind ratio | Warning time | Error time |", header[1]

    assert header[2] == len(header[2]) * header[2][0], header[2]  # -----
    pass


@then(u'the config output should contain a row for {keyspace}.{table}')
def step_validate_list_tables_row(context, keyspace, table):
    expected_row = table_row(keyspace, table)

    found_row = -1

    for idx, row in enumerate(context.rows):
        row = strip_and_collapse(row)
        if re.match(expected_row, row):
            found_row = int(idx)
            break

    assert found_row != -1, "{0} not found in {1}".format(expected_row, context.rows)
    del context.rows[found_row]
    pass


@then(u'the config output should not contain more rows')
def step_validate_list_rows_clear(context):
    rows = context.rows

    assert len(rows) == 1, "Expecting last element to be '---' in {0}".format(rows)
    assert rows[0] == len(rows[0]) * rows[0][0], rows[0]  # -----
    assert len(rows) == 1, "{0} not empty".format(rows)
    pass

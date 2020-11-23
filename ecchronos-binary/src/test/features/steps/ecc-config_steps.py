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


NAME_PATTERN = r'[a-zA-Z0-9]+'
DURATION_PATTERN = r'\d+ day\(s\) \d+h \d+m \d+s'
UNWIND_RATIO_PATTERN = r'\d+[.]\d+'


def run_ecc_config(context, params):
    cmd = [context.config.userdata.get("ecc-config")] + params
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    (context.out, context.err) = context.proc.communicate()


def table_row(keyspace, table):
    return table_pattern(keyspace, table)


def table_pattern(keyspace=NAME_PATTERN, table=NAME_PATTERN, interval=DURATION_PATTERN, unwind_ratio=UNWIND_RATIO_PATTERN,
                  warn_time=DURATION_PATTERN, error_time=DURATION_PATTERN):
    return r'\| .* \| {keyspace} \| {table} \| {interval} \| PARALLEL | {unwind} \| {warn} \| {error} \|'\
        .format(keyspace=keyspace, table=table, interval=interval, unwind=unwind_ratio,
                warn=warn_time, error=error_time)


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


@when(u'we list config for keyspace {keyspace} and table {table}')
def step_list_tables_for_keyspace(context, keyspace, table):
    run_ecc_config(context, [keyspace, table])

    output_data = context.out.lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:]
    pass


@when(u'we list config for keyspace {keyspace}')
def step_list_tables_for_keyspace(context, keyspace):
    run_ecc_config(context, [keyspace])

    output_data = context.out.lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:]
    pass

@when(u'we list a specific config for keyspace {keyspace} and table {table}')
def step_list_specific_config(context, keyspace, table):
    run_ecc_config(context, [keyspace, table])
    id = re.search('[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}', context.out).group(0)
    run_ecc_config(context, ['--id', id])
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
    assert header[1] == "| Id | Keyspace | Table | Interval | Parallelism | Unwind ratio | Warning time | Error time |", header[1]

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
    context.last_row = strip_and_collapse(context.rows[found_row])
    del context.rows[found_row]
    pass


@then(u'the repair interval is {interval}')
def step_validate_interval(context, interval):
    row = context.last_row
    assert row is not None

    expected_row = table_pattern(interval=re.escape(interval))
    assert re.match(expected_row, row), row


@then(u'the unwind ratio is {unwind_ratio}')
def step_validate_unwind_ratio(context, unwind_ratio):
    row = context.last_row
    assert row is not None

    expected_row = table_pattern(unwind_ratio=re.escape(unwind_ratio))
    assert re.match(expected_row, row), row


@then(u'the warning time is {warn_time}')
def step_validate_warn_time(context, warn_time):
    row = context.last_row
    assert row is not None

    expected_row = table_pattern(warn_time=re.escape(warn_time))
    assert re.match(expected_row, row), row


@then(u'the error time is {error_time}')
def step_validate_error_time(context, error_time):
    row = context.last_row
    assert row is not None

    expected_row = table_pattern(error_time=re.escape(error_time))
    assert re.match(expected_row, row), row


@then(u'the config output should not contain more rows')
def step_validate_list_rows_clear(context):
    rows = context.rows

    assert len(rows) == 1, "Expecting last element to be '---' in {0}".format(rows)
    assert rows[0] == len(rows[0]) * rows[0][0], rows[0]  # -----
    assert len(rows) == 1, "{0} not empty".format(rows)
    pass

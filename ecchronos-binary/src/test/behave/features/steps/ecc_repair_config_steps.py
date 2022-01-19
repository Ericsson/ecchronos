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

import re
from subprocess import Popen, PIPE
from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common_steps import match_and_remove_row, validate_header, validate_last_table_row

NAME_PATTERN = r'[a-zA-Z0-9]+'
DURATION_PATTERN = r'\d+ day\(s\) \d+h \d+m \d+s'
UNWIND_RATIO_PATTERN = r'\d+[.]\d+'
ID_PATTERN = r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'

CONFIG_HEADER = r'| Id | Keyspace | Table | Interval | Parallelism | Unwind ratio | Warning time | Error time |'


def run_ecc_repair_config(context, params):
    cmd = [context.config.userdata.get("ecc")] + ["repair-config"] + params
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE) # pylint: disable=consider-using-with
    (context.out, context.err) = context.proc.communicate()


def table_row(keyspace, table):
    return table_pattern(keyspace, table)


def table_pattern(keyspace=NAME_PATTERN,  # pylint: disable=too-many-arguments
                  table=NAME_PATTERN,
                  interval=DURATION_PATTERN,
                  unwind_ratio=UNWIND_RATIO_PATTERN,
                  warn_time=DURATION_PATTERN,
                  error_time=DURATION_PATTERN):
    return r'\| .* \| {keyspace} \| {table} \| {interval} \| PARALLEL \| {unwind} \| {warn} \| {error} \|'\
        .format(keyspace=keyspace, table=table, interval=interval, unwind=unwind_ratio,
                warn=warn_time, error=error_time)


@when(u'we list config')
def step_list_configs(context):
    run_ecc_repair_config(context, [])

    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:]


@when(u'we list config for keyspace {keyspace} and table {table}')
def step_list_config_for_table(context, keyspace, table):
    run_ecc_repair_config(context, ['--keyspace', keyspace, '--table', table])

    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:]


@when(u'we list config for keyspace {keyspace}')
def step_list_configs_for_keyspace(context, keyspace):
    run_ecc_repair_config(context, ['--keyspace', keyspace])

    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:]

@when(u'we list a specific config for keyspace {keyspace} and table {table}')
def step_list_specific_config(context, keyspace, table):
    run_ecc_repair_config(context, ['--keyspace', keyspace, '--table', table])
    job_id = re.search(ID_PATTERN, context.out.decode('ascii')).group(0)
    run_ecc_repair_config(context, ['--id', job_id])
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')

    context.header = output_data[0:3]
    context.rows = output_data[3:]


@then(u'the config output should contain a valid header')
def step_validate_list_tables_header(context):
    validate_header(context.header, CONFIG_HEADER)


@then(u'the config output should contain a row for {keyspace}.{table}')
def step_validate_list_tables_row(context, keyspace, table):
    expected_row = table_row(keyspace, table)
    context.last_row = match_and_remove_row(context.rows, expected_row)


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
    validate_last_table_row(context.rows)

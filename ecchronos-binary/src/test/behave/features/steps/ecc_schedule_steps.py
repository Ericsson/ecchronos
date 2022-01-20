#
# Copyright 2022 Telefonaktiebolaget LM Ericsson
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

import os
from subprocess import Popen, PIPE
from behave import given, when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common_steps import match_and_remove_row, validate_header, validate_last_table_row  # pylint: disable=line-too-long


TABLE_ROW_FORMAT_PATTERN = r'\| .* \| {0} \| {1} \| (COMPLETED|IN_QUEUE|WARNING|ERROR) \| \d+[.]\d+ \| .* \| .* \|'
TABLE_HEADER = r'| Id | Keyspace | Table | Status | Repaired(%) | Completed at | Next repair | Recurring |'


def run_ecc_schedule(context, params):
    cmd = [context.config.userdata.get("ecc-schedule")] + params
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE) # pylint: disable=consider-using-with
    (context.out, context.err) = context.proc.communicate()


def table_row(keyspace, table):
    return TABLE_ROW_FORMAT_PATTERN.format(keyspace, table)


@given(u'we have access to ecc-schedule')
def step_init(context):
    assert context.config.userdata.get("ecc-schedule") is not False
    assert os.path.isfile(context.config.userdata.get("ecc-schedule"))


@when(u'we schedule repair for keyspace {keyspace} and table {table}')
def step_schedule_repair(context, keyspace, table):
    run_ecc_schedule(context, ['--keyspace', keyspace, '--table', table])

    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:]


@then(u'the schedule output should contain a valid header')
def step_validate_list_tables_header(context):
    validate_header(context.header, TABLE_HEADER)


@then(u'the schedule output should contain a row for {keyspace}.{table}')
def step_validate_list_tables_row(context, keyspace, table):
    expected_row = table_row(keyspace, table)
    match_and_remove_row(context.rows, expected_row)


@then(u'the schedule output should not contain more rows')
def step_validate_list_rows_clear(context):
    validate_last_table_row(context.rows)

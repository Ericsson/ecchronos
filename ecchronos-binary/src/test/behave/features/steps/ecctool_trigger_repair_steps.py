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

from subprocess import Popen, PIPE
from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common_steps import match_and_remove_row, validate_header  # pylint: disable=line-too-long


TABLE_ROW_FORMAT_PATTERN = r'\| .* \| {0} \| {1} \| (COMPLETED|IN_QUEUE|WARNING|ERROR) \| \d+[.]\d+ \| .* \| .* \|'
TABLE_HEADER = r'| Id | Keyspace | Table | Status | Repaired(%) | Completed at | Next repair | Recurring |'


def run_ecc_trigger_repair(context, params):
    cmd = [context.config.userdata.get("ecctool")] + ["trigger-repair"] + params
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE) # pylint: disable=consider-using-with
    (context.out, context.err) = context.proc.communicate()


def table_row(keyspace, table):
    return TABLE_ROW_FORMAT_PATTERN.format(keyspace, table)


@when(u'we trigger repair for keyspace {keyspace} and table {table}')
def step_schedule_repair(context, keyspace, table):
    run_ecc_trigger_repair(context, ['--keyspace', keyspace, '--table', table])

    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.deprecated = output_data[0:1]
    context.header = output_data[1:4]
    context.rows = output_data[4:]

@then(u'the trigger repair output should contain a valid header')
def step_validate_list_tables_header(context):
    validate_header(context.header, TABLE_HEADER)


@then(u'the trigger repair output should contain a row for {keyspace}.{table}')
def step_validate_list_tables_row(context, keyspace, table):
    expected_row = table_row(keyspace, table)
    match_and_remove_row(context.rows, expected_row)

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

import re
from subprocess import Popen, PIPE
from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common_steps import match_and_remove_row, validate_header


ID_PATTERN = r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'

TABLE_REPAIR_HEADER = r'| Id | Host Id | Keyspace | Table | Status | Repaired(%) | Completed at |'
TABLE_REPAIR_ROW_FORMAT_PATTERN = r'\| .* \| .* \| {0} \| {1} \| (COMPLETED|IN_QUEUE|WARNING|ERROR) \| \d+[.]\d+ \|'


def run_ecc_repair_status(context, params):
    cmd = [context.config.userdata.get("ecctool")] + ["repairs"] + params
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE) # pylint: disable=consider-using-with
    (context.out, context.err) = context.proc.communicate()


def table_row(keyspace, table):
    return TABLE_REPAIR_ROW_FORMAT_PATTERN.format(keyspace, table)


def handle_repair_output(context):
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:-1]
    context.summary = output_data[-1:]


@when(u'we list all repairs')
def step_list_repairs(context):
    run_ecc_repair_status(context, [])
    handle_repair_output(context)


@when(u'we list all repairs with limit of {limit}')
def step_list_repairs_with_limit(context, limit):
    run_ecc_repair_status(context, ['--limit', limit])
    handle_repair_output(context)


@when(u'we list all repairs for keyspace {keyspace} with a limit of {limit}')
def step_list_repairs_for_keyspace_with_limit(context, keyspace, limit):
    run_ecc_repair_status(context, ['--keyspace', keyspace, '--limit', limit])
    handle_repair_output(context)


@when(u'we list all repairs for keyspace {keyspace}')
def step_list_repairs_for_keyspace(context, keyspace):
    run_ecc_repair_status(context, ['--keyspace', keyspace])
    handle_repair_output(context)


@when(u'we list repairs {keyspace}.{table} with a limit of {limit}')
def step_show_repair_with_limit(context, keyspace, table, limit):
    run_ecc_repair_status(context, ['--keyspace', keyspace, '--table', table])
    job_id = re.search(ID_PATTERN, context.out.decode('ascii')).group(0)
    assert job_id
    run_ecc_repair_status(context, ['--id', job_id, '--limit', limit])
    handle_repair_output(context)


@when(u'we list repairs for table {keyspace}.{table}')
def step_show_repair(context, keyspace, table):
    run_ecc_repair_status(context, ['--keyspace', keyspace, '--table', table])
    handle_repair_output(context)


@when(u'we list repairs for hostid and table {keyspace}.{table}')
def step_show_repair_with_nodeid(context, keyspace, table):
    run_ecc_repair_status(context, ['--keyspace', keyspace, '--table', table,
                                    '--hostid', '{0}'.format(context.environment.host_id)])
    handle_repair_output(context)


@then(u'the output should contain a valid repair header')
def step_validate_list_tables_header(context):
    validate_header(context.header, TABLE_REPAIR_HEADER)


@then(u'the output should contain a repair row for {keyspace}.{table}')
def step_validate_list_tables_row(context, keyspace, table):
    expected_row = table_row(keyspace, table)
    match_and_remove_row(context.rows, expected_row)


@then(u'the output should contain {limit:d} repair rows')
def step_validate_list_repairs_contains_rows_with_limit(context, limit):
    rows = context.rows

    assert len(rows) == limit + 1, "Expecting only {0} table element from {1}".format(limit, rows)

    for _ in range(limit):
        step_validate_list_tables_row(context, ".*", ".*")

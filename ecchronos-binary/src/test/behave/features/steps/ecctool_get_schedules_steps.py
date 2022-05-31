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
from ecc_step_library.common_steps import match_and_remove_row, strip_and_collapse, validate_header  # pylint: disable=line-too-long


ID_PATTERN = r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
SCHEDULE_SUMMARY = r'Summary: \d+ completed, \d+ on time, \d+ blocked, \d+ late, \d+ overdue'

TABLE_SCHEDULE_HEADER = r'| Id | Keyspace | Table | Status | Repaired(%) | Completed at | Next repair |'
TABLE_SNAPSHOT_HEADER = r'Snapshot as of .*'
TABLE_SCHEDULE_ROW_FORMAT_PATTERN = r'\| .* \| {0} \| {1} \| (COMPLETED|ON_TIME|LATE|OVERDUE) \| \d+[.]\d+ \| .* \|'


def run_ecc_schedule_status(context, params):
    cmd = [context.config.userdata.get("ecctool")] + ["schedules"] + params
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE) # pylint: disable=consider-using-with
    (context.out, context.err) = context.proc.communicate()

def handle_schedule_output(context):
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.snapshot = output_data[0:1]
    context.header = output_data[1:4]
    context.rows = output_data[4:-1]
    context.summary = output_data[-1:]

@when(u'we list all schedules')
def step_list_schedules(context):
    run_ecc_schedule_status(context, [])
    handle_schedule_output(context)

@when(u'we list all schedules with a limit of {limit}')
def step_list_schedules_with_limit(context, limit):
    run_ecc_schedule_status(context, ['--limit', limit])
    handle_schedule_output(context)

@when(u'we list all schedules for keyspace {keyspace} with a limit of {limit}')
def step_list_schedules_for_keyspace_with_limit(context, keyspace, limit):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--limit', limit])
    handle_schedule_output(context)

@when(u'we list all schedules for keyspace {keyspace}')
def step_list_schedules_for_keyspace(context, keyspace):
    run_ecc_schedule_status(context, ['--keyspace', keyspace])
    handle_schedule_output(context)

@when(u'we list schedules {keyspace}.{table} with a limit of {limit}')
def step_list_schedule_with_limit(context, keyspace, table, limit):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--table', table])
    job_id = re.search(ID_PATTERN, context.out.decode('ascii')).group(0)
    assert job_id
    run_ecc_schedule_status(context, ['--id', job_id, '--limit', limit])
    handle_schedule_output(context)

@when(u'we list schedules for table {keyspace}.{table}')
def step_show_schedule(context, keyspace, table):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--table', table])
    handle_schedule_output(context)

@then(u'the output should contain a schedule row for {keyspace}.{table}')
def step_validate_list_tables_row(context, keyspace, table):
    expected_row = table_row(keyspace, table)
    match_and_remove_row(context.rows, expected_row)

@when(u'we fetch schedule {keyspace}.{table} by id')
def step_show_schedule_with_id(context, keyspace, table):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--table', table])

    job_id = re.search(ID_PATTERN, context.out.decode('ascii')).group(0)
    assert job_id
    run_ecc_schedule_status(context, ['--id', job_id])
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.table_info = output_data[0:7]
    context.conf = output_data[7:8]

@when(u'we show schedule {keyspace}.{table} with a limit of {limit}')
def step_show_schedule_with_limit(context, keyspace, table, limit):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--table', table])

    job_id = re.search(ID_PATTERN, context.out.decode('ascii')).group(0)
    assert job_id
    run_ecc_schedule_status(context, ['--id', job_id, '--limit', limit, '--full'])
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')

    context.table_info = output_data[0:7]
    context.header = output_data[8:9]
    context.rows = output_data[11:]

@then(u'the output should contain a valid schedule summary')
def step_validate_list_schedule_contains_summary(context):
    assert len(context.summary) == 1, "Expecting only 1 row summary"

    summary = context.summary[0]
    assert re.match(SCHEDULE_SUMMARY, summary), "Faulty summary '{0}'".format(summary)

@then(u'the output should contain a valid schedule for {keyspace}.{table}')
def step_validate_list_schedule_contains_rows(context, keyspace, table):
    assert len(context.table_info) == 7, "Expecting 7 rows"
    assert len(context.conf) == 1, "Expecting 1 row"

    step_validate_expected_show_table_header(context, keyspace, table)

@then(u'the output should contain a valid snapshot header')
def step_validate_list_snapshot_header(context):
    match_and_remove_row(context.snapshot, TABLE_SNAPSHOT_HEADER)

@then(u'the output should contain a valid schedule header')
def step_validate_list_schedule_header(context):
    validate_header(context.header, TABLE_SCHEDULE_HEADER)

@then(u'the expected schedule header should be for {keyspace}.{table}')
def step_validate_expected_show_table_header(context, keyspace, table):
    table_info = context.table_info
    assert re.match("Id : .*", strip_and_collapse(table_info[0])), "Faulty Id '{0}'".format(table_info[0])
    assert strip_and_collapse(
        table_info[1]) == "Keyspace : {0}".format(keyspace), "Faulty keyspace '{0}'".format(table_info[1])
    assert strip_and_collapse(
        table_info[2]) == "Table : {0}".format(table), "Faulty table '{0}'".format(table_info[2])
    assert re.match("Status : (COMPLETED|ON_TIME|LATE|OVERDUE)", strip_and_collapse(table_info[3])), \
        "Faulty status '{0}'".format(table_info[3])
    assert re.match("Repaired\\(%\\) : \\d+[.]\\d+", strip_and_collapse(table_info[4])), \
        "Faulty repaired(%) '{0}'".format(table_info[4])
    assert re.match(
        "Completed at : .*", strip_and_collapse(table_info[5])), "Faulty repaired at '{0}'".format(table_info[5])
    assert re.match(
        "Next repair : .*", strip_and_collapse(table_info[6])), "Faulty next repair '{0}'".format(table_info[6])

def table_row(keyspace, table):
    return TABLE_SCHEDULE_ROW_FORMAT_PATTERN.format(keyspace, table)

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
from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common import get_job_id, match_and_remove_row, strip_and_collapse, validate_header, step_validate_list_rows_clear, run_ecctool, table_row  # pylint: disable=line-too-long

SCHEDULE_SUMMARY = r'Summary: \d+ completed, \d+ on time, \d+ blocked, \d+ late, \d+ overdue'

SCHEDULE_HEADER = r'| Id | Keyspace | Table | Status | Repaired(%) | Completed at | Next repair | Repair type |'
SNAPSHOT_HEADER = r'Snapshot as of .*'
SCHEDULE_ROW_FORMAT_PATTERN = r'\| .* \| {0} \| {1} \| (COMPLETED|ON_TIME|LATE|OVERDUE) \| \d+[.]\d+ \| .* \| {2} \|' # pylint: disable=line-too-long


def run_ecc_schedule_status(context, params):
    run_ecctool(context, ["schedules", "-c", "off"] + params)


def handle_schedule_output(context):
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.snapshot = output_data[0:1]
    context.header = output_data[1:4]
    context.rows = output_data[4:-1]
    context.summary = output_data[-1:]


@when('we list all schedules')
def step_list_schedules(context):
    run_ecc_schedule_status(context, [])
    handle_schedule_output(context)


@when('we list all schedules with a limit of {limit}')
def step_list_schedules_with_limit(context, limit):
    run_ecc_schedule_status(context, ['--limit', limit])
    handle_schedule_output(context)


@when('we list all schedules for keyspace {keyspace} with a limit of {limit}')
def step_list_schedules_for_keyspace_with_limit(context, keyspace, limit):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--limit', limit])
    handle_schedule_output(context)


@when('we list all schedules for keyspace {keyspace}')
def step_list_schedules_for_keyspace(context, keyspace):
    run_ecc_schedule_status(context, ['--keyspace', keyspace])
    handle_schedule_output(context)


@when('we list schedules {keyspace}.{table} with a limit of {limit}')
def step_list_schedule_with_limit(context, keyspace, table, limit):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--table', table])
    run_ecc_schedule_status(context, ['--id', get_job_id(context), '--limit', limit])
    handle_schedule_output(context)


@when('we list schedules for table {keyspace}.{table}')
def step_show_schedule(context, keyspace, table):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--table', table])
    handle_schedule_output(context)


@then('the output should contain a schedule row for {keyspace}.{table} with type {repair_type}')
def step_validate_list_tables_row(context, keyspace, table, repair_type):
    expected_row = table_row(SCHEDULE_ROW_FORMAT_PATTERN, keyspace, table, repair_type)
    match_and_remove_row(context.rows, expected_row)


@when('we fetch schedule {keyspace}.{table} by id')
def step_show_schedule_with_id(context, keyspace, table):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--table', table])
    run_ecc_schedule_status(context, ['--id', get_job_id(context)])
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.table_info = output_data[0:8]
    context.conf = output_data[8:9]


@when('we show schedule {keyspace}.{table} with a limit of {limit}')
def step_show_schedule_with_limit(context, keyspace, table, limit):
    run_ecc_schedule_status(context, ['--keyspace', keyspace, '--table', table])
    run_ecc_schedule_status(context, ['--id', get_job_id(context), '--limit', limit, '--full'])
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')

    context.table_info = output_data[0:8]
    context.header = output_data[9:10]
    context.rows = output_data[12:]


@then('the output should contain a valid schedule summary')
def step_validate_list_schedule_contains_summary(context):
    assert len(context.summary) == 1, "Expecting only 1 row summary"

    summary = context.summary[0]
    assert re.match(SCHEDULE_SUMMARY, summary), "Faulty summary '{0}'".format(summary)


@then('the output should contain a valid schedule for {keyspace}.{table} with type {repair_type}')
def step_validate_list_schedule_contains_rows(context, keyspace, table, repair_type):
    assert len(context.table_info) == 8, "Expecting 8 rows"
    assert len(context.conf) == 1, "Expecting 1 row"

    step_validate_expected_show_table_header(context, keyspace, table, repair_type)


@then('the output should contain a valid snapshot header')
def step_validate_list_snapshot_header(context):
    match_and_remove_row(context.snapshot, SNAPSHOT_HEADER)


@then('the output should contain a valid schedule header')
def step_validate_list_schedule_header(context):
    validate_header(context.header, SCHEDULE_HEADER)


@then('the output should contain {limit:d} row')
def step_validate_list_schedules_contains_rows_with_limit(context, limit):
    rows = context.rows

    assert len(rows) == limit + 1, "Expecting only {0} schedule element from {1}".format(limit, rows)

    for _ in range(limit):
        step_validate_list_tables_row(context, ".*", ".*", ".*")

    step_validate_list_rows_clear(context)


@then('the expected schedule header should be for {keyspace}.{table} with type {repair_type}')
def step_validate_expected_show_table_header(context, keyspace, table, repair_type):
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
    assert re.match(
        "Repair type : {0}".format(repair_type), strip_and_collapse(table_info[7])), \
        "Faulty repair type '{0}'".format(table_info[7])


@then('the token list should contain {limit:d} rows')
def step_validate_token_list(context, limit):
    for _ in range(limit):
        remove_token_row(context)

    step_validate_list_rows_clear(context)


def remove_token_row(context):
    expected_row = token_row()

    found_row = -1

    for idx, row in enumerate(context.rows):
        row = strip_and_collapse(row)
        if re.match(expected_row, row):
            found_row = int(idx)
            break

    assert found_row != -1, "{0} not found in {1}".format(expected_row, context.rows)
    del context.rows[found_row]


def token_row():
    return "\\| [-]?\\d+ \\| [-]?\\d+ \\| .* \\| .* \\| (True|False) \\|"

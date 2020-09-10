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
import time
import os
import re
from subprocess import Popen, PIPE


def run_ecc_status(context, params):
    cmd = [context.config.userdata.get("ecc-status")] + params
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    (context.out, context.err) = context.proc.communicate()


def table_row(keyspace, table):
    return "\\| {0} \\| {1} \\| (COMPLETED|IN_QUEUE|WARNING|ERROR) \\| \\d+[.]\\d+ \\| .* \\| .* \\|".format(keyspace, table)


def token_row():
    return "\\| [-]?\\d+ \\| [-]?\\d+ \\| .* \\| .* \\| (True|False) \\|"


def strip_and_collapse(line):
    return re.sub(' +', ' ', line.rstrip().lstrip())


@given(u'we have access to ecc-status')
def step_init(context):
    assert context.config.userdata.get("ecc-status") is not False
    assert os.path.isfile(context.config.userdata.get("ecc-status"))
    pass


@when(u'we list all tables')
def step_list_tables(context):
    run_ecc_status(context, [])

    output_data = context.out.lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:-1]
    context.summary = output_data[-1:]
    pass


@when(u'we list all tables with a limit of {limit}')
def step_list_tables_with_limit(context, limit):
    run_ecc_status(context, ['--limit', limit])

    output_data = context.out.lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:-1]
    context.summary = output_data[-1:]
    pass


@when(u'we list all tables for keyspace {keyspace} with a limit of {limit}')
def step_list_tables_for_keyspace(context, keyspace, limit):
    run_ecc_status(context, [keyspace, '--limit', limit])

    output_data = context.out.lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:-1]
    context.summary = output_data[-1:]
    pass


@when(u'we list all tables for keyspace {keyspace}')
def step_list_tables_for_keyspace(context, keyspace):
    run_ecc_status(context, [keyspace])

    output_data = context.out.lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:-1]
    context.summary = output_data[-1:]
    pass


@when(u'we show job {keyspace}.{table} with a limit of {limit}')
def step_show_table_with_limit(context, keyspace, table, limit):
    run_ecc_status(context, [keyspace, table])
    id = re.search('[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}',context.out).group(0)
    run_ecc_status(context, [keyspace, table, id, '--limit', limit])
    output_data = context.out.lstrip().rstrip().split('\n')

    context.table_info = output_data[0:7]
    context.header = output_data[8:9]
    context.rows = output_data[10:]
    pass

@when(u'we list jobs for table {keyspace}.{table}')
def step_show_table(context, keyspace, table):
    run_ecc_status(context, [keyspace, table])

    output_data = context.out.lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:-1]
    context.summary = output_data[-1:]
    pass


@then(u'the output should contain a valid header')
def step_validate_list_tables_header(context):
    header = context.header

    assert len(header) == 3, header

    assert header[0] == len(header[0]) * header[0][0], header[0]  # -----

    header[1] = strip_and_collapse(header[1])
    assert header[1] == "| Keyspace | Table | Status | Repaired(%) | Repaired at | Next repair | recurring | id |", header[1]

    assert header[2] == len(header[2]) * header[2][0], header[2]  # -----
    pass


@then(u'the output should contain a row for {keyspace}.{table}')
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


@then(u'the output should not contain more rows')
def step_validate_list_rows_clear(context):
    rows = context.rows

    assert len(rows) == 1, "Expecting last element to be '---' in {0}".format(rows)
    assert rows[0] == len(rows[0]) * rows[0][0], rows[0]  # -----
    assert len(rows) == 1, "{0} not empty".format(rows)
    pass


@then(u'the output should contain {limit:d} row')
def step_validate_list_tables_contains_rows(context, limit):
    rows = context.rows

    assert len(rows) == limit + 1, "Expecting only {0} table element from {1}".format(limit, rows)

    for idx in range(limit):
        step_validate_list_tables_row(context, ".*", ".*")

    step_validate_list_rows_clear(context)

    pass


@then(u'the output should contain summary')
def step_validate_list_tables_contains_rows(context):
    assert len(context.summary) == 1, "Expecting only 1 row summary"

    summary = context.summary[0]
    assert re.match("Summary: \d+ completed, \d+ in queue, \d+ warning, \d+ error", summary) , "Faulty summary '{0}'".format(summary)

    pass


@then(u'the expected header should be for {keyspace}.{table}')
def step_validate_expected_show_table_header(context, keyspace, table):
    table_info = context.table_info
    assert strip_and_collapse(table_info[0]) == "Keyspace : {0}".format(keyspace), "Faulty keyspace '{0}'".format(table_info[0])
    assert strip_and_collapse(table_info[1]) == "Table : {0}".format(table), "Faulty table '{0}'".format(table_info[1])
    assert re.match("Status : (COMPLETED|IN_QUEUE|WARNING|ERROR)", strip_and_collapse(table_info[2])), "Faulty status '{0}'".format(table_info[2])
    assert re.match("Repaired\\(%\\) : \\d+[.]\\d+", strip_and_collapse(table_info[3])), "Faulty repaired(%) '{0}'".format(table_info[3])
    assert re.match("Repaired at : .*", strip_and_collapse(table_info[4])), "Faulty repaired at '{0}'".format(table_info[4])
    assert re.match("Next repair : .*", strip_and_collapse(table_info[5])), "Faulty next repair '{0}'".format(table_info[5])

    pass


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


@then(u'the token list should contain {limit:d} rows')
def step_validate_token_list(context, limit):
    for idx in range(limit):
        remove_token_row(context)

    step_validate_list_rows_clear(context)

    pass

@then('the job for {keyspace}.{table} disappears when it is finished')
def verify_job_disappeared(context, keyspace, table):
    id = re.search('[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}', context.response.text).group(0)
    timeout = time.time() + (150)
    output_data = []
    while "Repair job not found" not in output_data:
        run_ecc_status(context, [keyspace, table, id, '--limit', "1"])
        output_data = context.out.lstrip().rstrip().split('\n')
        time.sleep(1)
        assert time.time() < timeout

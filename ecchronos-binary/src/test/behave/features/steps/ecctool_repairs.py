#
# Copyright 2025 Telefonaktiebolaget LM Ericsson
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

from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common import get_job_id, handle_repair_output, step_validate_repair_row, run_ecctool


def run_ecc_repair_status(context, params):
    run_ecctool(context, ["repairs"] + params)


@when("we list all repairs")
def step_list_repairs(context):
    run_ecc_repair_status(context, [])
    handle_repair_output(context)


@when("we list all repairs with limit of {limit}")
def step_list_repairs_with_limit(context, limit):
    run_ecc_repair_status(context, ["--limit", limit])
    handle_repair_output(context)


@when("we list all repairs for keyspace {keyspace} with a limit of {limit}")
def step_list_repairs_for_keyspace_with_limit(context, keyspace, limit):
    run_ecc_repair_status(context, ["--keyspace", keyspace, "--limit", limit])
    handle_repair_output(context)


@when("we list all repairs for keyspace {keyspace}")
def step_list_repairs_for_keyspace(context, keyspace):
    run_ecc_repair_status(context, ["--keyspace", keyspace])
    handle_repair_output(context)


@when("we list repairs {keyspace}.{table} with a limit of {limit}")
def step_show_repair_with_limit(context, keyspace, table, limit):
    run_ecc_repair_status(context, ["--keyspace", keyspace, "--table", table, "--limit", limit])
    handle_repair_output(context)


@when("we list repairs for table {keyspace}.{table}")
def step_show_repair(context, keyspace, table):
    run_ecc_repair_status(context, ["--keyspace", keyspace, "--table", table])
    handle_repair_output(context)


@when("we list repairs for hostid and table {keyspace}.{table}")
def step_show_repair_with_nodeid(context, keyspace, table):
    run_ecc_repair_status(
        context, ["--keyspace", keyspace, "--table", table, "--hostid", "{0}".format(context.environment.host_id)]
    )
    handle_repair_output(context)


@then("the output should contain {limit:d} repair rows")
def step_validate_list_repairs_contains_rows_with_limit(context, limit):
    rows = context.rows

    assert len(rows) == limit + 1, "Expecting only {0} table element from {1}".format(limit, rows)

    for _ in range(limit):
        step_validate_repair_row(context, ".*", ".*", ".*")

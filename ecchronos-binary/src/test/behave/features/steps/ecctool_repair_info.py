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

from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common import match_and_remove_row, validate_header, run_ecctool, table_row


REPAIR_INFO_HEADER = r"| Keyspace | Table | Repaired (%) | Repair time taken |"
REPAIR_INFO_ROW_FORMAT_PATTERN = r"\| {0} \| {1} \| \d+[.]\d+ \| .* \|"


def run_ecc_repair_info(context, params):
    run_ecctool(context, ["repair-info"] + params)


def handle_repair_info_output(context):
    output_data = context.out.decode("ascii").lstrip().rstrip().split("\n")
    context.time_window = output_data[0:1]
    context.header = output_data[1:4]
    context.rows = output_data[4:]


@when("we get repair-info with since {since} and duration {duration}")
def step_get_repair_info_with_since_and_duration(context, since, duration):
    run_ecc_repair_info(context, ["--since", since, "--duration", duration])
    handle_repair_info_output(context)


@when("we get repair-info with duration {duration} and limit {limit}")
def step_get_repair_info_with_duration_and_limit(context, duration, limit):
    run_ecc_repair_info(context, ["--duration", duration, "--limit", limit])
    handle_repair_info_output(context)


@when("we get repair-info with duration {duration}")
def step_get_repair_info_with_duration(context, duration):
    run_ecc_repair_info(context, ["--duration", duration])
    handle_repair_info_output(context)


@when("we get repair-info with since {since} and limit {limit}")
def step_get_repair_info_with_since_and_limit(context, since, limit):
    run_ecc_repair_info(context, ["--since", since, "--limit", limit])
    handle_repair_info_output(context)


@when("we get repair-info with since {since}")
def step_get_repair_info_with_since(context, since):
    run_ecc_repair_info(context, ["--since", since])
    handle_repair_info_output(context)


@when("we get local repair-info with since {since}")
def step_get_local_repair_info_with_since(context, since):
    run_ecc_repair_info(context, ["--local", "--since", since])
    handle_repair_info_output(context)


@when("we get repair-info for keyspace {keyspace} with since {since} and duration {duration}")
def step_get_repair_info_for_keyspace_with_since_and_duration(context, keyspace, since, duration):
    run_ecc_repair_info(context, ["--keyspace", keyspace, "--since", since, "--duration", duration])
    handle_repair_info_output(context)


@when("we get repair-info for keyspace {keyspace} with duration {duration}")
def step_get_repair_info_for_keyspace_with_duration(context, keyspace, duration):
    run_ecc_repair_info(context, ["--keyspace", keyspace, "--duration", duration])
    handle_repair_info_output(context)


@when("we get repair-info for keyspace {keyspace} with since {since}")
def step_get_repair_info_for_keyspace_with_since(context, keyspace, since):
    run_ecc_repair_info(context, ["--keyspace", keyspace, "--since", since])
    handle_repair_info_output(context)


@when("we get repair-info for table {keyspace}.{table} with since {since} and duration {duration}")
def step_get_repair_info_for_table_with_since_and_duration(context, keyspace, table, since, duration):
    run_ecc_repair_info(context, ["--keyspace", keyspace, "--table", table, "--since", since, "--duration", duration])
    handle_repair_info_output(context)


@when("we get repair-info for table {keyspace}.{table} with duration {duration}")
def step_get_repair_info_for_table_with_duration(context, keyspace, table, duration):
    run_ecc_repair_info(context, ["--keyspace", keyspace, "--table", table, "--duration", duration])
    handle_repair_info_output(context)


@when("we get repair-info for table {keyspace}.{table} with since {since}")
def step_get_repair_info_for_table_with_since(context, keyspace, table, since):
    run_ecc_repair_info(context, ["--keyspace", keyspace, "--table", table, "--since", since])
    handle_repair_info_output(context)


@when("we get repair-info for table {keyspace}.{table}")
def step_get_repair_info_for_table(context, keyspace, table):
    run_ecc_repair_info(context, ["--keyspace", keyspace, "--table", table])
    handle_repair_info_output(context)


@then("the output should contain a valid repair-info header")
def step_validate_repair_info_header(context):
    validate_header(context.header, REPAIR_INFO_HEADER)


@then("the output should contain a repair-info row for {keyspace}.{table}")
def step_validate_repair_info_row(context, keyspace, table):
    expected_row = table_row(REPAIR_INFO_ROW_FORMAT_PATTERN, keyspace, table)
    match_and_remove_row(context.rows, expected_row)

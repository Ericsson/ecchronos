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
from ecc_step_library.common import match_and_remove_row, validate_header, run_ecctool

REJECTIONS_HEADER = (
    r"| Keyspace | Table | Start Hour | Start Minute | End Hour | End Minute | DC Exclusions |"
)
REJECTIONS_ROW_FORMAT_PATTERN = (
    r"\| \* \| \* \| \d+ \| \d+ \| \d+ \| \d+ \| \[.*\] \|"
)

def run_ecc_rejections(context, params):
    run_ecctool(context, ["rejections"] + params)

def handle_rejections_output(context):
    output_data = context.out.decode("ascii").strip().split("\n")
    print("Output data:")
    print(output_data)
    context.header = output_data[0:6]
    print("header:")
    print(context.header)
    context.rows = output_data[6:]
    print("rows:")
    print(context.rows)

@then("the output should contain a valid rejections header")
def step_validate_rejection_header(context):
    validate_header(context.header, REJECTIONS_HEADER)

@then(
    "the output should contain a rejection row for {keyspace}.{table} with start hour {start_hour}, "
    "start minute {start_minute}, end hour {end_hour}, end minute {end_minute}, dc exclusions {dc_exclusions}"
)
def step_validate_rejection_row(
    context, keyspace, table, start_hour, start_minute, end_hour, end_minute, dc_exclusions
): # pylint: disable=too-many-arguments,too-many-positional-arguments
    expected_row = rejection_row(
        REJECTIONS_ROW_FORMAT_PATTERN,
        keyspace=keyspace,
        table=table,
        start_hour=start_hour,
        start_minute=start_minute,
        end_hour=end_hour,
        end_minute=end_minute,
        dc_exclusions=dc_exclusions,
    )
    match_and_remove_row(context.rows, expected_row)

@when(
    "we create repair rejection for keyspace {keyspace}, table {table} with start hour {start_hour}, "
    "start minute {start_minute}, end hour {end_hour}, end minute {end_minute}, dc exclusions {dc_exclusions}"
)
def step_create_repair_rejection(
    context, keyspace, table, start_hour, start_minute, end_hour, end_minute, dc_exclusions
): # pylint: disable=too-many-positional-arguments,too-many-arguments
    run_ecc_rejections(
        context,
        [
            "create",
            "--keyspace",
            keyspace,
            "--table",
            table,
            "--start-hour",
            start_hour,
            "--start-minute",
            start_minute,
            "--end-hour",
            end_hour,
            "--end-minute",
            end_minute,
            "--dc-exclusions",
            dc_exclusions,
        ],
    )
    handle_rejections_output(context)

@when("we get all rejections")
def step_get_all_rejections(context):
    run_ecc_rejections(context, ["get"])
    handle_rejections_output(context)

@when("we get all rejections for {keyspace}.{table}")
def step_get_rejections_for_keyspace_and_table(context, keyspace, table):
    run_ecc_rejections(context, ["get", "--keyspace", keyspace, "--table", table])
    handle_rejections_output(context)

@when("we get all rejections for {keyspace}")
def step_get_rejections_for_keyspace(context, keyspace):
    run_ecc_rejections(context, ["get", "--keyspace", keyspace])
    handle_rejections_output(context)

@then("the output should contain a valid rejection header")
def step_validate_list_tables_header(context):
    validate_header(context.header, REJECTIONS_HEADER)


def rejection_row(
    template, keyspace, table, start_hour, start_minute, end_hour, end_minute, dc_exclusions
): # pylint: disable=too-many-arguments,too-many-positional-arguments
    return template.format(keyspace, table, start_hour, start_minute, end_hour, end_minute, dc_exclusions)

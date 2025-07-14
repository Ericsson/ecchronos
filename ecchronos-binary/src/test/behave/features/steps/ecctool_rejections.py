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

import re
from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common import match_and_remove_row, validate_header, run_ecctool, check_row_not_exists

REJECTIONS_HEADER = r"| Keyspace | Table | Start Hour | Start Minute | End Hour | End Minute | DC Exclusions |"
REJECTIONS_ROW_FORMAT_PATTERN = r"\| {} +\| {} +\| {} +\| {} +\| {} +\| {} +\| \[.*\] +\|"
REJECTIONS_ROW_FORMAT_PATTERN_WITH_DC = r"\| {} +\| {} +\| {} +\| {} +\| {} +\| {} +\| {} +\|"


def run_ecc_rejections(context, params):
    run_ecctool(context, ["rejections"] + params)


def handle_rejections_output_with_summary(context):
    output_data = context.out.decode("ascii").lstrip().rstrip().split("\n")
    context.summary = output_data[0]
    context.header = output_data[1:4]
    context.rows = output_data[4:]


def handle_get_rejections_output(context):
    output_data = context.out.decode("ascii").lstrip().rstrip().split("\n")
    context.header = output_data[0:3]
    context.rows = output_data[3:]


@when(
    "we create repair rejection for keyspace {keyspace}, table {table} with start hour {start_hour}, "
    "start minute {start_minute}, end hour {end_hour}, end minute {end_minute}, dc exclusions {dc_exclusions}"
)
def step_create_repair_rejection(
    context, keyspace, table, start_hour, start_minute, end_hour, end_minute, dc_exclusions
):  # pylint: disable=too-many-positional-arguments,too-many-arguments
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
    handle_rejections_output_with_summary(context)


@when("we get all rejections")
def step_get_all_rejections(context):
    run_ecc_rejections(context, ["get"])
    handle_get_rejections_output(context)


@when("we get all rejections for {keyspace}")
def step_get_rejections_for_keyspace(context, keyspace):
    run_ecc_rejections(context, ["get", "--keyspace", keyspace])
    handle_get_rejections_output(context)


@when("we get rejections for {keyspace}.{table}")
def step_get_rejections_for_keyspace_and_table(context, keyspace, table):
    run_ecc_rejections(context, ["get", "--keyspace", keyspace, "--table", table])
    handle_get_rejections_output(context)


@when(
    "we update rejection with {keyspace}.{table} with start hour {start_hour}, start minute {start_minute} "
    "adding datacenter {dc_exclusions}"
)
def step_add_dc_exclusion_for_rejection(
    context, keyspace, table, start_hour, start_minute, dc_exclusions
):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    run_ecc_rejections(
        context,
        [
            "update",
            "--keyspace",
            keyspace,
            "--table",
            table,
            "--start-hour",
            start_hour,
            "--start-minute",
            start_minute,
            "--dc-exclusions",
            dc_exclusions,
        ],
    )
    handle_rejections_output_with_summary(context)


@when("we delete rejection with {keyspace}.{table} with start hour {start_hour}, start minute {start_minute}")
def step_delete_rejection(context, keyspace, table, start_hour, start_minute):
    run_ecc_rejections(
        context,
        [
            "delete",
            "--keyspace",
            keyspace,
            "--table",
            table,
            "--start-hour",
            start_hour,
            "--start-minute",
            start_minute,
        ],
    )
    handle_rejections_output_with_summary(context)


@when(
    "we delete a datacenter {dc} from rejection with {keyspace}.{table} with start hour {start_hour}, "
    "start minute {start_minute}"
)
def step_delete_dc_from_rejection(
    context, dc, keyspace, table, start_hour, start_minute
):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    run_ecc_rejections(
        context,
        [
            "delete",
            "--keyspace",
            keyspace,
            "--table",
            table,
            "--start-hour",
            start_hour,
            "--start-minute",
            start_minute,
            "--dc-exclusion",
            dc,
        ],
    )
    handle_rejections_output_with_summary(context)


@then("the output should contain a valid rejection header")
def step_validate_list_tables_header(context):
    validate_header(context.header, REJECTIONS_HEADER)


@then("the output should contain a valid rejections header")
def step_validate_rejection_header(context):
    validate_header(context.header, REJECTIONS_HEADER)


@then(
    "the output should contain a rejection row for {keyspace}.{table} with start hour {start_hour}, "
    "start minute {start_minute}, end hour {end_hour}, end minute {end_minute}, dc exclusions {dc_exclusions}"
)
def step_validate_rejection_row(
    context, keyspace, table, start_hour, start_minute, end_hour, end_minute, dc_exclusions
):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    expected_row = rejection_row(
        keyspace=keyspace,
        table=table,
        start_hour=start_hour,
        start_minute=start_minute,
        end_hour=end_hour,
        end_minute=end_minute,
        dc_exclusions=dc_exclusions,
    )
    match_and_remove_row(context.rows, expected_row)


@then(
    "the output should not contain a row for {keyspace}.{table} with start hour {start_hour} and "
    "start minute {start_minute}, end hour {end_hour}, end minute {end_minute} with {dc1}"
)
def step_validate_rejection_delete_row(
    context, keyspace, table, start_hour, start_minute, end_hour, end_minute, dc1
):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    expected_row = rejection_row(
        keyspace=keyspace,
        table=table,
        start_hour=start_hour,
        start_minute=start_minute,
        end_hour=end_hour,
        end_minute=end_minute,
        dc_exclusions=dc1,
    )
    assert check_row_not_exists(context.rows, expected_row)


@then(
    "the output should not contain a rejection row for {keyspace}.{table} with start hour {start_hour} and "
    "start minute {start_minute}, end hour {end_hour}, end minute {end_minute} with {dc1} and {dc2}"
)
def step_validate_delete_dc_from_rejection(
    context, keyspace, table, start_hour, start_minute, end_hour, end_minute, dc1, dc2
):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    expected_row = rejection_row(
        keyspace=keyspace,
        table=table,
        start_hour=start_hour,
        start_minute=start_minute,
        end_hour=end_hour,
        end_minute=end_minute,
        dc_exclusions=[dc1, dc2],
    )
    assert check_row_not_exists(context.rows, expected_row)


@then(
    "the output should contain a row for {keyspace}.{table} with start hour {start_hour}, "
    "start minute {start_minute}, end hour {end_hour}, end minute {end_minute}, dc exclusions {dc1} and {dc2}"
)
def step_validate_datacenter_in_rejection_row(
    context, keyspace, table, start_hour, start_minute, end_hour, end_minute, dc1, dc2
):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    expected_row = rejection_row(
        keyspace=keyspace,
        table=table,
        start_hour=start_hour,
        start_minute=start_minute,
        end_hour=end_hour,
        end_minute=end_minute,
        dc_exclusions=[dc1, dc2],
    )
    match_and_remove_row(context.rows, expected_row)


def rejection_row(
    keyspace, table, start_hour, start_minute, end_hour, end_minute, dc_exclusions
):  # pylint: disable=too-many-arguments,too-many-positional-arguments
    if isinstance(dc_exclusions, list):
        dc_pattern = re.escape(str(dc_exclusions))
        return REJECTIONS_ROW_FORMAT_PATTERN_WITH_DC.format(
            re.escape(keyspace), re.escape(table), start_hour, start_minute, end_hour, end_minute, dc_pattern
        )
    dc_pattern = re.escape(str([dc_exclusions]))
    return REJECTIONS_ROW_FORMAT_PATTERN.format(
        re.escape(keyspace), re.escape(table), start_hour, start_minute, end_hour, end_minute
    ).replace(r"\[\.\*\]", dc_pattern)

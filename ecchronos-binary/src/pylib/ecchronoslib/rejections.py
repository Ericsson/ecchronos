#
# Copyright 2026 Telefonaktiebolaget LM Ericsson
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

"""Handlers for the ecctool ``rejections`` subcommand actions."""

from __future__ import print_function

import sys

from ecchronoslib import rest, table_printer


def _print_result(arguments, result):
    """Print a rejections request result (table or exception)."""
    if result.is_successful():
        if arguments.output != "json":
            print(result.message)
        table_printer.print_rejections(result.data, columns=arguments.columns, output=arguments.output)
    else:
        print(result.format_exception())


def create_rejections(arguments):
    request = rest.RejectionsRequest(base_url=arguments.url)
    rejection_body = {
        "keyspaceName": arguments.keyspace,
        "tableName": arguments.table,
        "startHour": arguments.start_hour,
        "startMinute": arguments.start_minute,
        "endHour": arguments.end_hour,
        "endMinute": arguments.end_minute,
        "dcExclusions": arguments.dc_exclusions,
    }
    _print_result(arguments, request.create_rejection(rejection_body))


def delete_rejections(arguments):
    request = rest.RejectionsRequest(base_url=arguments.url)

    if arguments.all:
        result = request.truncate_rejections()
    elif None not in [arguments.keyspace, arguments.table, arguments.start_hour, arguments.start_minute]:
        rejection_body = {
            "keyspaceName": arguments.keyspace,
            "tableName": arguments.table,
            "startHour": arguments.start_hour,
            "startMinute": arguments.start_minute,
            "endHour": None,
            "endMinute": None,
            "dcExclusions": arguments.dc_exclusions or [],
        }
        result = request.delete_rejection(rejection_body)
    else:
        print("--keyspace, --table, --start-hour and --start-minute are mandatory arguments.")
        sys.exit(1)

    _print_result(arguments, result)


def get_rejections(arguments):
    request = rest.RejectionsRequest(base_url=arguments.url)
    if arguments.table and not arguments.keyspace:
        print("--keyspace is required.")
        sys.exit(1)
    result = request.list_rejections(keyspace=arguments.keyspace, table=arguments.table or None)
    if result.is_successful():
        table_printer.print_rejections(result.data, columns=arguments.columns, output=arguments.output)
    else:
        print(result.format_exception())


def update_rejections(arguments):
    request = rest.RejectionsRequest(base_url=arguments.url)
    rejection_body = {
        "keyspaceName": arguments.keyspace,
        "tableName": arguments.table,
        "startHour": arguments.start_hour,
        "startMinute": arguments.start_minute,
        "endHour": None,
        "endMinute": None,
        "dcExclusions": arguments.dc_exclusions,
    }
    _print_result(arguments, request.update_rejection(rejection_body))


_ACTIONS = {
    "create": create_rejections,
    "delete": delete_rejections,
    "get": get_rejections,
    "update": update_rejections,
}


def rejections(arguments):
    action = _ACTIONS.get(arguments.rejections_action)
    if action is None:
        print("Specify a valid action (create, delete, get or update) for subcommand 'rejections'.")
        sys.exit(1)
    action(arguments)

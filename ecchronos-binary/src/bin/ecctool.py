#!/usr/bin/env python3
# vi: syntax=python
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

from __future__ import print_function

import os
import json
import signal
import sys
import glob
import subprocess
from argparse import ArgumentParser
from io import open
from urllib.error import HTTPError

try:
    from ecchronoslib import rest, table_printer
except ImportError:
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    LIB_DIR = os.path.join(SCRIPT_DIR, "..", "pylib")
    sys.path.append(LIB_DIR)
    from ecchronoslib import rest, table_printer
from ecchronoslib.metrics_filter import filter_metrics_text
from ecchronoslib.argparse_types import comma_separated_ints, parse_duration_ms
from ecchronoslib.rejections import rejections

DEFAULT_PID_FILE = "ecc.pid"
SPRINGBOOT_MAIN_CLASS = "com.ericsson.bss.cassandra.ecchronos.application.SpringBooter"


# Argument configurations
ARG_COLUMNS = {
    "flags": ["-c", "--columns"],
    "type": comma_separated_ints,
    "help": "table columns to display (format: 0,1,2,...,N)",
    "default": None,
}
ARG_DC_EXCLUSIONS = {
    "flags": ["-dcs", "--dc-exclusions"],
    "nargs": "+",
    "help": "datacenters to exclude (format: <dc1> <dc2> ... <dcN>)",
}
ARG_DELETE_ALL = {"flags": ["-a", "--all"], "type": bool, "help": "delete all"}
ARG_DURATION = {
    "flags": ["-d", "--duration"],
    "type": str,
    "help": "repair information for specified duration (ISO8601 or simple format: 5s, 5m, 5h, 5d) from now-duration "
    "to now (required unless using --since or --keyspace/--table)",
    "default": None,
}
ARG_END_HOUR = {"flags": ["-eh", "--end-hour"], "type": int, "help": "end hour"}
ARG_END_MINUTE = {"flags": ["-em", "--end-minute"], "type": int, "help": "end minute"}
ARG_FORCE_DISABLED = {
    "flags": ["-e", "--forceRepairDisabled"],
    "help": "force repair of disabled tables",
    "required": False,
    "action": "store_true",
}
ARG_FORCE_TWCS = {
    "flags": ["-f", "--forceRepairTWCS"],
    "action": "store_true",
    "help": "force repair of TWCS tables",
    "required": False,
}
ARG_FOREGROUND = {
    "flags": ["-f", "--foreground"],
    "action": "store_true",
    "help": "run in foreground (executes in current terminal and logs to stdout)",
    "default": False,
}
ARG_FULL = {
    "flags": ["-f", "--full"],
    "action": "store_true",
    "help": "show full schedules with configuration and vnode state (requires -n/--node)",
    "default": False,
}
ARG_ID = {
    "flags": ["-n", "--node"],
    "type": str,
    "help": "only matching node id (mutually exclusive with -k/--keyspace and -t/--table)",
}
ARG_JOB_ID = {
    "flags": ["-i", "--id"],
    "type": str,
    "help": "only matching job id (mutually exclusive with -k/--keyspace and -t/--table)",
}
ARG_KEYSPACE = {"flags": ["-k", "--keyspace"], "type": str, "help": "keyspace"}
ARG_LIMIT = {"flags": ["-l", "--limit"], "type": int, "help": "limit output rows (use -1 for no limit)", "default": -1}
ARG_NODE_ID = {
    "flags": ["-n", "--node"],
    "type": str,
    "help": "only matching node id",
}
ARG_OUTPUT_JSON = {
    "flags": ["-o", "--output"],
    "type": str,
    "help": "output formats: json (defaults to no format)",
    "default": "",
}
ARG_OUTPUT_JSON_TABLE = {
    "flags": ["-o", "--output"],
    "type": str,
    "help": "output formats: json, table (default)",
    "default": "table",
}
ARG_PIDFILE_READ = {"flags": ["-p", "--pidfile"], "type": str, "help": "file containing process id"}
ARG_PIDFILE_WRITE = {"flags": ["-p", "--pidfile"], "type": str, "help": "file for storing process id"}
ARG_REPAIR_TYPE = {
    "flags": ["-r", "--repair_type"],
    "type": str,
    "help": "type of repair (accepted values: vnode, parallel_vnode and incremental)",
    "required": False,
}
ARG_RUN_ALL = {
    "flags": ["-a", "--all"],
    "action": "store_true",
    "help": "run repair for all nodes",
    "required": False,
}
ARG_SINCE = {
    "flags": ["-s", "--since"],
    "type": str,
    "help": "repair information from specified date (ISO8601 format) to now (required unless "
    "using --duration or --keyspace/--table)",
    "default": None,
}
ARG_START_HOUR = {"flags": ["-sh", "--start-hour"], "type": int, "help": "start hour"}
ARG_START_MINUTE = {"flags": ["-sm", "--start-minute"], "type": int, "help": "start minute"}
ARG_SESSION_ID = {
    "flags": ["-s", "--session"],
    "type": str,
    "help": "repair session id",
    "required": True,
}
ARG_FORCE = {
    "flags": ["-f", "--force"],
    "action": "store_true",
    "help": "force-fail the session on all managed nodes that report it, not only the coordinator",
    "required": False,
}
ARG_YES = {
    "flags": ["-y", "--yes"],
    "action": "store_true",
    "help": "skip the confirmation prompt",
    "required": False,
}
ARG_TABLE = {"flags": ["-t", "--table"], "type": str, "help": "table"}
ARG_URL = {
    "flags": ["-u", "--url"],
    "type": str,
    "help": "ecchronos host URL (format: http://<host>:<port>)",
    "default": None,
}
ARG_TIMEOUT = {
    "flags": ["-T", "--timeout"],
    "type": float,
    "help": "request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)",
    "default": None,
}


def get_parser():
    parser = ArgumentParser(
        description="ecctool is a command line utility used to perform operations toward an ecChronos instance. "
        "Run 'ecctool <subcommand> --help' to get more information about each subcommand."
    )
    sub_parsers = parser.add_subparsers(dest="subcommand", help="")

    add_config_subcommand(sub_parsers)
    add_metrics_subcommand(sub_parsers)
    add_rejections_subcommand(sub_parsers)
    add_repair_info_subcommand(sub_parsers)
    add_repair_sessions_subcommand(sub_parsers)
    add_repairs_subcommand(sub_parsers)
    add_run_repair_subcommand(sub_parsers)
    add_running_job_subcommand(sub_parsers)
    add_schedules_subcommand(sub_parsers)
    add_start_subcommand(sub_parsers)
    add_state_subcommand(sub_parsers)
    add_status_subcommand(sub_parsers)
    add_stop_subcommand(sub_parsers)

    return parser


def add_running_job_subcommand(sub_parsers):
    parser_repairs = sub_parsers.add_parser("running-job", description="Show which (if any) job is currently running.")
    add_common_arg(parser_repairs, ARG_OUTPUT_JSON)
    add_common_arg(parser_repairs, ARG_URL)
    add_common_arg(parser_repairs, ARG_TIMEOUT)


def add_config_subcommand(sub_parsers):
    parser_config = sub_parsers.add_parser("config", description="Show or update ecChronos configuration.")
    parser_config.add_argument("--session-window", type=str, help="session window duration (e.g. 5m, 30s, 300000)")
    parser_config.add_argument("--cooldown", type=str, help="cooldown duration (e.g. 5m, 30s, 300000)")
    parser_config.add_argument("--locks-per-resource", type=int, help="locks per resource")
    parser_config.add_argument(
        "--max-concurrency",
        type=int,
        help="max concurrent scheduler threads (0 = unbounded, one thread per managed node)",
    )
    parser_config.add_argument("--max-wait-time", type=int, help="max wait time in minutes for a repair (> 0)")
    parser_config.add_argument(
        "--hung-repair-recovery",
        type=str,
        choices=["on", "off"],
        help="enable or disable automatic recovery of hung incremental repair sessions",
    )
    parser_config.add_argument(
        "--hung-repair-threshold",
        type=str,
        help="stall threshold before a hung repair session is cancelled (e.g. 30m, 1800s, 1800000)",
    )
    parser_config.add_argument(
        "--hung-repair-bypass-coordinator",
        type=str,
        choices=["on", "off"],
        help="bypass the coordinator check and send the fail command to all managed nodes",
    )
    parser_config.add_argument(
        "--hung-repair-force",
        type=str,
        choices=["on", "off"],
        help="value of the force flag passed to failSession, used for all requests",
    )
    add_common_arg(parser_config, ARG_URL)
    add_common_arg(parser_config, ARG_TIMEOUT)


def _on_off_to_bool(value):
    return value == "on" if value is not None else None


def config(arguments):
    request = rest.ConfigRequest(base_url=arguments.url, timeout=arguments.timeout)
    has_updates = (
        arguments.session_window is not None
        or arguments.cooldown is not None
        or arguments.locks_per_resource is not None
        or arguments.max_concurrency is not None
        or arguments.max_wait_time is not None
        or arguments.hung_repair_recovery is not None
        or arguments.hung_repair_threshold is not None
        or arguments.hung_repair_bypass_coordinator is not None
        or arguments.hung_repair_force is not None
    )
    if has_updates:
        session_window_ms = parse_duration_ms(arguments.session_window) if arguments.session_window else None
        cooldown_ms = parse_duration_ms(arguments.cooldown) if arguments.cooldown else None
        hung_repair_stall_threshold_ms = (
            parse_duration_ms(arguments.hung_repair_threshold) if arguments.hung_repair_threshold else None
        )
        result = request.patch(
            session_window_ms=session_window_ms,
            cooldown_ms=cooldown_ms,
            locks_per_resource=arguments.locks_per_resource,
            max_concurrency=arguments.max_concurrency,
            max_wait_time_minutes=arguments.max_wait_time,
            hung_repair_recovery_enabled=_on_off_to_bool(arguments.hung_repair_recovery),
            hung_repair_stall_threshold_ms=hung_repair_stall_threshold_ms,
            hung_repair_bypass_coordinator_check=_on_off_to_bool(arguments.hung_repair_bypass_coordinator),
            hung_repair_force=_on_off_to_bool(arguments.hung_repair_force),
        )
    else:
        result = request.get()

    if result.is_successful():
        print(json.dumps(result.data, indent=2))
    else:
        print(result.format_exception())
        sys.exit(1)


def add_repairs_subcommand(sub_parsers):
    parser_repairs = sub_parsers.add_parser("repairs", description="Show the status of all manual repairs.")
    add_common_arg(parser_repairs, ARG_COLUMNS)

    keyspace_arg = ARG_KEYSPACE.copy()
    keyspace_arg["help"] = "keyspace (mutually exclusive with -n/--node)"
    add_common_arg(parser_repairs, keyspace_arg)

    table_arg = ARG_TABLE.copy()
    table_arg["help"] = "table (requires -k/--keyspace and is mutually exclusive with -n/--node)"
    add_common_arg(parser_repairs, table_arg)

    add_common_arg(parser_repairs, ARG_URL)
    add_common_arg(parser_repairs, ARG_TIMEOUT)

    add_common_arg(parser_repairs, ARG_ID)
    add_common_arg(parser_repairs, ARG_JOB_ID)

    add_common_arg(parser_repairs, ARG_LIMIT)
    add_common_arg(parser_repairs, ARG_OUTPUT_JSON_TABLE)


def add_schedules_subcommand(sub_parsers):
    parser_schedules = sub_parsers.add_parser("schedules", description="Show the status of schedules.")
    add_common_arg(parser_schedules, ARG_COLUMNS)
    add_common_arg(parser_schedules, ARG_FULL)
    add_common_arg(parser_schedules, ARG_ID)
    add_common_arg(parser_schedules, ARG_JOB_ID)

    keyspace_arg = ARG_KEYSPACE.copy()
    keyspace_arg["help"] = "keyspace (mutually exclusive with -n/--node)"
    add_common_arg(parser_schedules, keyspace_arg)

    add_common_arg(parser_schedules, ARG_LIMIT)
    add_common_arg(parser_schedules, ARG_OUTPUT_JSON_TABLE)

    table_arg = ARG_TABLE.copy()
    table_arg["help"] = "table (requires -k/--keyspace and is mutually exclusive with -n/--node)"
    add_common_arg(parser_schedules, table_arg)

    add_common_arg(parser_schedules, ARG_URL)
    add_common_arg(parser_schedules, ARG_TIMEOUT)


def add_run_repair_subcommand(sub_parsers):
    parser_run_repair = sub_parsers.add_parser(
        "run-repair",
        description="Triggers a manual repair in ecChronos. This will be done through the Cassandra JMX interface.",
    )
    add_common_arg(parser_run_repair, ARG_COLUMNS)
    add_common_arg(parser_run_repair, ARG_ID)
    add_common_arg(parser_run_repair, ARG_URL)
    add_common_arg(parser_run_repair, ARG_TIMEOUT)
    add_common_arg(parser_run_repair, ARG_OUTPUT_JSON_TABLE)
    add_common_arg(parser_run_repair, ARG_REPAIR_TYPE)
    add_common_arg(parser_run_repair, ARG_FORCE_TWCS)
    add_common_arg(parser_run_repair, ARG_FORCE_DISABLED)
    add_common_arg(parser_run_repair, ARG_RUN_ALL)

    keyspace_arg = ARG_KEYSPACE.copy()
    keyspace_arg["help"] = (
        "keyspace (applies to all tables within the keyspace with a replication factor greater than 1)"
    )
    keyspace_arg["required"] = False
    add_common_arg(parser_run_repair, keyspace_arg)

    table_arg = ARG_TABLE.copy()
    table_arg["help"] = "table (requires -k/--keyspace)"
    table_arg["required"] = False
    add_common_arg(parser_run_repair, table_arg)


def add_repair_info_subcommand(sub_parsers):
    parser_repair_info = sub_parsers.add_parser(
        "repair-info",
        description="Get information about repairs for tables. The repair information is based on repair history, "
        "meaning both manual and scheduled repairs will be a part of the repair information. This "
        "subcommand requires the user to provide either --since or --duration if --keyspace and --table "
        "is not provided. If repair info is fetched for a specific table using --keyspace and --table, "
        "the duration will default to the table's GC_GRACE_SECONDS.",
    )
    add_common_arg(parser_repair_info, ARG_COLUMNS)
    add_common_arg(parser_repair_info, ARG_NODE_ID)
    add_common_arg(parser_repair_info, ARG_KEYSPACE)
    add_common_arg(parser_repair_info, ARG_TABLE)
    add_common_arg(parser_repair_info, ARG_SINCE)
    add_common_arg(parser_repair_info, ARG_DURATION)
    add_common_arg(parser_repair_info, ARG_URL)
    add_common_arg(parser_repair_info, ARG_TIMEOUT)
    add_common_arg(parser_repair_info, ARG_LIMIT)
    add_common_arg(parser_repair_info, ARG_OUTPUT_JSON_TABLE)


def add_start_subcommand(sub_parsers):
    parser_start = sub_parsers.add_parser("start", description="Start the ecChronos service.")
    add_common_arg(parser_start, ARG_FOREGROUND)
    add_common_arg(parser_start, ARG_OUTPUT_JSON)
    add_common_arg(parser_start, ARG_PIDFILE_WRITE)


def add_stop_subcommand(sub_parsers):
    parser_stop = sub_parsers.add_parser(
        "stop",
        description="Stop the ecChronos service (sends SIGTERM to the process).",
    )
    add_common_arg(parser_stop, ARG_OUTPUT_JSON)
    add_common_arg(parser_stop, ARG_PIDFILE_READ)


def add_state_subcommand(sub_parsers):
    parser_state = sub_parsers.add_parser(
        "state",
        description="Get information of ecChronos internal state.",
    )

    state_subparsers = parser_state.add_subparsers(dest="state_subcommand")
    add_state_nodes_subcommand(state_subparsers)

    add_common_arg(parser_state, ARG_COLUMNS)
    add_common_arg(parser_state, ARG_OUTPUT_JSON)
    add_common_arg(parser_state, ARG_URL)
    add_common_arg(parser_state, ARG_TIMEOUT)


def add_state_nodes_subcommand(state_subparsers):
    parser_nodes = state_subparsers.add_parser("nodes", help="Get nodes managed by local instance.")
    add_common_arg(parser_nodes, ARG_URL)
    add_common_arg(parser_nodes, ARG_TIMEOUT)


def add_metrics_subcommand(sub_parsers):
    parser_metrics = sub_parsers.add_parser(
        "metrics",
        description="Fetch the agent metrics exposition text (native Prometheus/OpenMetrics passthrough).",
    )
    parser_metrics.add_argument(
        "--name",
        action="append",
        dest="name",
        default=None,
        metavar="SUBSTR",
        help="only show metrics whose name contains the given substring "
        "(repeatable, case-insensitive, '.' and '_' are equivalent)",
    )
    parser_metrics.add_argument(
        "--format",
        choices=["prometheus", "openmetrics"],
        default="prometheus",
        help="exposition format to request (default: prometheus)",
    )
    parser_metrics.add_argument(
        "--raw",
        action="store_true",
        default=False,
        help="suppress '# HELP' and '# TYPE' comment lines, showing only sample lines",
    )
    add_common_arg(parser_metrics, ARG_URL)
    add_common_arg(parser_metrics, ARG_TIMEOUT)


def add_common_arg(parser, arg_config, required=None):
    """Helper function to add common arguments to parsers."""
    kwargs = {k: v for k, v in arg_config.items() if k != "flags"}
    if required is not None:
        kwargs["required"] = required
    parser.add_argument(*arg_config["flags"], **kwargs)


def add_rejections_subcommand(sub_parsers):
    parser_rejections = sub_parsers.add_parser(
        "rejections",
        description="Manage ecchronos rejections. Use 'ecctool rejections <action> --help' for action information.",
    )
    add_common_arg(parser_rejections, ARG_URL)
    add_common_arg(parser_rejections, ARG_TIMEOUT)
    add_common_arg(parser_rejections, ARG_COLUMNS)
    add_common_arg(parser_rejections, ARG_OUTPUT_JSON_TABLE)

    rejections_subparsers = parser_rejections.add_subparsers(dest="rejections_action")

    add_rejections_create_action(rejections_subparsers)
    add_rejections_delete_action(rejections_subparsers)
    add_rejections_get_action(rejections_subparsers)
    add_rejections_update_action(rejections_subparsers)


def add_rejections_create_action(rejections_subparsers):
    parser_post = rejections_subparsers.add_parser("create", help="create a new rejection entry")
    add_common_arg(parser_post, ARG_KEYSPACE, required=True)
    add_common_arg(parser_post, ARG_TABLE, required=True)
    add_common_arg(parser_post, ARG_START_HOUR, required=True)
    add_common_arg(parser_post, ARG_START_MINUTE, required=True)
    add_common_arg(parser_post, ARG_END_HOUR, required=True)
    add_common_arg(parser_post, ARG_END_MINUTE, required=True)
    add_common_arg(parser_post, ARG_DC_EXCLUSIONS, required=True)
    add_common_arg(parser_post, ARG_URL)
    add_common_arg(parser_post, ARG_TIMEOUT)


def add_rejections_delete_action(rejections_subparsers):
    parser_delete = rejections_subparsers.add_parser("delete", help="delete a rejection entry")
    add_common_arg(parser_delete, ARG_DELETE_ALL, required=False)
    add_common_arg(parser_delete, ARG_KEYSPACE, required=False)
    add_common_arg(parser_delete, ARG_TABLE, required=False)
    add_common_arg(parser_delete, ARG_START_HOUR, required=False)
    add_common_arg(parser_delete, ARG_START_MINUTE, required=False)
    add_common_arg(parser_delete, ARG_DC_EXCLUSIONS, required=False)
    add_common_arg(parser_delete, ARG_URL)
    add_common_arg(parser_delete, ARG_TIMEOUT)


def add_rejections_get_action(rejections_subparsers):
    parser_get = rejections_subparsers.add_parser("get", help="get current rejections")
    add_common_arg(parser_get, ARG_KEYSPACE)
    add_common_arg(parser_get, ARG_TABLE)
    add_common_arg(parser_get, ARG_URL)
    add_common_arg(parser_get, ARG_TIMEOUT)


def add_rejections_update_action(rejections_subparsers):
    parser_update = rejections_subparsers.add_parser("update", help="update a rejection entry")
    add_common_arg(parser_update, ARG_KEYSPACE, required=True)
    add_common_arg(parser_update, ARG_TABLE, required=True)
    add_common_arg(parser_update, ARG_START_HOUR, required=True)
    add_common_arg(parser_update, ARG_START_MINUTE, required=True)
    add_common_arg(parser_update, ARG_DC_EXCLUSIONS, required=False)
    add_common_arg(parser_update, ARG_URL)
    add_common_arg(parser_update, ARG_TIMEOUT)


def add_repair_sessions_subcommand(sub_parsers):
    parser_repair_sessions = sub_parsers.add_parser(
        "repair-sessions",
        description="List and fail incremental repair sessions. "
        "Use 'ecctool repair-sessions <action> --help' for action information.",
    )
    add_common_arg(parser_repair_sessions, ARG_URL)
    add_common_arg(parser_repair_sessions, ARG_TIMEOUT)
    add_common_arg(parser_repair_sessions, ARG_COLUMNS)
    add_common_arg(parser_repair_sessions, ARG_OUTPUT_JSON_TABLE)

    repair_sessions_subparsers = parser_repair_sessions.add_subparsers(dest="repair_sessions_action")

    parser_list = repair_sessions_subparsers.add_parser(
        "list", help="list incremental repair sessions across managed nodes"
    )
    add_common_arg(parser_list, ARG_NODE_ID)
    add_common_arg(parser_list, ARG_URL)
    add_common_arg(parser_list, ARG_TIMEOUT)
    add_common_arg(parser_list, ARG_COLUMNS)
    add_common_arg(parser_list, ARG_OUTPUT_JSON_TABLE)

    parser_fail = repair_sessions_subparsers.add_parser("fail", help="fail (cancel) an incremental repair session")
    add_common_arg(parser_fail, ARG_SESSION_ID, required=True)
    add_common_arg(parser_fail, ARG_FORCE)
    add_common_arg(parser_fail, ARG_NODE_ID)
    add_common_arg(parser_fail, ARG_YES)
    add_common_arg(parser_fail, ARG_URL)
    add_common_arg(parser_fail, ARG_TIMEOUT)
    add_common_arg(parser_fail, ARG_OUTPUT_JSON_TABLE)


def add_status_subcommand(sub_parsers):
    parser_status = sub_parsers.add_parser("status", description="View status of the ecChronos instance.")
    add_common_arg(parser_status, ARG_URL)
    add_common_arg(parser_status, ARG_TIMEOUT)
    add_common_arg(parser_status, ARG_OUTPUT_JSON)


def repair_sessions(arguments):
    if arguments.repair_sessions_action == "list":
        _list_repair_sessions(arguments)
    elif arguments.repair_sessions_action == "fail":
        _fail_repair_session(arguments)
    else:
        print("Specify a valid action (list or fail) for subcommand 'repair-sessions'.")
        sys.exit(1)


def _list_repair_sessions(arguments):
    request = rest.RepairSessionsRequest(base_url=arguments.url, timeout=arguments.timeout)
    result = request.list_sessions(node_id=arguments.node)
    if result.is_successful():
        table_printer.print_repair_sessions(result.data, columns=arguments.columns, output=arguments.output)
    else:
        print(result.format_exception())
        sys.exit(1)


def _fail_repair_session(arguments):
    if not arguments.yes:
        scope = "all managed nodes that report it" if arguments.force else "its coordinator"
        answer = input("Fail repair session {0} on {1}? [y/N] ".format(arguments.session, scope))
        if answer.strip().lower() not in ("y", "yes"):
            print("Aborted.")
            return

    request = rest.RepairSessionsRequest(base_url=arguments.url, timeout=arguments.timeout)
    result = request.fail_session(arguments.session, force=arguments.force, node_id=arguments.node)
    if result.is_successful():
        table_printer.print_repair_sessions(result.data, columns=arguments.columns, output=arguments.output)
    else:
        print(result.format_exception())
        sys.exit(1)


def state(arguments):
    if arguments.state_subcommand == "nodes":
        _state_nodes(arguments)
    else:
        print("Specify a valid action (nodes) for subcommand 'state'.")
        sys.exit(1)


def _state_nodes(arguments):
    request = rest.StateManagementRequest(base_url=arguments.url, timeout=arguments.timeout)
    result = request.get_nodes()

    if result.is_successful():
        table_printer.print_nodes(result.data, columns=arguments.columns, output=arguments.output)
    else:
        print(result.format_exception())


def schedules(arguments):
    # pylint: disable=too-many-branches
    request = rest.RepairSchedulerRequest(base_url=arguments.url, timeout=arguments.timeout)
    full = False
    result = None
    if arguments.node or arguments.id:
        node = arguments.node or "all"
        if arguments.full and arguments.id is not None:
            result = request.get_schedule(
                node_id=node,
                keyspace=arguments.keyspace,
                table=arguments.table,
                job_id=arguments.id,
                full=True,
            )
            full = True
        elif arguments.id is not None:
            result = request.get_schedule(
                node_id=node,
                keyspace=arguments.keyspace,
                table=arguments.table,
                job_id=arguments.id,
                full=False,
            )
        else:
            result = request.get_schedule(node_id=node, keyspace=arguments.keyspace, table=arguments.table)
    elif arguments.full:
        print("Must specify --node and/or --job with --full.")
        sys.exit(1)
    elif arguments.table:
        if not arguments.keyspace:
            print("Must specify --keyspace if --table is specified.")
            sys.exit(1)
        result = request.list_schedules(keyspace=arguments.keyspace, table=arguments.table)
    else:
        result = request.list_schedules(keyspace=arguments.keyspace)

    if result.is_successful():
        if isinstance(result.data, list):
            table_printer.print_schedules(
                result.data, arguments.limit, columns=arguments.columns, output=arguments.output
            )
        else:
            table_printer.print_schedule(
                result.data, arguments.limit, full, columns=arguments.columns, output=arguments.output
            )
    else:
        print(result.format_exception())


def repairs(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url, timeout=arguments.timeout)
    if arguments.node or arguments.id:
        node = arguments.node or "all"
        result = request.get_repair(node_id=node, job_id=arguments.id)
        if result.is_successful():
            table_printer.print_repairs(
                result.data, arguments.limit, columns=arguments.columns, output=arguments.output
            )
        else:
            print(result.format_exception())
    elif arguments.table:
        if not arguments.keyspace:
            print("--keyspace is required.")
            sys.exit(1)
        result = request.list_repairs(keyspace=arguments.keyspace, table=arguments.table, host_id=arguments.node)
        if result.is_successful():
            table_printer.print_repairs(
                result.data, arguments.limit, columns=arguments.columns, output=arguments.output
            )
        else:
            print(result.format_exception())
    else:
        result = request.list_repairs(keyspace=arguments.keyspace, host_id=arguments.node)
        if result.is_successful():
            table_printer.print_repairs(
                result.data, arguments.limit, columns=arguments.columns, output=arguments.output
            )
        else:
            print(result.format_exception())


def run_repair(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url, timeout=arguments.timeout)
    if not arguments.keyspace and arguments.table:
        print("--keyspace must be specified if --table is specified.")
        sys.exit(1)
    if not (arguments.node or arguments.all) or (arguments.node and arguments.all):
        print("--node or --all must be specified, but not both.")
        sys.exit(1)
    result = request.post(
        node_id=arguments.node,
        keyspace=arguments.keyspace,
        table=arguments.table,
        repair_type=arguments.repair_type,
        allnodes=arguments.all,
        force_repair_twcs=arguments.forceRepairTWCS,
        force_repair_disabled=arguments.forceRepairDisabled,
    )
    if result.is_successful():
        table_printer.print_repairs(result.data, columns=arguments.columns, output=arguments.output)
    else:
        msg = "Repair Request Failed"
        if result.message:
            msg += ": " + result.message
        if result.message and "disabled" in result.message:
            msg += ". Use --forceRepairDisabled to force repair."
        print(msg)
        sys.exit(1)


def repair_info(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url, timeout=arguments.timeout)
    if not arguments.node:
        print("--node must be specified.")
        sys.exit(1)
    if not arguments.keyspace and arguments.table:
        print("--keyspace must be specified if --table is specified.")
        sys.exit(1)
    if not arguments.duration and not arguments.since and not arguments.table:
        print("Either --duration or --since or both must be provided if called without --keyspace and --table.")
        sys.exit(1)
    duration = None
    if arguments.duration:
        if arguments.duration[0] == "+" or arguments.duration[0] == "-":
            print("'+' and '-' is not allowed in duration, check help for more information.")
            sys.exit(1)
        duration = arguments.duration.upper()
    result = request.get_repair_info(
        node_id=arguments.node,
        keyspace=arguments.keyspace,
        table=arguments.table,
        since=arguments.since,
        duration=duration,
    )
    if result.is_successful():
        table_printer.print_repair_info(
            result.data, arguments.limit, columns=arguments.columns, output=arguments.output
        )
    else:
        print(result.format_exception())


def start(arguments):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    ecchronos_home_dir = os.path.join(script_dir, "..")
    conf_dir = os.path.join(ecchronos_home_dir, "conf")
    class_path = get_class_path(conf_dir, ecchronos_home_dir)
    jvm_opts = get_jvm_opts(conf_dir)
    command = "java {0} -cp {1} {2}".format(jvm_opts, class_path, SPRINGBOOT_MAIN_CLASS)
    run_ecc(ecchronos_home_dir, command, arguments)


def get_class_path(conf_dir, ecchronos_home_dir):
    class_path = conf_dir
    jar_glob = os.path.join(ecchronos_home_dir, "lib", "*.jar")
    for jar_file in glob.glob(jar_glob):
        class_path += ":{0}".format(jar_file)
    return class_path


def get_jvm_opts(conf_dir):
    jvm_opts = ""
    with open(os.path.join(conf_dir, "jvm.options"), "r", encoding="utf-8") as options_file:
        for line in options_file.readlines():
            if line.startswith("-"):
                jvm_opts += "{0} ".format(line.rstrip())
    return jvm_opts + "-Decchronos.config={0}".format(conf_dir)


def run_ecc(cwd, command, arguments):
    if arguments.foreground:
        command += " -f"
    proc = subprocess.Popen(
        command.split(" "),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # pylint: disable=consider-using-with
        cwd=cwd,
    )
    pid = proc.pid

    if arguments.output == "json":
        table_printer.output_json({"process_id": pid, "state": "started"})
    else:
        print("ecc started with pid {0}.".format(pid))

    pid_file = os.path.join(cwd, DEFAULT_PID_FILE)
    if arguments.pidfile:
        pid_file = arguments.pidfile
    with open(pid_file, "w", encoding="utf-8") as p_file:
        p_file.write("{0}".format(pid))
    if arguments.foreground:
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            sys.stdout.write(line.decode("utf-8"))
        proc.wait()


def stop(arguments):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    ecchronos_home_dir = os.path.join(script_dir, "..")
    pid_file = os.path.join(ecchronos_home_dir, DEFAULT_PID_FILE)
    if arguments.pidfile:
        pid_file = arguments.pidfile
    with open(pid_file, "r", encoding="utf-8") as p_file:
        pid = int(p_file.readline())
        try:
            os.kill(pid, signal.SIGTERM)
            if arguments.output == "json":
                table_printer.output_json({"process_id": pid, "state": "terminated"})
            else:
                print("Terminated ecc with pid {0}.".format(pid))
            os.remove(pid_file)
        except OSError:
            if arguments.output == "json":
                table_printer.output_json({"process_id": pid, "state": "unknown process id"})
            else:
                print("Process {0} is not running.".format(pid))
            sys.exit(1)


def status(arguments, print_running=False):
    request = rest.RepairSchedulerRequest(
        base_url=getattr(arguments, "url", None), timeout=getattr(arguments, "timeout", None)
    )
    result = request.list_schedules()
    output = getattr(arguments, "output", "")
    if result.is_successful():
        if print_running:
            if output == "json":
                table_printer.output_json({"running": True})
            elif print_running:
                print("ecChronos is running.")
    else:
        if output == "json":
            table_printer.output_json({"running": False})
        else:
            print("ecChronos is not running.")
        sys.exit(1)


def running_job(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url, timeout=arguments.timeout)
    result = request.running_job()

    if arguments.output == "json":
        table_printer.output_json({"running-job": result})
    else:
        if result == "":
            print("No repair job running.")
        else:
            print("Repair job with id " + result + " is running.")


def metrics(arguments):
    request = rest.MetricsRequest(base_url=arguments.url, timeout=arguments.timeout)
    result = request.get_metrics(open_metrics=arguments.format == "openmetrics")
    if isinstance(result, rest.RequestResult):
        # basic_request() reports connection failures as 404 as well, so only
        # a genuine HTTP 404 from the agent means statistics are disabled.
        if result.status_code == 404 and isinstance(result.exception, HTTPError):
            print("Metrics are not enabled on this instance (statistics.enabled is false).")
        else:
            print(result.format_exception())
        sys.exit(1)
    sys.stdout.write(filter_metrics_text(result, names=arguments.name, include_comments=not arguments.raw))


def _with_status(handler):
    """Wrap a handler so the status preflight runs before it."""

    def run(arguments):
        status(arguments)
        handler(arguments)

    return run


# Maps each subcommand to its handler. Most commands run a status preflight
# first; start/stop/status are handled directly.
SUBCOMMANDS = {
    "config": _with_status(config),
    "metrics": _with_status(metrics),
    "rejections": _with_status(rejections),
    "repair-info": _with_status(repair_info),
    "repair-sessions": _with_status(repair_sessions),
    "repairs": _with_status(repairs),
    "run-repair": _with_status(run_repair),
    "running-job": _with_status(running_job),
    "schedules": _with_status(schedules),
    "start": start,
    "state": _with_status(state),
    "status": lambda arguments: status(arguments, print_running=True),
    "stop": stop,
}


def run_subcommand(arguments):
    handler = SUBCOMMANDS.get(arguments.subcommand)
    if handler is not None:
        handler(arguments)


def main():
    parser = get_parser()
    args = parser.parse_args()

    if args.subcommand is None:
        parser.print_help()
        sys.exit(1)

    run_subcommand(args)


if __name__ == "__main__":
    main()

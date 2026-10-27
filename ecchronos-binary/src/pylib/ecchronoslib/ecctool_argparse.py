#!/usr/bin/env python3
# vi: syntax=python
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

from ecchronoslib.argparse_types import comma_separated_ints

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


def add_common_arg(parser, arg_config, required=None):
    """Helper function to add common arguments to parsers."""
    kwargs = {k: v for k, v in arg_config.items() if k != "flags"}
    if required is not None:
        kwargs["required"] = required
    parser.add_argument(*arg_config["flags"], **kwargs)


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


def add_unified_schedules_subcommand(sub_parsers):
    parser_unified = sub_parsers.add_parser(
        "unified-schedules",
        description="Show the status of consolidated UNIFIED_VNODE schedules, which coordinate one table "
        "across all of its nodes under a single job. With no node/job it shows a per-table aggregate "
        "(worst-case status across nodes, average ratio).",
    )
    add_common_arg(parser_unified, ARG_COLUMNS)
    add_common_arg(parser_unified, ARG_FULL)

    job_arg = ARG_JOB_ID.copy()
    job_arg["help"] = "show the nodes of this unified job id (use with -n/--node to show a single node)"
    add_common_arg(parser_unified, job_arg)

    node_arg = ARG_NODE_ID.copy()
    node_arg["help"] = "show only this node of the unified job (requires -i/--id)"
    add_common_arg(parser_unified, node_arg)

    keyspace_arg = ARG_KEYSPACE.copy()
    keyspace_arg["help"] = "filter aggregates on this keyspace"
    add_common_arg(parser_unified, keyspace_arg)

    table_arg = ARG_TABLE.copy()
    table_arg["help"] = "filter aggregates on this table (requires -k/--keyspace)"
    add_common_arg(parser_unified, table_arg)

    add_common_arg(parser_unified, ARG_LIMIT)
    add_common_arg(parser_unified, ARG_OUTPUT_JSON_TABLE)
    add_common_arg(parser_unified, ARG_URL)


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


def add_version_subcommand(sub_parsers):
    parser_version = sub_parsers.add_parser(
        "version",
        description="Show the ecctool client version and the connected ecChronos node's version.",
    )
    add_common_arg(parser_version, ARG_URL)
    add_common_arg(parser_version, ARG_TIMEOUT)
    add_common_arg(parser_version, ARG_OUTPUT_JSON)


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

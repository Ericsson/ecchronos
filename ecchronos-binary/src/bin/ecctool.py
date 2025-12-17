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
import signal
import sys
import glob
import subprocess
from argparse import ArgumentParser, ArgumentTypeError
from io import open

try:
    from ecchronoslib import rest, table_printer
except ImportError:
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    LIB_DIR = os.path.join(SCRIPT_DIR, "..", "pylib")
    sys.path.append(LIB_DIR)
    from ecchronoslib import rest, table_printer

DEFAULT_PID_FILE = "ecc.pid"
SPRINGBOOT_MAIN_CLASS = "com.ericsson.bss.cassandra.ecchronos.application.SpringBooter"


def comma_separated_ints(value):
    """Parse comma-separated integers for columns specification."""
    try:
        return [int(x.strip()) for x in value.split(",")]
    except ValueError as exc:
        raise ArgumentTypeError(f"'{value}' is not a valid comma-separated list of integers") from exc


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
    "help": "show full schedules with configuration and vnode state (requires -i/--id)",
    "default": False,
}
ARG_ID = {
    "flags": ["-i", "--id"],
    "type": str,
    "help": "only matching id (mutually exclusive with -k/--keyspace and -t/--table)",
}
ARG_JOB_ID = {
    "flags": ["-j", "--job"],
    "type": str,
    "help": "only matching job id (mutually exclusive with -k/--keyspace and -t/--table)",
}
ARG_KEYSPACE = {"flags": ["-k", "--keyspace"], "type": str, "help": "keyspace"}
ARG_LIMIT = {"flags": ["-l", "--limit"], "type": int, "help": "limit output rows (use -1 for no limit)", "default": -1}
ARG_NODE_ID = {
    "flags": ["-i", "--id"],
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
ARG_TABLE = {"flags": ["-t", "--table"], "type": str, "help": "table"}
ARG_URL = {
    "flags": ["-u", "--url"],
    "type": str,
    "help": "ecchronos host URL (format: http://<host>:<port>)",
    "default": None,
}


def get_parser():
    parser = ArgumentParser(
        description="ecctool is a command line utility used to perform operations toward an ecChronos instance. "
        "Run 'ecctool <subcommand> --help' to get more information about each subcommand."
    )
    sub_parsers = parser.add_subparsers(dest="subcommand", help="")

    add_rejections_subcommand(sub_parsers)
    add_repair_info_subcommand(sub_parsers)
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


def add_repairs_subcommand(sub_parsers):
    parser_repairs = sub_parsers.add_parser("repairs", description="Show the status of all manual repairs.")
    add_common_arg(parser_repairs, ARG_COLUMNS)

    keyspace_arg = ARG_KEYSPACE.copy()
    keyspace_arg["help"] = "keyspace (mutually exclusive with -i/--id)"
    add_common_arg(parser_repairs, keyspace_arg)

    table_arg = ARG_TABLE.copy()
    table_arg["help"] = "table (requires -k/--keyspace and is mutually exclusive with -i/--id)"
    add_common_arg(parser_repairs, table_arg)

    add_common_arg(parser_repairs, ARG_URL)

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
    keyspace_arg["help"] = "keyspace (mutually exclusive with -i/--id)"
    add_common_arg(parser_schedules, keyspace_arg)

    add_common_arg(parser_schedules, ARG_LIMIT)
    add_common_arg(parser_schedules, ARG_OUTPUT_JSON_TABLE)

    table_arg = ARG_TABLE.copy()
    table_arg["help"] = "table (requires -k/--keyspace and is mutually exclusive with -i/--id)"
    add_common_arg(parser_schedules, table_arg)

    add_common_arg(parser_schedules, ARG_URL)


def add_run_repair_subcommand(sub_parsers):
    parser_run_repair = sub_parsers.add_parser(
        "run-repair",
        description="Triggers a manual repair in ecChronos. This will be done through the Cassandra JMX interface.",
    )
    add_common_arg(parser_run_repair, ARG_COLUMNS)
    add_common_arg(parser_run_repair, ARG_ID)
    add_common_arg(parser_run_repair, ARG_URL)
    add_common_arg(parser_run_repair, ARG_OUTPUT_JSON_TABLE)
    add_common_arg(parser_run_repair, ARG_REPAIR_TYPE)
    add_common_arg(parser_run_repair, ARG_FORCE_TWCS)
    add_common_arg(parser_run_repair, ARG_FORCE_DISABLED)
    add_common_arg(parser_run_repair, ARG_RUN_ALL)

    keyspace_arg = ARG_KEYSPACE.copy()
    keyspace_arg[
        "help"
    ] = "keyspace (applies to all tables within the keyspace with a replication factor greater than 1)"
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


def add_state_nodes_subcommand(state_subparsers):
    parser_nodes = state_subparsers.add_parser("nodes", help="Get nodes managed by local instance.")
    add_common_arg(parser_nodes, ARG_URL)


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


def add_rejections_delete_action(rejections_subparsers):
    parser_delete = rejections_subparsers.add_parser("delete", help="delete a rejection entry")
    add_common_arg(parser_delete, ARG_DELETE_ALL, required=False)
    add_common_arg(parser_delete, ARG_KEYSPACE, required=False)
    add_common_arg(parser_delete, ARG_TABLE, required=False)
    add_common_arg(parser_delete, ARG_START_HOUR, required=False)
    add_common_arg(parser_delete, ARG_START_MINUTE, required=False)
    add_common_arg(parser_delete, ARG_DC_EXCLUSIONS, required=False)
    add_common_arg(parser_delete, ARG_URL)


def add_rejections_get_action(rejections_subparsers):
    parser_get = rejections_subparsers.add_parser("get", help="get current rejections")
    add_common_arg(parser_get, ARG_KEYSPACE)
    add_common_arg(parser_get, ARG_TABLE)
    add_common_arg(parser_get, ARG_URL)


def add_rejections_update_action(rejections_subparsers):
    parser_update = rejections_subparsers.add_parser("update", help="update a rejection entry")
    add_common_arg(parser_update, ARG_KEYSPACE, required=True)
    add_common_arg(parser_update, ARG_TABLE, required=True)
    add_common_arg(parser_update, ARG_START_HOUR, required=True)
    add_common_arg(parser_update, ARG_START_MINUTE, required=True)
    add_common_arg(parser_update, ARG_DC_EXCLUSIONS, required=False)
    add_common_arg(parser_update, ARG_URL)


def add_status_subcommand(sub_parsers):
    parser_status = sub_parsers.add_parser("status", description="View status of the ecChronos instance.")
    add_common_arg(parser_status, ARG_URL)
    add_common_arg(parser_status, ARG_OUTPUT_JSON)


def _create_rejections(arguments):
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

    result = request.create_rejection(rejection_body)

    if result.is_successful():
        if arguments.output != "json":
            print(result.message)
        table_printer.print_rejections(result.data, columns=arguments.columns, output=arguments.output)
    else:
        print(result.format_exception())


def _delete_rejections(arguments):
    request = rest.RejectionsRequest(base_url=arguments.url)
    result = None

    if arguments.all:
        result = request.truncate_rejections()
    elif not None in [arguments.keyspace, arguments.table, arguments.start_hour, arguments.start_minute]:
        dc_exclusions = arguments.dc_exclusions

        if dc_exclusions is None:
            dc_exclusions = []

        rejection_body = {
            "keyspaceName": arguments.keyspace,
            "tableName": arguments.table,
            "startHour": arguments.start_hour,
            "startMinute": arguments.start_minute,
            "endHour": None,
            "endMinute": None,
            "dcExclusions": dc_exclusions,
        }

        result = request.delete_rejection(rejection_body)
    else:
        print("--keyspace, --table, --start-hour and --start-minute are mandatory arguments.")
        sys.exit(1)

    if result.is_successful():
        if arguments.output != "json":
            print(result.message)
        table_printer.print_rejections(result.data, columns=arguments.columns, output=arguments.output)
    else:
        print(result.format_exception())


def _get_rejections(arguments):
    request = rest.RejectionsRequest(base_url=arguments.url)
    if arguments.table:
        if not arguments.keyspace:
            print("--keyspace is required.")
            sys.exit(1)
        result = request.list_rejections(keyspace=arguments.keyspace, table=arguments.table)
        if result.is_successful():
            table_printer.print_rejections(result.data, columns=arguments.columns, output=arguments.output)
        else:
            print(result.format_exception())
    else:
        result = request.list_rejections(keyspace=arguments.keyspace)
        if result.is_successful():
            table_printer.print_rejections(result.data, columns=arguments.columns, output=arguments.output)
        else:
            print(result.format_exception())


def _update_rejections(arguments):
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
    result = request.update_rejection(rejection_body)

    if result.is_successful():
        if arguments.output != "json":
            print(result.message)
        table_printer.print_rejections(result.data, columns=arguments.columns, output=arguments.output)
    else:
        print(result.format_exception())


def rejections(arguments):
    if arguments.rejections_action == "create":
        _create_rejections(arguments)
    elif arguments.rejections_action == "delete":
        _delete_rejections(arguments)
    elif arguments.rejections_action == "get":
        _get_rejections(arguments)
    elif arguments.rejections_action == "update":
        _update_rejections(arguments)


def state(arguments):
    if arguments.state_subcommand == "nodes":
        _state_nodes(arguments)


def _state_nodes(arguments):
    request = rest.StateManagementRequest(base_url=arguments.url)
    result = request.get_nodes()

    if result.is_successful():
        if arguments.output == "json":
            table_printer.output_json({"nodes": result.data})
        else:
            nodes = [rest.NodeSyncState(x) for x in result.data]
            table_printer.print_nodes(nodes, columns=arguments.columns)
    else:
        print(result.format_exception())


def schedules(arguments):
    # pylint: disable=too-many-branches
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    full = False
    result = None
    if arguments.id:
        if arguments.full and arguments.job is not None:
            result = request.get_schedule(
                node_id=arguments.id,
                keyspace=arguments.keyspace,
                table=arguments.table,
                job_id=arguments.job,
                full=True,
            )
            full = True
        elif arguments.job is not None:
            result = request.get_schedule(
                node_id=arguments.id,
                keyspace=arguments.keyspace,
                table=arguments.table,
                job_id=arguments.job,
                full=False,
            )
        else:
            result = request.get_schedule(node_id=arguments.id, keyspace=arguments.keyspace, table=arguments.table)
    elif arguments.full:
        print("Must specify --id and --job with --full.")
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
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    if arguments.id:
        result = request.get_repair(node_id=arguments.id, job_id=arguments.job)
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
        result = request.list_repairs(keyspace=arguments.keyspace, table=arguments.table, host_id=arguments.id)
        if result.is_successful():
            table_printer.print_repairs(
                result.data, arguments.limit, columns=arguments.columns, output=arguments.output
            )
        else:
            print(result.format_exception())
    else:
        result = request.list_repairs(keyspace=arguments.keyspace, host_id=arguments.id)
        if result.is_successful():
            table_printer.print_repairs(
                result.data, arguments.limit, columns=arguments.columns, output=arguments.output
            )
        else:
            print(result.format_exception())


def run_repair(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    if not arguments.keyspace and arguments.table:
        print("--keyspace must be specified if --table is specified.")
        sys.exit(1)
    if not (arguments.id or arguments.all) or (arguments.id and arguments.all):
        print("--id or --all must be specified, but not both.")
        sys.exit(1)
    result = request.post(
        node_id=arguments.id,
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
        print(result.format_exception())


def repair_info(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    if not arguments.id:
        print("--id must be specified.")
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
        node_id=arguments.id,
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
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    result = request.list_schedules()
    if result.is_successful():
        if print_running:
            if arguments.output == "json":
                table_printer.output_json({"running": True})
            elif print_running:
                print("ecChronos is running.")
    else:
        if arguments.output == "json":
            table_printer.output_json({"running": False})
        else:
            print("ecChronos is not running.")
        sys.exit(1)


def running_job(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    result = request.running_job()

    if arguments.output == "json":
        table_printer.output_json({"running-job": result})
    else:
        if result == "":
            print("No repair job running.")
        else:
            print("Repair job with id " + result + " is running.")


def run_subcommand(arguments):
    if arguments.subcommand == "rejections":
        status(arguments)
        rejections(arguments)
    elif arguments.subcommand == "repair-info":
        status(arguments)
        repair_info(arguments)
    elif arguments.subcommand == "repairs":
        status(arguments)
        repairs(arguments)
    elif arguments.subcommand == "run-repair":
        status(arguments)
        run_repair(arguments)
    elif arguments.subcommand == "running-job":
        status(arguments)
        running_job(arguments)
    elif arguments.subcommand == "schedules":
        status(arguments)
        schedules(arguments)
    elif arguments.subcommand == "start":
        start(arguments)
    elif arguments.subcommand == "state":
        status(arguments)
        state(arguments)
    elif arguments.subcommand == "status":
        status(arguments, print_running=True)
    elif arguments.subcommand == "stop":
        stop(arguments)


def main():
    parser = get_parser()
    args = parser.parse_args()

    if args.subcommand is None:
        parser.print_help()
        sys.exit(1)

    run_subcommand(args)


if __name__ == "__main__":
    main()

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
from ecchronoslib.argparse_types import parse_duration_ms
from ecchronoslib.rejections import rejections
from ecchronoslib.version import get_client_version
from ecctool_argparse import (
    add_config_subcommand,
    add_metrics_subcommand,
    add_rejections_subcommand,
    add_repair_info_subcommand,
    add_repair_sessions_subcommand,
    add_repairs_subcommand,
    add_run_repair_subcommand,
    add_running_job_subcommand,
    add_schedules_subcommand,
    add_unified_schedules_subcommand,
    add_start_subcommand,
    add_state_subcommand,
    add_status_subcommand,
    add_stop_subcommand,
    add_version_subcommand,
)

DEFAULT_PID_FILE = "ecc.pid"
SPRINGBOOT_MAIN_CLASS = "com.ericsson.bss.cassandra.ecchronos.application.SpringBooter"


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
    add_unified_schedules_subcommand(sub_parsers)
    add_start_subcommand(sub_parsers)
    add_state_subcommand(sub_parsers)
    add_status_subcommand(sub_parsers)
    add_stop_subcommand(sub_parsers)
    add_version_subcommand(sub_parsers)

    return parser


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


def unified_schedules(arguments):
    request = rest.UnifiedScheduleRequest(base_url=arguments.url)
    if arguments.id and arguments.node:
        result = request.get_unified_schedule_node(job_id=arguments.id, node_id=arguments.node, full=arguments.full)
        if result.is_successful():
            table_printer.print_schedule(
                result.data, arguments.limit, arguments.full, columns=arguments.columns, output=arguments.output
            )
        else:
            print(result.format_exception())
        return
    if arguments.id:
        result = request.list_unified_schedule_nodes(job_id=arguments.id)
        if result.is_successful():
            table_printer.print_schedules(
                result.data, arguments.limit, columns=arguments.columns, output=arguments.output
            )
        else:
            print(result.format_exception())
        return
    if arguments.node:
        print("Must specify -i/--id (the unified job id) when using -n/--node.")
        sys.exit(1)
    if arguments.full:
        print("Must specify -i/--id and -n/--node with --full.")
        sys.exit(1)
    if arguments.table and not arguments.keyspace:
        print("Must specify --keyspace if --table is specified.")
        sys.exit(1)
    result = request.list_unified_schedules(keyspace=arguments.keyspace, table=arguments.table)
    if result.is_successful():
        table_printer.print_unified_schedules(
            result.data, arguments.limit, columns=arguments.columns, output=arguments.output
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


def version(arguments):
    client_version = get_client_version()
    request = rest.VersionRequest(base_url=arguments.url, timeout=arguments.timeout)
    connected_url = request.base_url
    result = request.get_version()

    node_version = "unavailable"
    node_error = None
    if result.is_successful() and isinstance(result.data, dict):
        node_version = result.data.get("version", "unknown")
    else:
        node_error = result.format_exception()

    if arguments.output == "json":
        payload = {
            "ecctool_version": client_version,
            "connected_node": connected_url,
            "node_version": node_version,
        }
        if node_error is not None:
            payload["node_error"] = node_error
        table_printer.output_json({"version": payload})
        return

    print("ecctool version:   {0}".format(client_version))
    print("Connected node:    {0}".format(connected_url))
    print("Node version:      {0}".format(node_version))
    if node_error is not None:
        print("Note: could not retrieve node version ({0}).".format(node_error))


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
    "unified-schedules": _with_status(unified_schedules),
    "start": start,
    "state": _with_status(state),
    "status": lambda arguments: status(arguments, print_running=True),
    "stop": stop,
    "version": version,
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


def _exit_on_broken_pipe():
    """Exit cleanly when a downstream reader closed the pipe early.

    When output is piped to a reader that stops early (head, grep -q, less
    quit before the end), the pipe's read end closes and the next write
    raises BrokenPipeError. Redirect stdout to devnull so the interpreter's
    final flush does not raise a second BrokenPipeError during shutdown,
    then exit with the conventional 128 + SIGPIPE(13) status.
    """
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
    except (OSError, ValueError):
        pass
    sys.exit(141)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        _exit_on_broken_pipe()

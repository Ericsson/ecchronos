#!/usr/bin/env python3
# vi: syntax=python
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

from __future__ import print_function

import os
import signal
import sys
import glob
import subprocess
from argparse import ArgumentParser
from io import open

try:
    from ecchronoslib import rest, table_printer
except ImportError:
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    LIB_DIR = os.path.join(SCRIPT_DIR, "..", "pylib")
    sys.path.append(LIB_DIR)
    from ecchronoslib import rest, table_printer

DEFAULT_PID_FILE = "ecc.pid"
SPRINGBOOT_MAIN_CLASS = "com.ericsson.bss.cassandra.ecchronos.application.spring.SpringBooter"


def parse_arguments():
    parser = ArgumentParser(description="ecChronos utility command")
    sub_parsers = parser.add_subparsers(dest="subcommand")
    add_repairs_subcommand(sub_parsers)
    add_schedules_subcommand(sub_parsers)
    add_run_repair_subcommand(sub_parsers)
    add_repair_info_subcommand(sub_parsers)
    add_start_subcommand(sub_parsers)
    add_stop_subcommand(sub_parsers)
    add_status_subcommand(sub_parsers)

    return parser.parse_args()


def add_repairs_subcommand(sub_parsers):
    parser_repairs = sub_parsers.add_parser("repairs",
                                            description="Show the status of all manual repairs. This subcommand has "
                                                        "no mandatory parameters.")
    parser_repairs.add_argument("-k", "--keyspace", type=str,
                                help="Show repairs for the specified keyspace. This argument is mutually exclusive "
                                     "with -i and --id.")
    parser_repairs.add_argument("-t", "--table", type=str,
                                help="Show repairs for the specified table. Keyspace argument -k or --keyspace "
                                     "becomes mandatory if using this argument. This argument is mutually exclusive "
                                     "with -i and --id.")
    parser_repairs.add_argument("-u", "--url", type=str,
                                help="The ecChronos host to connect to, specified in the format http://<host>:<port>.",
                                default=None)
    parser_repairs.add_argument("-i", "--id", type=str,
                                help="Show repairs matching the specified ID. This argument is mutually exclusive "
                                     "with -k, --keyspace, -t and --table.")
    parser_repairs.add_argument("-l", "--limit", type=int,
                                help="Limits the number of rows printed in the output. Specified as a number, "
                                     "-1 to disable limit.",
                                default=-1)
    parser_repairs.add_argument("--hostid", type=str,
                                help='Show repairs for the specified host id. The host id corresponds to the '
                                     'Cassandra instance ecChronos is connected to.')


def add_schedules_subcommand(sub_parsers):
    parser_schedules = sub_parsers.add_parser("schedules",
                                              description="Show the status of schedules. This subcommand has no "
                                                          "mandatory parameters.")
    parser_schedules.add_argument("-k", "--keyspace", type=str,
                                  help="Show schedules for the specified keyspace. This argument is mutually "
                                       "exclusive with -i and --id.")
    parser_schedules.add_argument("-t", "--table", type=str,
                                  help="Show schedules for the specified table. Keyspace argument -k or --keyspace "
                                       "becomes mandatory if using this argument. This argument is mutually exclusive "
                                       "with -i and --id.")
    parser_schedules.add_argument("-u", "--url", type=str,
                                  help="The ecChronos host to connect to, specified in the format "
                                       "http://<host>:<port>.",
                                  default=None)
    parser_schedules.add_argument("-i", "--id", type=str,
                                  help="Show schedules matching the specified ID. This argument is mutually exclusive "
                                       "with -k, --keyspace, -t and --table.")
    parser_schedules.add_argument("-f", "--full", action="store_true",
                                  help="Show full schedules, can only be used with -i or --id. Full schedules include "
                                       "schedule configuration and repair state per vnode.",
                                  default=False)
    parser_schedules.add_argument("-l", "--limit", type=int,
                                  help="Limits the number of rows printed in the output. Specified as a number, "
                                       "-1 to disable limit.",
                                  default=-1)


def add_run_repair_subcommand(sub_parsers):
    parser_run_repair = sub_parsers.add_parser("run-repair",
                                               description="Run a manual repair. The manual repair will be triggered "
                                                           "in ecChronos, which will group sub-ranges by nodes that "
                                                           "have them in common. Afterwards, each sub-range will be "
                                                           "repaired once through Cassandra JMX interface. This "
                                                           "subcommand has no mandatory parameters.")
    parser_run_repair.add_argument("-u", "--url", type=str,
                                   help="The ecChronos host to connect to, specified in the format "
                                        "http://<host>:<port>.",
                                   default=None)
    parser_run_repair.add_argument("--local", action='store_true',
                                   help='Run repair for the local node only, i.e repair will only be performed for '
                                        'the ranges that the local node is a replica for.', default=False)
    parser_run_repair.add_argument("-k", "--keyspace", type=str,
                                   help="Run repair for the specified keyspace. Repair will be run for all tables "
                                        "within the keyspace with replication factor higher than 1.", required=False)
    parser_run_repair.add_argument("-t", "--table", type=str,
                                   help="Run repair for the specified table. Keyspace argument -k or --keyspace "
                                        "becomes mandatory if using this argument.", required=False)


def add_repair_info_subcommand(sub_parsers):
    parser_repair_info = sub_parsers.add_parser("repair-info",
                                                description="Get information about repairs for tables. The repair "
                                                            "information is based on repair history, meaning that "
                                                            "both manual repairs and schedules will contribute to the "
                                                            "repair information. This subcommand requires the user to "
                                                            "provide either --since or --duration if --keyspace and "
                                                            "--table is not provided. If repair info is fetched for a "
                                                            "specific table using --keyspace and --table, "
                                                            "the duration will default to the table's "
                                                            "GC_GRACE_SECONDS.")
    parser_repair_info.add_argument("-k", "--keyspace", type=str,
                                    help="Show repair information for all tables in the specified keyspace.")
    parser_repair_info.add_argument("-t", "--table", type=str,
                                    help="Show repair information for the specified table. Keyspace argument -k or "
                                         "--keyspace becomes mandatory if using this argument.")
    parser_repair_info.add_argument("-s", "--since", type=str,
                                    help="Show repair information since the specified date to now. Date must be "
                                         "specified in ISO8601 format. The time-window will be since to now. "
                                         "Mandatory if --duration or --keyspace and --table is not specified.",
                                    default=None)
    parser_repair_info.add_argument("-d", "--duration", type=str,
                                    help="Show repair information for the duration. Duration can be specified as "
                                         "ISO8601 format or as simple format in form: 5s, 5m, 5h, 5d. The time-window "
                                         "will be now-duration to now. Mandatory if --since or --keyspace and --table "
                                         "is not specified.",
                                    default=None)
    parser_repair_info.add_argument("--local", action='store_true',
                                    help='Show repair information only for the local node.',
                                    default=False)
    parser_repair_info.add_argument("-u", "--url", type=str,
                                    help="The ecChronos host to connect to, specified in the format "
                                         "http://<host>:<port>.",
                                    default=None)
    parser_repair_info.add_argument("-l", "--limit", type=int,
                                    help="Limits the number of rows printed in the output. Specified as a number, "
                                         "-1 to disable limit.",
                                    default=-1)


def add_start_subcommand(sub_parsers):
    parser_config = sub_parsers.add_parser("start",
                                           description="Start the ecChronos service. This subcommand has no mandatory "
                                                       "parameters.")
    parser_config.add_argument("-f", "--foreground", action="store_true",
                               help="Start the ecChronos instance in foreground mode (exec in current terminal and "
                                    "log to stdout)", default=False)
    parser_config.add_argument("-p", "--pidfile", type=str,
                               help="Start the ecChronos instance and store the pid in the specified pid file.")


def add_stop_subcommand(sub_parsers):
    parser_stop = sub_parsers.add_parser("stop",
                                         description="Stop the ecChronos instance. Stopping of ecChronos is done by "
                                                     "using kill with SIGTERM signal (same as kill in shell) for the "
                                                     "pid. This subcommand has no mandatory parameters.")
    parser_stop.add_argument("-p", "--pidfile", type=str,
                             help="Stops the ecChronos instance by pid fetched from the specified pid file.")


def add_status_subcommand(sub_parsers):
    parser_status = sub_parsers.add_parser("status",
                                           description="View status of ecChronos instance. This subcommand has no "
                                                       "mandatory parameters.")
    parser_status.add_argument("-u", "--url", type=str,
                               help="The ecChronos host to connect to, specified in the format "
                                    "http://<host>:<port>.",
                               default=None)


def schedules(arguments):
    # pylint: disable=too-many-branches
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    full = False
    if arguments.id:
        if arguments.full:
            result = request.get_schedule(job_id=arguments.id, full=True)
            full = True
        else:
            result = request.get_schedule(job_id=arguments.id)

        if result.is_successful():
            table_printer.print_schedule(result.data, arguments.limit, full)
        else:
            print(result.format_exception())
    elif arguments.full:
        print("Must specify id with full")
        sys.exit(1)
    elif arguments.table:
        if not arguments.keyspace:
            print("Must specify keyspace")
            sys.exit(1)
        result = request.list_schedules(keyspace=arguments.keyspace, table=arguments.table)
        if result.is_successful():
            table_printer.print_schedules(result.data, arguments.limit)
        else:
            print(result.format_exception())
    else:
        result = request.list_schedules(keyspace=arguments.keyspace)
        if result.is_successful():
            table_printer.print_schedules(result.data, arguments.limit)
        else:
            print(result.format_exception())


def repairs(arguments):
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    if arguments.id:
        result = request.get_repair(job_id=arguments.id, host_id=arguments.hostid)
        if result.is_successful():
            table_printer.print_repairs(result.data, arguments.limit)
        else:
            print(result.format_exception())
    elif arguments.table:
        if not arguments.keyspace:
            print("Must specify keyspace")
            sys.exit(1)
        result = request.list_repairs(keyspace=arguments.keyspace, table=arguments.table, host_id=arguments.hostid)
        if result.is_successful():
            table_printer.print_repairs(result.data, arguments.limit)
        else:
            print(result.format_exception())
    else:
        result = request.list_repairs(keyspace=arguments.keyspace, host_id=arguments.hostid)
        if result.is_successful():
            table_printer.print_repairs(result.data, arguments.limit)
        else:
            print(result.format_exception())


def run_repair(arguments):
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    if not arguments.keyspace and arguments.table:
        print("--keyspace must be specified if table is specified")
        sys.exit(1)
    result = request.post(keyspace=arguments.keyspace, table=arguments.table, local=arguments.local)
    if result.is_successful():
        table_printer.print_repairs(result.data)
    else:
        print(result.format_exception())


def repair_info(arguments):
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    if not arguments.keyspace and arguments.table:
        print("--keyspace must be specified if table is specified")
        sys.exit(1)
    if not arguments.duration and not arguments.since and not arguments.table:
        print("Either --duration or --since or both must be provided if called without --keyspace and --table")
        sys.exit(1)
    duration = None
    if arguments.duration:
        if arguments.duration[0] == "+" or arguments.duration[0] == "-":
            print("'+' and '-' is not allowed in duration, check help for more information")
            sys.exit(1)
        duration = arguments.duration.upper()
    result = request.get_repair_info(keyspace=arguments.keyspace, table=arguments.table,
                                     since=arguments.since, duration=duration,
                                     local=arguments.local)
    if result.is_successful():
        table_printer.print_repair_info(result.data, arguments.limit)
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
    proc = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, # pylint: disable=consider-using-with
                            cwd=cwd)
    pid = proc.pid
    print("ecc started with pid {0}".format(pid))
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
            sys.stdout.write(line)
        proc.wait()


def stop(arguments):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    ecchronos_home_dir = os.path.join(script_dir, "..")
    pid_file = os.path.join(ecchronos_home_dir, DEFAULT_PID_FILE)
    if arguments.pidfile:
        pid_file = arguments.pidfile
    with open(pid_file, "r", encoding="utf-8") as p_file:
        pid = int(p_file.readline())
        print("Killing ecc with pid {0}".format(pid))
        os.kill(pid, signal.SIGTERM)
    os.remove(pid_file)


def status(arguments, print_running=False):
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    result = request.list_schedules()
    if result.is_successful():
        if print_running:
            print("ecChronos is running")
    else:
        print("ecChronos is not running")
        sys.exit(1)


def run_subcommand(arguments):
    if arguments.subcommand == "repairs":
        status(arguments)
        repairs(arguments)
    elif arguments.subcommand == "schedules":
        status(arguments)
        schedules(arguments)
    elif arguments.subcommand == "run-repair":
        status(arguments)
        run_repair(arguments)
    elif arguments.subcommand == "repair-info":
        status(arguments)
        repair_info(arguments)
    elif arguments.subcommand == "start":
        start(arguments)
    elif arguments.subcommand == "stop":
        stop(arguments)
    elif arguments.subcommand == "status":
        status(arguments, print_running=True)


def main():
    run_subcommand(parse_arguments())


if __name__ == "__main__":
    main()

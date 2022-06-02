#!/bin/sh
""""exec python -B -- "$0" ${1+"$@"} # """
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
    from ecchronoslib import rest, table_printer, table_printer_v2
except ImportError:
    SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
    LIB_DIR = os.path.join(SCRIPT_DIR, "..", "pylib")
    sys.path.append(LIB_DIR)
    from ecchronoslib import rest, table_printer, table_printer_v2

DEFAULT_PID_FILE = "ecc.pid"
SPRINGBOOT_MAIN_CLASS = "com.ericsson.bss.cassandra.ecchronos.application.spring.SpringBooter"

def parse_arguments():
    parser = ArgumentParser(description="ecChronos utility command")
    sub_parsers = parser.add_subparsers(dest="subcommand")
    add_repair_status_subcommand(sub_parsers)
    add_repairs_subcommand(sub_parsers)
    add_schedules_subcommand(sub_parsers)
    add_repair_config_subcommand(sub_parsers)
    add_trigger_repair_subcommand(sub_parsers)
    add_run_repair_subcommand(sub_parsers)
    add_start_subcommand(sub_parsers)
    add_stop_subcommand(sub_parsers)
    add_status_subcommand(sub_parsers)

    return parser.parse_args()

def add_repair_status_subcommand(sub_parsers):
    parser_repair_status = sub_parsers.add_parser("repair-status",
                                                  description="Show status of all repairs and schedules")
    parser_repair_status.add_argument("-k", "--keyspace", type=str,
                                      help="Print status(es) for a specific keyspace")
    parser_repair_status.add_argument("-t", "--table", type=str,
                                      help="Print status(es) for a specific table (Must be specified with keyspace)")
    parser_repair_status.add_argument("-u", "--url", type=str,
                                      help="The host to connect to with the format (http://<host>:port)",
                                      default=None)
    parser_repair_status.add_argument("-i", "--id", type=str,
                                      help="Print verbose status for a specific job")
    parser_repair_status.add_argument("-l", "--limit", type=int,
                                      help="Limit the number of tables or virtual nodes printed (-1 to disable)",
                                      default=-1)
    parser_repair_status.set_defaults(func=repair_status)

def add_repairs_subcommand(sub_parsers):
    parser_repairs = sub_parsers.add_parser("repairs",
                                            description="Show status of triggered repairs")
    parser_repairs.add_argument("-k", "--keyspace", type=str,
                                help="Print status(es) for a specific keyspace")
    parser_repairs.add_argument("-t", "--table", type=str,
                                help="Print status(es) for a specific table (Must be specified with keyspace)")
    parser_repairs.add_argument("-u", "--url", type=str,
                                help="The host to connect to with the format (http://<host>:port)",
                                default=None)
    parser_repairs.add_argument("-i", "--id", type=str,
                                help="Print status for a specific repair")
    parser_repairs.add_argument("-l", "--limit", type=int,
                                help="Limit the number of tables or virtual nodes printed (-1 to disable)",
                                default=-1)
    parser_repairs.add_argument("--local", action='store_true',
                                help='Show only repairs for local node, default is False', default=False)

def add_schedules_subcommand(sub_parsers):
    parser_schedules = sub_parsers.add_parser("schedules",
                                              description="Show status of schedules")
    parser_schedules.add_argument("-k", "--keyspace", type=str,
                                  help="Print status(es) for a specific keyspace")
    parser_schedules.add_argument("-t", "--table", type=str,
                                  help="Print status(es) for a specific table (Must be specified with keyspace)")
    parser_schedules.add_argument("-u", "--url", type=str,
                                  help="The host to connect to with the format (http://<host>:port)",
                                  default=None)
    parser_schedules.add_argument("-i", "--id", type=str,
                                  help="Print status for a specific schedule")
    parser_schedules.add_argument("-f", "--full", action="store_true",
                                  help="Print all information for a specific job (Can only be used with id)",
                                  default=False)
    parser_schedules.add_argument("-l", "--limit", type=int,
                                  help="Limit the number of tables or virtual nodes printed (-1 to disable)",
                                  default=-1)

def add_repair_config_subcommand(sub_parsers):
    parser_repair_config = sub_parsers.add_parser("repair-config",
                                                  description="Show repair configuration")
    parser_repair_config.add_argument("-k", "--keyspace", type=str,
                                      help="Show config for a specific keyspace")
    parser_repair_config.add_argument("-t", "--table", type=str,
                                      help="Show config for a specific table")
    parser_repair_config.add_argument("-i", "--id", type=str,
                                      help="Show config for a specific job")
    parser_repair_config.add_argument("-u", "--url", type=str,
                                      help="The host to connect to with the format (http://<host>:port)",
                                      default=None)

def add_trigger_repair_subcommand(sub_parsers):
    parser_trigger_repair = sub_parsers.add_parser("trigger-repair",
                                                   description="Trigger a single repair")
    parser_trigger_repair.add_argument("-u", "--url", type=str,
                                       help="The host to connect to with the format (http://<host>:port)",
                                       default=None)
    required_args = parser_trigger_repair.add_argument_group("required arguments")
    required_args.add_argument("-k", "--keyspace", type=str,
                               help="Keyspace where the repair should be triggered", required=True)
    required_args.add_argument("-t", "--table", type=str,
                               help="Table where the repair should be triggered", required=True)

def add_run_repair_subcommand(sub_parsers):
    parser_trigger_repair = sub_parsers.add_parser("run-repair",
                                                   description="Run a single repair on a table")
    parser_trigger_repair.add_argument("-u", "--url", type=str,
                                       help="The host to connect to with the format (http://<host>:port)",
                                       default=None)
    parser_trigger_repair.add_argument("--local", action='store_true',
                                       help='repair will run for the local node, default is False', default=False)
    required_args = parser_trigger_repair.add_argument_group("required arguments")
    required_args.add_argument("-k", "--keyspace", type=str,
                               help="Keyspace where the repair should be triggered", required=True)
    required_args.add_argument("-t", "--table", type=str,
                               help="Table where the repair should be triggered", required=True)

def add_start_subcommand(sub_parsers):
    parser_config = sub_parsers.add_parser("start",
                                           description="Start ecChronos service")
    parser_config.add_argument("-f", "--foreground", action="store_true",
                               help="Start in foreground", default=False)
    parser_config.add_argument("-p", "--pidfile", type=str,
                               help="Pidfile where to store the pid, default $ECCHRONOS_HOME/ecc.pid")

def add_stop_subcommand(sub_parsers):
    parser_stop = sub_parsers.add_parser("stop",
                                         description="Stop ecChronos service")
    parser_stop.add_argument("-p", "--pidfile", type=str,
                             help="Pidfile where to retrieve the pid, default $ECCHRONOS_HOME/ecc.pid")

def add_status_subcommand(sub_parsers):
    parser_status = sub_parsers.add_parser("status",
                                           description="Show status of ecChronos service")
    parser_status.add_argument("-u", "--url", type=str,
                               help="The host to connect to with the format (http://<host>:port)",
                               default=None)

def repair_status(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url)

    if arguments.id:
        result = request.get(job_id=arguments.id)
        if result.is_successful():
            table_printer.print_verbose_repair_job(result.data, arguments.limit)
        else:
            print(result.format_exception())
    elif arguments.table:
        if not arguments.keyspace:
            print("Must specify keyspace")
            sys.exit(1)
        result = request.list(keyspace=arguments.keyspace, table=arguments.table)
        if result.is_successful():
            table_printer.print_repair_jobs(result.data, arguments.limit)
        else:
            print(result.format_exception())
    else:
        result = request.list(keyspace=arguments.keyspace)
        if result.is_successful():
            table_printer.print_repair_jobs(result.data, arguments.limit)
        else:
            print(result.format_exception())


def schedules(arguments):
    #pylint: disable=too-many-branches
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    printer = table_printer_v2
    full = False
    if arguments.id:
        if arguments.full:
            result = request.get_schedule(job_id=arguments.id, full=True)
            full = True
        else:
            result = request.get_schedule(job_id=arguments.id)

        if result.is_successful():
            printer.print_schedule(result.data, arguments.limit, full)
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
            printer.print_schedules(result.data, arguments.limit)
        else:
            print(result.format_exception())
    else:
        result = request.list_schedules(keyspace=arguments.keyspace)
        if result.is_successful():
            printer.print_schedules(result.data, arguments.limit)
        else:
            print(result.format_exception())


def repairs(arguments):
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    printer = table_printer_v2
    if arguments.id:
        result = request.get_repair(job_id=arguments.id, local=arguments.local)
        if result.is_successful():
            printer.print_repairs(result.data, arguments.limit)
        else:
            print(result.format_exception())
    elif arguments.table:
        if not arguments.keyspace:
            print("Must specify keyspace")
            sys.exit(1)
        result = request.list_repairs(keyspace=arguments.keyspace, table=arguments.table, local=arguments.local)
        if result.is_successful():
            printer.print_repairs(result.data, arguments.limit)
        else:
            print(result.format_exception())
    else:
        result = request.list_repairs(keyspace=arguments.keyspace, local=arguments.local)
        if result.is_successful():
            printer.print_repairs(result.data, arguments.limit)
        else:
            print(result.format_exception())


def repair_config(arguments):
    request = rest.RepairConfigRequest(base_url=arguments.url)

    if arguments.id:
        if arguments.keyspace or arguments.table:
            print("id must be specified alone")
            sys.exit(1)
        result = request.get(job_id=arguments.id)
    else:
        result = request.list(keyspace=arguments.keyspace, table=arguments.table)

    if result.is_successful():
        table_printer.print_table_config(result.data)
    else:
        print(result.format_exception())

def run_repair(arguments):
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    printer = table_printer_v2
    result = request.post(keyspace=arguments.keyspace, table=arguments.table, local=arguments.local)
    if result.is_successful():
        printer.print_repair(result.data)
    else:
        print(result.format_exception())

def trigger_repair(arguments):
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    result = request.post(keyspace=arguments.keyspace, table=arguments.table)
    if result.is_successful():
        table_printer.print_repair_job(result.data)
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
                jvm_opts += "{0} ".format(line)
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
        p_file.write(u"{0}".format(pid))
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
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    result = request.list()
    print("Deprecated protocol, use 'repairs', 'schedules' or 'run-repair'")
    if result.is_successful():
        if print_running:
            print("ecChronos is running")
    else:
        print("ecChronos is not running")
        sys.exit(1)

def status_v2(arguments, print_running=False):
    request = rest.V2RepairSchedulerRequest(base_url=arguments.url)
    result = request.list_repairs()
    if result.is_successful():
        if print_running:
            print("ecChronos is running")
    else:
        print("ecChronos is not running")
        sys.exit(1)

def run_subcommand(arguments):
    if arguments.subcommand == "repair-status":
        status(arguments)
        repair_status(arguments)
    elif arguments.subcommand == "repairs":
        status_v2(arguments)
        repairs(arguments)
    elif arguments.subcommand == "schedules":
        status_v2(arguments)
        schedules(arguments)
    elif arguments.subcommand == "repair-config":
        status(arguments)
        repair_config(arguments)
    elif arguments.subcommand == "trigger-repair":
        status(arguments)
        trigger_repair(arguments)
    elif arguments.subcommand == "run-repair":
        status_v2(arguments)
        run_repair(arguments)
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

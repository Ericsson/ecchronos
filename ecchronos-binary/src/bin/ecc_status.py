#!/bin/sh
''''exec python -B -- "$0" ${1+"$@"} # '''
# vi: syntax=python
#
# Copyright 2019 Telefonaktiebolaget LM Ericsson
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
import sys
from argparse import ArgumentParser

try:
    from ecchronoslib import rest, table_formatter
except ImportError:
    SCRIPT_DIR = os.path.dirname(__file__)
    LIB_DIR = os.path.join(SCRIPT_DIR, "..", "pylib")
    sys.path.append(LIB_DIR)
    from ecchronoslib import rest, table_formatter


def add_vnode_state_to_table(vnode_state, table):
    entry = list()
    entry.append(vnode_state.start_token)
    entry.append(vnode_state.end_token)
    entry.append(', '.join(vnode_state.replicas))
    entry.append(vnode_state.get_last_repaired_at())
    entry.append(vnode_state.repaired)

    table.append(entry)


def print_verbose_repair_job(repair_job, max_lines):
    if not repair_job.is_valid():
        print('Repair job not found')
        return

    verbose_print_format = "{0:15s}: {1}"

    print(verbose_print_format.format("Keyspace", repair_job.keyspace))
    print(verbose_print_format.format("Table", repair_job.table))
    print(verbose_print_format.format("Status", repair_job.status))
    print(verbose_print_format.format("Repaired(%)", repair_job.get_repair_percentage()))
    print(verbose_print_format.format("Repaired at", repair_job.get_last_repaired_at()))
    print(verbose_print_format.format("Next repair", repair_job.get_next_repair()))
    print(verbose_print_format.format("Recurring", repair_job.recurring))

    vnode_state_table = list()
    vnode_state_table.append(["Start token", "End token", "Replicas", "Repaired at", "Repaired"])

    sorted_vnode_states = sorted(repair_job.vnode_states, key=lambda vnode: vnode.last_repaired_at_in_ms)

    if max_lines > -1:
        sorted_vnode_states = sorted_vnode_states[:max_lines]

    for vnode_state in sorted_vnode_states:
        add_vnode_state_to_table(vnode_state, vnode_state_table)

    table_formatter.format_table(vnode_state_table)


def convert_repair_job(repair_job):
    entry = list()
    entry.append(repair_job.keyspace)
    entry.append(repair_job.table)
    entry.append(repair_job.status)
    entry.append(repair_job.get_repair_percentage())
    entry.append(repair_job.get_last_repaired_at())
    entry.append(repair_job.get_next_repair())
    entry.append(repair_job.recurring)
    entry.append(repair_job.job_id)

    return entry


def print_summary(repair_jobs):
    status_list = [job.status for job in repair_jobs]
    summary_format = "Summary: {0} completed, {1} in queue, {2} warning, {3} error"
    print(summary_format.format(status_list.count('COMPLETED'),
                                status_list.count('IN_QUEUE'),
                                status_list.count('WARNING'),
                                status_list.count('ERROR')))


def print_repair_jobs(repair_jobs, max_lines):
    repair_jobs_table = list()
    repair_jobs_table.append(["Keyspace", "Table", "Status", "Repaired(%)",
                              "Repaired at", "Next repair", "recurring", "id"])
    sorted_repair_jobs = sorted(repair_jobs, key=lambda job: job.last_repaired_at_in_ms)

    if max_lines > -1:
        sorted_repair_jobs = sorted_repair_jobs[:max_lines]

    for repair_job in sorted_repair_jobs:
        repair_jobs_table.append(convert_repair_job(repair_job))
    table_formatter.format_table(repair_jobs_table)

    print_summary(repair_jobs)


def parse_arguments():
    parser = ArgumentParser(description='Show repair scheduler status')
    parser.add_argument('-k', '--keyspace', type=str,
                        help='Print status(es) for a specific keyspace')
    parser.add_argument('-t', '--table', type=str,
                        help='Print status(es) for a specific table (Must be specified with keyspace)')
    parser.add_argument('-i', '--id', type=str,
                        help='Print verbose status for a specific job')
    parser.add_argument('-l', '--limit', type=int,
                        help='Limit the number of tables or virtual nodes printed (-1 to disable)',
                        default=15)
    parser.add_argument('-u', '--url', type=str,
                        help='The host to connect to with the format (http://<host>:port)',
                        default=None)
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    request = rest.RepairSchedulerRequest(base_url=arguments.url)

    if arguments.id:
        result = request.get(job_id=arguments.id)
        if result.is_successful():
            print_verbose_repair_job(result.data, arguments.limit)
        else:
            print(result.format_exception())
    elif arguments.table:
        if not arguments.keyspace:
            print("Must specify keyspace")
            sys.exit(1)
        result = request.list(keyspace=arguments.keyspace, table=arguments.table)
        if result.is_successful():
            print_repair_jobs(result.data, arguments.limit)
        else:
            print(result.format_exception())
    else:
        result = request.list(keyspace=arguments.keyspace)
        if result.is_successful():
            print_repair_jobs(result.data, arguments.limit)
        else:
            print(result.format_exception())


if __name__ == "__main__":
    main()

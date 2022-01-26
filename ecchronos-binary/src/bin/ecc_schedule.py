#!/bin/sh
''''exec python -B -- "$0" ${1+"$@"} # '''
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
import sys
from argparse import ArgumentParser

try:
    from ecchronoslib import rest, table_formatter
except ImportError:
    SCRIPT_DIR = os.path.dirname(__file__)
    LIB_DIR = os.path.join(SCRIPT_DIR, "..", "pylib")
    sys.path.append(LIB_DIR)
    from ecchronoslib import rest, table_formatter

def convert_repair_job(repair_job):
    entry = [repair_job.job_id, repair_job.keyspace, repair_job.table, repair_job.status,
             repair_job.get_repair_percentage(), repair_job.get_last_repaired_at(), repair_job.get_next_repair(),
             repair_job.recurring]

    return entry

def print_repair_job(repair_job):
    repair_jobs_table = [["Id", "Keyspace", "Table", "Status", "Repaired(%)",
                          "Completed at", "Next repair", "Recurring"]]
    repair_jobs_table.append(convert_repair_job(repair_job))
    table_formatter.format_table(repair_jobs_table)


def parse_arguments():
    parser = ArgumentParser(description='Schedule a repair')
    parser.add_argument('-k', '--keyspace', type=str,
                        help='Keyspace where the repair should be scheduled', required=True)
    parser.add_argument('-t', '--table', type=str,
                        help='Table where the repair should be scheduled', required=True)
    parser.add_argument('-u', '--url', type=str,
                        help='The host to connect to with the format (http://<host>:port)',
                        default=None)
    return parser.parse_args()


def main():
    print("Deprecated, please use ecctool instead")
    arguments = parse_arguments()
    request = rest.RepairSchedulerRequest(base_url=arguments.url)
    result = request.post(keyspace=arguments.keyspace, table=arguments.table)
    if result.is_successful():
        print_repair_job(result.data)
    else:
        print(result.format_exception())


if __name__ == "__main__":
    main()

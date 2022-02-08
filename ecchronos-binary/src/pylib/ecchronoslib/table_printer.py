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
from ecchronoslib import table_formatter


def print_verbose_repair_job(repair_job, max_lines):
    if not repair_job.is_valid():
        print('Repair job not found')
        return

    verbose_print_format = "{0:15s}: {1}"

    print(verbose_print_format.format("Id", repair_job.job_id))
    print(verbose_print_format.format("Keyspace", repair_job.keyspace))
    print(verbose_print_format.format("Table", repair_job.table))
    print(verbose_print_format.format("Status", repair_job.status))
    print(verbose_print_format.format("Repaired(%)", repair_job.get_repair_percentage()))
    print(verbose_print_format.format("Completed at", repair_job.get_last_repaired_at()))
    print(verbose_print_format.format("Next repair", repair_job.get_next_repair()))
    print(verbose_print_format.format("Recurring", repair_job.recurring))

    vnode_state_table = [["Start token", "End token", "Replicas", "Repaired at", "Repaired"]]

    sorted_vnode_states = sorted(repair_job.vnode_states, key=lambda vnode: vnode.last_repaired_at_in_ms, reverse=True)

    if max_lines > -1:
        sorted_vnode_states = sorted_vnode_states[:max_lines]

    for vnode_state in sorted_vnode_states:
        _add_vnode_state_to_table(vnode_state, vnode_state_table)

    table_formatter.format_table(vnode_state_table)

def _add_vnode_state_to_table(vnode_state, table):
    entry = [vnode_state.start_token, vnode_state.end_token, ', '.join(vnode_state.replicas),
             vnode_state.get_last_repaired_at(), vnode_state.repaired]

    table.append(entry)

def print_summary(repair_jobs):
    status_list = [job.status for job in repair_jobs]
    summary_format = "Summary: {0} completed, {1} in queue, {2} warning, {3} error"
    print(summary_format.format(status_list.count('COMPLETED'),
                                status_list.count('IN_QUEUE'),
                                status_list.count('WARNING'),
                                status_list.count('ERROR')))

def print_repair_jobs(repair_jobs, max_lines):
    repair_jobs_table = [["Id", "Keyspace", "Table", "Status", "Repaired(%)",
                          "Completed at", "Next repair", "Recurring"]]
    sorted_repair_jobs = sorted(repair_jobs, key=lambda job: job.last_repaired_at_in_ms, reverse=True)

    if max_lines > -1:
        sorted_repair_jobs = sorted_repair_jobs[:max_lines]

    for repair_job in sorted_repair_jobs:
        repair_jobs_table.append(_convert_repair_job(repair_job))
    table_formatter.format_table(repair_jobs_table)

    print_summary(repair_jobs)

def print_repair_job(repair_job):
    repair_jobs_table = [["Id", "Keyspace", "Table", "Status", "Repaired(%)",
                          "Completed at", "Next repair", "Recurring"]]
    repair_jobs_table.append(_convert_repair_job(repair_job))
    table_formatter.format_table(repair_jobs_table)

def _convert_repair_job(repair_job):
    entry = [repair_job.job_id, repair_job.keyspace, repair_job.table, repair_job.status,
             repair_job.get_repair_percentage(), repair_job.get_last_repaired_at(), repair_job.get_next_repair(),
             repair_job.recurring]

    return entry

def print_table_config(config_data):
    config_table = [["Id", "Keyspace", "Table", "Interval",
                     "Parallelism", "Unwind ratio", "Warning time", "Error time"]]
    if isinstance(config_data, list):
        sorted_config_data = sorted(config_data, key=lambda config: (config.keyspace, config.table))
        for config in sorted_config_data:
            if config.is_valid():
                config_table.append(_convert_config(config))
    elif config_data.is_valid():
        config_table.append(_convert_config(config_data))

    table_formatter.format_table(config_table)

def _convert_config(config):
    entry = [config.job_id, config.keyspace, config.table, config.get_repair_interval(), config.repair_parallelism,
             config.repair_unwind_ratio, config.get_repair_warning_time(), config.get_repair_error_time()]

    return entry

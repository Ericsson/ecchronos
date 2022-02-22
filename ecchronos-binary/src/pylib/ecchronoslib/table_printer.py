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

def print_summary(repair_jobs):
    status_list = [job.status for job in repair_jobs]
    summary_format = "Summary: {0} completed, {1} in queue, {2} warning, {3} error"
    print(summary_format.format(status_list.count('COMPLETED'),
                                status_list.count('IN_QUEUE'),
                                status_list.count('WARNING'),
                                status_list.count('ERROR')))

def print_schedules(schedules, max_lines):
    schedules_table = [["Id", "Keyspace", "Table", "Status", "Repaired(%)",
                        "Last repair", "Next repair"]]
    sorted_schedules = sorted(schedules, key=lambda schedule: schedule.last_repaired_at_in_ms, reverse=True)

    if max_lines > -1:
        sorted_schedules = sorted_schedules[:max_lines]

    for schedule in sorted_schedules:
        schedules_table.append(_convert_schedule(schedule))
    table_formatter.format_table(schedules_table)

def print_schedule(schedule):
    schedules_table = [["Id", "Keyspace", "Table", "Status", "Repaired(%)",
                        "Last repair", "Next repair"]]
    schedules_table.append(_convert_schedule(schedule))
    table_formatter.format_table(schedules_table)

def _convert_schedule(schedule):
    entry = [schedule.job_id, schedule.keyspace, schedule.table, schedule.status,
             schedule.get_repair_percentage(), schedule.get_last_repaired_at(), schedule.get_next_repair()]
    return entry

def print_verbose_schedule(schedule, max_lines):
    if not schedule.is_valid():
        print('Schedule not found')
        return

    verbose_print_format = "{0:15s}: {1}"

    print(verbose_print_format.format("Id", schedule.job_id))
    print(verbose_print_format.format("Keyspace", schedule.keyspace))
    print(verbose_print_format.format("Table", schedule.table))
    print(verbose_print_format.format("Status", schedule.status))
    print(verbose_print_format.format("Repaired(%)", schedule.get_repair_percentage()))
    print(verbose_print_format.format("Last repair", schedule.get_last_repaired_at()))
    print(verbose_print_format.format("Next repair", schedule.get_next_repair()))
    vnode_state_table = [["Start token", "End token", "Replicas", "Repaired at", "Repaired"]]
    sorted_vnode_states = sorted(schedule.vnode_states, key=lambda vnode: vnode.last_repaired_at_in_ms, reverse=True)

    if max_lines > -1:
        sorted_vnode_states = sorted_vnode_states[:max_lines]

    for vnode_state in sorted_vnode_states:
        _add_vnode_state_to_table(vnode_state, vnode_state_table)

    table_formatter.format_table(vnode_state_table)

def _add_vnode_state_to_table(vnode_state, table):
    entry = [vnode_state.start_token, vnode_state.end_token, ', '.join(vnode_state.replicas),
             vnode_state.get_last_repaired_at(), vnode_state.repaired]

    table.append(entry)

def print_repairs(repairs, max_lines):
    repairs_table = [["Id", "Keyspace", "Table", "Status", "Remaining tasks",
                      "Triggered by", "Started at", "Finished at"]]
    sorted_repairs = sorted(repairs, key=lambda repair: repair.finished_at_in_ms, reverse=True)

    if max_lines > -1:
        sorted_repairs = sorted_repairs[:max_lines]

    for repair in sorted_repairs:
        repairs_table.append(_convert_repair(repair))
    table_formatter.format_table(repairs_table)

def print_repair(repair):
    repairs_table = [["Id", "Keyspace", "Table", "Status", "Remaining tasks",
                      "Triggered by", "Started at", "Finished at"]]
    repairs_table.append(_convert_repair(repair))
    table_formatter.format_table(repairs_table)

def _convert_repair(repair):
    entry = [repair.repair_id, repair.keyspace, repair.table, repair.status,
             repair.remaining_tasks, repair.triggered_by, repair.get_started_at(), repair.get_finished_at()]
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

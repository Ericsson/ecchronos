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
from datetime import datetime
from ecchronoslib import table_formatter


def print_schedule(schedule, max_lines, full=False):
    if not schedule.is_valid():
        print("Schedule not found")
        return

    verbose_print_format = "{0:15s}: {1}"

    print(verbose_print_format.format("Id", schedule.job_id))
    print(verbose_print_format.format("Keyspace", schedule.keyspace))
    print(verbose_print_format.format("Table", schedule.table))
    print(verbose_print_format.format("Status", schedule.status))
    print(verbose_print_format.format("Repaired(%)", schedule.get_repair_percentage()))
    print(verbose_print_format.format("Completed at", schedule.get_last_repaired_at()))
    print(verbose_print_format.format("Next repair", schedule.get_next_repair()))
    print(verbose_print_format.format("Repair type", schedule.repair_type))
    print(verbose_print_format.format("Config", schedule.get_config()))

    if full:
        vnode_state_table = [["Start token", "End token", "Replicas", "Repaired at", "Repaired"]]

        sorted_vnode_states = sorted(
            schedule.vnode_states, key=lambda vnode: vnode.last_repaired_at_in_ms, reverse=True
        )

        if max_lines > -1:
            sorted_vnode_states = sorted_vnode_states[:max_lines]

        for vnode_state in sorted_vnode_states:
            _add_vnode_state_to_table(vnode_state, vnode_state_table)

        table_formatter.format_table(vnode_state_table)


def _add_vnode_state_to_table(vnode_state, table):
    entry = [
        vnode_state.start_token,
        vnode_state.end_token,
        ", ".join(vnode_state.replicas),
        vnode_state.get_last_repaired_at(),
        vnode_state.repaired,
    ]

    table.append(entry)


def print_summary(schedules):
    status_list = [schedule.status for schedule in schedules]
    summary_format = "Summary: {0} completed, {1} on time, {2} blocked, {3} late, {4} overdue"
    print(
        summary_format.format(
            status_list.count("COMPLETED"),
            status_list.count("ON_TIME"),
            status_list.count("BLOCKED"),
            status_list.count("LATE"),
            status_list.count("OVERDUE"),
        )
    )


def print_repair_summary(repairs):
    status_list = [repair.status for repair in repairs]
    summary_format = "Summary: {0} completed, {1} in queue, {2} blocked, {3} warning, {4} error"
    print(
        summary_format.format(
            status_list.count("COMPLETED"),
            status_list.count("IN_QUEUE"),
            status_list.count("BLOCKED"),
            status_list.count("WARNING"),
            status_list.count("ERROR"),
        )
    )


def print_schedules(schedules, max_lines):
    schedule_table = [
        ["NodeID", "JobID", "Keyspace", "Table", "Status", "Repaired(%)", "Completed at", "Next repair", "Repair type"]
    ]
    print("Snapshot as of", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_schedule_table(schedule_table, schedules, max_lines)
    print_summary(schedules)


def print_repairs(repairs, max_lines=-1):
    repair_table = [["NodeID", "JobID", "Keyspace", "Table", "Status", "Repaired(%)", "Completed at", "Repair type"]]
    print_repair_table(repair_table, repairs, max_lines)
    print_repair_summary(repairs)


def print_schedule_table(schedule_table, schedules, max_lines):
    sorted_schedules = sorted(schedules, key=lambda x: (x.last_repaired_at_in_ms, x.repaired_ratio), reverse=False)
    if max_lines > -1:
        sorted_schedules = sorted_schedules[:max_lines]

    for schedule in sorted_schedules:
        schedule_table.append(_convert_schedule(schedule))
    table_formatter.format_table(schedule_table)


def print_repair_table(repair_table, repairs, max_lines):
    sorted_repairs = sorted(repairs, key=lambda x: (x.completed_at, x.repaired_ratio), reverse=False)
    if max_lines > -1:
        sorted_repairs = sorted_repairs[:max_lines]

    for repair in sorted_repairs:
        repair_table.append(_convert_repair(repair))
    table_formatter.format_table(repair_table)


def print_repair(repair):
    repair_table = [
        ["Id", "Host Id", "Keyspace", "Table", "Status", "Repaired(%)", "Completed at", "Repair type"],
        _convert_repair(repair),
    ]
    table_formatter.format_table(repair_table)


def _convert_repair(repair):
    entry = [
        repair.host_id,
        repair.job_id,
        repair.keyspace,
        repair.table,
        repair.status,
        repair.get_repair_percentage(),
        repair.get_completed_at(),
        repair.repair_type,
    ]
    return entry


def _convert_schedule(schedule):
    entry = [
        schedule.node_id,
        schedule.job_id,
        schedule.keyspace,
        schedule.table,
        schedule.status,
        schedule.get_repair_percentage(),
        schedule.get_last_repaired_at(),
        schedule.get_next_repair(),
        schedule.repair_type,
    ]
    return entry


def print_repair_info(repair_info, max_lines=-1):
    print("Time window between '{0}' and '{1}'".format(repair_info.get_since(), repair_info.get_to()))
    print_repair_stats(repair_info.repair_stats, max_lines)


def print_repair_stats(repair_stats, max_lines=-1):
    repair_stats_table = [["Keyspace", "Table", "Repaired (%)", "Repair time taken"]]
    sorted_repair_stats = sorted(repair_stats, key=lambda x: (x.repaired_ratio, x.keyspace, x.table), reverse=False)
    if max_lines > -1:
        sorted_repair_stats = sorted_repair_stats[:max_lines]

    for repair_stat in sorted_repair_stats:
        repair_stats_table.append(_convert_repair_stat(repair_stat))
    table_formatter.format_table(repair_stats_table)


def _convert_repair_stat(repair_stat):
    entry = [
        repair_stat.keyspace,
        repair_stat.table,
        repair_stat.get_repaired_percentage(),
        repair_stat.get_repair_time_taken(),
    ]
    return entry

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
from datetime import datetime
from ecchronoslib import table_formatter, displaying


def print_schedule(schedule, max_lines, full=False, colors="auto"):
    if not schedule.is_valid():
        print('Schedule not found')
        return

    verbose_print_format = "{0:25s}: {1}"

    print(verbose_print_format.format(
        displaying.color_key("Id", colors ),
        displaying.color_str(schedule.job_id, colors, "UUID")))
    print(verbose_print_format.format(
        displaying.color_key("Keyspace", colors ),
        displaying.color_str(schedule.keyspace, colors, "TEXT")))
    print(verbose_print_format.format(
        displaying.color_key("Table", colors ),
        displaying.color_str(schedule.table, colors, "TEXT")))
    print(verbose_print_format.format(
        displaying.color_key("Status", colors ),
        displaying.color_key(schedule.status, colors)))
    print(verbose_print_format.format(
        displaying.color_key("Repaired(%)", colors ),
        displaying.color_str(schedule.get_repair_percentage(), colors, "TEXT")))
    print(verbose_print_format.format(
        displaying.color_key("Completed at", colors ),
        displaying.color_str(schedule.get_last_repaired_at(), colors, "DATETIME")))
    print(verbose_print_format.format(
        displaying.color_key("Next repair", colors ),
        displaying.color_str(schedule.get_next_repair(), colors, "DATETIME")))
    print(verbose_print_format.format(
        displaying.color_key("Repair type", colors ),
        displaying.color_str(schedule.repair_type, colors, "TEXT")))
    print(verbose_print_format.format(
        displaying.color_key("Config", colors ),
        displaying.color_str(schedule.get_config(), colors, "Collection")))

    if full:
        vnode_index = ["Start token", "End token", "Replicas", "Repaired at", "Repaired"]
        vnode_state_table = [displaying.color_index(vnode_index, colors)]

        sorted_vnode_states = sorted(schedule.vnode_states, key=lambda vnode: vnode.last_repaired_at_in_ms,
                                     reverse=True)

        if max_lines > -1:
            sorted_vnode_states = sorted_vnode_states[:max_lines]

        for vnode_state in sorted_vnode_states:
            _add_vnode_state_to_table(vnode_state, vnode_state_table, colors)

        table_formatter.format_table(vnode_state_table, displaying.should_color(colors))


def _add_vnode_state_to_table(vnode_state, table, colors):
    entry = [
        displaying.color_str(vnode_state.start_token, colors, "Start token"),
        displaying.color_str(vnode_state.end_token, colors, "End token"),
        displaying.color_str((', '.join(vnode_state.replicas)), colors, "Replicas"),
        displaying.color_str(vnode_state.get_last_repaired_at(), colors, "DATETIME"),
        displaying.color_str(vnode_state.repaired, colors, "Repaired")
    ]

    table.append(entry)


def print_summary(schedules):
    status_list = [schedule.status for schedule in schedules]
    summary_format = "Summary: {0} completed, {1} on time, {2} blocked, {3} late, {4} overdue"
    print(summary_format.format(status_list.count('COMPLETED'),
                                status_list.count('ON_TIME'),
                                status_list.count('BLOCKED'),
                                status_list.count('LATE'),
                                status_list.count('OVERDUE')))


def print_repair_summary(repairs):
    status_list = [repair.status for repair in repairs]
    summary_format = "Summary: {0} completed, {1} in queue, {2} blocked, {3} warning, {4} error"
    print(summary_format.format(status_list.count('COMPLETED'),
                                status_list.count('IN_QUEUE'),
                                status_list.count('BLOCKED'),
                                status_list.count('WARNING'),
                                status_list.count('ERROR')))


def print_schedules(schedules, max_lines, colors):
    summary = ["Id", "Keyspace", "Table", "Status", "Repaired(%)",
               "Completed at", "Next repair", "Repair type"]
    colored_summary = displaying.color_index(summary, colors)
    schedule_table = [colored_summary]
    print("Snapshot as of", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print_schedule_table(schedule_table, schedules, max_lines, colors)
    print_summary(schedules)


def print_repairs(repairs, max_lines=-1, colors="auto"):
    summary = ["Id", "Host Id", "Keyspace", "Table", "Status", "Repaired(%)",
                "Completed at", "Repair type"]
    colored_summary = displaying.color_index(summary, colors)
    repair_table = [colored_summary]
    print_repair_table(repair_table, repairs, max_lines, colors)
    print_repair_summary(repairs)


def print_schedule_table(schedule_table, schedules, max_lines, colors):
    sorted_schedules = sorted(schedules, key=lambda x: (x.last_repaired_at_in_ms, x.repaired_ratio),
                              reverse=False)
    if max_lines > -1:
        sorted_schedules = sorted_schedules[:max_lines]

    for schedule in sorted_schedules:
        schedule_table.append(_convert_schedule(schedule, colors))
    table_formatter.format_table(schedule_table, displaying.should_color(colors))


def print_repair_table(repair_table, repairs, max_lines, colors):
    sorted_repairs = sorted(repairs, key=lambda x: (x.completed_at, x.repaired_ratio), reverse=False)
    if max_lines > -1:
        sorted_repairs = sorted_repairs[:max_lines]

    for repair in sorted_repairs:
        repair_table.append(_convert_repair(repair, colors))
    table_formatter.format_table(repair_table, displaying.should_color(colors))


def print_repair(repair, colors):
    repair_table = [["Id", "Host Id", "Keyspace", "Table", "Status", "Repaired(%)",
                     "Completed at", "Repair type"], _convert_repair(repair, colors)]
    table_formatter.format_table(repair_table, colors)


def _convert_repair(repair, colors):
    entry = [displaying.color_str(repair.job_id, colors, "UUID"),
             displaying.color_str(repair.host_id, colors, "UUID"),
             displaying.color_str(repair.keyspace, colors, "TEXT"),
             displaying.color_str(repair.table, colors, "TEXT"),
             displaying.color_key(repair.status, colors),
             displaying.color_str(repair.get_repair_percentage(), colors, "TEXT"),
             displaying.color_str(repair.get_completed_at(), colors, "DATETIME"),
             displaying.color_str(repair.repair_type, colors, "TEXT")]
    return entry


def _convert_schedule(schedule, colors):
    entry = [displaying.color_str(schedule.job_id, colors, "UUID"),
             displaying.color_str(schedule.keyspace, colors, "TEXT"),
             displaying.color_str(schedule.table, colors, "TEXT"),
             displaying.color_key(schedule.status, colors),
             displaying.color_str(schedule.get_repair_percentage(), colors, "TEXT"),
             displaying.color_str(schedule.get_last_repaired_at(), colors, "DATETIME"),
             displaying.color_str(schedule.get_next_repair(), colors, "DATETIME"),
             displaying.color_str(schedule.repair_type, colors, "TEXT")]

    return entry


def print_repair_info(repair_info, max_lines=-1, colors="auto"):
    print("Time window between '{0}' and '{1}'".format(repair_info.get_since(), repair_info.get_to()))
    print_repair_stats(repair_info.repair_stats, max_lines, colors)


def print_repair_stats(repair_stats, max_lines=-1, colors="auto"):
    summary = ["Keyspace", "Table", "Repaired(%)",
                "Repair time taken"]
    colored_summary = displaying.color_index(summary, colors)
    repair_stats_table = [colored_summary]
    sorted_repair_stats = sorted(repair_stats, key=lambda x: (x.repaired_ratio, x.keyspace, x.table), reverse=False)
    if max_lines > -1:
        sorted_repair_stats = sorted_repair_stats[:max_lines]

    for repair_stat in sorted_repair_stats:
        repair_stats_table.append(_convert_repair_stat(repair_stat, colors))
    table_formatter.format_table(repair_stats_table, displaying.should_color(colors))


def _convert_repair_stat(repair_stat, colors):
    entry = [displaying.color_str(repair_stat.keyspace, colors, "TEXT"),
             displaying.color_str(repair_stat.table, colors, "TEXT"),
             displaying.color_str(repair_stat.get_repaired_percentage(), colors, "TEXT"),
             displaying.color_str(repair_stat.get_repair_time_taken(), colors, "DATETIME")]
    return entry

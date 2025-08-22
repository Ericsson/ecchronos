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
import json
from datetime import datetime
from ecchronoslib import table_formatter


def print_rejections(rejections):
    _print_rejections_table_format(rejections)


def print_schedule(schedule, max_lines, full=False, columns=None, output="table"):
    if not schedule.is_valid():
        print("Schedule not found")
        return

    if output == "json":
        _print_schedule_json_format(schedule=schedule, max_lines=max_lines, full=full)
    else:
        _print_schedule_table_format(schedule=schedule, max_lines=max_lines, full=full, columns=columns)


def _print_schedule_table_format(schedule, max_lines, full=False, columns=None):
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
            schedule.vnode_states,
            key=lambda vnode: vnode.last_repaired_at_in_ms,
            reverse=True,
        )

        if max_lines > -1:
            sorted_vnode_states = sorted_vnode_states[:max_lines]

        for vnode_state in sorted_vnode_states:
            _add_vnode_state_to_table(vnode_state, vnode_state_table)

        table_formatter.format_table(vnode_state_table, columns)


def _print_schedule_json_format(schedule, max_lines=-1, full=False):
    if full:
        sorted_vnode_states = sorted(
            schedule.vnode_states,
            key=lambda vnode: vnode.last_repaired_at_in_ms,
            reverse=True,
        )

        if max_lines > -1:
            sorted_vnode_states = sorted_vnode_states[:max_lines]
            schedule.vnode_states = sorted_vnode_states
    print(json.dumps(schedule.to_dict(), indent=4))


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


def print_schedules(schedules, max_lines, columns=None, output="table"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if output == "json":
        _print_schedules_json_format(timestamp, schedules, max_lines)
    else:
        print("Snapshot as of", timestamp)
        _print_schedules_table_format(schedules, max_lines, columns)
        print_summary(schedules)


def _print_schedules_table_format(schedules, max_lines, columns=None):
    schedule_table = [
        [
            "Id",
            "Keyspace",
            "Table",
            "Status",
            "Repaired(%)",
            "Completed at",
            "Next repair",
            "Repair type",
        ]
    ]
    print_schedule_table(schedule_table, schedules, max_lines, columns)


def _print_rejections_table_format(rejections):
    rejections_table = [
        [
            "Keyspace",
            "Table",
            "Start Hour",
            "Start Minute",
            "End Hour",
            "End Minute",
            "DC Exclusions",
        ]
    ]
    print_rejections_table(rejections_table, rejections)


def print_rejections_table(rejections_table, rejections):
    for rejection in rejections:
        rejections_table.append(_convert_rejection(rejection))
    table_formatter.format_table(rejections_table, None)


def _print_schedules_json_format(timestamp, schedules, max_lines):
    sorted_schedules = sorted(
        schedules,
        key=lambda x: (x.last_repaired_at_in_ms, x.repaired_ratio),
        reverse=False,
    )
    if max_lines > -1:
        sorted_schedules = sorted_schedules[:max_lines]
    schedules_dict = [s.to_dict() for s in sorted_schedules]
    data = {"snapshot_time": timestamp, "scheules": schedules_dict}
    print(json.dumps((data), indent=4))


def print_repairs(repairs, max_lines=-1, columns=None, output="table"):
    if output == "json":
        _print_repair_json_format(repairs, max_lines)
    else:
        _print_repair_table_format(repairs, max_lines, columns)
        print_repair_summary(repairs)


def _print_repair_json_format(repairs, max_lines=-1):
    sorted_repairs = sorted(repairs, key=lambda x: (x.completed_at, x.repaired_ratio), reverse=False)
    if max_lines > -1:
        sorted_repairs = sorted_repairs[:max_lines]

    repairs_dict = [r.to_dict() for r in sorted_repairs]
    print(json.dumps(repairs_dict, indent=4))


def print_schedule_table(schedule_table, schedules, max_lines, columns):
    sorted_schedules = sorted(
        schedules,
        key=lambda x: (x.last_repaired_at_in_ms, x.repaired_ratio),
        reverse=False,
    )
    if max_lines > -1:
        sorted_schedules = sorted_schedules[:max_lines]

    for schedule in sorted_schedules:
        schedule_table.append(_convert_schedule(schedule))
    table_formatter.format_table(schedule_table, columns)


def _print_repair_table_format(repairs, max_lines, columns):
    repair_table = [
        [
            "Id",
            "Host Id",
            "Keyspace",
            "Table",
            "Status",
            "Repaired(%)",
            "Completed at",
            "Repair type",
        ]
    ]
    sorted_repairs = sorted(repairs, key=lambda x: (x.completed_at, x.repaired_ratio), reverse=False)
    if max_lines > -1:
        sorted_repairs = sorted_repairs[:max_lines]

    for repair in sorted_repairs:
        repair_table.append(_convert_repair(repair))
    table_formatter.format_table(repair_table, columns)


def _convert_repair(repair):
    entry = [
        repair.job_id,
        repair.host_id,
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


def _convert_rejection(rejection):
    entry = [
        rejection.keyspace,
        rejection.table,
        rejection.start_hour,
        rejection.start_minute,
        rejection.end_hour,
        rejection.end_minute,
        rejection.dc_exclusions,
    ]
    return entry


def print_repair_info(repair_info, max_lines=-1, columns=None, output="table"):
    info_from = repair_info.get_since()
    info_to = repair_info.get_to()
    if output == "json":
        _print_repair_info_json_format(info_from, info_to, repair_info.repair_stats, max_lines)
    else:
        print("Time window between '{0}' and '{1}'".format(info_from, info_to))
        print_repair_stats(repair_info.repair_stats, max_lines, columns)


def _print_repair_info_json_format(info_from, info_to, repair_stats, max_lines=-1):
    sorted_repair_stats = sorted(
        repair_stats,
        key=lambda x: (x.repaired_ratio, x.keyspace, x.table),
        reverse=False,
    )
    if max_lines > -1:
        sorted_repair_stats = sorted_repair_stats[:max_lines]
    repair_stats_dict = [rs.to_dict() for rs in sorted_repair_stats]
    data = {"time_window_from": info_from, "time_window_to": info_to, "repair_info": repair_stats_dict}
    print(json.dumps(data, indent=4))


def print_repair_stats(repair_stats, max_lines=-1, columns=None):
    repair_stats_table = [["Keyspace", "Table", "Repaired (%)", "Repair time taken"]]
    sorted_repair_stats = sorted(
        repair_stats,
        key=lambda x: (x.repaired_ratio, x.keyspace, x.table),
        reverse=False,
    )
    if max_lines > -1:
        sorted_repair_stats = sorted_repair_stats[:max_lines]

    for repair_stat in sorted_repair_stats:
        repair_stats_table.append(_convert_repair_stat(repair_stat))
    table_formatter.format_table(repair_stats_table, columns)


def _convert_repair_stat(repair_stat):
    entry = [
        repair_stat.keyspace,
        repair_stat.table,
        repair_stat.get_repaired_percentage(),
        repair_stat.get_repair_time_taken(),
    ]
    return entry

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

import datetime
import json


def parse_interval(time_in_ms):
    time = time_in_ms

    time = time / 1000
    seconds = int(time % 60)
    time = time / 60
    minutes = int(time % 60)
    time = time / 60
    hours = int(time % 24)
    time = time / 24
    days = int(time)

    return "{0:2d} day(s) {1:02d}h {2:02d}m {3:02d}s".format(days, hours, minutes, seconds)


class VnodeState(object):
    # pylint: disable=too-few-public-methods

    def __init__(self, data):
        self.start_token = data["startToken"] if "startToken" in data else "UNKNOWN"
        self.end_token = data["endToken"] if "endToken" in data else "UNKNOWN"
        self.replicas = data["replicas"] if "replicas" in data else []
        self.last_repaired_at_in_ms = int(data["lastRepairedAtInMs"] if "lastRepairedAtInMs" in data else -1)
        self.repaired = data["repaired"] if "repaired" in data else "False"

    def get_last_repaired_at(self):
        return datetime.datetime.fromtimestamp(self.last_repaired_at_in_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')


class RepairJob(object):
    # pylint: disable=too-many-instance-attributes

    def __init__(self, data):
        self.keyspace = data["keyspace"] if "keyspace" in data else "<UNKNOWN>"
        self.table = data["table"] if "table" in data else "<UNKNOWN>"
        self.last_repaired_at_in_ms = int(data["lastRepairedAtInMs"] if "lastRepairedAtInMs" in data else -1)
        self.repaired_ratio = float(data["repairedRatio"] if "repairedRatio" in data else 0)
        self.status = data["status"] if "status" in data else "<UNKNOWN>"
        self.next_repair_in_ms = int(data["nextRepairInMs"] if "nextRepairInMs" in data else -1)
        self.recurring = data["recurring"] if "recurring" in data else "<UNKNOWN>"
        self.job_id = data["id"] if "id" in data else "<UNKNOWN>"

    def is_valid(self):
        return self.keyspace != "<UNKNOWN>"

    def get_last_repaired_at(self):
        if self.last_repaired_at_in_ms == -1:
            return "-"
        return datetime.datetime.fromtimestamp(self.last_repaired_at_in_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

    def get_repair_percentage(self):
        return "{0:.2f}".format(self.repaired_ratio * 100.0)

    def get_next_repair(self):
        if self.next_repair_in_ms == -1:
            return "-"
        return datetime.datetime.fromtimestamp(self.next_repair_in_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

class Job(object):

    def __init__(self, data):
        self.keyspace = data["keyspace"] if "keyspace" in data else "<UNKNOWN>"
        self.table = data["table"] if "table" in data else "<UNKNOWN>"
        self.repaired_ratio = float(data["repairedRatio"] if "repairedRatio" in data else 0)
        self.status = data["status"] if "status" in data else "<UNKNOWN>"
        self.job_id = data["id"] if "id" in data else "<UNKNOWN>"

    def is_valid(self):
        return self.keyspace != "<UNKNOWN>"

    def get_repair_percentage(self):
        return "{0:.2f}".format(self.repaired_ratio * 100.0)

class Repair(Job):

    def __init__(self, data):
        Job.__init__(self, data)
        self.completed_at = int(data["completedAt"] if "completedAt" in data else -1)

    def get_completed_at(self):
        if self.completed_at == -1:
            return "-"
        return datetime.datetime.fromtimestamp(self.completed_at / 1000).strftime('%Y-%m-%d %H:%M:%S')


class Schedule(Job):

    def __init__(self, data):
        Job.__init__(self, data)
        self.last_repaired_at_in_ms = int(data["lastRepairedAtInMs"] if "lastRepairedAtInMs" in data else -1)
        self.status = data["status"] if "status" in data else "<UNKNOWN>"
        self.next_repair_in_ms = int(data["nextRepairInMs"] if "nextRepairInMs" in data else -1)
        self.config = data["config"] if "config" in data else "<UNKNOWN>"

    def get_config(self):
        return json.dumps(self.config).strip('{}')

    def get_next_repair(self):
        if self.next_repair_in_ms == -1:
            return "-"
        return datetime.datetime.fromtimestamp(self.next_repair_in_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

    def get_last_repaired_at(self):
        if self.last_repaired_at_in_ms == -1:
            return "-"
        return datetime.datetime.fromtimestamp(self.last_repaired_at_in_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')


class FullSchedule(Schedule):
    def __init__(self, data):
        Schedule.__init__(self, data)
        self.vnode_states = []
        if "virtualNodeStates" in data:
            for vnode_data in data["virtualNodeStates"]:
                self.vnode_states.append(VnodeState(vnode_data))

class VerboseRepairJob(RepairJob):
    def __init__(self, data):
        RepairJob.__init__(self, data)
        self.vnode_states = []
        if "virtualNodeStates" in data:
            for vnode_data in data["virtualNodeStates"]:
                self.vnode_states.append(VnodeState(vnode_data))


class TableConfig(object):
    # pylint: disable=too-many-instance-attributes

    def __init__(self, data):
        self.job_id = data["id"] if "id" in data else "<UNKNOWN>"
        self.keyspace = data["keyspace"] if "keyspace" in data else "<UNKNOWN>"
        self.table = data["table"] if "table" in data else "<UNKNOWN>"
        self.repair_interval_in_ms = int(data["repairIntervalInMs"] if "repairIntervalInMs" in data else 0)
        self.repair_parallelism = data["repairParallelism"] if "repairParallelism" in data else "<UNKNOWN>"
        self.repair_unwind_ratio = float(data["repairUnwindRatio"] if "repairUnwindRatio" in data else 0)
        self.repair_warning_time_in_ms = int(data["repairWarningTimeInMs"] if "repairWarningTimeInMs" in data else 0)
        self.repair_error_time_in_ms = int(data["repairErrorTimeInMs"] if "repairErrorTimeInMs" in data else 0)

    def is_valid(self):
        return self.keyspace != "<UNKNOWN>"

    def get_repair_interval(self):
        return parse_interval(self.repair_interval_in_ms)

    def get_repair_warning_time(self):
        return parse_interval(self.repair_warning_time_in_ms)

    def get_repair_error_time(self):
        return parse_interval(self.repair_error_time_in_ms)

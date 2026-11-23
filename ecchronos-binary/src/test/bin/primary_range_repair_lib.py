#!/usr/bin/env python3
#
# Copyright 2026 Telefonaktiebolaget LM Ericsson
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

"""Cluster-free helpers for the PRIMARY_RANGE integration test (test_primary_range_repair.py).

Nothing here talks to Docker or Cassandra. It builds and parses the CQL the test runs through cqlsh and checks the
results, so all of it is unit tested without a cluster (see test_primary_range_repair_lib.py). The cqlsh output the
parsers handle was captured from a real Cassandra 5.0 node.
"""

import json
import re
from collections import defaultdict

REPAIR_TYPE_PRIMARY_RANGE = "PRIMARY_RANGE"

# A primary range history row covers the whole token space only as a marker, see PrimaryRangeRepairTask.
FULL_RANGE_BEGIN = str(-(2**63))
FULL_RANGE_END = str(2**63 - 1)

STATUS_SUCCESS = "SUCCESS"

# Slots 1..N exist per lock resource, N being locks_per_resource (3 by default). Taking a few more than that is
# harmless and keeps the test independent of the configured value.
MAX_LOCK_SLOTS = 5

_UUID_PATTERN = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
# Lock resource names are RepairResource-<name>-<slot>, where <name> is keyspace.table, a datacenter, a prefixed
# datacenter or a node id. Only these characters are ever interpolated into CQL.
_RESOURCE_PATTERN = re.compile(r"^RepairResource-[A-Za-z0-9._-]+-[0-9]+$")
# One result line of an LWT statement in cqlsh: "      True" or "     False | RepairResource-x-1 | ...".
_LWT_RESULT_PATTERN = re.compile(r"^\s*(True|False)\s*(\|.*)?$")


def require_uuid(value):
    """Return value as a string if it is a UUID, otherwise raise. Guards values interpolated into CQL."""
    text = str(value)
    if not _UUID_PATTERN.match(text):
        raise ValueError(f"Not a UUID: {value!r}")
    return text


def require_resource(value):
    """Return value if it is a lock resource name, otherwise raise. Guards values interpolated into CQL."""
    text = str(value)
    if not _RESOURCE_PATTERN.match(text):
        raise ValueError(f"Not a lock resource name: {value!r}")
    return text


def parse_json_rows(cqlsh_output):
    """Rows of a 'cqlsh -e "SELECT JSON ..."' result, one dict per row.

    cqlsh prints a '[json]' header, a separator, one JSON document per line, indented by a varying amount, and a
    '(N rows)' footer. cqlsh -e fetches every page, so there is one header however many rows there are. Only the
    lines that are JSON documents are returned. A line that looks like a document but is not valid JSON raises.
    """
    rows = []
    for line in cqlsh_output.splitlines():
        text = line.strip()
        if text.startswith("{") and text.endswith("}"):
            rows.append(json.loads(text))
    return rows


def lwt_results(cqlsh_output):
    """Whether each lightweight transaction statement in a cqlsh output was applied, in statement order.

    An applied statement prints '[applied]' and 'True'. A statement that was not applied prints 'False' followed by
    the current row, for example the holder of a lock. Nothing else in the output of an LWT statement starts with
    True or False.
    """
    results = []
    for line in cqlsh_output.splitlines():
        match = _LWT_RESULT_PATTERN.match(line)
        if match:
            results.append(match.group(1) == "True")
    return results


# The consistency levels ecChronos itself uses for its lock statements, so a lock taken by the test is seen by the
# agent exactly like one taken by another ecChronos instance.
LOCK_CONSISTENCY_PREFIX = "CONSISTENCY LOCAL_QUORUM; SERIAL CONSISTENCY SERIAL; "


def lock_insert_script(resources, holder, ttl_seconds):
    """CQL, for one cqlsh -e call, that takes every resource as ecChronos does: a lightweight transaction that is only
    applied if the lock is free. The TTL makes the locks expire by themselves if the test dies before it releases."""
    statements = [
        f"INSERT INTO ecchronos.lock (resource, node, metadata) VALUES ('{require_resource(resource)}', "
        f"{require_uuid(holder)}, {{'holder': 'primary-range-test'}}) IF NOT EXISTS USING TTL {int(ttl_seconds)};"
        for resource in resources
    ]
    return LOCK_CONSISTENCY_PREFIX + " ".join(statements)


def lock_delete_script(resources, holder):
    """CQL, for one cqlsh -e call, that releases every resource, but only where holder still holds it."""
    statements = [
        f"DELETE FROM ecchronos.lock WHERE resource='{require_resource(resource)}' IF node={require_uuid(holder)};"
        for resource in resources
    ]
    return LOCK_CONSISTENCY_PREFIX + " ".join(statements)


def primary_range_lock_resources(keyspace, table, datacenters):
    """The locks a primary range repair of keyspace.table takes: the table lock, shared with incremental repair, and
    one lock per datacenter. Names as returned by RepairResource.getResourceName for slot 1, the only slot."""
    resources = [f"RepairResource-{keyspace}.{table}-1"]
    resources.extend(f"RepairResource-PrimaryRange-{datacenter}-1" for datacenter in datacenters)
    return resources


def vnode_lock_resources(node_ids, datacenters):
    """Every lock vnode repair can take, in every slot: one per node and one per datacenter.

    A primary range repair must not need any of them. Holding all of them shows that it is not coordinated with
    vnode repair, whatever lock_type is configured.
    """
    resources = []
    for slot in range(1, MAX_LOCK_SLOTS + 1):
        for node_id in node_ids:
            resources.append(f"RepairResource-{require_uuid(node_id)}-{slot}")
        for datacenter in datacenters:
            resources.append(f"RepairResource-{datacenter}-{slot}")
    return resources


def history_problems(rows_by_node):
    """What is wrong with the repair_history rows of a primary range repaired table, empty if nothing.

    rows_by_node maps a node id to its rows, each a dict with repair_type, status, range_begin and range_end. Every
    node must have a successful row, and every row must be typed PRIMARY_RANGE and carry the whole-ring marker, so
    none of them can be mistaken for the repair state of a vnode.
    """
    problems = []
    for node, rows in sorted(rows_by_node.items()):
        if not rows:
            problems.append(f"node {node} has no repair history")
            continue
        if not any(row.get("status") == STATUS_SUCCESS for row in rows):
            problems.append(f"node {node} has no {STATUS_SUCCESS} repair history row")
        for row in rows:
            if row.get("repair_type") != REPAIR_TYPE_PRIMARY_RANGE:
                problems.append(f"node {node} has a row of type {row.get('repair_type')}")
            if row.get("range_begin") != FULL_RANGE_BEGIN or row.get("range_end") != FULL_RANGE_END:
                problems.append(f"node {node} has a row for {row.get('range_begin')}:{row.get('range_end')}")
    return problems


def successful_ranges_by_coordinator(rows):
    """Token ranges that were repaired successfully, per coordinator, from system_distributed.repair_history."""
    ranges = defaultdict(set)
    for row in rows:
        if row.get("status") == STATUS_SUCCESS:
            ranges[row["coordinator"]].add((row["range_begin"], row["range_end"]))
    return dict(ranges)


def overlapping_ranges(ranges_by_coordinator):
    """Pairs of coordinators that repaired the same token range, as (first, second, shared ranges).

    A primary range repair only repairs the ranges the coordinator is primary for, so no range is repaired by two
    coordinators.
    """
    overlaps = []
    coordinators = sorted(ranges_by_coordinator)
    for index, first in enumerate(coordinators):
        for second in coordinators[index + 1 :]:
            shared = ranges_by_coordinator[first] & ranges_by_coordinator[second]
            if shared:
                overlaps.append((first, second, sorted(shared)))
    return overlaps

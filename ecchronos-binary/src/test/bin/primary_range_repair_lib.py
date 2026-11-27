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

"""Cluster-free helpers for the integration tests of the repair types that run one repair session per node:
PRIMARY_RANGE (test_primary_range_repair.py) and ALL_REPLICATED_RANGES (test_all_replicated_ranges_repair.py). The two
types share their locks and the shape of their history, so they share this module.

Nothing here talks to Docker or Cassandra. It builds and parses the CQL the test runs through cqlsh and checks the
results, so all of it is unit tested without a cluster (see test_primary_range_repair_lib.py). The cqlsh output the
parsers handle was captured from a real Cassandra 5.0 node.
"""

import json
import re
import uuid
from collections import defaultdict

REPAIR_TYPE_PRIMARY_RANGE = "PRIMARY_RANGE"
REPAIR_TYPE_ALL_REPLICATED_RANGES = "ALL_REPLICATED_RANGES"

# A history row of these repair types covers the whole token space only as a marker, see SingleSessionRepairTask.
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
    one lock per datacenter. Names as returned by RepairResource.getResourceName for slot 1, the only slot.

    A repair of type ALL_REPLICATED_RANGES takes exactly the same locks, through the same resource factory.
    """
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


def history_problems(rows_by_node, repair_type=REPAIR_TYPE_PRIMARY_RANGE):
    """What is wrong with the repair_history rows of a table repaired with repair_type, empty if nothing.

    rows_by_node maps a node id to its rows, each a dict with repair_type, status, range_begin and range_end. Every
    node must have a successful row, and every row must be typed repair_type and carry the whole-ring marker, so
    none of them can be mistaken for the repair state of a vnode, or for a row of the other repair type.
    """
    problems = []
    for node, rows in sorted(rows_by_node.items()):
        if not rows:
            problems.append(f"node {node} has no repair history")
            continue
        if not any(row.get("status") == STATUS_SUCCESS for row in rows):
            problems.append(f"node {node} has no {STATUS_SUCCESS} repair history row")
        for row in rows:
            if row.get("repair_type") != repair_type:
                problems.append(f"node {node} has a row of type {row.get('repair_type')}")
            if row.get("range_begin") != FULL_RANGE_BEGIN or row.get("range_end") != FULL_RANGE_END:
                problems.append(f"node {node} has a row for {row.get('range_begin')}:{row.get('range_end')}")
    return problems


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


# --- ranges repaired by every replica ------------------------------------------------------------------------------

# 100 nanosecond intervals between the start of the UUID epoch, 1582-10-15, and the Unix epoch.
_UUID_EPOCH_OFFSET = 0x01B21DD213814000


def timeuuid_millis(value):
    """Epoch milliseconds a time based (version 1) UUID was created at. The id of a row in
    system_distributed.repair_history is one, created when the repair session starts."""
    parsed = uuid.UUID(require_uuid(value))
    if parsed.version != 1:
        raise ValueError(f"Not a time based UUID: {value!r}")
    return (parsed.time - _UUID_EPOCH_OFFSET) // 10_000


def cluster_nodes(local_rows, peer_rows):
    """The nodes of the cluster as dicts with host_id, address and datacenter, ordered by address.

    local_rows are the rows of 'SELECT JSON host_id, broadcast_address, data_center FROM system.local', peer_rows those
    of 'SELECT JSON host_id, peer, data_center FROM system.peers_v2'. The address is the one Cassandra records as
    coordinator and participant in system_distributed.repair_history.
    """
    nodes = [
        {"host_id": require_uuid(row["host_id"]), "address": row["broadcast_address"], "datacenter": row["data_center"]}
        for row in local_rows
    ]
    nodes.extend(
        {"host_id": require_uuid(row["host_id"]), "address": row["peer"], "datacenter": row["data_center"]}
        for row in peer_rows
    )
    return sorted(nodes, key=lambda node: node["address"])


def addresses_in(nodes, datacenter):
    """The addresses of the nodes in datacenter."""
    return [node["address"] for node in nodes if node["datacenter"] == datacenter]


# Cassandra writes a token range as Range.toString() does: "(left,right]".
_RANGE_PATTERN = re.compile(r"^\(\s*(-?[0-9]+)\s*,\s*(-?[0-9]+)\s*\]$")


def parse_range(text):
    """A token range of system_distributed.parent_repair_history, '(left,right]', as a (left, right) tuple of strings,
    the form range_begin and range_end have in system_distributed.repair_history. Anything else raises, so a change of
    format fails the test and does not pass it by an empty comparison."""
    match = _RANGE_PATTERN.match(str(text))
    if not match:
        raise ValueError(f"Not a token range: {text!r}")
    return match.group(1), match.group(2)


def parse_ranges(texts):
    """The set of token ranges of a set or list of range texts. None is no ranges."""
    return {parse_range(text) for text in texts or []}


def parent_repairs(history_rows, parent_rows):
    """The repairs a node coordinated, one dict per parent repair, oldest first.

    system_distributed.repair_history says which node coordinated a repair, but not which ranges it repaired: it has one
    row for every repair session, keyed by the id of the session, so of the ranges of a session only the last one that
    was written is left. The ranges are in system_distributed.parent_repair_history, which has one row for the whole
    repair, with all the ranges that were requested and the ones that were repaired, and the options it was started with.

    history_rows are the rows of 'SELECT JSON parent_id, coordinator FROM system_distributed.repair_history' and
    parent_rows those of 'SELECT JSON parent_id, requested_ranges, successful_ranges, options FROM
    system_distributed.parent_repair_history'. A repair whose parent row is not written yet has no ranges and no options.
    Each dict has parent_id, coordinator, started_ms (the parent id is a time based UUID), requested and successful (sets
    of (left, right) tuples) and options.
    """
    coordinators = {}
    for row in history_rows:
        parent_id = require_uuid(row["parent_id"])
        if coordinators.setdefault(parent_id, row["coordinator"]) != row["coordinator"]:
            raise ValueError(f"Repair {parent_id} has more than one coordinator")
    parents = {require_uuid(row["parent_id"]): row for row in parent_rows}
    repairs = []
    for parent_id, coordinator in coordinators.items():
        parent = parents.get(parent_id, {})
        repairs.append(
            {
                "parent_id": parent_id,
                "coordinator": coordinator,
                "started_ms": timeuuid_millis(parent_id),
                "requested": parse_ranges(parent.get("requested_ranges")),
                "successful": parse_ranges(parent.get("successful_ranges")),
                "options": parent.get("options") or {},
            }
        )
    return sorted(repairs, key=lambda repair: repair["started_ms"])


def repaired_ranges_by_coordinator(repairs):
    """The token ranges each coordinator repaired successfully, over all its repairs in repairs."""
    ranges = defaultdict(set)
    for repair in repairs:
        ranges[repair["coordinator"]].update(repair["successful"])
    return dict(ranges)


def coordinators_of_ranges(ranges_by_coordinator):
    """For every token range, the coordinators that repaired it. A repair of everything a node replicates repairs a
    range once for every replica that coordinates one, where a primary range repair repairs it once."""
    coordinators = defaultdict(set)
    for coordinator, ranges in ranges_by_coordinator.items():
        for token_range in ranges:
            coordinators[token_range].add(coordinator)
    return dict(coordinators)


def repairs_not_started_as(repairs, **expected_options):
    """The repairs whose options differ from expected_options, for example primaryRange='false', as (repair, key,
    actual) tuples. A repair that has no options yet is reported too, with None as the actual value."""
    wrong = []
    for repair in repairs:
        for key, expected in expected_options.items():
            actual = repair["options"].get(key)
            if actual != expected:
                wrong.append((repair, key, actual))
    return wrong


def missing_ranges(expected, repaired):
    """The ranges in expected that are not in repaired, sorted."""
    return sorted(set(expected) - set(repaired))


def rows_without_participant_in(rows, addresses):
    """The rows whose repair session had none of addresses as participant, for example no node of a datacenter."""
    wanted = set(addresses)
    return [row for row in rows if not wanted & set(row.get("participants") or [])]


def participants_of(rows):
    """Every address that took part in the repair sessions of rows."""
    seen = set()
    for row in rows:
        seen.update(row.get("participants") or [])
    return seen

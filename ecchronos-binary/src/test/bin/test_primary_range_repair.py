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

"""Integration test of the PRIMARY_RANGE repair type against a real 4 node, 2 datacenter cluster.

What it shows, in order:
  * a table configured as primary_range is scheduled as PRIMARY_RANGE on every node, and a scheduled cycle runs to
    completion
  * Cassandra repaired primary ranges only: every repair was started with primaryRange=true, and the ranges the 4
    coordinators repaired, as recorded by Cassandra in system_distributed.parent_repair_history, do not overlap
  * ecChronos recorded the repairs in ecchronos.repair_history as PRIMARY_RANGE rows with the whole ring marker
  * after a restart the last repair is read back from that history, so the table is not taken for never repaired
  * an on-demand primary range repair is not coordinated with vnode repair: it completes while every lock a vnode
    repair can take is held
  * an on-demand primary range repair needs the table lock and the lock of every datacenter: it does not complete
    while the test holds any one of them, and completes once the lock is released

The locks are held by the test itself, in ecchronos.lock, the way another ecChronos instance would hold them. Locks
only exist while a repair runs, so watching the lock table could never prove which locks a repair takes.

The tests depend on each other and share one agent, see the dependency names. The cqlsh and ecctool output parsing and
the checks live in primary_range_repair_lib.py, which is unit tested without a cluster.
"""

import logging
import time
import uuid

import pytest

import global_variables as global_vars
import incremental_repair_lib as irl
import primary_range_repair_lib as lib
from conftest import get_repairs_json, run_repair_json
from ecc_config import EcchronosConfig

logger = logging.getLogger(__name__)

# A new keyspace for every run. The history Cassandra keeps for it in system_distributed outlives the keyspace, so a
# fixed name would mix the repairs of earlier runs into this one.
KEYSPACE = f"pr_repair_{uuid.uuid4().hex[:8]}"
TABLE = "data"

EXPECTED_NODES = 4
# datacenter1:2, datacenter2:1 -> the table lives on all 4 nodes, one schedule per node.
EXPECTED_SCHEDULES = 4
DATACENTERS = [global_vars.DC1, global_vars.DC2]

AGENT_SUFFIX = "primary-range"
CONTAINER_NAME = f"ecchronos-agent-{AGENT_SUFFIX}"
# The agent always runs on one fixed slot. Agents that earlier suites in the same pytest session left running hold it.
AGENT_BIND_PORT = 8080
AGENT_IP = global_vars.ECC_CONTAINER_IP

# Long enough that the scheduled repair runs exactly once during the test.
REPAIR_INTERVAL_MINUTES = 60
SCHEDULE_DISCOVERY_TIMEOUT_SECONDS = 300
CYCLE_TIMEOUT_SECONDS = 900
ON_DEMAND_TIMEOUT_SECONDS = 900
HISTORY_TIMEOUT_SECONDS = 180
POLL_INTERVAL_SECONDS = 10

# While a lock is held, the on-demand repair is watched for as long as the repair needed to complete unhindered, but at
# least MIN and at most MAX seconds.
MIN_BLOCK_SECONDS = 90
MAX_BLOCK_SECONDS = 240
# The test's locks are not renewed, so they must outlive the time they are held. The TTL also removes them if the test
# dies before it releases them.
LOCK_TTL_MARGIN_SECONDS = 120
LOCK_TAKE_TIMEOUT_SECONDS = 300
LOCK_RETRY_SECONDS = 10
# The repair is recorded in the history just before the job notes the time, so the two differ by a few seconds.
RESTART_TOLERANCE_MS = 60_000

TERMINAL_FAILURES = ("FAILED", "ERROR")

# State handed from one test to the next.
_STATE = {"started_at_ms": None, "node_ids": [], "table_id": None, "unblocked_seconds": None}


# ==================================================================================
# Helpers
# ==================================================================================


def _config():
    """Only the test table is primary range, the other tables stay as they are. system_auth and ecchronos are
    disabled, so the test table is the only schedule that matters."""
    return EcchronosConfig(
        instance_name=CONTAINER_NAME,
        schedule_keyspaces=irl.repair_schedule_keyspaces(
            KEYSPACE, TABLE, lib.REPAIR_TYPE_PRIMARY_RANGE, REPAIR_INTERVAL_MINUTES
        ),
    )


def _schedules():
    return irl.schedules_for(KEYSPACE, TABLE, container_name=CONTAINER_NAME)


def _wait_until(produce, timeout_seconds, description):
    """Call produce until it returns something other than None or False, and return that. Fail on timeout."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        value = produce()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out after {timeout_seconds}s waiting for {description}")
        time.sleep(POLL_INTERVAL_SECONDS)


def _wait_for_schedules():
    def produce():
        schedules = _schedules()
        if len(schedules) >= EXPECTED_SCHEDULES:
            return schedules
        logger.info(
            "Waiting for %s.%s schedules: have %s, want %s", KEYSPACE, TABLE, len(schedules), EXPECTED_SCHEDULES
        )
        return None

    return _wait_until(
        produce, SCHEDULE_DISCOVERY_TIMEOUT_SECONDS, f"{EXPECTED_SCHEDULES} schedules for {KEYSPACE}.{TABLE}"
    )


def _stop_all_agents(test_environment):
    """Stop and remove every tracked agent so the fixed slot (IP and port) is free to reuse, including an agent an
    earlier suite of the same pytest session left running."""
    stopped = False
    for container in list(test_environment.instances):
        try:
            container.stop()
            container.remove()
        except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
            logger.warning("Could not stop agent %s: %s", getattr(container, "name", "?"), exc)
        test_environment.instances.remove(container)
        stopped = True
    # Let the ring drop the retired heartbeat before a new agent reuses the address.
    if stopped:
        time.sleep(5)


def _start_agent(install_cassandra_cluster, test_environment):
    _stop_all_agents(test_environment)
    test_environment.start_ecchronos(
        suffix=AGENT_SUFFIX,
        cassandra_network=install_cassandra_cluster.network,
        ecchronos_config=_config(),
        bind_port=AGENT_BIND_PORT,
        fixed_ipv4_address=AGENT_IP,
    )
    test_environment.wait_for_ecchronos_ready()


def _create_table_id(cluster):
    out = cluster.run_cql(
        f"SELECT JSON id FROM system_schema.tables WHERE keyspace_name='{KEYSPACE}' AND table_name='{TABLE}';"
    )
    rows = lib.parse_json_rows(out)
    assert len(rows) == 1, f"Expected one table {KEYSPACE}.{TABLE}, got {out!r}"
    return lib.require_uuid(rows[0]["id"])


def _history_rows_by_node(cluster, table_id, node_ids):
    """The rows ecChronos wrote to ecchronos.repair_history for each node. CONSISTENCY ALL, because ecChronos writes
    with LOCAL_QUORUM and the row may not have reached the other datacenter yet."""
    rows = {}
    for node_id in node_ids:
        out = cluster.run_cql(
            "CONSISTENCY ALL; SELECT JSON repair_type, status, range_begin, range_end FROM ecchronos.repair_history "
            f"WHERE table_id={lib.require_uuid(table_id)} AND node_id={lib.require_uuid(node_id)};"
        )
        rows[node_id] = lib.parse_json_rows(out)
    return rows


def _history_rows(cluster):
    """The repair sessions Cassandra recorded for the test table in system_distributed.repair_history, with the repair
    they belong to, its coordinator, and the replicas that took part in the session.

    This is not a list of the repaired ranges. A session has one row for each of its ranges, but they all have the id of
    the session as key, so only the last range written is left. A node with vnodes repairs hundreds of ranges in a few
    sessions, so the ranges are read from the parent repair history, see _cassandra_repairs.
    """
    out = cluster.run_cql(
        "CONSISTENCY ALL; SELECT JSON id, parent_id, coordinator, participants, status FROM "
        f"system_distributed.repair_history WHERE keyspace_name='{KEYSPACE}' AND columnfamily_name='{TABLE}';"
    )
    return lib.parse_json_rows(out)


def _parent_rows(cluster, parent_ids):
    """The parent repairs of parent_ids in system_distributed.parent_repair_history: all the ranges that were requested
    and the ones that were repaired, and the options the repair was started with, such as primaryRange."""
    if not parent_ids:
        return []
    ids = ", ".join(lib.require_uuid(parent_id) for parent_id in parent_ids)
    out = cluster.run_cql(
        "CONSISTENCY ALL; SELECT JSON parent_id, requested_ranges, successful_ranges, options FROM "
        f"system_distributed.parent_repair_history WHERE parent_id IN ({ids});"
    )
    return lib.parse_json_rows(out)


def _cassandra_repairs(cluster):
    """The repairs of the test table that Cassandra recorded, as (repairs, sessions): the repairs with coordinator,
    ranges and options, see lib.parent_repairs, and the rows of their sessions, for who took part."""
    sessions = _history_rows(cluster)
    parents = _parent_rows(cluster, sorted({row["parent_id"] for row in sessions}))
    return lib.parent_repairs(sessions, parents), sessions


def _scheduled_repairs(cluster):
    """The repairs of the scheduled cycle, once every node has coordinated one that repaired all it asked for and
    nothing changes any more. Cassandra writes its history while and just after a repair, so a read straight after the
    last repair may not have all of it."""
    previous = {"repairs": None}

    def produce():
        repairs, _ = _cassandra_repairs(cluster)
        done = [repair for repair in repairs if repair["requested"] and repair["requested"] == repair["successful"]]
        for repair in repairs:
            if repair not in done:
                logger.info(
                    "Repair %s by %s has repaired %s of the %s ranges it requested so far",
                    repair["parent_id"],
                    repair["coordinator"],
                    len(repair["successful"]),
                    len(repair["requested"]),
                )
        stable = len({repair["coordinator"] for repair in done}) >= EXPECTED_NODES and done == previous["repairs"]
        previous["repairs"] = done
        return done if stable else None

    return _wait_until(produce, HISTORY_TIMEOUT_SECONDS, f"the repairs of {EXPECTED_NODES} coordinators in the history")


# --- locks held by the test -------------------------------------------------------


def _release_locks(cluster, resources, holder):
    """Release the locks holder holds. Locks someone else holds, or that expired, are left alone."""
    out = cluster.run_cql(lib.lock_delete_script(resources, holder))
    results = lib.lwt_results(out)
    logger.info("Released %s of %s locks held by the test", sum(results), len(resources))


def _hold_locks(cluster, resources, holder, ttl_seconds, require_all=True):
    """Take locks in ecchronos.lock the way ecChronos does. Returns how many were taken.

    With require_all every resource must end up held by the test: if some are held by someone else, the ones taken are
    released and the attempt is repeated until it works or LOCK_TAKE_TIMEOUT_SECONDS has passed. Without it, a
    resource that is already held by someone else counts as held, which is all that matters when the point is that
    nothing in the set is free.
    """
    deadline = time.monotonic() + LOCK_TAKE_TIMEOUT_SECONDS
    while True:
        out = cluster.run_cql(lib.lock_insert_script(resources, holder, ttl_seconds))
        results = lib.lwt_results(out)
        assert len(results) == len(resources), f"Expected {len(resources)} lock results, got {results} from {out!r}"
        if all(results) or not require_all:
            logger.info("Holding %s of %s locks, the rest are held by others", sum(results), len(resources))
            return sum(results)
        _release_locks(cluster, resources, holder)
        if time.monotonic() >= deadline:
            raise AssertionError(f"Could not take the locks {resources}, others hold {results}")
        logger.info("Locks %s are held by someone else, retrying in %ss", resources, LOCK_RETRY_SECONDS)
        time.sleep(LOCK_RETRY_SECONDS)


# --- on-demand repairs --------------------------------------------------------------


def _start_on_demand_repair():
    """Run a primary range repair of the test table on every node. Returns the job ids, one per node."""
    created = run_repair_json(
        ["--keyspace", KEYSPACE, "--table", TABLE, "--repair_type", "primary_range", "--all"],
        container_name=CONTAINER_NAME,
    )
    irl.assert_all_repair_type(created, lib.REPAIR_TYPE_PRIMARY_RANGE, "on-demand repair", KEYSPACE, TABLE)
    job_ids = [row["job_id"] for row in created]
    assert len(set(job_ids)) == len(job_ids), f"Duplicate job ids {job_ids}"
    # Every managed node repairs its own primary ranges, so there is exactly one job per node and no more.
    assert len(job_ids) == EXPECTED_NODES, f"Expected one job for each of {EXPECTED_NODES} nodes, got {len(job_ids)}"
    assert len({row["host_id"] for row in created}) == EXPECTED_NODES, f"Jobs are not on {EXPECTED_NODES} nodes"
    return job_ids


def _statuses(job_ids):
    rows = {
        row["job_id"]: row
        for row in get_repairs_json(
            ["--keyspace", KEYSPACE, "--table", TABLE, "-c", "50"], container_name=CONTAINER_NAME
        )
    }
    statuses = []
    for job_id in job_ids:
        row = rows.get(job_id)
        if row is not None:
            assert (
                row["repair_type"] == lib.REPAIR_TYPE_PRIMARY_RANGE
            ), f"On-demand job {job_id} ran as {row['repair_type']}, expected {lib.REPAIR_TYPE_PRIMARY_RANGE}"
        statuses.append(row["status"] if row is not None else None)
    return statuses


def _wait_for_completion(job_ids):
    def produce():
        statuses = _statuses(job_ids)
        failed = [status for status in statuses if status in TERMINAL_FAILURES]
        assert not failed, f"On-demand primary range repair ended in {failed}"
        logger.info("On-demand primary range repair: %s", statuses)
        return all(status == "COMPLETED" for status in statuses)

    started = time.monotonic()
    _wait_until(produce, ON_DEMAND_TIMEOUT_SECONDS, "the on-demand primary range repairs to complete")
    return time.monotonic() - started


def _assert_none_complete_for(job_ids, seconds, reason):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        statuses = _statuses(job_ids)
        finished = [status for status in statuses if status == "COMPLETED" or status in TERMINAL_FAILURES]
        assert not finished, f"The repair finished although {reason}: {statuses}"
        time.sleep(POLL_INTERVAL_SECONDS)
    logger.info("No repair finished in %ss while %s", seconds, reason)


def _block_window_seconds():
    """How long to watch a blocked repair: as long as an unhindered one needed, within limits."""
    unblocked = _STATE["unblocked_seconds"] or MIN_BLOCK_SECONDS
    return int(max(MIN_BLOCK_SECONDS, min(MAX_BLOCK_SECONDS, unblocked)))


def _repair_is_blocked_by(cluster, resource):
    """Hold resource, start an on-demand repair, and show that it does not complete until resource is released."""
    window = _block_window_seconds()
    holder = str(uuid.uuid4())
    _hold_locks(cluster, [resource], holder, ttl_seconds=window + LOCK_TTL_MARGIN_SECONDS)
    try:
        job_ids = _start_on_demand_repair()
        _assert_none_complete_for(job_ids, window, f"the lock {resource} is held")
    finally:
        _release_locks(cluster, [resource], holder)
    elapsed = _wait_for_completion(job_ids)
    logger.info("The repair completed %.0fs after %s was released", elapsed, resource)


# ==================================================================================
# Bring-up
# ==================================================================================


@pytest.mark.dependency(name="pr_install_cluster")
def test_install_cassandra_cluster(install_cassandra_cluster):
    assert install_cassandra_cluster.verify_node_count(EXPECTED_NODES)
    irl.create_schema(install_cassandra_cluster, KEYSPACE, TABLE)
    # Data for the repairs to look at. The test does not make the replicas differ, it shows that the repairs ran.
    irl.write_data(install_cassandra_cluster, KEYSPACE, TABLE, rows=1000)
    _STATE["table_id"] = _create_table_id(install_cassandra_cluster)


@pytest.mark.dependency(name="pr_install_ecchronos", depends=["pr_install_cluster"])
def test_install_ecchronos_primary_range(install_cassandra_cluster, test_environment):
    _STATE["started_at_ms"] = int(time.time() * 1000)
    _start_agent(install_cassandra_cluster, test_environment)
    schedules = _wait_for_schedules()
    assert len(schedules) == EXPECTED_SCHEDULES, f"Expected {EXPECTED_SCHEDULES} schedules, got {len(schedules)}"


# ==================================================================================
# Scheduled repair
# ==================================================================================


@pytest.mark.dependency(name="pr_all_primary_range", depends=["pr_install_ecchronos"])
def test_all_schedules_are_primary_range():
    schedules = _schedules()
    assert len(schedules) == EXPECTED_SCHEDULES, f"Expected {EXPECTED_SCHEDULES} schedules, got {len(schedules)}"
    irl.assert_all_repair_type(schedules, lib.REPAIR_TYPE_PRIMARY_RANGE, "schedule", KEYSPACE, TABLE)
    node_ids = [schedule["node_id"] for schedule in schedules]
    assert len(set(node_ids)) == EXPECTED_SCHEDULES, f"Expected one schedule per node, got nodes {node_ids}"
    _STATE["node_ids"] = [lib.require_uuid(node_id) for node_id in node_ids]


@pytest.mark.dependency(name="pr_cycle", depends=["pr_all_primary_range"])
def test_scheduled_cycle_completes():
    """Every node repairs its primary ranges once. Before that a schedule's last repair is an hour in the past, which
    is how a table that was never repaired looks, so a later time means a repair ran."""
    started_at = _STATE["started_at_ms"]

    def produce():
        schedules = _schedules()
        irl.assert_all_repair_type(schedules, lib.REPAIR_TYPE_PRIMARY_RANGE, "schedule", KEYSPACE, TABLE)
        logger.info(
            "Primary range %s.%s: %s",
            KEYSPACE,
            TABLE,
            [(s["status"], s["last_repaired_at_in_ms"] >= started_at, s["repaired_ratio"]) for s in schedules],
        )
        if len(schedules) < EXPECTED_SCHEDULES:
            return None
        done = all(s["last_repaired_at_in_ms"] >= started_at and s["repaired_ratio"] >= 1.0 for s in schedules)
        return schedules if done else None

    schedules = _wait_until(produce, CYCLE_TIMEOUT_SECONDS, "every node to complete a primary range repair")
    for schedule in schedules:
        assert schedule["status"] in irl.HEALTHY_STATUSES, f"{schedule['node_id']} is {schedule['status']}"


@pytest.mark.dependency(name="pr_history", depends=["pr_cycle"])
def test_repairs_are_recorded_as_primary_range_history(install_cassandra_cluster):
    """ecChronos kept the history apart from the repair state of vnodes, and Cassandra repaired primary ranges only:
    every repair was started as a primary range repair, and no range was repaired by two coordinators."""
    node_ids = _STATE["node_ids"]
    table_id = _STATE["table_id"]
    cluster = install_cassandra_cluster

    def ecchronos_history_ok():
        problems = lib.history_problems(_history_rows_by_node(cluster, table_id, node_ids))
        if problems:
            logger.info("ecchronos.repair_history is not complete yet: %s", problems)
        return not problems

    _wait_until(ecchronos_history_ok, HISTORY_TIMEOUT_SECONDS, "PRIMARY_RANGE rows in ecchronos.repair_history")

    repairs = _scheduled_repairs(cluster)
    # The options ecChronos sent are the ones Cassandra started the repairs with.
    wrong = lib.repairs_not_started_as(repairs, primaryRange="true", incremental="false")
    assert not wrong, f"Repairs started with other options: {[(r['coordinator'], k, v) for r, k, v in wrong]}"

    by_coordinator = lib.repaired_ranges_by_coordinator(repairs)
    assert (
        len(by_coordinator) == EXPECTED_NODES
    ), f"Expected {EXPECTED_NODES} coordinators, got {sorted(by_coordinator)}"
    logger.info("Ranges repaired per coordinator: %s", {a: len(r) for a, r in by_coordinator.items()})
    assert all(by_coordinator.values()), f"A coordinator repaired no ranges: {by_coordinator}"
    # With vnodes, a repair of everything a node replicates would overlap the ranges of the other coordinators.
    overlaps = lib.overlapping_ranges(by_coordinator)
    assert not overlaps, (
        "Coordinators repaired the same token ranges, so more than the primary ranges: "
        f"{[(first, second, len(shared)) for first, second, shared in overlaps]}"
    )


@pytest.mark.dependency(name="pr_restart", depends=["pr_history"])
def test_restart_reads_the_last_repair_from_history(install_cassandra_cluster, test_environment):
    """Without the history, a restarted agent would take the table for never repaired and repair it again."""
    before = {s["node_id"]: s["last_repaired_at_in_ms"] for s in _schedules()}
    assert len(before) == EXPECTED_SCHEDULES

    _start_agent(install_cassandra_cluster, test_environment)
    schedules = _wait_for_schedules()

    for schedule in schedules:
        node_id = schedule["node_id"]
        assert schedule["repaired_ratio"] >= 1.0, f"{node_id} was not read as repaired after the restart: {schedule}"
        assert schedule["last_repaired_at_in_ms"] >= before[node_id] - RESTART_TOLERANCE_MS, (
            f"{node_id} was last repaired at {before[node_id]} before the restart but at "
            f"{schedule['last_repaired_at_in_ms']} after it"
        )


# ==================================================================================
# On-demand repair and locks
# ==================================================================================


@pytest.mark.dependency(name="pr_not_coordinated_with_vnode", depends=["pr_restart"])
def test_on_demand_repair_is_not_coordinated_with_vnode_repair(install_cassandra_cluster):
    """Every lock a vnode repair can take is held, and the primary range repair still completes."""
    resources = lib.vnode_lock_resources(_STATE["node_ids"], DATACENTERS)
    holder = str(uuid.uuid4())
    taken = _hold_locks(
        install_cassandra_cluster,
        resources,
        holder,
        ttl_seconds=ON_DEMAND_TIMEOUT_SECONDS + LOCK_TTL_MARGIN_SECONDS,
        require_all=False,
    )
    assert taken > 0, "The test took none of the vnode locks, so it proves nothing"
    try:
        job_ids = _start_on_demand_repair()
        _STATE["unblocked_seconds"] = _wait_for_completion(job_ids)
    finally:
        _release_locks(install_cassandra_cluster, resources, holder)
    logger.info("The repair completed in %.0fs with the vnode locks held", _STATE["unblocked_seconds"])


@pytest.mark.dependency(name="pr_table_lock", depends=["pr_not_coordinated_with_vnode"])
def test_on_demand_repair_needs_the_table_lock(install_cassandra_cluster):
    """The table lock is the one incremental repair takes, so the two exclude each other."""
    table_lock = lib.primary_range_lock_resources(KEYSPACE, TABLE, DATACENTERS)[0]
    _repair_is_blocked_by(install_cassandra_cluster, table_lock)


@pytest.mark.dependency(name="pr_first_dc_lock", depends=["pr_not_coordinated_with_vnode"])
def test_on_demand_repair_needs_the_lock_of_the_first_datacenter(install_cassandra_cluster):
    resource = lib.primary_range_lock_resources(KEYSPACE, TABLE, DATACENTERS)[1]
    _repair_is_blocked_by(install_cassandra_cluster, resource)


@pytest.mark.dependency(name="pr_second_dc_lock", depends=["pr_not_coordinated_with_vnode"])
def test_on_demand_repair_needs_the_lock_of_the_second_datacenter(install_cassandra_cluster):
    """All involved datacenters are locked, not just the first."""
    resource = lib.primary_range_lock_resources(KEYSPACE, TABLE, DATACENTERS)[2]
    _repair_is_blocked_by(install_cassandra_cluster, resource)

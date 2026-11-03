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

import json
import logging
import time

import pytest
from ecc_config import EcchronosConfig
import global_variables as global_vars
from conftest import (
    assert_nodes_size_is_equal,
    run_ecctool_state_nodes,
    run_ecctool_unified_schedules,
    wait_for_ecctool_row_count,
)

logger = logging.getLogger(__name__)

ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS = 180
DEFAULT_WAIT_TIME_IN_SECS = 120

# Coverage experiment knobs.
COVERAGE_KEYSPACE = "inc_cov_test"
COVERAGE_TABLE = "t1"
# RF 1 per DC: each node owns a distinct slice of its DC's tokens, so coverage of the
# whole ring requires more than a single coordinator -- which is exactly what lets us
# show that a single unified job (via the coordinators that run) still reaches 100%
# on every node.
COVERAGE_KEYSPACE_CQL = (
    f"CREATE KEYSPACE IF NOT EXISTS {COVERAGE_KEYSPACE} WITH replication = "
    "{'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 1};"
)
COVERAGE_TABLE_CQL = (
    f"CREATE TABLE IF NOT EXISTS {COVERAGE_KEYSPACE}.{COVERAGE_TABLE} "
    "(key1 text, key2 int, value int, PRIMARY KEY (key1, key2));"
)
COVERAGE_ROW_COUNT = 50
REPAIR_RUN_TIMEOUT_IN_SECS = 600
REPAIR_POLL_INTERVAL_IN_SECS = 10

# The four nodes defined in cassandra-test-image/src/main/docker/docker-compose.yml.
CLUSTER_NODE_CONTAINERS = [
    "cassandra-seed-dc1-rack1-node1",
    "cassandra-seed-dc2-rack1-node1",
    "cassandra-node-dc1-rack1-node2",
    "cassandra-node-dc2-rack1-node2",
]


class UnifiedIncrementalConfig(EcchronosConfig):
    """EcchronosConfig variant that enables the unified_incremental repair type.

    The global type and every per-table override are set to ``unified_incremental``
    so the whole instance uses the unified subsystem (mixed families are rejected at
    startup). The coverage table is added to the schedule with a short interval so the
    repair runs within the test window.
    """

    def _modify_scheduler_configuration(self, data):
        repair = data.setdefault("repair", {})
        repair["repair_type"] = "unified_incremental"
        # Short interval + initial delay so the scheduled repair fires during the test.
        repair.setdefault("interval", {})["time"] = 1
        repair["interval"]["unit"] = "minutes"
        repair.setdefault("initial_delay", {})["time"] = 1
        repair["initial_delay"]["unit"] = "minutes"
        return data

    def _default_test_schedule_keyspaces(self):
        keyspaces = super()._default_test_schedule_keyspaces()
        for keyspace in keyspaces:
            for table in keyspace.get("tables") or []:
                # Every per-table override must stay in the unified family.
                if table.get("enabled", True) and table.get("repair_type") is not None:
                    table["repair_type"] = "unified_incremental"
                elif table.get("enabled", True):
                    table["repair_type"] = "unified_incremental"
        # Add the dedicated coverage table with a short interval.
        keyspaces.append(
            {
                "name": COVERAGE_KEYSPACE,
                "tables": [
                    {
                        "name": COVERAGE_TABLE,
                        "enabled": True,
                        "repair_type": "unified_incremental",
                        "interval": {"time": 1, "unit": "minutes"},
                        "initial_delay": {"time": 1, "unit": "minutes"},
                    }
                ],
            }
        )
        return keyspaces


# --------------------------------------------------------------------------------------
# Topology assertions (mirror test_unified_vnode_topology.py)
# --------------------------------------------------------------------------------------
def _get_node_count_for_job(job_id):
    """Return the number of per-node rows for a given unified job id."""
    out, exit_code = run_ecctool_unified_schedules(params=["-i", job_id, "-o", "json"])
    if exit_code != 0:
        return 0
    return len(json.loads(out.decode("utf-8")).get("schedules", []))


def _assert_no_duplicate_jobs(jobs):
    """Each (keyspace, table) must map to exactly one unified job (no repetition)."""
    assert jobs, "unified-schedules returned no jobs"

    tables = [(job["keyspace"], job["table"]) for job in jobs]
    duplicates = sorted({t for t in tables if tables.count(t) > 1})
    assert not duplicates, f"unified-schedules has repeated jobs for tables: {duplicates}"

    job_ids = [job["job_id"] for job in jobs]
    assert len(job_ids) == len(set(job_ids)), f"unified-schedules has repeated job ids: {job_ids}"


def _assert_every_job_has_node_count(jobs, expected_nodes):
    """Each unified job must expand into exactly ``expected_nodes`` per-node sub-jobs."""
    for job in jobs:
        node_rows = _get_node_count_for_job(job["job_id"])
        assert node_rows == expected_nodes, (
            f"Expected {expected_nodes} per-node rows for job "
            f"{job['keyspace']}.{job['table']} ({job['job_id']}), got {node_rows}"
        )


def _get_unified_jobs_in_json():
    """Return the list of unified jobs in json format (as returned by ``unified-schedules``)."""
    out, exit_code = run_ecctool_unified_schedules(params=["-o", "json"])
    assert exit_code == 0, f"unified-schedules failed with exit code {exit_code}"
    return json.loads(out.decode("utf-8"))["unifiedSchedules"]


def _wait_for_every_job_to_have_node_count(expected_nodes, timeout_in_secs, poll_interval_in_secs=5):
    """Poll until every unified job reports ``expected_nodes`` sub-jobs.

    ecChronos reacts to topology changes asynchronously, so the per-node sub-job
    count lags the Cassandra ring; polling avoids racing the convergence.
    Returns the final parsed job list (which the caller then asserts on).
    """
    deadline = time.time() + timeout_in_secs
    jobs = []
    while time.time() < deadline:
        jobs = _get_unified_jobs_in_json()
        if jobs and all(_get_node_count_for_job(job["job_id"]) == expected_nodes for job in jobs):
            return jobs
        time.sleep(poll_interval_in_secs)
    return jobs


# --------------------------------------------------------------------------------------
# Coverage-experiment helpers (automate the manual steps)
# --------------------------------------------------------------------------------------
def _nodetool(cluster, container, args):
    """Run 'nodetool <args>' inside a node container, handling SSL/auth like cass_config.

    Returns (stdout, returncode). Never raises; callers assert on the output.
    """
    import subprocess

    if global_vars.LOCAL != "true" and global_vars.PEM_ENABLED != "true":
        base = ["docker", "exec", container, "bash", "-c", f"nodetool --ssl -u cassandra -pw cassandra {args}"]
    else:
        base = ["docker", "exec", container, "bash", "-c", f"nodetool -u cassandra -pw cassandra {args}"]
    result = subprocess.run(base, capture_output=True, text=True)
    if result.returncode != 0:
        logger.warning(
            "nodetool %s on %s failed (rc=%s): %s", args, container, result.returncode, result.stderr.strip()
        )
    return result.stdout, result.returncode


def _populate_coverage_table(cluster):
    """Create the coverage keyspace/table and insert rows spread across tokens.

    Uses an UNLOGGED BATCH of single-partition inserts with distinct partition keys
    so the data is distributed across every node's owned token ranges.
    """
    cluster.run_cql(COVERAGE_KEYSPACE_CQL)
    cluster.run_cql(COVERAGE_TABLE_CQL)
    inserts = "\n".join(
        f"  INSERT INTO {COVERAGE_KEYSPACE}.{COVERAGE_TABLE} (key1, key2, value) "
        f"VALUES ('k{index:03d}', 1, {index});"
        for index in range(COVERAGE_ROW_COUNT)
    )
    cluster.run_cql(f"BEGIN UNLOGGED BATCH\n{inserts}\nAPPLY BATCH;")


def _flush_coverage_table_on_all_nodes(cluster):
    """nodetool flush on every node so there are on-disk SSTables to anticompact."""
    for container in CLUSTER_NODE_CONTAINERS:
        _nodetool(cluster, container, f"flush {COVERAGE_KEYSPACE} {COVERAGE_TABLE}")


def _percent_repaired(cluster, container):
    """Parse 'Percent repaired' for the coverage table from 'nodetool tablestats'.

    Returns a float, or -1.0 if it could not be read.
    """
    out, rc = _nodetool(cluster, container, f"tablestats {COVERAGE_KEYSPACE}.{COVERAGE_TABLE}")
    if rc != 0:
        return -1.0
    for line in out.splitlines():
        stripped = line.strip()
        if stripped.startswith("Percent repaired:"):
            try:
                return float(stripped.split(":", 1)[1].strip())
            except ValueError:
                return -1.0
    return -1.0


def _percent_repaired_all_nodes(cluster):
    """Map container name -> percent repaired for the coverage table."""
    return {container: _percent_repaired(cluster, container) for container in CLUSTER_NODE_CONTAINERS}


def _coverage_table_id(cluster):
    """Resolve the coverage table's id from system_schema.tables."""
    out = cluster.run_cql(
        "SELECT id FROM system_schema.tables WHERE keyspace_name = "
        f"'{COVERAGE_KEYSPACE}' AND table_name = '{COVERAGE_TABLE}';"
    )
    for line in out.splitlines():
        token = line.strip()
        # The id row is a bare uuid (36 chars with 4 dashes).
        if token.count("-") == 4 and len(token) == 36:
            return token
    raise AssertionError(f"Could not resolve table id for {COVERAGE_KEYSPACE}.{COVERAGE_TABLE}:\n{out}")


def _successful_coordinators(cluster, table_id):
    """Distinct coordinator_ids that recorded a SUCCESS incremental repair for the table.

    The unified incremental job writes a repair_history row per replica keyed by
    coordinator_id; counting distinct coordinators that reached SUCCESS tells us how
    many nodes actually had to coordinate a session.
    """
    out = cluster.run_cql(
        "SELECT coordinator_id, status FROM ecchronos.repair_history " f"WHERE table_id = {table_id} ALLOW FILTERING;"
    )
    coordinators = set()
    for line in out.splitlines():
        stripped = line.strip()
        if "SUCCESS" not in stripped:
            continue
        parts = [p.strip() for p in stripped.split("|")]
        if parts and parts[0].count("-") == 4 and len(parts[0]) == 36:
            coordinators.add(parts[0])
    return coordinators


def _wait_for_full_repair_coverage(cluster, timeout_in_secs, poll_interval_in_secs):
    """Poll 'Percent repaired' on every node until all reach 100 (or time out).

    Returns the final per-node mapping (the caller asserts on it).
    """
    deadline = time.time() + timeout_in_secs
    last = {}
    while time.time() < deadline:
        last = _percent_repaired_all_nodes(cluster)
        if last and all(value >= 100.0 for value in last.values()):
            return last
        logger.info("Waiting for incremental repair coverage; current percent repaired: %s", last)
        time.sleep(poll_interval_in_secs)
    return last


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------
@pytest.mark.dependency(name="test_unified_incremental_install_cassandra_cluster")
def test_unified_incremental_install_cassandra_cluster(install_cassandra_cluster):
    assert install_cassandra_cluster.verify_node_count(4)


@pytest.mark.dependency(
    name="test_unified_incremental_install_ecchronos",
    depends=["test_unified_incremental_install_cassandra_cluster"],
)
def test_unified_incremental_install_ecchronos(install_cassandra_cluster, test_environment):
    ecchronos_config = UnifiedIncrementalConfig()
    test_environment.start_ecchronos(
        cassandra_network=install_cassandra_cluster.network,
        ecchronos_config=ecchronos_config,
    )
    test_environment.wait_for_ecchronos_ready()

    out = wait_for_ecctool_row_count(run_ecctool_state_nodes, 4, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    assert_nodes_size_is_equal(out, 4)

    jobs = _wait_for_every_job_to_have_node_count(4, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    _assert_no_duplicate_jobs(jobs)
    _assert_every_job_has_node_count(jobs, 4)


@pytest.mark.dependency(
    name="test_unified_incremental_coverage",
    depends=["test_unified_incremental_install_ecchronos"],
)
def test_unified_incremental_coverage(install_cassandra_cluster):
    """Automate the manual experiment: populate, flush, 0% -> run -> 100% on all nodes."""
    cluster = install_cassandra_cluster

    # 1-2. Populate and flush on every node so there are unrepaired SSTables.
    _populate_coverage_table(cluster)
    _flush_coverage_table_on_all_nodes(cluster)

    # 3. Every node must start unrepaired (0%). A freshly written+flushed table that
    #    has never been incrementally repaired reports 0.0 percent repaired.
    initial = _percent_repaired_all_nodes(cluster)
    logger.info("Initial percent repaired per node: %s", initial)
    for container, value in initial.items():
        assert value == 0.0, f"Expected 0%% repaired on {container} before repair, got {value}"

    table_id = _coverage_table_id(cluster)

    # 4-6. Wait for the scheduled unified_incremental repair to bring every node to 100%.
    final = _wait_for_full_repair_coverage(
        cluster,
        timeout_in_secs=REPAIR_RUN_TIMEOUT_IN_SECS,
        poll_interval_in_secs=REPAIR_POLL_INTERVAL_IN_SECS,
    )
    logger.info("Final percent repaired per node: %s", final)

    # 5. Confirm it was a single logical job, not one full run per node: the number of
    #    distinct coordinators that recorded SUCCESS is less than the node count, yet
    #    coverage is complete. (A single coordinator covers only its owned ranges, so
    #    the union of the few coordinators that ran is what reaches 100% everywhere.)
    coordinators = _successful_coordinators(cluster, table_id)
    logger.info("Distinct SUCCESS coordinators: %s", coordinators)
    assert coordinators, "No SUCCESS incremental repair recorded in repair_history"
    assert len(coordinators) < len(CLUSTER_NODE_CONTAINERS), (
        "Expected fewer coordinators than nodes (a single unified job synchronising replicas), "
        f"but every node coordinated: {coordinators}"
    )

    # 6. Despite only a subset coordinating, every node must be fully repaired.
    for container, value in final.items():
        assert value >= 100.0, (
            f"Expected 100%% repaired on {container} after the unified_incremental repair, got {value}. "
            f"Full per-node state: {final}"
        )

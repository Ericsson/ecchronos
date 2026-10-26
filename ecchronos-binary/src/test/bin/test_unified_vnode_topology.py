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

import logging
import time

import pytest
from ecc_config import EcchronosConfig
from conftest import (
    assert_nodes_size_is_equal,
    run_ecctool,
    run_ecctool_state_nodes,
    run_ecctool_unified_schedules,
    wait_for_ecctool_row_count,
    _ecctool_row_count,
)

logger = logging.getLogger(__name__)

ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS = 180
DEFAULT_WAIT_TIME_IN_SECS = 120


class UnifiedVnodeConfig(EcchronosConfig):
    """EcchronosConfig variant that enables the unified_vnode repair type."""

    def _modify_scheduler_configuration(self, data):
        data.setdefault("repair", {})["repair_type"] = "unified_vnode"
        return data

    def _default_test_schedule_keyspaces(self):
        keyspaces = super()._default_test_schedule_keyspaces()
        for keyspace in keyspaces:
            for table in keyspace.get("tables") or []:
                table["repair_type"] = "unified_vnode"
        return keyspaces


def _parse_unified_jobs():
    """Parse the unfiltered ``unified-schedules`` output into a list of jobs.

    The ``unified-schedules`` table columns are:
        JobID | Keyspace | Table | Status(worst) | Repaired(%)avg |
        Completed at | Next repair | Nodes

    Returns a list of dicts ``{"job_id", "keyspace", "table"}`` (one per data
    row), or an empty list if the command failed.
    """
    out, exit_code = run_ecctool_unified_schedules()
    if exit_code != 0:
        return []
    lines = out.decode("ascii").strip().split("\n")
    # Table output: border, header, border, data rows..., border
    data_rows = lines[3:-1]
    jobs = []
    for row in data_rows:
        cols = [c.strip() for c in row.split("|") if c.strip()]
        if len(cols) >= 3:
            jobs.append({"job_id": cols[0], "keyspace": cols[1], "table": cols[2]})
    return jobs


def _get_node_count_for_job(job_id):
    """Return the number of per-node rows for a given unified job id."""
    out, exit_code = run_ecctool(["unified-schedules", "-i", job_id])
    if exit_code != 0:
        return 0
    return _ecctool_row_count(out)


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


def _wait_for_every_job_to_have_node_count(expected_nodes, timeout_in_secs, poll_interval_in_secs=5):
    """Poll until every unified job reports ``expected_nodes`` sub-jobs.

    ecChronos reacts to topology changes asynchronously, so the per-node sub-job
    count lags the Cassandra ring; polling avoids racing the convergence.
    Returns the final parsed job list (which the caller then asserts on).
    """
    deadline = time.time() + timeout_in_secs
    jobs = []
    while time.time() < deadline:
        jobs = _parse_unified_jobs()
        if jobs and all(_get_node_count_for_job(job["job_id"]) == expected_nodes for job in jobs):
            return jobs
        time.sleep(poll_interval_in_secs)
    return jobs


@pytest.mark.dependency(name="test_unified_install_cassandra_cluster")
def test_unified_install_cassandra_cluster(install_cassandra_cluster):
    assert install_cassandra_cluster.verify_node_count(4)


@pytest.mark.dependency(name="test_unified_install_ecchronos", depends=["test_unified_install_cassandra_cluster"])
def test_unified_install_ecchronos(install_cassandra_cluster, test_environment):
    ecchronos_config = UnifiedVnodeConfig()
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


@pytest.mark.dependency(name="test_unified_add_node", depends=["test_unified_install_ecchronos"])
def test_unified_add_node(install_cassandra_cluster, test_environment):
    install_cassandra_cluster.add_node()
    install_cassandra_cluster._wait_for_nodes_to_be_up(5, DEFAULT_WAIT_TIME_IN_SECS * 1000)
    assert install_cassandra_cluster.verify_node_count(5)

    out = wait_for_ecctool_row_count(run_ecctool_state_nodes, 5, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    assert_nodes_size_is_equal(out, 5)

    # The set of jobs must stay one-per-table after adding a node — the new
    # node is absorbed into the existing per-table jobs — and every job must
    # now expand into 5 per-node sub-jobs.
    jobs = _wait_for_every_job_to_have_node_count(5, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    _assert_no_duplicate_jobs(jobs)
    _assert_every_job_has_node_count(jobs, 5)


@pytest.mark.dependency(name="test_unified_remove_node", depends=["test_unified_add_node"])
def test_unified_remove_node(install_cassandra_cluster, test_environment):
    install_cassandra_cluster.stop_extra_node()
    install_cassandra_cluster._wait_for_nodes_to_be_up(4, DEFAULT_WAIT_TIME_IN_SECS * 1000)
    assert install_cassandra_cluster.verify_node_count(4)

    out = wait_for_ecctool_row_count(run_ecctool_state_nodes, 4, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    assert_nodes_size_is_equal(out, 4)

    # The set of jobs must stay one-per-table after removing a node, and every
    # job must be back to 4 per-node sub-jobs.
    jobs = _wait_for_every_job_to_have_node_count(4, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    _assert_no_duplicate_jobs(jobs)
    _assert_every_job_has_node_count(jobs, 4)

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

import pytest
from ecc_config import EcchronosConfig
from conftest import (
    assert_nodes_size_is_equal,
    assert_schedules_size_is_equal,
    run_ecctool,
    run_ecctool_state_nodes,
    run_ecctool_schedules,
    run_ecctool_unified_schedules,
    wait_for_ecctool_row_count,
    _ecctool_row_count,
)

logger = logging.getLogger(__name__)

# In unified_vnode mode there is one job per *table* (not per node), so the
# schedule count is independent of cluster size.  The default test schedule
# keyspaces defined in EcchronosConfig._default_test_schedule_keyspaces()
# have 4 enabled tables:
#   test.table1, test2.table1, test2.table2, ecchronos.repair_history
UNIFIED_SCHEDULE_COUNT = 4

# The legacy per-node schedule endpoint must return 0 jobs when the instance
# runs in unified_vnode mode (no TableRepairJob objects are created).
LEGACY_SCHEDULE_COUNT = 0

ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS = 180
DEFAULT_WAIT_TIME_IN_SECS = 120


class UnifiedVnodeConfig(EcchronosConfig):
    """EcchronosConfig variant that enables the unified_vnode repair type."""

    def _modify_scheduler_configuration(self, data):
        data.setdefault("repair", {})["repair_type"] = "unified_vnode"
        return data

    def _default_test_schedule_keyspaces(self):
        # Strip all per-table repair_type overrides: unified_vnode mode requires
        # every per-table override to also be unified_vnode (or absent).
        keyspaces = super()._default_test_schedule_keyspaces()
        for keyspace in keyspaces:
            for table in keyspace.get("tables") or []:
                table.pop("repair_type", None)
        return keyspaces


def _get_first_unified_job_id():
    """Return the job ID of the first unified schedule, or None."""
    out, exit_code = run_ecctool_unified_schedules()
    if exit_code != 0:
        return None
    lines = out.decode("ascii").strip().split("\n")
    # Table output: header border, header, border, data rows..., border
    data_rows = lines[3:-1]
    for row in data_rows:
        cols = [c.strip() for c in row.split("|") if c.strip()]
        if cols:
            return cols[0]  # first column is jobID
    return None


def _get_node_count_for_job(job_id):
    """Return the number of per-node rows for a given unified job id."""
    out, exit_code = run_ecctool(["unified-schedules", "-i", job_id])
    if exit_code != 0:
        return 0
    return _ecctool_row_count(out)


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

    # unified-schedules: one job per table regardless of node count
    out = wait_for_ecctool_row_count(
        run_ecctool_unified_schedules,
        UNIFIED_SCHEDULE_COUNT,
        timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS,
    )
    assert_schedules_size_is_equal(out, UNIFIED_SCHEDULE_COUNT)

    # legacy schedules endpoint must be empty in unified_vnode mode
    out, _ = run_ecctool_schedules()
    assert_schedules_size_is_equal(out, LEGACY_SCHEDULE_COUNT)

    # each unified job must track exactly 4 nodes (one sub-job per node)
    job_id = _get_first_unified_job_id()
    assert job_id is not None, "Could not retrieve a unified job id"
    node_rows = _get_node_count_for_job(job_id)
    assert node_rows == 4, f"Expected 4 per-node rows for job {job_id}, got {node_rows}"


@pytest.mark.dependency(name="test_unified_add_node", depends=["test_unified_install_ecchronos"])
def test_unified_add_node(install_cassandra_cluster, test_environment):
    install_cassandra_cluster.add_node()
    install_cassandra_cluster._wait_for_nodes_to_be_up(5, DEFAULT_WAIT_TIME_IN_SECS * 1000)
    assert install_cassandra_cluster.verify_node_count(5)

    out = wait_for_ecctool_row_count(run_ecctool_state_nodes, 5, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    assert_nodes_size_is_equal(out, 5)

    # unified-schedules count must stay the same after adding a node —
    # the new node is absorbed into the existing per-table jobs.
    out = wait_for_ecctool_row_count(
        run_ecctool_unified_schedules,
        UNIFIED_SCHEDULE_COUNT,
        timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS,
    )
    assert_schedules_size_is_equal(out, UNIFIED_SCHEDULE_COUNT)

    # but each job must now track 5 nodes
    job_id = _get_first_unified_job_id()
    assert job_id is not None, "Could not retrieve a unified job id"

    def _poll_node_count():
        out_inner, _ = run_ecctool(["unified-schedules", "-i", job_id])
        return out_inner

    node_out = wait_for_ecctool_row_count(_poll_node_count, 5, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    assert _ecctool_row_count(node_out) == 5, "Expected 5 per-node rows after adding a node"


@pytest.mark.dependency(name="test_unified_remove_node", depends=["test_unified_add_node"])
def test_unified_remove_node(install_cassandra_cluster, test_environment):
    install_cassandra_cluster.stop_extra_node()
    install_cassandra_cluster._wait_for_nodes_to_be_up(4, DEFAULT_WAIT_TIME_IN_SECS * 1000)
    assert install_cassandra_cluster.verify_node_count(4)

    out = wait_for_ecctool_row_count(run_ecctool_state_nodes, 4, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    assert_nodes_size_is_equal(out, 4)

    # unified-schedules count must stay the same after removing a node.
    out = wait_for_ecctool_row_count(
        run_ecctool_unified_schedules,
        UNIFIED_SCHEDULE_COUNT,
        timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS,
    )
    assert_schedules_size_is_equal(out, UNIFIED_SCHEDULE_COUNT)

    # and each job must be back to 4 nodes
    job_id = _get_first_unified_job_id()
    assert job_id is not None, "Could not retrieve a unified job id"

    def _poll_node_count():
        out_inner, _ = run_ecctool(["unified-schedules", "-i", job_id])
        return out_inner

    node_out = wait_for_ecctool_row_count(_poll_node_count, 4, timeout_in_secs=ECCHRONOS_CONVERGE_TIMEOUT_IN_SECS)
    assert _ecctool_row_count(node_out) == 4, "Expected 4 per-node rows after removing a node"

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
from tenacity import retry, retry_if_result, stop_after_delay, wait_fixed

import incremental_repair_lib as lib
from ecc_config import EcchronosConfig
from conftest import get_repairs_json, run_repair_json

logger = logging.getLogger(__name__)

KEYSPACE = "inc_repair"
TABLE = "data"

EXPECTED_NODES = 4
# datacenter1:2, datacenter2:1 -> the table lives on all 4 nodes, one schedule per node.
EXPECTED_SCHEDULES = 4

# Incremental repair of a small table over 4 nodes is quick, but CI can be slow; keep a
# generous budget and poll often so completion is seen promptly.
REPAIR_TIMEOUT_SECONDS = 600
POLL_INTERVAL_SECONDS = 10
SCHEDULE_DISCOVERY_TIMEOUT_SECONDS = 300

# Short per-table interval so the scheduler fires within the test window.
REPAIR_INTERVAL_MINUTES = 2


def _incremental_config():
    """EcchronosConfig with incremental as the global default AND the per-table type."""
    return EcchronosConfig(
        repair_type="incremental",
        schedule_keyspaces=lib.repair_schedule_keyspaces(
            KEYSPACE, TABLE, lib.REPAIR_TYPE_INCREMENTAL, REPAIR_INTERVAL_MINUTES
        ),
    )


def _schedules():
    return lib.schedules_for(KEYSPACE, TABLE)


@retry(
    stop=stop_after_delay(SCHEDULE_DISCOVERY_TIMEOUT_SECONDS),
    wait=wait_fixed(POLL_INTERVAL_SECONDS),
    retry=retry_if_result(lambda ok: not ok),
)
def _wait_for_schedules_discovered():
    schedules = _schedules()
    if len(schedules) >= EXPECTED_SCHEDULES:
        return True
    logger.info("Waiting for %s.%s schedules: have %s, want %s", KEYSPACE, TABLE, len(schedules), EXPECTED_SCHEDULES)
    return False


# ==================================================================================
# Bring-up: incremental everywhere (ecc.yml default + per-table schedule)
# ==================================================================================


@pytest.mark.dependency(name="inc_install_cluster")
def test_install_cassandra_cluster(install_cassandra_cluster):
    assert install_cassandra_cluster.verify_node_count(EXPECTED_NODES)
    lib.create_schema(install_cassandra_cluster, KEYSPACE, TABLE)


@pytest.mark.dependency(name="inc_install_ecchronos", depends=["inc_install_cluster"])
def test_install_ecchronos_incremental(install_cassandra_cluster, test_environment):
    test_environment.start_ecchronos(
        cassandra_network=install_cassandra_cluster.network,
        ecchronos_config=_incremental_config(),
    )
    test_environment.wait_for_ecchronos_ready()
    assert _wait_for_schedules_discovered(), f"ecChronos did not discover {EXPECTED_SCHEDULES} schedules"


# ==================================================================================
# Everything is incremental
# ==================================================================================


@pytest.mark.dependency(name="inc_all_incremental", depends=["inc_install_ecchronos"])
def test_all_schedules_are_incremental():
    """Every scheduled table reports repair_type INCREMENTAL (instance is fully incremental)."""
    schedules = _schedules()
    assert (
        len(schedules) == EXPECTED_SCHEDULES
    ), f"Expected {EXPECTED_SCHEDULES} schedules for {KEYSPACE}.{TABLE}, got {len(schedules)}"
    lib.assert_all_repair_type(schedules, lib.REPAIR_TYPE_INCREMENTAL, "schedule", KEYSPACE, TABLE)


# ==================================================================================
# A real incremental cycle advances lastRepaired from "never" to a timestamp at 100%
# ==================================================================================


@pytest.mark.dependency(name="inc_cycle_progress", depends=["inc_all_incremental"])
def test_incremental_cycle_advances_repaired_at(install_cassandra_cluster):
    """Populate data, flush, confirm lastRepaired is unset, then wait for an incremental
    cycle and confirm lastRepaired advanced and repaired_ratio reached 100%.

    Incremental repair reconciles SSTables, so the written data is flushed to disk first.
    Incremental repair reconciles SSTables, so the written data is flushed to disk first.
    Capturing the current lastRepaired as a baseline and observing it advance after writing
    fresh data is the direct evidence that a new incremental cycle actually ran for the
    table (a freshly started agent may already have completed an initial cycle, so we must
    not assume lastRepaired starts unset).
    """
    # Baseline: whatever lastRepaired currently is (may already be set by an initial cycle).
    schedules = _schedules()
    lib.assert_all_repair_type(schedules, lib.REPAIR_TYPE_INCREMENTAL, "schedule", KEYSPACE, TABLE)
    baseline = lib.max_last_repaired(schedules)
    logger.info("Baseline lastRepaired for %s.%s: %s", KEYSPACE, TABLE, baseline)

    # Write data and flush every node so incremental repair has on-disk SSTables to process.
    lib.write_data(install_cassandra_cluster, KEYSPACE, TABLE, rows=1000)
    install_cassandra_cluster.flush_all_nodes(keyspace=KEYSPACE)

    @retry(
        stop=stop_after_delay(REPAIR_TIMEOUT_SECONDS),
        wait=wait_fixed(POLL_INTERVAL_SECONDS),
        retry=retry_if_result(lambda done: not done),
    )
    def _cycle_completed():
        current = _schedules()
        lib.assert_all_repair_type(current, lib.REPAIR_TYPE_INCREMENTAL, "schedule", KEYSPACE, TABLE)
        statuses = {s["status"] for s in current}
        assert "BLOCKED" not in statuses, f"{KEYSPACE}.{TABLE} incremental schedule is BLOCKED"
        last_repaired = lib.max_last_repaired(current)
        ratio = lib.max_repaired_ratio(current)
        logger.info(
            "Incremental %s.%s: lastRepaired=%s repaired_ratio=%.2f statuses=%s",
            KEYSPACE,
            TABLE,
            last_repaired,
            ratio,
            statuses,
        )
        # A new cycle has completed once lastRepaired advanced past the baseline and the
        # table is fully repaired.
        return last_repaired > baseline and ratio >= 1.0

    assert _cycle_completed(), (
        f"Incremental repair cycle for {KEYSPACE}.{TABLE} did not advance lastRepaired to "
        "100% repaired within the timeout"
    )


# ==================================================================================
# Operator-triggered on-demand incremental repair completes
# ==================================================================================


@pytest.mark.dependency(name="inc_on_demand", depends=["inc_cycle_progress"])
def test_on_demand_incremental_repair_completes(install_cassandra_cluster):
    """UC4: an on-demand INCREMENTAL repair is triggered and driven to COMPLETED.

    Fresh data is written and flushed so the on-demand repair has new unrepaired SSTables.
    Every created job must be INCREMENTAL, and every job must reach COMPLETED.
    """
    lib.write_data(install_cassandra_cluster, KEYSPACE, TABLE, rows=500, offset=100000)
    install_cassandra_cluster.flush_all_nodes(keyspace=KEYSPACE)

    created = run_repair_json(["--keyspace", KEYSPACE, "--table", TABLE, "--repair_type", "incremental", "--all"])
    lib.assert_all_repair_type(created, lib.REPAIR_TYPE_INCREMENTAL, "on-demand repair", KEYSPACE, TABLE)
    job_ids = {row["job_id"] for row in created}
    assert job_ids, "run-repair created no jobs"

    @retry(
        stop=stop_after_delay(REPAIR_TIMEOUT_SECONDS),
        wait=wait_fixed(POLL_INTERVAL_SECONDS),
        retry=retry_if_result(lambda done: not done),
    )
    def _all_completed():
        rows = {r["job_id"]: r for r in get_repairs_json(["--keyspace", KEYSPACE, "--table", TABLE, "-c", "50"])}
        done = 0
        for job_id in job_ids:
            row = rows.get(job_id)
            if row is None:
                return False
            assert (
                row["repair_type"] == lib.REPAIR_TYPE_INCREMENTAL
            ), f"On-demand job {job_id} ran as {row['repair_type']}, expected {lib.REPAIR_TYPE_INCREMENTAL}"
            if row["status"] in ("FAILED", "ERROR"):
                raise AssertionError(f"On-demand job {job_id} ended in {row['status']}")
            if row["status"] == "COMPLETED":
                done += 1
        logger.info("On-demand incremental: %s/%s jobs COMPLETED", done, len(job_ids))
        return done == len(job_ids)

    assert _all_completed(), "On-demand incremental repair did not complete for all jobs"
